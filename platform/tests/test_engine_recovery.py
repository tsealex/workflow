import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from engine import WorkflowEngine
from models import (
    Base,
    ExecutionStatus,
    NodeExecution,
    WorkflowExecution,
    WorkflowSnapshot,
)


class EngineRecoveryTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        database = create_async_engine("sqlite+aiosqlite://")
        self.addAsyncCleanup(database.dispose)
        async with database.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(database)
        self.engine = WorkflowEngine(self.sessions)

        self.workflow_id = uuid4()
        self.workflow_xid = uuid4()
        self.node_xid = uuid4()
        self.completed_xid = uuid4()
        started_at = datetime.now(UTC)
        async with self.sessions.begin() as session:
            session.add_all(
                [
                    WorkflowSnapshot(
                        workflow_id=self.workflow_id,
                        revision=revision,
                        root={},
                        states={"type": "object"},
                        ref_count=ref_count,
                    )
                    for revision, ref_count in ((1, 0), (2, 1))
                ]
            )
            session.add(
                WorkflowExecution(
                    xid=self.workflow_xid,
                    workflow_id=self.workflow_id,
                    workflow_revision=2,
                    status=ExecutionStatus.RUNNING,
                    started_at=started_at,
                    states={"item": 0},
                )
            )
            await session.flush()
            session.add_all(
                [
                    NodeExecution(
                        xid=xid,
                        workflow_xid=self.workflow_xid,
                        node_id=uuid4(),
                        status=status,
                        started_at=started_at,
                        exec_metadata={"item_idx": 0},
                    )
                    for xid, status in (
                        (self.node_xid, ExecutionStatus.RUNNING),
                        (self.completed_xid, ExecutionStatus.COMPLETED),
                    )
                ]
            )

    async def test_start_suspends_running_records_and_caches_all_node_executions(
        self,
    ):
        await self.engine.start()

        cache = self.engine._executions[self.workflow_xid]
        self.assertEqual(cache.workflow.status, ExecutionStatus.SUSPENDED)
        self.assertEqual(set(cache.nodes), {self.node_xid, self.completed_xid})
        self.assertEqual(
            cache.nodes[self.node_xid].exec_metadata, {"item_idx": 0}
        )
        self.assertEqual(
            cache.nodes[self.completed_xid].status, ExecutionStatus.COMPLETED
        )
        snapshots = self.engine._workflow_snapshots
        self.assertEqual(set(snapshots), {(self.workflow_id, 2)})
        self.assertEqual(snapshots[(self.workflow_id, 2)].ref_count, 1)

        async with self.sessions() as session:
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            running = await session.get(NodeExecution, self.node_xid)
            completed = await session.get(NodeExecution, self.completed_xid)
            self.assertEqual(workflow.status, ExecutionStatus.SUSPENDED)
            self.assertEqual(running.status, ExecutionStatus.SUSPENDED)
            self.assertEqual(completed.status, ExecutionStatus.COMPLETED)

    async def test_cached_records_can_be_read_and_updated_after_rollback(self):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        async with self.sessions() as session:
            workflow = await self.engine._get_workflow_execution(
                session, self.workflow_xid
            )
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            workflow.states = {"item": 1}
            node.exec_metadata = {"item_idx": 1}
            await session.rollback()

        async with self.sessions.begin() as session:
            self.assertIs(
                await self.engine._get_workflow_execution(
                    session, self.workflow_xid, cache=cache
                ),
                workflow,
            )
            self.assertIs(
                await self.engine._get_node_execution(
                    session, self.workflow_xid, self.node_xid, cache=cache
                ),
                node,
            )
            self.assertEqual(workflow.states, {"item": 0})
            self.assertEqual(node.exec_metadata, {"item_idx": 0})
            node.exec_metadata = {"item_idx": 2}
        self.assertEqual(node.exec_metadata, {"item_idx": 2})

        async with self.sessions() as session:
            stored = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(stored.exec_metadata, {"item_idx": 2})

    async def test_create_node_execution_saves_and_caches_each_execution(self):
        await self.engine.start()
        inputs = {"item": {"quantity": 2}}
        node_id, executor_id = uuid4(), uuid4()
        cached = self.engine._executions[self.workflow_xid].nodes
        async with self.sessions.begin() as session:
            first = await self.engine.create_node_execution(
                session, self.workflow_xid, node_id, executor_id, inputs
            )
            second = await self.engine.create_node_execution(
                session, self.workflow_xid, node_id, None, {}
            )
            self.assertIsInstance(first, NodeExecution)
            self.assertIsInstance(second, NodeExecution)
            self.assertEqual(first.status, ExecutionStatus.RUNNING)
            self.assertEqual(set(cached), {self.node_xid, self.completed_xid})
        self.assertNotEqual(first.xid, second.xid)
        inputs["item"]["quantity"] = 3

        self.assertEqual(
            set(cached),
            {self.node_xid, self.completed_xid, first.xid, second.xid},
        )
        self.assertIs(cached[first.xid], first)
        self.assertIs(cached[second.xid], second)
        self.assertEqual(first.inputs, {"item": {"quantity": 2}})
        self.assertEqual(first.started_at.tzinfo, UTC)

        async with self.sessions() as session:
            saved = await session.get(NodeExecution, first.xid)
            self.assertEqual(saved.workflow_xid, self.workflow_xid)
            self.assertEqual(saved.node_id, node_id)
            self.assertEqual(saved.executor_id, executor_id)
            self.assertEqual(saved.inputs, first.inputs)
            self.assertEqual(saved.status, ExecutionStatus.RUNNING)
            self.assertEqual(saved.prev_xids, [])
            self.assertIsNone(saved.exec_metadata)
            self.assertIsNone(saved.ended_at)
            other = await session.get(NodeExecution, second.xid)
            self.assertEqual(other.node_id, node_id)
            self.assertIsNone(other.executor_id)

        async with self.sessions() as session:
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, first.xid
            )
            self.assertIs(node, first)

    async def test_predecessor_ids_survive_storage_and_restart(self):
        await self.engine.start()
        predecessors = [self.node_xid, self.completed_xid]
        async with self.sessions.begin() as session:
            created = await self.engine.create_node_execution(
                session,
                self.workflow_xid,
                uuid4(),
                None,
                {},
                prev_xids=predecessors,
            )
            predecessors.clear()

        xid = created.xid
        expected = [self.node_xid, self.completed_xid]
        cached = self.engine._executions[self.workflow_xid].nodes[xid]
        self.assertIs(cached, created)
        self.assertEqual(cached.prev_xids, expected)
        additional_predecessor = uuid4()
        async with self.sessions.begin() as session:
            saved = await session.get(NodeExecution, xid)
            self.assertEqual(saved.prev_xids, expected)
            saved.prev_xids.append(additional_predecessor)
            initial = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(initial.prev_xids, [])

        expected.append(additional_predecessor)
        restarted = WorkflowEngine(self.sessions)
        await restarted.start()
        recovered = restarted._executions[self.workflow_xid].nodes[xid]
        self.assertEqual(recovered.prev_xids, expected)

    async def test_create_node_execution_leaves_cache_unchanged_on_db_failure(
        self,
    ):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        with patch("engine.uuid4", return_value=self.node_xid):
            with self.assertRaises(IntegrityError):
                async with self.sessions.begin() as session:
                    await self.engine.create_node_execution(
                        session, self.workflow_xid, uuid4(), None, {}
                    )
        self.assertEqual(set(cache.nodes), {self.node_xid, self.completed_xid})

    async def test_status_updates_keep_node_executions_until_workflow_ends(
        self,
    ):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        async with self.sessions.begin() as session:
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            node.set_status(ExecutionStatus.INTERRUPTED)
            first_end = cache.nodes[self.node_xid].ended_at
            self.assertEqual(first_end.tzinfo, UTC)
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            node.set_status(ExecutionStatus.INTERRUPTED)
            self.assertEqual(cache.nodes[self.node_xid].ended_at, first_end)
            self.assertIn(self.workflow_xid, self.engine._executions)
            self.assertEqual(len(cache.nodes), 2)

        self.assertEqual(set(cache.nodes), {self.node_xid, self.completed_xid})
        self.assertEqual(
            cache.nodes[self.node_xid].status, ExecutionStatus.INTERRUPTED
        )
        async with self.sessions.begin() as session:
            workflow = await self.engine._get_workflow_execution(
                session, self.workflow_xid
            )
            workflow.set_status(ExecutionStatus.FAILED)
            staged = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            self.assertIn(self.workflow_xid, self.engine._executions)

        self.assertNotIn(self.workflow_xid, self.engine._executions)
        self.assertNotIn(staged.xid, cache.nodes)
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(node.ended_at.replace(tzinfo=UTC), first_end)
            self.assertEqual(node.status, ExecutionStatus.INTERRUPTED)
            self.assertEqual(workflow.status, ExecutionStatus.FAILED)
            self.assertIsNotNone(workflow.ended_at)
            self.assertIsNotNone(await session.get(NodeExecution, staged.xid))

        restarted = WorkflowEngine(self.sessions)
        await restarted.start()
        self.assertNotIn(self.workflow_xid, restarted._executions)

    async def test_rollback_discards_pending_additions_and_terminal_evictions(
        self,
    ):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        original = cache.nodes[self.node_xid]
        async with self.sessions() as session:
            created = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            xid = created.xid
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            node.set_status(ExecutionStatus.FAILED)
            workflow = await self.engine._get_workflow_execution(
                session, self.workflow_xid
            )
            workflow.set_status(ExecutionStatus.COMPLETED)
            await session.flush()
            await session.rollback()

        self.assertIs(self.engine._executions[self.workflow_xid], cache)
        self.assertEqual(set(cache.nodes), {self.node_xid, self.completed_xid})
        self.assertIs(cache.nodes[self.node_xid], original)
        async with self.sessions() as session:
            self.assertIsNone(await session.get(NodeExecution, xid))
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            workflow = await self.engine._get_workflow_execution(
                session, self.workflow_xid
            )
            self.assertIs(node, original)
            self.assertEqual(node.status, ExecutionStatus.SUSPENDED)
            self.assertEqual(workflow.status, ExecutionStatus.SUSPENDED)
            self.assertIsNone(node.ended_at)
            self.assertIsNone(workflow.ended_at)
