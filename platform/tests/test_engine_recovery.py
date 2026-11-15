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
                        metadata={"item_idx": 0},
                    )
                    for xid, status in (
                        (self.node_xid, ExecutionStatus.RUNNING),
                        (self.completed_xid, ExecutionStatus.COMPLETED),
                    )
                ]
            )

    async def test_start_suspends_running_records_and_caches_suspended_visits(
        self,
    ):
        await self.engine.start()

        cache = self.engine._executions[self.workflow_xid]
        self.assertEqual(cache.workflow.status, ExecutionStatus.SUSPENDED)
        self.assertEqual(list(cache.nodes), [self.node_xid])
        self.assertEqual(cache.nodes[self.node_xid].metadata, {"item_idx": 0})
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
        async with self.sessions() as session:
            workflow = await self.engine._get_workflow_execution(
                session, self.workflow_xid
            )
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, self.node_xid
            )
            workflow.states = {"item": 1}
            node.metadata = {"item_idx": 1}
            await session.rollback()

        async with self.sessions.begin() as session:
            self.assertIs(
                await self.engine._get_workflow_execution(
                    session, self.workflow_xid
                ),
                workflow,
            )
            self.assertIs(
                await self.engine._get_node_execution(
                    session, self.workflow_xid, self.node_xid
                ),
                node,
            )
            self.assertEqual(workflow.states, {"item": 0})
            self.assertEqual(node.metadata, {"item_idx": 0})
            node.metadata = {"item_idx": 2}
        self.assertEqual(node.metadata, {"item_idx": 2})

        async with self.sessions() as session:
            stored = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(stored.metadata, {"item_idx": 2})

    async def test_create_node_execution_saves_and_caches_each_visit(self):
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
            self.assertEqual(list(cached), [self.node_xid])
        self.assertNotEqual(first, second)
        inputs["item"]["quantity"] = 3

        self.assertEqual(list(cached), [self.node_xid, first, second])
        self.assertEqual(cached[first].inputs, {"item": {"quantity": 2}})
        self.assertEqual(cached[first].started_at.tzinfo, UTC)

        async with self.sessions() as session:
            saved = await session.get(NodeExecution, first)
            self.assertEqual(saved.workflow_xid, self.workflow_xid)
            self.assertEqual(saved.node_id, node_id)
            self.assertEqual(saved.executor_id, executor_id)
            self.assertEqual(saved.inputs, cached[first].inputs)
            self.assertEqual(saved.status, ExecutionStatus.RUNNING)
            self.assertIsNone(saved.metadata)
            self.assertIsNone(saved.ended_at)
            other = await session.get(NodeExecution, second)
            self.assertEqual(other.node_id, node_id)
            self.assertIsNone(other.executor_id)

        async with self.sessions() as session:
            node = await self.engine._get_node_execution(
                session, self.workflow_xid, first
            )
            self.assertIs(node, cached[first])

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
        self.assertEqual(list(cache.nodes), [self.node_xid])

    async def test_status_updates_record_end_times_and_evict_on_commit(self):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        async with self.sessions.begin() as session:
            await self.engine.update_node_execution_status(
                session,
                self.workflow_xid,
                self.node_xid,
                ExecutionStatus.INTERRUPTED,
            )
            first_end = cache.nodes[self.node_xid].ended_at
            self.assertEqual(first_end.tzinfo, UTC)
            await self.engine.update_node_execution_status(
                session,
                self.workflow_xid,
                self.node_xid,
                ExecutionStatus.INTERRUPTED,
            )
            self.assertEqual(cache.nodes[self.node_xid].ended_at, first_end)
            self.assertIn(self.workflow_xid, self.engine._executions)
            self.assertEqual(len(cache.nodes), 1)

        self.assertEqual(cache.nodes, {})
        async with self.sessions.begin() as session:
            await self.engine.update_workflow_execution_status(
                session, self.workflow_xid, ExecutionStatus.FAILED
            )
            staged = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            self.assertIn(self.workflow_xid, self.engine._executions)

        self.assertNotIn(self.workflow_xid, self.engine._executions)
        self.assertEqual(cache.nodes, {})
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(node.ended_at.replace(tzinfo=UTC), first_end)
            self.assertEqual(node.status, ExecutionStatus.INTERRUPTED)
            self.assertEqual(workflow.status, ExecutionStatus.FAILED)
            self.assertIsNotNone(workflow.ended_at)
            self.assertIsNotNone(await session.get(NodeExecution, staged))

    async def test_rollback_discards_pending_additions_and_terminal_evictions(
        self,
    ):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        original = cache.nodes[self.node_xid]
        async with self.sessions() as session:
            xid = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            await self.engine.update_node_execution_status(
                session,
                self.workflow_xid,
                self.node_xid,
                ExecutionStatus.FAILED,
            )
            await self.engine.update_workflow_execution_status(
                session, self.workflow_xid, ExecutionStatus.COMPLETED
            )
            await session.flush()
            await session.rollback()

        self.assertIs(self.engine._executions[self.workflow_xid], cache)
        self.assertEqual(cache.nodes, {self.node_xid: original})
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

    async def test_savepoints_publish_final_node_status_only_after_outer_commit(
        self,
    ):
        await self.engine.start()
        cache = self.engine._executions[self.workflow_xid]
        async with self.sessions.begin() as session:
            rolled_back = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            committed = await self.engine.create_node_execution(
                session, self.workflow_xid, uuid4(), None, {}
            )
            async with session.begin_nested() as savepoint:
                await self.engine.update_node_execution_status(
                    session,
                    self.workflow_xid,
                    rolled_back,
                    ExecutionStatus.FAILED,
                )
                await session.flush()
                await savepoint.rollback()
            async with session.begin_nested():
                await self.engine.update_node_execution_status(
                    session,
                    self.workflow_xid,
                    committed,
                    ExecutionStatus.COMPLETED,
                )
                await self.engine.update_node_execution_status(
                    session,
                    self.workflow_xid,
                    committed,
                    ExecutionStatus.SUSPENDED,
                )
            self.assertEqual(list(cache.nodes), [self.node_xid])

        async with self.sessions() as session:
            resumed = await self.engine._get_node_execution(
                session, self.workflow_xid, rolled_back
            )
            suspended = await self.engine._get_node_execution(
                session, self.workflow_xid, committed
            )
            self.assertEqual(resumed.status, ExecutionStatus.RUNNING)
            self.assertIsNone(resumed.ended_at)
            self.assertEqual(suspended.status, ExecutionStatus.SUSPENDED)
            self.assertIsNone(suspended.ended_at)
