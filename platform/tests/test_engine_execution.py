import asyncio
import json
import unittest
from datetime import UTC, datetime
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import grpc
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

import execution_pb2 as messages
from engine import (
    WorkflowEngine,
    _CachedNodeExecution,
    _CachedWorkflowExecution,
    _WorkflowExecutorConnection,
)
from models import (
    Base,
    ExecutionStatus,
    NodeExecution,
    WorkflowExecution,
    WorkflowExecutorRegistration,
    WorkflowSnapshot,
)
from nodes import ActionNode, RegionNode, WaitNode


class EngineExecutionTest(unittest.IsolatedAsyncioTestCase):
    """Check engine failures with synthetic executions in a real database."""

    async def asyncSetUp(self):
        # Each test owns its database and cached execution objects.
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = create_async_engine(
            f"sqlite+aiosqlite:///{directory.name}/engine.db"
        )
        self.addAsyncCleanup(database.dispose)
        async with database.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(database, expire_on_commit=False)
        self.engine = WorkflowEngine(self.sessions)
        self.executor_id = uuid4()
        self.connection = _WorkflowExecutorConnection(
            self.executor_id, self.engine
        )
        self.engine._executors[self.executor_id] = self.connection
        self.workflow_xid = uuid4()
        self.node_xid = uuid4()
        workflow_id = uuid4()
        node_id = uuid4()
        root_id = uuid4()
        started_at = datetime.now(UTC)
        action = ActionNode(
            id=node_id,
            name="Synthetic action",
            description="Return the selected input.",
            code="synthetic fixture",
            input_states=["value"],
            output_states=[],
        )
        root = RegionNode(id=root_id, name="Root", nodes=[action], edges=[])
        self.snapshot = WorkflowSnapshot(
            workflow_id=workflow_id,
            revision=1,
            root=root.model_dump(mode="json"),
            states={"type": "object"},
        )
        self.workflow = WorkflowExecution(
            xid=self.workflow_xid,
            workflow_id=workflow_id,
            workflow_revision=1,
            status=ExecutionStatus.RUNNING,
            started_at=started_at,
            states={"value": 7},
        )
        self.node = NodeExecution(
            xid=self.node_xid,
            workflow_xid=self.workflow_xid,
            node_id=node_id,
            status=ExecutionStatus.SUSPENDED,
            started_at=started_at,
            inputs=None,
        )
        registration = WorkflowExecutorRegistration(
            id=self.executor_id,
            token_hash="synthetic token hash",
            created_at=started_at,
        )
        async with self.sessions() as session:
            session.add_all(
                [self.snapshot, self.workflow, self.node, registration]
            )
            await session.commit()
        workflow_cache = _CachedWorkflowExecution(
            self.workflow, {self.node_xid}
        )
        self.engine._workflow_executions[self.workflow_xid] = workflow_cache
        self.engine._node_executions[self.node_xid] = _CachedNodeExecution(
            self.node
        )
        self.engine._workflow_snapshots[workflow_id, 1] = self.snapshot
        self.engine._suspended_node_xids.add(self.node_xid)

    async def test_dispatch_saves_inputs_and_assignment_before_queueing(self):
        # The executor queue must stay empty while the assignment is saved.
        original_commit = AsyncSession.commit

        async def commit(session):
            self.assertTrue(self.connection.queue.empty())
            await original_commit(session)

        with patch.object(AsyncSession, "commit", commit):
            await self.engine._execute_suspended_nodes(self.executor_id)
        request = self.connection.queue.get_nowait().execute_node
        self.assertEqual(json.loads(request.inputs), {"value": 7})
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(node.status, ExecutionStatus.RUNNING)
            self.assertEqual(node.inputs, {"value": 7})
            self.assertEqual(node.executor_id, self.executor_id)

    async def test_failed_dispatch_save_sends_nothing_and_can_retry(self):
        # A failed save must not dispatch work or block a later retry.
        with patch.object(
            AsyncSession, "commit", side_effect=RuntimeError("save failed")
        ):
            with self.assertRaisesRegex(RuntimeError, "save failed"):
                await self.engine._execute_suspended_nodes(self.executor_id)
        self.assertTrue(self.connection.queue.empty())
        self.assertNotIn(self.node_xid, self.engine._running_node_xids)
        self.assertIn(self.node_xid, self.engine._suspended_node_xids)
        await self.engine._execute_suspended_nodes(self.executor_id)
        self.assertFalse(self.connection.queue.empty())

    async def test_dispatch_preserves_saved_empty_inputs(self):
        # An empty input object is saved input, not a missing input value.
        async with self.sessions() as session:
            session.add(self.node)
            self.node.inputs = {}
            await session.commit()
        await self.engine._execute_suspended_nodes(self.executor_id)
        request = self.connection.queue.get_nowait().execute_node
        self.assertEqual(json.loads(request.inputs), {})

    async def test_engine_owned_nodes_are_not_dispatched(self):
        # Region and wait nodes do not execute on an executor.
        root = self.snapshot.get_root()
        wait = WaitNode(
            id=self.node.node_id, name="Wait", until=datetime.now(UTC)
        )
        for definition in (root, wait):
            with self.subTest(type=definition.type):
                self.snapshot._node_map[self.node.node_id] = definition
                await self.engine._execute_suspended_nodes(self.executor_id)
                self.assertTrue(self.connection.queue.empty())
                self.assertEqual(self.node.status, ExecutionStatus.SUSPENDED)

    async def test_disconnect_during_dispatch_save_restores_suspension(self):
        # The connection can close while the assignment is saved.
        original_commit = AsyncSession.commit

        async def commit(session):
            await original_commit(session)
            self.connection.close()

        with patch.object(AsyncSession, "commit", commit):
            await self.engine._execute_suspended_nodes(self.executor_id)
        self.assertTrue(self.connection.queue.empty())
        self.assertNotIn(self.node_xid, self.engine._running_node_xids)
        self.assertIn(self.node_xid, self.engine._suspended_node_xids)
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(node.status, ExecutionStatus.SUSPENDED)
            self.assertIsNone(node.executor_id)

    async def test_suspend_accepts_expired_cached_node(self):
        # Rollback can expire every attribute before disconnect cleanup runs.
        async with self.sessions() as session:
            session.add(self.node)
            self.node.status = ExecutionStatus.RUNNING
            await session.commit()
            session.expire(self.node)
        self.engine._running_node_xids[self.node_xid] = self.executor_id
        await self.engine._suspend_running_nodes([self.node_xid])
        self.assertEqual(self.node.status, ExecutionStatus.SUSPENDED)
        self.assertNotIn(self.node_xid, self.engine._running_node_xids)

    async def test_disconnect_removes_connection_when_suspend_fails(self):
        # A failed suspension save must still remove the closed connection.
        del self.engine._executors[self.executor_id]

        async def requests():
            yield messages.ExecutorMessage(shutdown=messages.ExecutorShutdown())

        with (
            patch.object(
                self.engine,
                "_authenticate_executor",
                return_value=self.executor_id,
            ),
            patch.object(
                self.engine,
                "_suspend_running_nodes",
                side_effect=RuntimeError("save failed"),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "save failed"):
                await self.engine.connect_executor(
                    requests(), self.make_context()
                )
        self.assertNotIn(self.executor_id, self.engine._executors)

    async def test_concurrent_connections_do_not_replace_each_other(self):
        # Both calls pass authentication before either registers its connection.
        del self.engine._executors[self.executor_id]
        first_saved = asyncio.Event()
        second_saved = asyncio.Event()
        connected = asyncio.Event()
        disconnect = asyncio.Event()
        original_commit = AsyncSession.commit
        commits = 0

        async def commit(session):
            nonlocal commits
            await original_commit(session)
            commits += 1
            if commits == 1:
                first_saved.set()
                await second_saved.wait()
            else:
                second_saved.set()

        async def requests():
            connected.set()
            await disconnect.wait()
            yield messages.ExecutorMessage(shutdown=messages.ExecutorShutdown())

        first_context = self.make_context()
        second_context = self.make_context()
        with (
            patch.object(
                self.engine,
                "_authenticate_executor",
                return_value=self.executor_id,
            ),
            patch.object(AsyncSession, "commit", commit),
        ):
            first = asyncio.create_task(
                self.engine.connect_executor(requests(), first_context)
            )
            await first_saved.wait()
            second = asyncio.create_task(
                self.engine.connect_executor(requests(), second_context)
            )
            await connected.wait()
            disconnect.set()
            results = await asyncio.gather(
                first, second, return_exceptions=True
            )
        self.assertEqual(
            sum(isinstance(result, ConnectionError) for result in results), 1
        )
        aborts = (
            first_context.abort.call_args_list
            + second_context.abort.call_args_list
        )
        self.assertEqual(len(aborts), 1)
        self.assertEqual(aborts[0].args[0], grpc.StatusCode.ALREADY_EXISTS)

    async def test_receive_propagates_handler_failure(self):
        # A receive failure must reach the caller instead of being discarded.
        async def requests():
            yield messages.ExecutorMessage(
                node_result=messages.ExecuteNodeResponse()
            )

        with patch.object(
            self.engine,
            "handle_executor_message",
            side_effect=RuntimeError("handler failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "handler failed"):
                await self.connection.loop_recv_messages(requests())

    @staticmethod
    def make_context():
        context = AsyncMock()
        context.abort.side_effect = ConnectionError("connection rejected")
        return context
