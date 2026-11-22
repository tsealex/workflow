import asyncio
import json
import unittest
from datetime import UTC, datetime
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import grpc
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    create_async_engine,
)

import execution_pb2 as messages
from database import AsyncSession
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
from nodes import INITIAL, ActionNode, Edge, RegionNode, SignalNode, WaitNode


class EngineExecutionTest(unittest.IsolatedAsyncioTestCase):
    """Check engine failures with synthetic executions in a real database."""

    async def asyncSetUp(self):
        # Each test owns its database and cached execution objects.
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        database = create_async_engine(
            f"sqlite+aiosqlite:///{directory.name}/engine.db"
        )
        self.database = database
        self.addAsyncCleanup(database.dispose)
        async with database.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(
            database, class_=AsyncSession, expire_on_commit=False
        )
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
        self.engine._add_schedulable_nodes(self.node_xid)

    def test_add_schedulable_nodes_signals_only_for_new_ids(self):
        engine = WorkflowEngine(self.sessions)
        self.assertFalse(engine._has_schedulable_nodes.is_set())
        engine._add_schedulable_nodes(self.node_xid)
        self.assertTrue(engine._has_schedulable_nodes.is_set())
        engine._has_schedulable_nodes.clear()
        engine._add_schedulable_nodes(self.node_xid, self.node_xid)
        self.assertFalse(engine._has_schedulable_nodes.is_set())
        second_xid = uuid4()
        engine._add_schedulable_nodes(self.node_xid, second_xid)
        self.assertEqual(
            engine._schedulable_node_xids, {self.node_xid, second_xid}
        )
        self.assertTrue(engine._has_schedulable_nodes.is_set())

    async def test_start_restores_iteration_history_and_schedulable_nodes(self):
        completed = NodeExecution(
            xid=uuid4(),
            workflow_xid=self.workflow_xid,
            node_id=self.node.node_id,
            status=ExecutionStatus.COMPLETED,
            started_at=self.node.started_at,
        )
        pending = NodeExecution(
            xid=uuid4(),
            workflow_xid=self.workflow_xid,
            node_id=self.node.node_id,
            started_at=self.node.started_at,
        )
        self.assertEqual(pending.status, ExecutionStatus.PENDING)
        async with self.sessions() as session:
            session.add_all([completed, pending])
            await session.commit()

        engine = WorkflowEngine(self.sessions)
        engine._executors[self.executor_id] = self.connection
        await engine.start()
        self.addAsyncCleanup(engine.stop)
        all_xids = {completed.xid, pending.xid, self.node_xid}
        self.assertEqual(
            engine._workflow_executions[self.workflow_xid].node_xids, all_xids
        )
        self.assertEqual(set(engine._node_executions), all_xids)
        self.assertEqual(
            engine._schedulable_node_xids, {pending.xid, self.node_xid}
        )
        dispatched = set()
        for _ in range(2):
            message = await asyncio.wait_for(
                self.connection.queue.get(), timeout=5
            )
            dispatched.add(message.execute_node.node_xid)
        self.assertEqual(dispatched, {str(self.node_xid), str(pending.xid)})
        self.assertEqual(
            set(engine._executor_running_node_xids), {pending.xid, self.node_xid}
        )

    async def test_round_robin_skips_closing_executors_across_batches(self):
        engine = self.engine
        closing_id = uuid4()
        second_id = uuid4()
        closing = _WorkflowExecutorConnection(closing_id, engine)
        second = _WorkflowExecutorConnection(second_id, engine)
        engine._executors[closing_id] = closing
        engine._executors[second_id] = second
        await closing.close()

        nodes = [
            NodeExecution(
                xid=uuid4(),
                workflow_xid=self.workflow_xid,
                node_id=self.node.node_id,
                started_at=self.node.started_at,
            )
            for _ in range(3)
        ]
        async with self.sessions() as session:
            session.add_all(nodes)
            await session.commit()
        for node in nodes:
            engine._node_executions[node.xid] = _CachedNodeExecution(node)
            engine._workflow_executions[self.workflow_xid].node_xids.add(
                node.xid
            )

        engine._add_schedulable_nodes(nodes[0].xid, nodes[1].xid)
        await engine._schedule_nodes()
        self.assertEqual(self.connection.queue.qsize(), 2)
        self.assertEqual(second.queue.qsize(), 1)

        engine._add_schedulable_nodes(nodes[2].xid)
        await engine._schedule_nodes()
        self.assertEqual(engine._executor_running_node_xids[nodes[2].xid], second_id)
        self.assertTrue(closing.queue.empty())

    async def test_scheduler_dispatches_other_nodes_after_a_failed_save(self):
        pending = NodeExecution(
            xid=uuid4(),
            workflow_xid=self.workflow_xid,
            node_id=self.node.node_id,
            started_at=self.node.started_at,
        )
        async with self.sessions() as session:
            session.add(pending)
            await session.commit()
        engine = WorkflowEngine(self.sessions)
        engine._executors[self.executor_id] = self.connection
        await engine.start()
        self.addAsyncCleanup(engine.stop)
        failed = False

        def fail_first_update(
            connection, cursor, statement, parameters, context, many
        ):
            nonlocal failed
            if not failed and statement.startswith("UPDATE node_execution"):
                failed = True
                raise RuntimeError("save failed")

        event.listen(
            self.database.sync_engine,
            "before_cursor_execute",
            fail_first_update,
        )
        try:
            with self.assertLogs("engine", level="ERROR"):
                message = await asyncio.wait_for(
                    self.connection.queue.get(), timeout=5
                )
        finally:
            event.remove(
                self.database.sync_engine,
                "before_cursor_execute",
                fail_first_update,
            )
        self.assertTrue(message.HasField("execute_node"))
        self.assertEqual(len(engine._schedulable_node_xids), 1)
        self.assertEqual(len(engine._executor_running_node_xids), 1)

    async def test_new_executor_wakes_scheduler_for_recovered_work(self):
        engine = WorkflowEngine(self.sessions)
        await engine.start()
        await asyncio.sleep(0)
        self.assertFalse(engine._has_schedulable_nodes.is_set())
        received = asyncio.Queue()
        context = self.make_context()
        context.write.side_effect = received.put

        async def requests():
            await asyncio.Event().wait()
            yield messages.ExecutorMessage()

        with patch.object(
            engine, "_authenticate_executor", return_value=self.executor_id
        ):
            connection = asyncio.create_task(
                engine.connect_executor(requests(), context)
            )
            try:
                message = await asyncio.wait_for(received.get(), timeout=5)
                self.assertEqual(
                    message.execute_node.node_xid, str(self.node_xid)
                )
            finally:
                await asyncio.wait_for(engine.stop(), timeout=5)
                await asyncio.wait_for(connection, timeout=5)
        self.assertTrue(received.get_nowait().HasField("shutdown"))

    async def test_dispatch_saves_assignment_and_omits_missing_inputs(self):
        # The executor queue must stay empty while the assignment is saved.
        original_commit = AsyncSession.commit

        async def commit(session):
            self.assertTrue(self.connection.queue.empty())
            await original_commit(session)

        with patch.object(AsyncSession, "commit", commit):
            await self.engine._schedule_nodes()
        request = self.connection.queue.get_nowait().execute_node
        self.assertFalse(request.HasField("inputs"))
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(node.status, ExecutionStatus.RUNNING)
            self.assertIsNone(node.inputs)
            self.assertEqual(node.executor_id, self.executor_id)

    async def test_failed_dispatch_save_sends_nothing_and_can_retry(self):
        # A failed save must not dispatch work or block a later retry.
        def fail_node_update(
            connection, cursor, statement, parameters, context, many
        ):
            if statement.startswith("UPDATE node_execution"):
                raise RuntimeError("save failed")

        def fail_before_commit(session):
            raise RuntimeError("save failed")

        for target, event_name, listener in (
            (self.database.sync_engine, "before_cursor_execute", fail_node_update),
            (AsyncSession.sync_session_class, "before_commit", fail_before_commit),
        ):
            with self.subTest(event=event_name):
                event.listen(target, event_name, listener)
                try:
                    with self.assertLogs("engine", level="ERROR") as logs:
                        await self.engine._schedule_nodes()
                    self.assertIn("RuntimeError: save failed", logs.output[0])
                finally:
                    event.remove(target, event_name, listener)
                self.assertTrue(self.connection.queue.empty())
                self.assertNotIn(self.node_xid, self.engine._executor_running_node_xids)
                self.assertIn(self.node_xid, self.engine._schedulable_node_xids)
        await self.engine._schedule_nodes()
        self.assertFalse(self.connection.queue.empty())

    async def test_dispatch_preserves_saved_empty_inputs(self):
        # An empty input object is saved input, not a missing input value.
        async with self.sessions() as session:
            session.add(self.node)
            self.node.inputs = {}
            await session.commit()
        await self.engine._schedule_nodes()
        request = self.connection.queue.get_nowait().execute_node
        self.assertEqual(json.loads(request.inputs), {})

    async def test_pending_region_starts_its_initial_children_once(self):
        root = self.snapshot.get_root()
        action = root.nodes[0]
        wait = WaitNode(id=uuid4(), name="Wait", until=datetime.now(UTC))
        later = action.model_copy(update={"id": uuid4()})
        root.nodes.extend([wait, later])
        root.edges = [
            Edge(from_=INITIAL, to=action.id),
            Edge(from_=INITIAL, to=wait.id),
            Edge(from_=action.id, to=later.id),
        ]
        async with self.sessions() as session:
            self.snapshot.root = root.model_dump(mode="json")
            self.node.node_id = root.id
            self.node.set_status(ExecutionStatus.PENDING)
            session.add_all([self.snapshot, self.node])
            await session.commit()

        engine = WorkflowEngine(self.sessions)
        await engine._load_executions()

        def fail_before_commit(session):
            raise RuntimeError("save failed")

        event.listen(AsyncSession.sync_session_class, "before_commit", fail_before_commit)
        try:
            with self.assertLogs("engine", level="ERROR"):
                await engine._schedule_nodes()
        finally:
            event.remove(AsyncSession.sync_session_class, "before_commit", fail_before_commit)
        self.assertEqual(set(engine._node_executions), {self.node_xid})
        self.assertEqual(engine._schedulable_node_xids, {self.node_xid})

        await engine._schedule_nodes()
        async with self.sessions() as session:
            region = await session.get(NodeExecution, self.node_xid)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            children = (
                await session.scalars(
                    select(NodeExecution).where(NodeExecution.region_id == root.id)
                )
            ).all()
        self.assertEqual(region.status, ExecutionStatus.RUNNING)
        self.assertIsNone(region.executor_id)
        self.assertEqual(workflow.status, ExecutionStatus.RUNNING)
        self.assertCountEqual(
            [child.node_id for child in children], [action.id, wait.id]
        )
        for child in children:
            self.assertEqual(child.status, ExecutionStatus.PENDING)
            self.assertEqual(child.workflow_xid, self.workflow_xid)
            self.assertIsNotNone(child.started_at)
        child_xids = {child.xid for child in children}
        all_xids = child_xids | {self.node_xid}
        self.assertEqual(set(engine._node_executions), all_xids)
        self.assertEqual(
            engine._workflow_executions[self.workflow_xid].node_xids, all_xids
        )
        self.assertEqual(engine._schedulable_node_xids, child_xids)

        resumed = WorkflowEngine(self.sessions)
        await resumed._load_executions()
        with self.assertLogs("engine", level="ERROR"):
            await resumed._schedule_nodes()
        self.assertEqual(set(resumed._node_executions), all_xids)
        self.assertEqual(resumed._schedulable_node_xids, child_xids)
        async with self.sessions() as session:
            region = await session.get(NodeExecution, self.node_xid)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(region.status, ExecutionStatus.RUNNING)
            self.assertEqual(workflow.status, ExecutionStatus.RUNNING)

    async def test_pending_region_with_no_initial_children_starts(self):
        root = self.snapshot.get_root()
        self.node.node_id = root.id
        self.node.set_status(ExecutionStatus.PENDING)
        await self.engine._schedule_nodes()
        async with self.sessions() as session:
            region = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(region.status, ExecutionStatus.RUNNING)
        self.assertEqual(set(self.engine._node_executions), {self.node_xid})
        self.assertFalse(self.engine._schedulable_node_xids)

    async def test_engine_owned_nodes_are_not_dispatched(self):
        # Region and wait nodes do not execute on an executor.
        root = self.snapshot.get_root()
        wait = WaitNode(
            id=self.node.node_id, name="Wait", until=datetime.now(UTC)
        )
        for definition, expected_status in (
            (root, ExecutionStatus.RUNNING),
            (wait, ExecutionStatus.SUSPENDED),
        ):
            with self.subTest(type=definition.type):
                self.node.set_status(ExecutionStatus.SUSPENDED)
                self.engine._add_schedulable_nodes(self.node_xid)
                self.snapshot._node_map[self.node.node_id] = definition
                await self.engine._schedule_nodes()
                self.assertTrue(self.connection.queue.empty())
                self.assertEqual(self.node.status, expected_status)

    async def test_scheduler_routes_engine_nodes_without_an_executor(self):
        self.engine._executors.clear()
        node_id = self.node.node_id
        region = self.snapshot.get_root()
        wait = WaitNode(id=node_id, name="Wait", until=datetime.now(UTC))
        signal = SignalNode(
            id=node_id, name="Signal", event_id=uuid4(), input_states=[]
        )
        for node, method, expected_args in (
            (region, "_execute_region_node", (self.snapshot, self.workflow, self.node)),
            (wait, "_execute_wait_node", (self.node,)),
            (signal, "_execute_signal_node", (self.node,)),
        ):
            with (
                self.subTest(type=node.type),
                patch.object(self.engine, method, return_value=None) as handler,
            ):
                self.snapshot._node_map[node_id] = node
                self.node.set_status(ExecutionStatus.SUSPENDED)
                self.engine._add_schedulable_nodes(self.node_xid)
                await self.engine._schedule_nodes()
                handler.assert_awaited_once()
                session, *args = handler.await_args.args
                self.assertIsInstance(session, AsyncSession)
                self.assertEqual(tuple(args), expected_args)

    async def schedule_local_wait_task(self, finish):
        async def wait():
            await finish.wait()

        node_id = self.node.node_id
        self.snapshot.get_root()
        self.snapshot._node_map[node_id] = WaitNode(
            id=node_id, name="Wait", until=datetime.now(UTC)
        )
        task = asyncio.create_task(wait())
        with patch.object(self.engine, "_execute_wait_node", return_value=task):
            await self.engine._schedule_nodes()
        return task

    async def test_local_task_is_removed_on_completion(self):
        finish = asyncio.Event()
        task = await self.schedule_local_wait_task(finish)
        self.assertIs(self.engine._engine_running_node_xids[self.node_xid], task)
        finish.set()
        await task
        self.assertNotIn(self.node_xid, self.engine._engine_running_node_xids)

    async def test_interrupt_cancels_local_task_after_saving_status(self):
        task = await self.schedule_local_wait_task(asyncio.Event())
        async with self.sessions() as session:
            await self.engine._interrupt_nodes(session, self.workflow_xid)
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertNotIn(self.node_xid, self.engine._engine_running_node_xids)
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(node.status, ExecutionStatus.INTERRUPTED)

    async def test_shutdown_waits_for_local_task_cancellation(self):
        task = await self.schedule_local_wait_task(asyncio.Event())
        await self.engine.stop()
        self.assertTrue(task.cancelled())
        self.assertFalse(self.engine._engine_running_node_xids)

    async def test_failed_save_cancels_local_task(self):
        with (
            patch.object(self.engine, "_save_running_node", side_effect=RuntimeError),
            self.assertLogs("engine", level="ERROR"),
        ):
            task = await self.schedule_local_wait_task(asyncio.Event())
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertNotIn(self.node_xid, self.engine._engine_running_node_xids)
        self.assertIn(self.node_xid, self.engine._schedulable_node_xids)

    async def test_disconnect_during_dispatch_save_restores_suspension(self):
        # Disconnect cleanup waits for the assignment to finish.
        del self.engine._executors[self.executor_id]
        connected = asyncio.Event()
        disconnect = asyncio.Event()
        shutdown_requested = asyncio.Event()
        original_commit = AsyncSession.commit

        async def requests():
            connected.set()
            await disconnect.wait()
            shutdown_requested.set()
            yield messages.ExecutorMessage(shutdown=messages.ExecutorShutdown())

        async def commit(session):
            await original_commit(session)
            disconnect.set()
            await shutdown_requested.wait()

        with patch.object(
            self.engine, "_authenticate_executor", return_value=self.executor_id
        ):
            connection_task = asyncio.create_task(
                self.engine.connect_executor(requests(), self.make_context())
            )
            try:
                await connected.wait()
                with patch.object(AsyncSession, "commit", commit):
                    await self.engine._schedule_nodes()
                await connection_task
            finally:
                connection_task.cancel()
                await asyncio.gather(connection_task, return_exceptions=True)
        self.assertNotIn(self.executor_id, self.engine._executors)
        self.assertNotIn(self.node_xid, self.engine._executor_running_node_xids)
        self.assertIn(self.node_xid, self.engine._schedulable_node_xids)
        async with self.sessions() as session:
            node = await session.get(NodeExecution, self.node_xid)
            self.assertEqual(node.status, ExecutionStatus.SUSPENDED)
            self.assertIsNone(node.executor_id)

    async def test_executor_disconnect_keeps_workflow_with_running_region_active(self):
        region_xid = uuid4()
        region = NodeExecution(
            xid=region_xid,
            workflow_xid=self.workflow_xid,
            node_id=self.snapshot.get_root().id,
            started_at=self.node.started_at,
        )
        async with self.sessions() as session:
            session.add(region)
            await session.commit()
        self.engine._node_executions[region_xid] = _CachedNodeExecution(region)
        self.engine._workflow_executions[self.workflow_xid].node_xids.add(region_xid)
        self.engine._add_schedulable_nodes(region_xid)
        await self.engine._schedule_nodes()
        self.assertEqual(self.engine._executor_running_node_xids, {
            self.node_xid: self.executor_id
        })

        await self.engine._suspend_running_nodes([self.node_xid])
        async with self.sessions() as session:
            action = await session.get(NodeExecution, self.node_xid)
            region = await session.get(NodeExecution, region_xid)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(action.status, ExecutionStatus.SUSPENDED)
            self.assertEqual(region.status, ExecutionStatus.RUNNING)
            self.assertEqual(workflow.status, ExecutionStatus.RUNNING)

    async def test_suspend_accepts_expired_cached_node(self):
        # Rollback can expire every attribute before disconnect cleanup runs.
        async with self.sessions() as session:
            session.add(self.node)
            self.node.status = ExecutionStatus.RUNNING
            await session.commit()
            session.expire(self.node)
        self.engine._executor_running_node_xids[self.node_xid] = self.executor_id
        await self.engine._suspend_running_nodes([self.node_xid])
        self.assertEqual(self.node.status, ExecutionStatus.SUSPENDED)
        self.assertEqual(self.workflow.status, ExecutionStatus.SUSPENDED)
        self.assertNotIn(self.node_xid, self.engine._executor_running_node_xids)

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
            await self.engine.connect_executor(requests(), self.make_context())
        self.assertNotIn(self.executor_id, self.engine._executors)

    async def test_concurrent_connections_do_not_replace_each_other(self):
        # The second connection arrives while the first saves its timestamp.
        del self.engine._executors[self.executor_id]
        first_saved = asyncio.Event()
        release_save = asyncio.Event()
        original_commit = AsyncSession.commit

        async def commit(session):
            await original_commit(session)
            first_saved.set()
            await release_save.wait()

        async def requests():
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
            try:
                await first_saved.wait()
                connection = self.engine._executors[self.executor_id]
                with self.assertRaises(ConnectionError):
                    await asyncio.wait_for(
                        self.engine.connect_executor(
                            requests(), second_context
                        ),
                        timeout=5,
                    )
                self.assertIs(
                    self.engine._executors[self.executor_id], connection
                )
            finally:
                release_save.set()
                await first
        first_context.abort.assert_not_awaited()
        second_context.abort.assert_awaited_once_with(
            grpc.StatusCode.ALREADY_EXISTS, "Executor already connected."
        )
        self.assertNotIn(self.executor_id, self.engine._executors)

    async def test_failed_connection_save_removes_connection(self):
        del self.engine._executors[self.executor_id]

        def fail_executor_update(
            connection, cursor, statement, parameters, context, many
        ):
            if statement.startswith("UPDATE workflow_executor"):
                raise RuntimeError("save failed")

        async def requests():
            yield messages.ExecutorMessage(shutdown=messages.ExecutorShutdown())

        event.listen(
            self.database.sync_engine,
            "before_cursor_execute",
            fail_executor_update,
        )
        try:
            with patch.object(
                self.engine,
                "_authenticate_executor",
                return_value=self.executor_id,
            ):
                with self.assertRaisesRegex(RuntimeError, "save failed"):
                    await self.engine.connect_executor(
                        requests(), self.make_context()
                    )
        finally:
            event.remove(
                self.database.sync_engine,
                "before_cursor_execute",
                fail_executor_update,
            )
        self.assertNotIn(self.executor_id, self.engine._executors)

    @staticmethod
    def make_context():
        context = AsyncMock()
        context.abort.side_effect = ConnectionError("connection rejected")
        return context
