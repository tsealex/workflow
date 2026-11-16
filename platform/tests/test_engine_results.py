import asyncio
import unittest
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import execution_pb2 as messages
from engine import _ExecutorConnection
from models import Base, ExecutionStatus, NodeExecution, WorkflowExecution
from nodes import ActionNode, decode_data_map
from test_engine_scheduling import make_engine


class EngineResultTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        database = create_async_engine("sqlite+aiosqlite://")
        self.addAsyncCleanup(database.dispose)
        async with database.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        self.sessions = async_sessionmaker(database, expire_on_commit=False)
        self.executor_id = uuid4()

    async def seed(
        self,
        edges,
        start,
        plan,
        decisions=None,
        *,
        actions=None,
        states=None,
        schema=None
    ):
        self.engine, self.workflow_xid, ids, root = make_engine(
            edges, start, decisions
        )
        self.engine._sessions = self.sessions
        self.cache = self.engine._executions[self.workflow_xid]
        if states is not None:
            self.cache.workflow.states = states
        workflow = self.cache.workflow
        snapshot = self.engine._workflow_snapshots[
            (workflow.workflow_id, workflow.workflow_revision)
        ]
        if schema is not None:
            snapshot.states = schema
        if actions:
            root.nodes = [
                (
                    ActionNode(
                        id=node.id,
                        name=node.name,
                        description="Output fixture",
                        code="",
                        input_states=[],
                        output_states=actions[node.name],
                    )
                    if node.name in actions
                    else node
                )
                for node in root.nodes
            ]
            snapshot.root = root.model_dump(mode="json")
        self.connection = _ExecutorConnection(self.executor_id)
        self.engine._executors[self.executor_id] = self.connection
        self.node_executions = {}
        for name, status, predecessors in plan:
            node = NodeExecution(
                xid=uuid4(),
                workflow_xid=self.workflow_xid,
                node_id=ids[name],
                status=status,
                started_at=datetime.now(UTC),
                prev_xids=[
                    self.node_executions[previous].xid
                    for previous in predecessors
                ],
                executor_id=self.executor_id,
                inputs={},
            )
            self.node_executions[name] = node
            self.cache.nodes[node.xid] = node
            if status == ExecutionStatus.RUNNING:
                self.engine._running_node_xids[node.xid] = self.executor_id
                self.connection.running_node_xids.add(node.xid)
        async with self.sessions.begin() as session:
            session.add_all(self.engine._workflow_snapshots.values())
            session.add(self.cache.workflow)
            await session.flush()
            session.add_all(self.node_executions.values())
        return ids

    @staticmethod
    def result(node, status=messages.COMPLETED, outputs=None):
        result = messages.ExecuteNodeResponse(
            node_xid=str(node.xid), status=status
        )
        result.started_at.FromDatetime(node.started_at)
        result.ended_at.FromDatetime(datetime.now(UTC))
        if outputs is not None:
            result.outputs = outputs
        return result

    async def test_dispatches_suspended_nodes_once_after_commit(self):
        await self.seed(
            {"A": ["B"], "B": [], "Wait": []},
            "A",
            [
                (name, ExecutionStatus.SUSPENDED, [])
                for name in ("A", "B", "Wait")
            ],
            decisions={"B": {"default": "B"}},
            actions={"A": []},
            states={"value": 7},
        )
        workflow = self.cache.workflow
        snapshot = self.engine._workflow_snapshots[(workflow.workflow_id, 1)]
        root = snapshot.get_root()
        for definition in root.nodes:
            if definition.name == "B":
                definition.input_states = ["value"]
        snapshot.root = root.model_dump(mode="json")
        first, second = self.node_executions["A"], self.node_executions["B"]
        async with self.sessions.begin() as session:
            session.add_all([workflow, first, second])
            workflow.set_status(ExecutionStatus.SUSPENDED)
            first.inputs = {"saved": 3}
            second.inputs = None
            first.executor_id = uuid4()
        committed = []
        record_commit = committed.append
        event.listen(
            self.sessions.class_.sync_session_class,
            "after_commit",
            record_commit,
        )
        self.addCleanup(
            event.remove,
            self.sessions.class_.sync_session_class,
            "after_commit",
            record_commit,
        )
        send = self.engine._sned_execute_node_request

        def check_commit(executor_id, request):
            self.assertTrue(committed)
            self.assertFalse(
                committed[-1].in_transaction() and committed[-1].is_active
            )
            send(executor_id, request)

        with patch.object(
            self.engine, "_sned_execute_node_request", check_commit
        ):
            await asyncio.gather(
                self.engine._execute_suspended_nodes(self.executor_id),
                self.engine._execute_suspended_nodes(self.executor_id),
            )
        requests = {}
        while not self.connection.queue.empty():
            request = self.connection.queue.get_nowait().execute_node
            requests[request.node_xid] = request
        self.assertEqual(set(requests), {str(first.xid), str(second.xid)})
        self.assertEqual(
            decode_data_map(requests[str(first.xid)].inputs), {"saved": 3}
        )
        self.assertEqual(
            decode_data_map(requests[str(second.xid)].inputs), {"value": 7}
        )
        async with self.sessions() as session:
            stored = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(stored.status, ExecutionStatus.RUNNING)
            for node in (first, second):
                stored_node = await session.get(NodeExecution, node.xid)
                self.assertEqual(stored_node.status, ExecutionStatus.RUNNING)
                self.assertEqual(stored_node.executor_id, self.executor_id)
        self.assertEqual(
            self.node_executions["Wait"].status, ExecutionStatus.SUSPENDED
        )

    async def test_does_not_dispatch_to_absent_or_closing_executor(self):
        await self.seed(
            {"A": []},
            "A",
            [("A", ExecutionStatus.SUSPENDED, [])],
            actions={"A": []},
        )
        await self.engine._execute_suspended_nodes(uuid4())
        self.connection.closing = True
        await self.engine._execute_suspended_nodes(self.executor_id)
        self.assertTrue(self.connection.queue.empty())
        self.assertEqual(
            self.node_executions["A"].status, ExecutionStatus.SUSPENDED
        )

    async def test_commits_successors_before_removing_running_node(self):
        ids = await self.seed(
            {"A": ["B", "C"], "B": [], "C": []},
            "A",
            [("A", ExecutionStatus.RUNNING, [])],
        )
        node = self.node_executions["A"]
        result = self.result(node)
        remove = self.engine._remove_running_node

        def check_committed(connection, node_xid):
            self.assertEqual(
                self.cache.nodes[node_xid].status, ExecutionStatus.COMPLETED
            )
            successors = [
                item
                for xid, item in self.cache.nodes.items()
                if xid != node_xid
            ]
            self.assertEqual(
                {item.node_id for item in successors}, {ids["B"], ids["C"]}
            )
            self.assertIn(node_xid, connection.running_node_xids)
            remove(connection, node_xid)

        with patch.object(
            self.engine, "_remove_running_node", side_effect=check_committed
        ):
            await self.engine._process_node_result(
                self.executor_id, node.xid, result
            )
        self.assertEqual(node.status, ExecutionStatus.COMPLETED)
        self.assertIs(self.cache.nodes[node.xid], node)
        self.assertEqual(self.connection.running_node_xids, set())
        async with self.sessions() as session:
            stored = await session.get(NodeExecution, node.xid)
            self.assertEqual(stored.status, ExecutionStatus.COMPLETED)
            self.assertEqual(stored.ended_at, result.ended_at.ToDatetime())
            successors = (
                await session.scalars(
                    select(NodeExecution).where(NodeExecution.xid != node.xid)
                )
            ).all()
            for successor in successors:
                self.assertEqual(successor.status, ExecutionStatus.SUSPENDED)
                self.assertEqual(successor.prev_xids, [node.xid])
                self.assertIsNone(successor.executor_id)
                self.assertIsNone(successor.inputs)
                self.assertIsNone(successor.ended_at)

    async def test_failed_commit_preserves_cache_and_running_tracking(self):
        await self.seed_outputs()
        node = self.node_executions["A"]
        node_xid = node.xid
        result = self.result(
            node, outputs=b'{"account":{"amount":2},"other":3}'
        )
        session = self.sessions()

        def fail_commit(_session):
            self.assertEqual(set(self.cache.nodes), {node_xid})
            self.assertEqual(node.status, ExecutionStatus.COMPLETED)
            raise RuntimeError("Commit failure fixture")

        event.listen(session.sync_session, "before_commit", fail_commit)
        with patch.object(self.engine, "_sessions", return_value=session):
            with self.assertRaisesRegex(RuntimeError, "Commit failure fixture"):
                await self.engine._process_node_result(
                    self.executor_id, node.xid, result
                )
        self.assertEqual(self.connection.running_node_xids, {node_xid})
        self.assertEqual(
            self.engine._running_node_xids, {node_xid: self.executor_id}
        )
        self.assertEqual(set(self.cache.nodes), {node_xid})
        async with self.sessions() as session:
            stored = (await session.scalars(select(NodeExecution))).all()
            self.assertEqual(len(stored), 1)
            self.assertEqual(stored[0].status, ExecutionStatus.RUNNING)
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(workflow.states["account"]["amount"], 1)
        async with self.sessions.begin() as session:
            self.assertIs(
                await self.engine._get_node_execution(
                    session, self.workflow_xid, node_xid
                ),
                node,
            )
            self.assertIs(
                await self.engine._get_workflow_execution(
                    session, self.workflow_xid
                ),
                self.cache.workflow,
            )
            self.assertEqual(node.status, ExecutionStatus.RUNNING)
        self.assertEqual(self.cache.workflow.states["account"]["amount"], 1)
        await self.engine._process_node_result(
            self.executor_id, node.xid, result
        )
        self.assertEqual(len(self.cache.nodes), 2)

    async def test_decision_creates_only_selected_successor(self):
        ids = await self.seed(
            {"A": ["B", "C"], "B": [], "C": []},
            "A",
            [("A", ExecutionStatus.RUNNING, [])],
            {"A": {"selected": "B", "default": "C"}},
        )
        node = self.node_executions["A"]
        await self.engine._process_node_result(
            self.executor_id,
            node.xid,
            self.result(node, outputs=b'{"branch":"selected"}'),
        )
        self.assertEqual(
            {
                item.node_id
                for xid, item in self.cache.nodes.items()
                if xid != node.xid
            },
            {ids["B"]},
        )

    async def test_concurrent_branch_results_create_one_join(self):
        ids = await self.seed(
            {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []},
            "A",
            [
                ("A", ExecutionStatus.COMPLETED, []),
                ("B", ExecutionStatus.RUNNING, ["A"]),
                ("C", ExecutionStatus.RUNNING, ["A"]),
            ],
        )
        await asyncio.gather(
            *(
                self.engine._process_node_result(
                    self.executor_id, node.xid, self.result(node)
                )
                for node in (
                    self.node_executions["B"],
                    self.node_executions["C"],
                )
            )
        )
        joins = [
            node
            for node in self.cache.nodes.values()
            if node.node_id == ids["D"]
        ]
        self.assertEqual(len(joins), 1)
        self.assertEqual(
            joins[0].prev_xids,
            sorted(
                [self.node_executions["B"].xid, self.node_executions["C"].xid]
            ),
        )
        self.assertEqual(self.engine._running_node_xids, {})

    async def test_receive_cancellation_waits_for_commit_and_tracking_cleanup(
        self,
    ):
        await self.seed(
            {"A": ["B"], "B": []},
            "A",
            [("A", ExecutionStatus.RUNNING, [])],
        )
        node = self.node_executions["A"]
        binding = asyncio.Event()
        proceed = asyncio.Event()
        bind = self.engine._get_node_execution

        async def bind_after_release(*args, **kwargs):
            binding.set()
            await proceed.wait()
            return await bind(*args, **kwargs)

        async def requests():
            yield messages.ExecutorMessage(node_result=self.result(node))

        async with asyncio.timeout(5):
            with patch.object(
                self.engine,
                "_get_node_execution",
                side_effect=bind_after_release,
            ):
                receiver = asyncio.create_task(
                    self.engine._receive_executor_messages(
                        self.connection, requests()
                    )
                )
                await binding.wait()
                receiver.cancel()
                self.assertEqual(self.connection.running_node_xids, {node.xid})
                proceed.set()
                with self.assertRaises(asyncio.CancelledError):
                    await receiver
        self.assertEqual(self.connection.running_node_xids, set())
        self.assertEqual(len(self.cache.nodes), 2)
        async with self.sessions() as session:
            stored = await session.get(NodeExecution, node.xid)
            self.assertEqual(stored.status, ExecutionStatus.COMPLETED)

    async def test_noncompleted_results_do_not_create_successors(self):
        for status in (
            messages.FAILED,
            messages.SUSPENDED,
            messages.INTERRUPTED,
        ):
            with self.subTest(status=status):
                await self.seed(
                    {"A": ["B"], "B": []},
                    "A",
                    [("A", ExecutionStatus.RUNNING, [])],
                )
                node = self.node_executions["A"]
                await self.engine._process_node_result(
                    self.executor_id, node.xid, self.result(node, status)
                )
                self.assertEqual(set(self.cache.nodes), {node.xid})
                stored = self.cache.nodes[node.xid]
                self.assertEqual(
                    stored.status,
                    ExecutionStatus.from_proto(status),
                )
                self.assertEqual(
                    stored.ended_at is None, status == messages.SUSPENDED
                )
                self.assertEqual(self.connection.running_node_xids, set())

    async def seed_outputs(self, paths=None):
        return await self.seed(
            {"A": ["B"], "B": []},
            "A",
            [("A", ExecutionStatus.RUNNING, [])],
            actions={
                "A": ["account.amount", "other"] if paths is None else paths
            },
            states={"account": {"amount": 1, "keep": True}, "other": 0},
            schema={
                "type": "object",
                "properties": {
                    "account": {
                        "type": "object",
                        "properties": {
                            "amount": {"type": "integer"},
                            "keep": {"type": "boolean"},
                        },
                        "required": ["amount", "keep"],
                    },
                    "other": {"type": "integer"},
                },
                "required": ["account", "other"],
            },
        )

    async def test_output_paths_update_state_with_successors_atomically(self):
        await self.seed_outputs()
        node = self.node_executions["A"]
        original = self.cache.workflow
        await self.engine._process_node_result(
            self.executor_id,
            node.xid,
            self.result(
                node,
                outputs=b'{"account":{"amount":9007199254740993,"keep":false},"other":4,"ignored":9}',
            ),
        )
        expected = {
            "account": {"amount": 9007199254740993, "keep": True},
            "other": 4,
        }
        self.assertIs(self.cache.workflow, original)
        self.assertEqual(self.cache.workflow.states, expected)
        async with self.sessions() as session:
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(workflow.states, expected)
            self.assertEqual(workflow.status, ExecutionStatus.RUNNING)
            node_executions = (
                await session.scalars(
                    select(NodeExecution).where(
                        NodeExecution.workflow_xid == self.workflow_xid
                    )
                )
            ).all()
            self.assertEqual(len(node_executions), 2)

    async def test_invalid_outputs_fail_without_state_writes_or_successors(
        self,
    ):
        for outputs in (
            b'{"account":{"amount":2},"other":"wrong"}',
            b'{"account":{"amount":2}}',
            b'{"account":2,"other":3}',
            b'{"account":{"amount":NaN},"other":3}',
            b"not JSON",
        ):
            with self.subTest(outputs=outputs):
                await self.seed_outputs()
                node = self.node_executions["A"]

                async def requests():
                    yield messages.ExecutorMessage(
                        node_result=self.result(node, outputs=outputs)
                    )

                await self.engine._receive_executor_messages(
                    self.connection, requests()
                )
                self.assertNotIn(self.workflow_xid, self.engine._executions)
                self.assertEqual(self.engine._running_node_xids, {})
                async with self.sessions() as session:
                    workflow = await session.get(
                        WorkflowExecution, self.workflow_xid
                    )
                    self.assertEqual(workflow.status, ExecutionStatus.FAILED)
                    self.assertIsNotNone(workflow.ended_at)
                    self.assertEqual(
                        workflow.states,
                        {"account": {"amount": 1, "keep": True}, "other": 0},
                    )
                    node_executions = (
                        await session.scalars(
                            select(NodeExecution).where(
                                NodeExecution.workflow_xid == self.workflow_xid
                            )
                        )
                    ).all()
                    self.assertEqual(len(node_executions), 1)
                    self.assertEqual(
                        node_executions[0].status, ExecutionStatus.FAILED
                    )

    async def test_failure_interrupts_assigned_executors_after_commit(self):
        await self.seed(
            {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []},
            "A",
            [
                ("A", ExecutionStatus.COMPLETED, []),
                ("B", ExecutionStatus.RUNNING, ["A"]),
                ("C", ExecutionStatus.RUNNING, ["A"]),
                ("D", ExecutionStatus.SUSPENDED, ["B", "C"]),
            ],
            actions={"B": ["value"]},
            states={"value": 1},
            schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
            },
        )
        current, sibling = self.node_executions["B"], self.node_executions["C"]
        other_executor = uuid4()
        other_connection = _ExecutorConnection(
            other_executor, running_node_xids={sibling.xid}
        )
        self.connection.running_node_xids.remove(sibling.xid)
        self.engine._executors[other_executor] = other_connection
        self.engine._running_node_xids[sibling.xid] = other_executor
        unrelated_xid = uuid4()
        other_connection.running_node_xids.add(unrelated_xid)
        self.engine._running_node_xids[unrelated_xid] = other_executor
        interrupt = self.engine._sned_interrupt_node_request

        def check_committed(executor_id, node_xid):
            self.assertNotIn(self.workflow_xid, self.engine._executions)
            interrupt(executor_id, node_xid)

        with patch.object(
            self.engine,
            "_sned_interrupt_node_request",
            side_effect=check_committed,
        ):
            await self.engine._process_node_result(
                self.executor_id,
                current.xid,
                self.result(current, outputs=b'{"value":"invalid"}'),
            )
        self.assertTrue(self.connection.queue.empty())
        self.assertEqual(
            (other_connection.queue.get_nowait()).interrupt_node.node_xid,
            str(sibling.xid),
        )
        self.assertTrue(other_connection.queue.empty())
        async with self.sessions() as session:
            statuses = {
                node.node_id: node.status
                for node in await session.scalars(select(NodeExecution))
            }
            self.assertEqual(
                statuses[self.node_executions["A"].node_id],
                ExecutionStatus.COMPLETED,
            )
            self.assertEqual(statuses[current.node_id], ExecutionStatus.FAILED)
            self.assertEqual(
                statuses[sibling.node_id], ExecutionStatus.INTERRUPTED
            )
            self.assertEqual(
                statuses[self.node_executions["D"].node_id],
                ExecutionStatus.INTERRUPTED,
            )
        await self.engine._process_node_result(
            other_executor, sibling.xid, self.result(sibling)
        )
        self.assertEqual(
            self.engine._running_node_xids, {unrelated_xid: other_executor}
        )
        self.assertEqual(other_connection.running_node_xids, {unrelated_xid})

    async def test_validation_failure_commit_error_does_not_interrupt(self):
        await self.seed(
            {"A": ["B"], "B": []},
            "A",
            [
                ("A", ExecutionStatus.RUNNING, []),
                ("B", ExecutionStatus.RUNNING, ["A"]),
            ],
            actions={"A": ["value"]},
            states={"value": 1},
            schema={
                "type": "object",
                "properties": {"value": {"type": "integer"}},
            },
        )
        node = self.node_executions["A"]
        node_xids = {item.xid for item in self.node_executions.values()}
        session = self.sessions()

        def fail_commit(_session):
            raise RuntimeError("Failure transaction rejected")

        event.listen(session.sync_session, "before_commit", fail_commit)
        with patch.object(self.engine, "_sessions", return_value=session):
            with self.assertRaisesRegex(
                RuntimeError, "Failure transaction rejected"
            ):
                await self.engine._process_node_result(
                    self.executor_id, node.xid, self.result(node, outputs=b"{}")
                )
        self.assertIn(self.workflow_xid, self.engine._executions)
        async with self.sessions.begin() as session:
            self.assertIs(
                await self.engine._get_workflow_execution(
                    session, self.workflow_xid
                ),
                self.cache.workflow,
            )
            for node_xid in node_xids:
                refreshed = await self.engine._get_node_execution(
                    session, self.workflow_xid, node_xid
                )
                self.assertIs(refreshed, self.cache.nodes[node_xid])
                self.assertEqual(refreshed.status, ExecutionStatus.RUNNING)
        self.assertEqual(self.cache.workflow.status, ExecutionStatus.RUNNING)
        self.assertEqual(self.connection.running_node_xids, node_xids)
        self.assertTrue(self.connection.queue.empty())
        async with self.sessions() as session:
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(workflow.status, ExecutionStatus.RUNNING)
            self.assertEqual(
                (await session.get(NodeExecution, node.xid)).status,
                ExecutionStatus.RUNNING,
            )

    async def test_empty_outputs_with_no_output_paths(self):
        await self.seed_outputs([])
        node = self.node_executions["A"]
        await self.engine._process_node_result(
            self.executor_id, node.xid, self.result(node, outputs=b"{}")
        )
        self.assertEqual(self.cache.workflow.status, ExecutionStatus.RUNNING)

    async def test_invalid_decision_branch_fails_without_state_updates(self):
        await self.seed(
            {"A": ["B"], "B": []},
            "A",
            [("A", ExecutionStatus.RUNNING, [])],
            {"A": {"selected": "B"}},
            states={"branch": "unchanged"},
        )
        node = self.node_executions["A"]
        await self.engine._process_node_result(
            self.executor_id,
            node.xid,
            self.result(node, outputs=b'{"branch":"unknown"}'),
        )
        async with self.sessions() as session:
            workflow = await session.get(WorkflowExecution, self.workflow_xid)
            self.assertEqual(workflow.status, ExecutionStatus.FAILED)
            self.assertEqual(workflow.states, {"branch": "unchanged"})
