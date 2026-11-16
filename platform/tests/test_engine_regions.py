"""Check pure region traversal with synthetic workflows."""

import unittest
from copy import deepcopy
from datetime import UTC, datetime
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import execution_pb2 as messages
from engine import WorkflowEngine, WorkflowExecutionCache, _ExecutorConnection
from models import (
    Base,
    ExecutionStatus,
    NodeExecution,
    Workflow,
    WorkflowExecution,
    WorkflowSnapshot,
)
from nodes import ActionNode, DecisionNode, Edge, RegionNode


def action(name):
    node_id = uuid4()
    return ActionNode(
        id=node_id,
        name=name,
        description="Synthetic region test action",
        code="",
        input_states=[],
        output_states=[],
    )


def region(name, nodes):
    node_id = uuid4()
    result = RegionNode(id=node_id, name=name, nodes=nodes, edges=[])
    path = [result.initial_id, *(node.id for node in nodes), result.final_id]
    result.edges = [
        Edge(from_=source, to=target) for source, target in zip(path, path[1:])
    ]
    return result


class RegionSchedulingTest(unittest.TestCase):
    def set_graph(self, root):
        workflow_xid, workflow_id = uuid4(), uuid4()
        workflow = WorkflowExecution(
            xid=workflow_xid,
            workflow_id=workflow_id,
            workflow_revision=1,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(UTC),
            states={"unchanged": [1, 2]},
        )
        snapshot = WorkflowSnapshot(
            workflow_id=workflow_id,
            revision=1,
            root=root.model_dump(mode="json"),
            states={"type": "object"},
        )
        self.engine = WorkflowEngine(async_sessionmaker())
        self.cache = WorkflowExecutionCache(workflow)
        self.engine._executions[workflow_xid] = self.cache
        self.engine._workflow_snapshots[(workflow_id, 1)] = snapshot
        self.workflow_xid = workflow_xid
        self.snapshot = snapshot

    def record(self, node, previous=(), status=ExecutionStatus.COMPLETED):
        xid = uuid4()
        previous_xids = [item.xid for item in previous]
        execution = NodeExecution(
            xid=xid,
            workflow_xid=self.workflow_xid,
            node_id=node.id,
            prev_xids=previous_xids,
            status=status,
            inputs={"saved": node.name},
            exec_metadata={},
        )
        self.cache.nodes[xid] = execution
        return execution

    def query(self, current, **arguments):
        def state():
            return deepcopy(
                (
                    self.snapshot.root,
                    self.cache.workflow.status,
                    self.cache.workflow.states,
                    {
                        xid: (
                            node.node_id,
                            node.status,
                            node.prev_xids,
                            node.inputs,
                            node.exec_metadata,
                            node.ended_at,
                        )
                        for xid, node in self.cache.nodes.items()
                    },
                )
            )

        before = state()
        result = self.engine.get_next_node_ids(
            self.workflow_xid, current.xid, **arguments
        )
        self.assertEqual(state(), before)
        self.assertEqual(
            result,
            self.engine.get_next_node_ids(
                self.workflow_xid, current.xid, **arguments
            ),
        )
        self.assertEqual(state(), before)
        return result

    def test_entry_and_exit_skip_nested_control_nodes(self):
        first, second, third, last = [action(name) for name in "ABCD"]
        nested = region("R", [region("Inner", [second, third])])
        root = region("Root", [first, nested, last])
        self.set_graph(root)
        first_visit = self.record(first)
        self.assertEqual(
            self.query(first_visit), {second.id: [first_visit.xid]}
        )
        second_visit = self.record(second, [first_visit])
        self.assertEqual(self.query(first_visit), {})
        self.assertEqual(
            self.query(second_visit), {third.id: [second_visit.xid]}
        )
        third_visit = self.record(third, [second_visit])
        self.assertEqual(self.query(third_visit), {last.id: [third_visit.xid]})
        self.assertEqual(
            {node.node_id for node in self.cache.nodes.values()},
            {first.id, second.id, third.id},
        )

    def test_root_entry_query_keeps_supplied_execution_state(self):
        child = action("Child")
        root = region("Root", [region("Nested", [child])])
        self.set_graph(root)
        root_visit = self.record(root, status=ExecutionStatus.RUNNING)
        root_visit.exec_metadata = {"item_idx": 3}
        self.assertEqual(self.query(root_visit), {child.id: [root_visit.xid]})
        self.assertEqual(root_visit.status, ExecutionStatus.RUNNING)
        self.assertEqual(root_visit.exec_metadata, {"item_idx": 3})

    def test_final_join_returns_all_real_predecessors(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                first, left, right, last = [action(name) for name in "ABCD"]
                nested = region("R", [left, right])
                nested.edges = [
                    Edge(from_=nested.initial_id, to=node.id)
                    for node in (left, right)
                ] + [
                    Edge(from_=node.id, to=nested.final_id)
                    for node in (left, right)
                ]
                self.set_graph(region("Root", [first, nested, last]))
                start = self.record(first)
                self.assertEqual(
                    self.query(start),
                    {left.id: [start.xid], right.id: [start.xid]},
                )
                branches = [left, right]
                if reverse:
                    branches.reverse()
                first_done = self.record(branches[0], [start])
                pending = self.record(
                    branches[1], [start], ExecutionStatus.RUNNING
                )
                self.assertEqual(self.query(first_done), {})
                pending.status = ExecutionStatus.COMPLETED
                self.cache.nodes = dict(
                    reversed(list(self.cache.nodes.items()))
                )
                predecessors = sorted([first_done.xid, pending.xid])
                self.assertEqual(self.query(pending), {last.id: predecessors})
                self.record(
                    last, [first_done, pending], ExecutionStatus.RUNNING
                )
                self.assertEqual(self.query(pending), {})

    def test_region_join_preserves_enclosing_fork(self):
        for sibling_first in (False, True):
            with self.subTest(sibling_first=sibling_first):
                start, left, right, middle, sibling, join = [
                    action(name) for name in ("S", "L", "R", "M", "K", "J")
                ]
                nested = region("Nested", [left, right])
                nested.edges = [
                    Edge(from_=nested.initial_id, to=node.id)
                    for node in (left, right)
                ] + [
                    Edge(from_=node.id, to=nested.final_id)
                    for node in (left, right)
                ]
                root = region("Root", [start, nested, middle, sibling, join])
                root.edges = [
                    Edge(from_=root.initial_id, to=start.id),
                    Edge(from_=start.id, to=nested.id),
                    Edge(from_=start.id, to=sibling.id),
                    Edge(from_=nested.id, to=middle.id),
                    Edge(from_=middle.id, to=join.id),
                    Edge(from_=sibling.id, to=join.id),
                    Edge(from_=join.id, to=root.final_id),
                ]
                self.set_graph(root)
                fork = self.record(start)
                self.assertEqual(
                    self.query(fork),
                    {node.id: [fork.xid] for node in (left, right, sibling)},
                )
                left_visit = self.record(left, [fork])
                right_visit = self.record(right, [fork])
                sibling_status = (
                    ExecutionStatus.COMPLETED
                    if sibling_first
                    else ExecutionStatus.RUNNING
                )
                sibling_visit = self.record(sibling, [fork], sibling_status)
                predecessors = sorted([left_visit.xid, right_visit.xid])
                self.assertEqual(
                    self.query(right_visit), {middle.id: predecessors}
                )
                middle_visit = self.record(middle, [left_visit, right_visit])
                if not sibling_first:
                    self.assertEqual(self.query(middle_visit), {})
                    sibling_visit.status = ExecutionStatus.COMPLETED
                predecessors = sorted([middle_visit.xid, sibling_visit.xid])
                current = middle_visit if sibling_first else sibling_visit
                self.assertEqual(self.query(current), {join.id: predecessors})

    def test_decision_selects_region_entry_without_a_region_visit(self):
        choice_id = uuid4()
        choice = DecisionNode(
            id=choice_id,
            name="Choice",
            description="Synthetic decision",
            code="",
            input_states=[],
            branches=["enter"],
        )
        child, last = action("Child"), action("Last")
        nested = region("Nested", [child])
        root = region("Root", [choice, nested, last])
        root.edges = [
            Edge(from_=root.initial_id, to=choice.id),
            Edge(from_=choice.id, to=nested.id, branch="enter"),
            Edge(from_=choice.id, to=last.id, branch="default"),
            Edge(from_=nested.id, to=last.id),
            Edge(from_=last.id, to=root.final_id),
        ]
        self.set_graph(root)
        decision = self.record(choice)
        self.assertEqual(
            self.query(decision, branch="enter"), {child.id: [decision.xid]}
        )
        child_visit = self.record(child, [decision])
        self.assertEqual(self.query(child_visit), {last.id: [child_visit.xid]})
        self.assertEqual(self.query(decision), {})

    def test_repeated_region_traversal_uses_new_actual_visits(self):
        first, child = action("First"), action("Child")
        nested = region("Nested", [child])
        root = region("Root", [first, nested])
        root.edges = [
            Edge(from_=root.initial_id, to=first.id),
            Edge(from_=first.id, to=nested.id),
            Edge(from_=nested.id, to=first.id),
        ]
        self.set_graph(root)
        first_visit = self.record(first)
        self.assertEqual(self.query(first_visit), {child.id: [first_visit.xid]})
        child_visit = self.record(child, [first_visit])
        self.assertEqual(self.query(child_visit), {first.id: [child_visit.xid]})
        repeated = self.record(first, [child_visit])
        self.assertEqual(self.query(repeated), {child.id: [repeated.xid]})
        self.assertEqual(self.query(first_visit), {})

    def test_interruptible_exit_ignores_recorded_sibling_progress(self):
        for reverse in (False, True):
            with self.subTest(reverse=reverse):
                first, escape, worker, following, skipped, last = [
                    action(name)
                    for name in (
                        "A",
                        "Escape",
                        "Worker",
                        "Next",
                        "Skipped",
                        "D",
                    )
                ]
                nested = region("R", [escape, worker, following])
                nested.edges = [
                    Edge(from_=nested.initial_id, to=escape.id),
                    Edge(from_=nested.initial_id, to=worker.id),
                    Edge(from_=escape.id, to=last.id),
                    Edge(from_=worker.id, to=following.id),
                    Edge(from_=following.id, to=nested.final_id),
                ]
                root = region("Root", [first, nested, skipped, last])
                self.set_graph(root)
                first_visit = self.record(first)
                worker_visit = self.record(worker, [first_visit])
                following_visit = self.record(
                    following, [worker_visit], ExecutionStatus.RUNNING
                )
                escape_visit = self.record(escape, [first_visit])
                if reverse:
                    self.cache.nodes = dict(
                        reversed(list(self.cache.nodes.items()))
                    )
                self.assertEqual(
                    self.query(escape_visit), {last.id: [escape_visit.xid]}
                )
                self.assertEqual(
                    following_visit.status, ExecutionStatus.RUNNING
                )
                self.assertEqual(worker_visit.status, ExecutionStatus.COMPLETED)

    def test_stored_decision_choice_disambiguates_empty_region_paths(self):
        choice_id = uuid4()
        choice = DecisionNode(
            id=choice_id,
            name="Choice",
            description="Synthetic decision",
            code="",
            input_states=[],
            branches=["first"],
        )
        first, second = region("First", []), region("Second", [])
        last = action("Last")
        root = region("Root", [choice, first, second, last])
        root.edges = [
            Edge(from_=root.initial_id, to=choice.id),
            Edge(from_=choice.id, to=first.id, branch="first"),
            Edge(from_=choice.id, to=second.id, branch="default"),
            Edge(from_=first.id, to=last.id),
            Edge(from_=second.id, to=last.id),
            Edge(from_=last.id, to=root.final_id),
        ]
        self.set_graph(root)
        decision = self.record(choice)
        decision.exec_metadata = {"branch": "first"}
        self.assertEqual(self.query(decision), {last.id: [decision.xid]})
        self.record(last, [decision], ExecutionStatus.RUNNING)
        self.assertEqual(self.query(decision), {})

    def test_supplied_region_execution_selects_entry_or_outgoing_nodes(self):
        first, child, last = [action(name) for name in "ABD"]
        nested = region("R", [child])
        root = region("Root", [first, nested, last])
        self.set_graph(root)
        first_visit = self.record(first)
        region_visit = self.record(
            nested, [first_visit], ExecutionStatus.RUNNING
        )
        self.assertEqual(
            self.query(region_visit), {child.id: [region_visit.xid]}
        )
        self.assertEqual(region_visit.status, ExecutionStatus.RUNNING)
        region_visit.status = ExecutionStatus.COMPLETED
        self.assertEqual(
            self.query(region_visit), {last.id: [region_visit.xid]}
        )
        self.assertEqual(region_visit.status, ExecutionStatus.COMPLETED)
        self.set_graph(root)
        root_visit = self.record(root)
        self.assertEqual(self.query(root_visit), {})
        self.assertEqual(root_visit.status, ExecutionStatus.COMPLETED)

    def test_completed_supplied_region_starts_parent_fork(self):
        first, child, left, right, last = [action(name) for name in "ABKLD"]
        nested = region("R", [child])
        root = region("Root", [first, nested, left, right, last])
        root.edges = [
            Edge(from_=root.initial_id, to=first.id),
            Edge(from_=first.id, to=nested.id),
            Edge(from_=nested.id, to=left.id),
            Edge(from_=nested.id, to=right.id),
            Edge(from_=left.id, to=last.id),
            Edge(from_=right.id, to=last.id),
            Edge(from_=last.id, to=root.final_id),
        ]
        self.set_graph(root)
        first_visit = self.record(first)
        region_visit = self.record(nested, [first_visit])
        self.assertEqual(
            self.query(region_visit),
            {left.id: [region_visit.xid], right.id: [region_visit.xid]},
        )
        left_visit = self.record(left, [region_visit])
        right_visit = self.record(
            right, [region_visit], ExecutionStatus.RUNNING
        )
        self.assertEqual(self.query(left_visit), {})
        right_visit.status = ExecutionStatus.COMPLETED
        self.assertEqual(
            self.query(right_visit),
            {last.id: sorted([left_visit.xid, right_visit.xid])},
        )
        self.assertEqual(region_visit.status, ExecutionStatus.COMPLETED)


class EngineRegionSuccessorTest(unittest.IsolatedAsyncioTestCase):
    async def test_engine_persists_and_dispatches_scheduler_successors(self):
        database = create_async_engine("sqlite+aiosqlite://")
        self.addAsyncCleanup(database.dispose)
        async with database.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
        sessions = async_sessionmaker(database, expire_on_commit=False)
        engine = WorkflowEngine(sessions)
        executor_id = uuid4()
        connection = _ExecutorConnection(executor_id)
        engine._executors[executor_id] = connection
        first, second, third, last = [action(name) for name in "ABCD"]
        nested = region("R", [second, third])
        root = region("Root", [first, nested, last])
        workflow_id = uuid4()
        workflow = Workflow(
            id=workflow_id,
            revision=1,
            name="Synthetic region workflow",
            root_json=root.model_dump(mode="json"),
            states={"type": "object"},
        )
        async with sessions.begin() as session:
            session.add(workflow)
        execution = await engine.create_workflow_execution(workflow_id, {})
        previous_xid = None
        for name in "ABCD":
            await engine._execute_suspended_nodes(executor_id)
            message = connection.queue.get_nowait()
            self.assertTrue(message.HasField("execute_node"))
            request = message.execute_node
            self.assertEqual(request.node["name"], name)
            self.assertTrue(connection.queue.empty())
            node_xid = UUID(request.node_xid)
            async with sessions() as session:
                node = await session.get(NodeExecution, node_xid)
                expected_previous = [previous_xid] if previous_xid else []
                self.assertEqual(node.prev_xids, expected_previous)
            result = messages.ExecuteNodeResponse(
                node_xid=request.node_xid,
                status=messages.COMPLETED,
            )
            result.started_at.CopyFrom(request.started_at)
            result.ended_at.FromDatetime(datetime.now(UTC))
            await engine._process_node_result(executor_id, node_xid, result)
            previous_xid = node_xid
        async with sessions() as session:
            node_ids = list(
                await session.scalars(
                    select(NodeExecution.node_id).where(
                        NodeExecution.workflow_xid == execution.xid
                    )
                )
            )
        self.assertEqual(
            set(node_ids),
            {node.id for node in (first, second, third, last)},
        )
        self.assertEqual(len(node_ids), 4)

    async def test_initial_fork_joins_after_recovery_without_region_records(
        self,
    ):
        for nested_entry in (False, True):
            for reverse in (False, True):
                with self.subTest(nested_entry=nested_entry, reverse=reverse):
                    database = create_async_engine("sqlite+aiosqlite://")
                    self.addAsyncCleanup(database.dispose)
                    async with database.begin() as connection:
                        await connection.run_sync(Base.metadata.create_all)
                    sessions = async_sessionmaker(
                        database, expire_on_commit=False
                    )
                    left, right, join = [action(name) for name in "ABC"]
                    root = region("Root", [left, right, join])
                    owner = root
                    if nested_entry:
                        owner = region("Nested", [left, right])
                        root = region("Root", [owner, join])
                    destination = owner.final_id if nested_entry else join.id
                    owner.edges = [
                        Edge(from_=owner.initial_id, to=left.id),
                        Edge(from_=owner.initial_id, to=right.id),
                        Edge(from_=left.id, to=destination),
                        Edge(from_=right.id, to=destination),
                    ]
                    workflow_id = uuid4()
                    root_json = root.model_dump(mode="json")
                    workflow = Workflow(
                        id=workflow_id,
                        revision=1,
                        name="Synthetic initial fork workflow",
                        root_json=root_json,
                        states={"type": "object"},
                    )
                    async with sessions.begin() as session:
                        session.add(workflow)
                    engine = WorkflowEngine(sessions)
                    execution = await engine.create_workflow_execution(
                        workflow_id, {}
                    )
                    engine = WorkflowEngine(sessions)
                    await engine.start()
                    executor_id = uuid4()
                    connection = _ExecutorConnection(executor_id)
                    engine._executors[executor_id] = connection
                    await engine._execute_suspended_nodes(executor_id)
                    requests = [
                        connection.queue.get_nowait().execute_node
                        for _ in range(2)
                    ]
                    self.assertEqual(
                        {request.node["name"] for request in requests},
                        {"A", "B"},
                    )
                    if reverse:
                        requests.reverse()
                    for index, request in enumerate(requests):
                        result = messages.ExecuteNodeResponse(
                            node_xid=request.node_xid,
                            status=messages.COMPLETED,
                        )
                        result.started_at.CopyFrom(request.started_at)
                        await engine._process_node_result(
                            executor_id, UUID(request.node_xid), result
                        )
                        if index == 0:
                            self.assertTrue(connection.queue.empty())
                    await engine._execute_suspended_nodes(executor_id)
                    request = connection.queue.get_nowait().execute_node
                    self.assertEqual(request.node["name"], "C")
                    self.assertTrue(connection.queue.empty())
                    async with sessions() as session:
                        records = list(
                            await session.scalars(
                                select(NodeExecution).where(
                                    NodeExecution.workflow_xid == execution.xid
                                )
                            )
                        )
                    self.assertEqual(
                        {record.node_id for record in records},
                        {left.id, right.id, join.id},
                    )
                    initial_xids = {
                        UUID(request.node_xid) for request in requests
                    }
                    for record in records:
                        expected = (
                            initial_xids if record.node_id == join.id else set()
                        )
                        self.assertEqual(set(record.prev_xids), expected)
