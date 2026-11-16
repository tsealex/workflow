import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import unittest
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from engine import WorkflowEngine, WorkflowExecutionCache
from models import (
    Base,
    ExecutionStatus,
    NodeExecution,
    WorkflowExecution,
    WorkflowSnapshot,
)
from nodes import (
    DecisionNode,
    Edge,
    ExecMetadata,
    RegionExecMetadata,
    RegionNode,
    WaitNode,
)


def make_engine(edges, start, decisions=None):
    """Build a synthetic workflow for scheduling tests."""
    decisions = decisions or {}
    ids = {name: uuid5(NAMESPACE_URL, name) for name in edges}
    nodes = []
    for name, node_id in ids.items():
        if name in decisions:
            nodes.append(
                DecisionNode(
                    id=node_id,
                    name=name,
                    description="Test decision",
                    code="",
                    input_states=[],
                    branches=[
                        label for label in decisions[name] if label != "default"
                    ],
                )
            )
        else:
            nodes.append(WaitNode(id=node_id, name=name, duration=timedelta(0)))
    root = RegionNode(id=uuid4(), name="Scheduling test", nodes=nodes, edges=[])
    root.edges.append(Edge(from_=root.initial_id, to=ids[start]))
    for name, successors in edges.items():
        if name in decisions:
            root.edges.extend(
                Edge(from_=ids[name], to=ids[target], branch=label)
                for label, target in decisions[name].items()
            )
        else:
            root.edges.extend(
                Edge(from_=ids[name], to=ids[target]) for target in successors
            )
    workflow = WorkflowExecution(
        xid=uuid4(),
        workflow_id=uuid4(),
        workflow_revision=1,
        status=ExecutionStatus.RUNNING,
        started_at=datetime.now(UTC),
        states={},
    )
    snapshot = WorkflowSnapshot(
        workflow_id=workflow.workflow_id,
        revision=1,
        root=root.model_dump(mode="json"),
        states={"type": "object"},
        ref_count=1,
    )
    engine = WorkflowEngine(async_sessionmaker())
    engine._executions[workflow.xid] = WorkflowExecutionCache(workflow)
    engine._workflow_snapshots[(workflow.workflow_id, 1)] = snapshot
    return engine, workflow.xid, ids, root


class EngineSchedulingTest(unittest.TestCase):
    def check_orders(self, edges, start, plan, decisions=None, choices=None):
        """Check all completion orders against an explicit execution graph."""
        engine, workflow_xid, ids, _ = make_engine(edges, start, decisions)
        choices = choices or {}
        cache = engine._executions[workflow_xid]
        first = None
        for label, (_, previous) in plan.items():
            if not previous:
                first = label
                break
        self.assertIsNotNone(first)
        first_node_execution = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids[start],
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(UTC),
            prev_xids=[],
        )
        pending = [
            (
                {first_node_execution.xid: first_node_execution},
                {first: first_node_execution.xid},
                set(),
            )
        ]
        orders = 0
        while pending:
            node_executions, labels, completed = pending.pop()
            ready = [label for label in labels if label not in completed]
            if not ready:
                self.assertEqual(set(plan), completed)
                orders += 1
                continue
            for label in ready:
                following = deepcopy(node_executions)
                following_labels = dict(labels)
                following_completed = completed | {label}
                current = following[labels[label]]
                current.status = ExecutionStatus.COMPLETED
                current.ended_at = datetime.now(UTC)
                # Recovery can return rows in a different order.
                cache.nodes = dict(reversed(list(following.items())))
                if len(following_completed) == len(plan):
                    pending.append(
                        (following, following_labels, following_completed)
                    )
                    continue
                expected = [
                    next_label
                    for next_label, (_, previous) in plan.items()
                    if next_label not in labels
                    and set(previous) <= following_completed
                ]
                arguments = (
                    {"branch": choices[label]} if label in choices else {}
                )
                result = engine.get_next_node_ids(
                    workflow_xid, current.xid, **arguments
                )
                self.assertEqual(
                    result,
                    {
                        ids[plan[item][0]]: sorted(
                            following_labels[previous]
                            for previous in plan[item][1]
                        )
                        for item in expected
                    },
                    (label, completed),
                )
                self.assertEqual(
                    result,
                    engine.get_next_node_ids(
                        workflow_xid, current.xid, **arguments
                    ),
                )
                for next_label in expected:
                    node_name, previous = plan[next_label]
                    node_execution = NodeExecution(
                        xid=uuid4(),
                        workflow_xid=workflow_xid,
                        node_id=ids[node_name],
                        status=ExecutionStatus.RUNNING,
                        started_at=datetime.now(UTC),
                        prev_xids=result[ids[node_name]],
                    )
                    following[node_execution.xid] = node_execution
                    following_labels[next_label] = node_execution.xid
                cache.nodes = following
                self.assertEqual(
                    {},
                    engine.get_next_node_ids(
                        workflow_xid, current.xid, **arguments
                    ),
                )
                pending.append(
                    (following, following_labels, following_completed)
                )
        return orders

    def test_initial_join_and_repeated_join(self):
        edges = {
            "S": ["A", "K"],
            "A": ["B"],
            "B": ["C"],
            "K": ["C"],
            "C": ["B", "E"],
            "E": ["C"],
        }
        plan = {
            "S1": ("S", []),
            "A1": ("A", ["S1"]),
            "K1": ("K", ["S1"]),
            "B1": ("B", ["A1"]),
            "C1": ("C", ["B1", "K1"]),
            "B2": ("B", ["C1"]),
            "E1": ("E", ["C1"]),
            "C2": ("C", ["B2", "E1"]),
            "B3": ("B", ["C2"]),
            "E2": ("E", ["C2"]),
            "C3": ("C", ["B3", "E2"]),
        }
        self.assertEqual(self.check_orders(edges, "S", plan), 12)

    def test_loop_waits_for_short_and_long_branches(self):
        edges = {
            "A": ["B"],
            "B": ["C", "D"],
            "C": ["B"],
            "D": ["E"],
            "E": ["B"],
        }
        plan = {
            "A1": ("A", []),
            "B1": ("B", ["A1"]),
            "C1": ("C", ["B1"]),
            "D1": ("D", ["B1"]),
            "E1": ("E", ["D1"]),
            "B2": ("B", ["C1", "E1"]),
            "C2": ("C", ["B2"]),
            "D2": ("D", ["B2"]),
            "E2": ("E", ["D2"]),
            "B3": ("B", ["C2", "E2"]),
        }
        self.assertEqual(self.check_orders(edges, "A", plan), 9)

    def test_decision_selects_named_or_default_branch(self):
        edges = {"A": ["B", "C"], "B": ["D"], "C": ["D"], "D": []}
        decisions = {"A": {"b": "B", "default": "C"}}
        for selected, choices in (("B", {"A1": "b"}), ("C", {})):
            with self.subTest(selected=selected):
                plan = {
                    "A1": ("A", []),
                    "selected": (selected, ["A1"]),
                    "D1": ("D", ["selected"]),
                }
                self.assertEqual(
                    self.check_orders(edges, "A", plan, decisions, choices), 1
                )

    def test_old_completion_does_not_satisfy_new_loop(self):
        edges = {
            "A": ["G", "D"],
            "G": ["B"],
            "D": ["B"],
            "B": ["C", "E"],
            "C": ["B"],
            "E": ["D"],
        }
        plan = {
            "A1": ("A", []),
            "G1": ("G", ["A1"]),
            "D1": ("D", ["A1"]),
            "B1": ("B", ["G1", "D1"]),
            "C1": ("C", ["B1"]),
            "E1": ("E", ["B1"]),
            "D2": ("D", ["E1"]),
            "B2": ("B", ["C1", "D2"]),
            "C2": ("C", ["B2"]),
            "E2": ("E", ["B2"]),
            "D3": ("D", ["E2"]),
            "B3": ("B", ["C2", "D3"]),
        }
        self.assertEqual(self.check_orders(edges, "A", plan), 18)

    def test_concurrent_node_executions_to_mutually_looping_nodes(self):
        edges = {"S": ["A", "B"], "A": ["B", "F"], "B": ["A", "F"], "F": []}
        decisions = {
            "A": {"repeat": "B", "default": "F"},
            "B": {"repeat": "A", "default": "F"},
        }
        plan = {
            "S1": ("S", []),
            "A1": ("A", ["S1"]),
            "B1": ("B", ["S1"]),
            "B2": ("B", ["A1"]),
            "A2": ("A", ["B1"]),
            "F1": ("F", ["B2", "A2"]),
        }
        self.assertEqual(
            self.check_orders(
                edges, "S", plan, decisions, {"A1": "repeat", "B1": "repeat"}
            ),
            6,
        )

    def test_nested_forks_share_join(self):
        edges = {
            "A": ["B", "C"],
            "B": ["D", "E"],
            "C": ["J"],
            "D": ["J"],
            "E": ["J"],
            "J": [],
        }
        plan = {
            "A1": ("A", []),
            "B1": ("B", ["A1"]),
            "C1": ("C", ["A1"]),
            "D1": ("D", ["B1"]),
            "E1": ("E", ["B1"]),
            "J1": ("J", ["C1", "D1", "E1"]),
        }
        self.assertEqual(self.check_orders(edges, "A", plan), 8)

    def test_direct_edge_to_join_counts_fork_completion(self):
        edges = {"A": ["B", "J"], "B": ["J"], "J": []}
        plan = {"A1": ("A", []), "B1": ("B", ["A1"]), "J1": ("J", ["A1", "B1"])}
        self.assertEqual(self.check_orders(edges, "A", plan), 1)

    def test_nested_distinct_joins_return_immediate_predecessors(self):
        edges = {
            "S": ["A", "K"],
            "A": ["B", "C"],
            "B": ["I"],
            "C": ["I"],
            "I": ["J"],
            "K": ["J"],
            "J": [],
        }
        plan = {
            "S1": ("S", []),
            "A1": ("A", ["S1"]),
            "K1": ("K", ["S1"]),
            "B1": ("B", ["A1"]),
            "C1": ("C", ["A1"]),
            "I1": ("I", ["B1", "C1"]),
            "J1": ("J", ["I1", "K1"]),
        }
        self.assertEqual(self.check_orders(edges, "S", plan), 10)

    def test_nested_direct_arrivals_return_all_contributing_node_executions(
        self,
    ):
        edges = {"A": ["B", "J"], "B": ["C", "J"], "C": ["J"], "J": []}
        plan = {
            "A1": ("A", []),
            "B1": ("B", ["A1"]),
            "C1": ("C", ["B1"]),
            "J1": ("J", ["A1", "B1", "C1"]),
        }
        self.assertEqual(self.check_orders(edges, "A", plan), 1)

    def test_decision_inside_fork_preserves_branch(self):
        edges = {
            "S": ["A", "D"],
            "A": ["B", "C"],
            "B": ["J"],
            "C": ["J"],
            "D": ["J"],
            "J": [],
        }
        decisions = {"A": {"b": "B", "default": "C"}}
        plan = {
            "S1": ("S", []),
            "A1": ("A", ["S1"]),
            "D1": ("D", ["S1"]),
            "B1": ("B", ["A1"]),
            "J1": ("J", ["B1", "D1"]),
        }
        self.assertEqual(
            self.check_orders(edges, "S", plan, decisions, {"A1": "b"}), 3
        )

    def test_status_and_unknown_execution_checks(self):
        engine, workflow_xid, ids, _ = make_engine({"A": ["B"], "B": []}, "A")
        node_execution = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids["A"],
            prev_xids=[],
        )
        engine._executions[workflow_xid].nodes[
            node_execution.xid
        ] = node_execution
        for status in (
            ExecutionStatus.RUNNING,
            ExecutionStatus.SUSPENDED,
            ExecutionStatus.FAILED,
            ExecutionStatus.INTERRUPTED,
        ):
            node_execution.status = status
            self.assertEqual(
                engine.get_next_node_ids(workflow_xid, node_execution.xid), {}
            )
        with self.assertRaises(KeyError):
            engine.get_next_node_ids(uuid4(), node_execution.xid)
        with self.assertRaises(KeyError):
            engine.get_next_node_ids(workflow_xid, uuid4())

        node_execution.status = ExecutionStatus.COMPLETED
        for status in (ExecutionStatus.FAILED, ExecutionStatus.COMPLETED):
            engine._executions[workflow_xid].workflow.status = status
            self.assertEqual(
                engine.get_next_node_ids(workflow_xid, node_execution.xid), {}
            )

    def test_recorded_decision_is_not_selected_again(self):
        engine, workflow_xid, ids, _ = make_engine(
            {"A": ["B", "C"], "B": [], "C": []},
            "A",
            {"A": {"b": "B", "default": "C"}},
        )
        decision = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids["A"],
            status=ExecutionStatus.COMPLETED,
            prev_xids=[],
        )
        selected = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids["B"],
            status=ExecutionStatus.RUNNING,
            prev_xids=[decision.xid],
        )
        engine._executions[workflow_xid].nodes = {
            decision.xid: decision,
            selected.xid: selected,
        }
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, decision.xid), {}
        )
        with self.assertRaisesRegex(ValueError, "conflicts"):
            engine.get_next_node_ids(
                workflow_xid, decision.xid, branch="default"
            )
        with self.assertRaisesRegex(ValueError, "no unique branch"):
            engine.get_next_node_ids(
                workflow_xid, decision.xid, branch="unknown"
            )
        decision.exec_metadata = {"branch": "b"}
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, decision.xid), {}
        )
        with self.assertRaisesRegex(ValueError, "conflicts"):
            engine.get_next_node_ids(
                workflow_xid, decision.xid, branch="default"
            )
        decision.exec_metadata = {"branch": "default"}
        with self.assertRaisesRegex(ValueError, "conflicts"):
            engine.get_next_node_ids(workflow_xid, decision.xid)

    def test_missing_predecessor_history_is_reported(self):
        engine, workflow_xid, ids, _ = make_engine({"A": ["B"], "B": []}, "A")
        node_execution = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids["B"],
            status=ExecutionStatus.COMPLETED,
            prev_xids=[],
        )
        engine._executions[workflow_xid].nodes = {
            node_execution.xid: node_execution
        }
        with self.assertRaisesRegex(ValueError, "no predecessor history"):
            engine.get_next_node_ids(workflow_xid, node_execution.xid)
        node_execution.prev_xids = [uuid4()]
        with self.assertRaises(KeyError):
            engine.get_next_node_ids(workflow_xid, node_execution.xid)

    def test_long_history_in_reverse_order(self):
        engine, workflow_xid, ids, _ = make_engine({"A": ["A"]}, "A")
        history = []
        for _ in range(1200):
            node_execution = NodeExecution(
                xid=uuid4(),
                workflow_xid=workflow_xid,
                node_id=ids["A"],
                status=ExecutionStatus.COMPLETED,
                prev_xids=[history[-1].xid] if history else [],
            )
            history.append(node_execution)
        engine._executions[workflow_xid].nodes = {
            node_execution.xid: node_execution
            for node_execution in reversed(history)
        }
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, history[-1].xid),
            {ids["A"]: [history[-1].xid]},
        )

    def test_optional_convergence_requires_explicit_semantics(self):
        engine, workflow_xid, ids, _ = make_engine(
            {"S": ["B", "C"], "B": ["D", "F"], "C": ["D"], "D": ["F"], "F": []},
            "S",
            {"B": {"d": "D", "default": "F"}},
        )
        node_execution = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=ids["S"],
            status=ExecutionStatus.COMPLETED,
            prev_xids=[],
        )
        engine._executions[workflow_xid].nodes = {
            node_execution.xid: node_execution
        }
        with self.assertRaisesRegex(ValueError, "optional convergence"):
            engine.get_next_node_ids(workflow_xid, node_execution.xid)

    def test_region_boundaries_and_implicit_edges(self):
        engine, workflow_xid, ids, root = make_engine({"A": []}, "A")
        cache = engine._executions[workflow_xid]
        region_xid = uuid4()
        region_execution = NodeExecution(
            xid=region_xid,
            workflow_xid=workflow_xid,
            node_id=root.id,
            status=ExecutionStatus.RUNNING,
            prev_xids=[],
            exec_metadata=RegionExecMetadata(item_idx=0).model_dump(
                mode="json"
            ),
        )
        cache.nodes[region_xid] = region_execution
        result = engine.get_next_node_ids(workflow_xid, region_xid)
        self.assertEqual(result, {ids["A"]: [region_xid]})
        action_xid = uuid4()
        exec_metadata = ExecMetadata().model_dump(mode="json")
        action = NodeExecution(
            xid=action_xid,
            workflow_xid=workflow_xid,
            node_id=ids["A"],
            status=ExecutionStatus.COMPLETED,
            prev_xids=result[ids["A"]],
            exec_metadata=exec_metadata,
        )
        cache.nodes[action.xid] = action
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, action.xid),
            {},
        )
        self.assertEqual(
            {node.node_id for node in cache.nodes.values()},
            {root.id, ids["A"]},
        )

    def test_implicit_region_entry(self):
        engine, workflow_xid, ids, root = make_engine({"A": []}, "A")
        root.edges = [Edge(from_=ids["A"], to=root.final_id)]
        workflow = engine._executions[workflow_xid].workflow
        snapshot = engine._workflow_snapshots[
            (workflow.workflow_id, workflow.workflow_revision)
        ]
        snapshot.root = root.model_dump(mode="json")
        region_xid = uuid4()
        exec_metadata = RegionExecMetadata(item_idx=0).model_dump(mode="json")
        node_execution = NodeExecution(
            xid=region_xid,
            workflow_xid=workflow_xid,
            node_id=root.id,
            status=ExecutionStatus.RUNNING,
            prev_xids=[],
            exec_metadata=exec_metadata,
        )
        engine._executions[workflow_xid].nodes = {
            node_execution.xid: node_execution
        }
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, node_execution.xid),
            {ids["A"]: [node_execution.xid]},
        )

    def test_region_queries_return_nodes_without_boundary_visits(self):
        engine, workflow_xid, ids, root = make_engine(
            {"A": ["R"], "R": ["D"], "D": []}, "A"
        )
        first_id, last_id = uuid4(), uuid4()
        first = WaitNode(id=first_id, name="B", duration=timedelta(0))
        last = WaitNode(id=last_id, name="C", duration=timedelta(0))
        region = RegionNode(
            id=ids["R"], name="R", nodes=[first, last], edges=[]
        )
        region.edges = [
            Edge(from_=region.initial_id, to=first.id),
            Edge(from_=first.id, to=last.id),
            Edge(from_=last.id, to=region.final_id),
        ]
        root.nodes = [
            region if node.id == region.id else node for node in root.nodes
        ]
        cache = engine._executions[workflow_xid]
        workflow = cache.workflow
        snapshot = engine._workflow_snapshots[
            (workflow.workflow_id, workflow.workflow_revision)
        ]
        snapshot.root = root.model_dump(mode="json")

        def record(node_id, previous, status, exec_metadata=None):
            xid = uuid4()
            execution = NodeExecution(
                xid=xid,
                workflow_xid=workflow_xid,
                node_id=node_id,
                prev_xids=[node.xid for node in previous],
                status=status,
                exec_metadata=exec_metadata,
            )
            cache.nodes[xid] = execution
            return execution

        predecessor = record(ids["A"], [], ExecutionStatus.COMPLETED)
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, predecessor.xid),
            {first.id: [predecessor.xid]},
        )
        first_execution = record(
            first.id, [predecessor], ExecutionStatus.COMPLETED
        )
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, first_execution.xid),
            {last.id: [first_execution.xid]},
        )
        last_execution = record(
            last.id, [first_execution], ExecutionStatus.COMPLETED
        )
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, last_execution.xid),
            {ids["D"]: [last_execution.xid]},
        )
        self.assertEqual(
            {node.node_id for node in cache.nodes.values()},
            {ids["A"], first.id, last.id},
        )

    def test_interruptible_edge_returns_enclosing_node(self):
        engine, workflow_xid, ids, root = make_engine(
            {"Outside": []}, "Outside"
        )
        inner_node = WaitNode(id=uuid4(), name="Inner", duration=timedelta(0))
        inner_region = RegionNode(
            id=uuid4(), name="Nested", nodes=[inner_node], edges=[]
        )
        inner_region.edges = [
            Edge(from_=inner_region.initial_id, to=inner_node.id),
            Edge(from_=inner_node.id, to=ids["Outside"]),
        ]
        root.nodes.append(inner_region)
        workflow = engine._executions[workflow_xid].workflow
        snapshot = engine._workflow_snapshots[
            (workflow.workflow_id, workflow.workflow_revision)
        ]
        snapshot.root = root.model_dump(mode="json")
        node_execution = NodeExecution(
            xid=uuid4(),
            workflow_xid=workflow_xid,
            node_id=inner_node.id,
            status=ExecutionStatus.COMPLETED,
            prev_xids=[],
        )
        engine._executions[workflow_xid].nodes = {
            node_execution.xid: node_execution
        }
        self.assertEqual(
            engine.get_next_node_ids(workflow_xid, node_execution.xid),
            {ids["Outside"]: [node_execution.xid]},
        )

    def test_interruptible_exit_preserves_outer_fork_branch(self):
        for target in ("J", "M"):
            with self.subTest(target=target):
                engine, workflow_xid, ids, root = make_engine(
                    {
                        "S": ["Region", "K"],
                        "Region": ["M"],
                        "M": ["J"],
                        "K": ["J"],
                        "J": [],
                    },
                    "S",
                )
                inner_node = WaitNode(
                    id=uuid4(), name="Inner", duration=timedelta(0)
                )
                region = RegionNode(
                    id=ids["Region"],
                    name="Region",
                    nodes=[inner_node],
                    edges=[],
                )
                region.edges = [
                    Edge(from_=region.initial_id, to=inner_node.id),
                    Edge(from_=inner_node.id, to=ids[target]),
                ]
                root.nodes = [
                    region if node.id == region.id else node
                    for node in root.nodes
                ]
                workflow = engine._executions[workflow_xid].workflow
                snapshot = engine._workflow_snapshots[
                    (workflow.workflow_id, workflow.workflow_revision)
                ]
                snapshot.root = root.model_dump(mode="json")
                cache = engine._executions[workflow_xid]

                def add_node_execution(node_id, previous, status):
                    node_execution = NodeExecution(
                        xid=uuid4(),
                        workflow_xid=workflow_xid,
                        node_id=node_id,
                        status=status,
                        prev_xids=[item.xid for item in previous],
                    )
                    cache.nodes[node_execution.xid] = node_execution
                    return node_execution

                fork = add_node_execution(
                    ids["S"], [], ExecutionStatus.COMPLETED
                )
                sibling = add_node_execution(
                    ids["K"], [fork], ExecutionStatus.RUNNING
                )
                inner = add_node_execution(
                    inner_node.id,
                    [fork],
                    ExecutionStatus.COMPLETED,
                )
                if target == "M":
                    result = engine.get_next_node_ids(workflow_xid, inner.xid)
                    self.assertEqual(
                        result,
                        {ids["M"]: [inner.xid]},
                    )
                    arrival = add_node_execution(
                        ids["M"],
                        [cache.nodes[xid] for xid in result[ids["M"]]],
                        ExecutionStatus.COMPLETED,
                    )
                else:
                    arrival = inner
                self.assertEqual(
                    engine.get_next_node_ids(workflow_xid, arrival.xid), {}
                )
                sibling.status = ExecutionStatus.COMPLETED
                result = engine.get_next_node_ids(workflow_xid, sibling.xid)
                self.assertEqual(
                    result,
                    {ids["J"]: sorted([arrival.xid, sibling.xid])},
                )
                add_node_execution(
                    ids["J"],
                    [cache.nodes[xid] for xid in result[ids["J"]]],
                    ExecutionStatus.RUNNING,
                )
                self.assertEqual(
                    engine.get_next_node_ids(workflow_xid, arrival.xid), {}
                )
                self.assertEqual(
                    engine.get_next_node_ids(workflow_xid, sibling.xid), {}
                )

    def test_recovery_reconstructs_pending_join(self):
        async def run():
            engine, workflow_xid, ids, _ = make_engine(
                {"A": ["B", "C"], "B": ["J"], "C": ["J"], "J": []}, "A"
            )
            database = create_async_engine("sqlite+aiosqlite://")
            try:
                async with database.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
                sessions = async_sessionmaker(database)
                workflow = engine._executions[workflow_xid].workflow
                snapshot = engine._workflow_snapshots[(workflow.workflow_id, 1)]
                fork = NodeExecution(
                    xid=uuid4(),
                    workflow_xid=workflow_xid,
                    node_id=ids["A"],
                    status=ExecutionStatus.COMPLETED,
                    started_at=datetime.now(UTC),
                    prev_xids=[],
                )
                first = NodeExecution(
                    xid=uuid4(),
                    workflow_xid=workflow_xid,
                    node_id=ids["B"],
                    status=ExecutionStatus.COMPLETED,
                    started_at=datetime.now(UTC),
                    prev_xids=[fork.xid],
                )
                second = NodeExecution(
                    xid=uuid4(),
                    workflow_xid=workflow_xid,
                    node_id=ids["C"],
                    status=ExecutionStatus.RUNNING,
                    started_at=datetime.now(UTC),
                    prev_xids=[fork.xid],
                )
                first_xid, second_xid = first.xid, second.xid
                async with sessions.begin() as session:
                    session.add_all([snapshot, workflow])
                    await session.flush()
                    session.add_all([fork, first, second])
                recovered = WorkflowEngine(sessions)
                await recovered.start()
                async with sessions.begin() as session:
                    node = await recovered._get_node_execution(
                        session, workflow_xid, second_xid
                    )
                    node.set_status(ExecutionStatus.COMPLETED)
                self.assertEqual(
                    recovered.get_next_node_ids(workflow_xid, second_xid),
                    {ids["J"]: sorted([first_xid, second_xid])},
                )
            finally:
                await database.dispose()

        asyncio.run(run())
