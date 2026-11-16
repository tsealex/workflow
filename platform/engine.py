import asyncio
import json
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from graphlib import TopologicalSorter
from hashlib import sha256
from secrets import compare_digest, token_urlsafe
from uuid import UUID, uuid4

import grpc
from jsonschema import ValidationError
from sqlalchemy import event, inspect, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session

import execution_pb2 as messages
from execution_pb2_grpc import WorkflowEngineServicer
from models import (
    ExecutionStatus,
    NodeExecution,
    TerminalExecutionStatus,
    Workflow,
    WorkflowExecution,
    WorkflowExecutorRegistration,
    WorkflowSnapshot,
    WorkflowSnapshotID,
)
from nodes import (
    ActionNode,
    BaseNode,
    DataMap,
    DecisionNode,
    ExecMetadata,
    RegionNode,
    WaitNode,
    decode_data_map,
)


@dataclass
class WorkflowExecutionCache:
    """Group the records that the engine keeps for one workflow execution."""

    # The workflow execution record shared by all executions in this cache.
    workflow: WorkflowExecution
    # All node executions by execution ID, including completed executions.
    nodes: dict[UUID, NodeExecution] = field(default_factory=dict)
    result_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _ExecutorConnection:
    """Track outgoing messages and the executor's active node executions."""

    executor_id: UUID
    queue: asyncio.Queue[messages.EngineMessage] = field(
        default_factory=asyncio.Queue
    )
    running_node_xids: set[UUID] = field(default_factory=set)
    closing: bool = False


ForkID = tuple[UUID, tuple[UUID, ...]]
BranchID = tuple[ForkID, UUID]


@dataclass
class _ForkJoin:
    """Derive one fork's join and arrivals for a scheduling query."""

    join_node_id: UUID
    pending_branches: set[UUID]
    enclosing_branch: BranchID | None
    prev_xids: set[UUID] = field(default_factory=set)
    region_id: UUID | None = None


class _NodeScheduler:
    """Reconstruct branch progress from a snapshot and recorded executions."""

    def __init__(
        self,
        root: RegionNode,
        node_executions: dict[UUID, NodeExecution],
        current: NodeExecution,
        branch: str | None,
    ) -> None:
        """Read the graph and visit history without changing either object.

        Branch arrivals and control traversal belong to this query only.
        """
        self.root = root
        self.node_executions = node_executions
        self.current = current
        self.branch = branch
        self.nodes: dict[UUID, BaseNode] = {root.id: root}
        self.regions: dict[UUID, RegionNode | None] = {root.id: None}
        self._collect_nodes(root)
        self.graphs: dict[UUID, dict[UUID, list[UUID]]] = {}
        self.children: dict[UUID, list[NodeExecution]] = {}
        for execution in node_executions.values():
            for predecessor in execution.prev_xids:
                self.children.setdefault(predecessor, []).append(execution)
        self.forks: dict[ForkID, _ForkJoin] = {}
        self.joins: dict[tuple[UUID, UUID | None], UUID] = {}
        self.ready: dict[tuple[UUID, tuple[UUID, ...]], BranchID | None] = {}
        self.recorded_regions = {
            (execution.node_id, tuple(sorted(execution.prev_xids)))
            for execution in node_executions.values()
            if isinstance(self.nodes[execution.node_id], RegionNode)
        }
        self.cancelled_forks: set[ForkID] = set()
        self.branches_by_execution: dict[UUID, BranchID | None] = {}

    def _collect_nodes(self, region: RegionNode) -> None:
        """Index `region`'s descendants and their owners.

        Boundary IDs enter the ownership map, but not the node map.
        """
        for boundary_id in (region.initial_id, region.final_id):
            # Boundaries retain their snapshot IDs when selected as successors.
            self.regions[boundary_id] = region
        for node in region.nodes:
            self.nodes[node.id] = node
            self.regions[node.id] = region
            if isinstance(node, RegionNode):
                self._collect_nodes(node)

    def _collect_edges(self, region: RegionNode) -> dict[UUID, list[UUID]]:
        """Return and cache `region`'s node-to-successor map.

        Include implicit entry and exit edges without changing the snapshot.
        """
        if region.id in self.graphs:
            return self.graphs[region.id]
        edges = {
            node_id: []
            for node_id, owner in self.regions.items()
            if owner is region
        }
        for edge in region.edges:
            if edge.to not in edges[edge.from_]:
                edges[edge.from_].append(edge.to)

        # A path ending at final receives an implicit edge from initial.
        reaches_final = {region.final_id}
        while True:
            added = {
                node_id
                for node_id, successors in edges.items()
                if node_id not in reaches_final
                and any(target in reaches_final for target in successors)
            }
            if not added:
                break
            reaches_final.update(added)
        incoming = {
            target for successors in edges.values() for target in successors
        }
        for node in region.nodes:
            if node.id in reaches_final and node.id not in incoming:
                edges[region.initial_id].append(node.id)

        # A terminal non-decision node reached from initial continues to final.
        reachable = set()
        pending = [region.initial_id]
        while pending:
            node_id = pending.pop()
            if node_id not in reachable:
                reachable.add(node_id)
                pending.extend(edges.get(node_id, ()))
        for node in region.nodes:
            if (
                node.id in reachable
                and not edges[node.id]
                and not isinstance(node, DecisionNode)
            ):
                edges[node.id].append(region.final_id)
        self.graphs[region.id] = edges
        return edges

    def _find_join(
        self, source: UUID, enclosing: UUID | None, region: RegionNode
    ) -> UUID:
        """Return and cache the earliest common join ID for `source`.

        The fork `source` and optional `enclosing` join bound the search.
        Ambiguous joins or optional convergence raise ValueError.
        """
        edges = self._collect_edges(region)
        key = source, enclosing
        if key in self.joins:
            return self.joins[key]
        boundaries = {source}
        # A return to this fork starts its next execution. An enclosing join ends its branch.
        if enclosing is not None:
            boundaries.add(enclosing)
        exits = {
            target
            for successors in edges.values()
            for target in successors
            if target not in edges
        }
        required: dict[UUID, set[UUID]] = {}
        # Work backward from each candidate until all outgoing paths lead to it.
        for target in edges:
            reached = {target}
            if target == region.final_id:
                # An exit cancels this region instead of arriving at its final.
                reached.update(exits)
            while True:
                added = {
                    node_id
                    for node_id, successors in edges.items()
                    if node_id not in reached
                    and node_id not in boundaries
                    and successors
                    and all(successor in reached for successor in successors)
                }
                if not added:
                    break
                reached.update(added)
            required[target] = reached
        selected = set(edges[source])
        candidates = {
            target
            for target, reached in required.items()
            if selected <= reached
        }
        if not candidates:
            # Cyclic branches can join after they leave their loops.
            required = {
                target: self._find_nodes_before_exit(
                    target, boundaries, edges, region.final_id
                )
                for target in edges
            }
            candidates = {
                target
                for target, reached in required.items()
                if selected <= reached
            }
        earliest = [
            target
            for target in candidates
            if all(target in required[other] for other in candidates)
        ]
        if len(earliest) != 1:
            raise ValueError(f"Cannot infer one join for fork {source}.")
        target = earliest[0]
        # Branches that meet only conditionally need a different join rule.
        self._check_branch_overlap(source, target, boundaries | exits, edges)
        self.joins[key] = target
        return target

    def _find_reachable(
        self, start: UUID, boundaries: set[UUID], edges: dict[UUID, list[UUID]]
    ) -> set[UUID]:
        """Return IDs reachable from `start` without entering `boundaries`.

        Include `start` unless it is a boundary.
        """
        reached = set()
        pending = [start]
        while pending:
            node_id = pending.pop()
            if node_id not in reached and node_id not in boundaries:
                reached.add(node_id)
                pending.extend(edges.get(node_id, ()))
        return reached

    def _check_branch_overlap(
        self,
        source: UUID,
        target: UUID,
        boundaries: set[UUID],
        edges: dict[UUID, list[UUID]],
    ) -> None:
        """Reject optional convergence between fork `source` and join `target`.

        Stop traversal at `boundaries`. Overlap raises ValueError unless the
        branch entries belong to the same loop.
        """
        boundaries = boundaries | {target}
        branches: dict[UUID, set[UUID]] = {}
        for successor in edges[source]:
            for node_id in self._find_reachable(successor, boundaries, edges):
                branches.setdefault(node_id, set()).add(successor)
        for node_id, entries in branches.items():
            # Mutually looping branch entries remain independent until their join.
            if len(entries) > 1 and not entries <= self._find_reachable(
                node_id, boundaries, edges
            ):
                raise ValueError(
                    f"Fork {source} has optional convergence at {node_id} "
                    f"before its inferred join {target}."
                )

    def _find_nodes_before_exit(
        self,
        target: UUID,
        boundaries: set[UUID],
        edges: dict[UUID, list[UUID]],
        final_id: UUID,
    ) -> set[UUID]:
        """Return nodes whose terminating paths must pass through `target`.

        Permit loops, but exclude paths that can reach `boundaries` or another
        exit without reaching `target`.
        """
        reaches_target = {target}
        if target == final_id:
            reaches_target.update(
                successor
                for successors in edges.values()
                for successor in successors
                if successor not in edges
            )
        # Ignore repeated loop executions, but reject any exit that bypasses the join.
        avoids_target = {
            node_id
            for node_id, successors in edges.items()
            if node_id != target and (node_id in boundaries or not successors)
        }
        while True:
            reached = {
                node_id
                for node_id, successors in edges.items()
                if node_id not in boundaries
                and any(successor in reaches_target for successor in successors)
            }
            avoided = {
                node_id
                for node_id, successors in edges.items()
                if node_id != target
                and any(
                    successor in avoids_target
                    or (successor not in edges and target != final_id)
                    for successor in successors
                )
            }
            if reached <= reaches_target and avoided <= avoids_target:
                return reaches_target - avoids_target
            reaches_target.update(reached)
            avoids_target.update(avoided)

    def _get_successors(self, node_id: UUID) -> list[UUID]:
        """Return graph edges, including region entry and exit control edges."""
        definition = self.nodes.get(node_id)
        if isinstance(definition, RegionNode):
            return [definition.initial_id]
        owner = self.regions[node_id]
        if node_id == owner.final_id:
            parent = self.regions[owner.id]
            if parent is None:
                return []
            return self._collect_edges(parent)[owner.id]
        return self._collect_edges(owner)[node_id]

    def _get_first_nodes(
        self, target: UUID, stop: UUID | None = None
    ) -> set[UUID]:
        """Resolve control paths for decision history and entry validation."""
        result = set()
        pending = [target]
        seen = set()
        while pending:
            node_id = pending.pop()
            if node_id in seen:
                continue
            seen.add(node_id)
            if node_id == stop or (
                node_id in self.nodes
                and not isinstance(self.nodes[node_id], RegionNode)
            ):
                result.add(node_id)
            else:
                pending.extend(self._get_successors(node_id))
        return result

    def _select_successors(
        self, execution: NodeExecution, enclosing_branch: BranchID | None
    ) -> list[UUID]:
        """Use the supplied decision choice or recorded successor history."""
        definition = self.nodes[execution.node_id]
        if (
            isinstance(definition, RegionNode)
            and execution.status == ExecutionStatus.COMPLETED
        ):
            owner = self.regions[definition.id]
            return self._collect_edges(owner)[definition.id] if owner else []
        successors = self._get_successors(execution.node_id)
        if not isinstance(definition, DecisionNode):
            return successors
        exec_metadata = ExecMetadata.model_validate(
            execution.exec_metadata or {}
        )
        supplied = self.branch if execution.xid == self.current.xid else None
        if (
            exec_metadata.branch is not None
            and supplied is not None
            and supplied != exec_metadata.branch
        ):
            raise ValueError(
                f"Decision branch conflicts with execution {execution.xid}."
            )
        children = {
            child.node_id for child in self.children.get(execution.xid, ())
        }
        selected_branch = (
            exec_metadata.branch
            if exec_metadata.branch is not None
            else supplied
        )
        if selected_branch is None and children:
            recorded = [
                target
                for target in successors
                if all(
                    child in self._get_first_nodes(target, child)
                    for child in children
                )
            ]
            if len(recorded) != 1:
                raise ValueError(
                    f"Decision history conflicts for execution {execution.xid}."
                )
            return recorded
        if selected_branch is None and execution.xid == self.current.xid:
            selected_branch = "default"
        if selected_branch is not None:
            owner = self.regions[execution.node_id]
            selected = [
                edge.to
                for edge in owner.edges
                if edge.from_ == execution.node_id
                and edge.branch == selected_branch
            ]
            if len(selected) != 1:
                raise ValueError(
                    f"Decision has no unique branch {selected_branch!r}."
                )
            if any(
                child not in self._get_first_nodes(selected[0], child)
                for child in children
            ):
                raise ValueError(
                    f"Decision branch conflicts with execution {execution.xid}."
                )
            return selected
        if enclosing_branch is not None:
            join = self.forks[enclosing_branch[0]].join_node_id
            selected = [
                target
                for target in successors
                if join in self._get_first_nodes(target, join)
            ]
            if len(selected) == 1:
                return selected
        raise ValueError(
            f"Decision history is incomplete for execution {execution.xid}."
        )

    def _is_cancelled(self, branch: BranchID | None) -> bool:
        """Check query-local cancellation of an exited region's branches."""
        while branch is not None:
            if branch[0] in self.cancelled_forks:
                return True
            branch = self.forks[branch[0]].enclosing_branch
        return False

    def _follow_edges(
        self,
        source: UUID,
        successors: list[UUID],
        predecessors: set[UUID],
        branch: BranchID | None,
        controls: frozenset[UUID] = frozenset(),
    ) -> None:
        """Traverse ready control edges and retain actual-node arrivals."""
        if not successors:
            return
        definition = self.nodes.get(source)
        owner = self.regions[source]
        graph_source = source
        if isinstance(definition, RegionNode):
            if successors == [definition.initial_id]:
                owner = definition
        elif source == owner.final_id:
            graph_source = owner.id
            owner = self.regions[owner.id]
        if not isinstance(definition, DecisionNode) and len(successors) > 1:
            fork_id = source, tuple(sorted(predecessors))
            enclosing = self.forks[branch[0]].join_node_id if branch else None
            join = self._find_join(graph_source, enclosing, owner)
            self.forks.setdefault(
                fork_id,
                _ForkJoin(join, set(successors), branch, region_id=owner.id),
            )
        else:
            fork_id = None
        for target in successors:
            target_branch = (fork_id, target) if fork_id else branch
            destination = self.regions[target]
            exited = set()
            ancestor = owner
            while ancestor is not None and ancestor is not destination:
                exited.add(ancestor.id)
                ancestor = self.regions[ancestor.id]
            # Region entry is a downward control edge, not an interruption.
            if ancestor is destination:
                while target_branch is not None:
                    fork = self.forks[target_branch[0]]
                    if fork.region_id not in exited:
                        break
                    self.cancelled_forks.add(target_branch[0])
                    target_branch = fork.enclosing_branch
            self._arrive(target, predecessors, target_branch, controls)

    def _arrive(
        self,
        target: UUID,
        predecessors: set[UUID],
        branch: BranchID | None,
        controls: frozenset[UUID],
    ) -> None:
        """Wait at joins, then pass control vertices without creating visits."""
        while branch is not None:
            fork = self.forks[branch[0]]
            if fork.join_node_id != target:
                break
            fork.prev_xids.update(predecessors)
            fork.pending_branches.discard(branch[1])
            if fork.pending_branches:
                return
            predecessors = set(fork.prev_xids)
            branch = fork.enclosing_branch
        definition = self.nodes.get(target)
        key = target, tuple(sorted(predecessors))
        if (
            definition is not None and not isinstance(definition, RegionNode)
        ) or key in self.recorded_regions:
            if key in self.ready and self.ready[key] != branch:
                raise ValueError("A node arrival combines unrelated branches.")
            self.ready[key] = branch
            return
        if target in controls:
            raise ValueError("A control path must reach an executable node.")
        self._follow_edges(
            target,
            self._get_successors(target),
            predecessors,
            branch,
            controls | {target},
        )

    def get_next_node_ids(self) -> dict[UUID, list[UUID]]:
        """Return ready actual-node IDs; do not change execution records.

        Replay predecessor history through query-local control vertices.
        Missing or conflicting visit history raises KeyError or ValueError.
        """
        order = TopologicalSorter(
            {
                xid: execution.prev_xids
                for xid, execution in self.node_executions.items()
            }
        ).static_order()
        for xid in order:
            execution = self.node_executions[xid]
            key = execution.node_id, tuple(sorted(execution.prev_xids))
            if execution.prev_xids:
                if key in self.ready:
                    branch = self.ready.pop(key)
                else:
                    branch = None
                    for predecessor in execution.prev_xids:
                        previous_branch = self.branches_by_execution[
                            predecessor
                        ]
                        if self._is_cancelled(previous_branch):
                            branch = previous_branch
                            break
                    if branch is None:
                        raise ValueError(
                            f"Execution {xid} has incomplete predecessor history."
                        )
            else:
                owner = self.regions[execution.node_id]
                entries = self._get_first_nodes(owner.id) if owner else set()
                if not isinstance(
                    self.nodes[execution.node_id], RegionNode
                ) and (execution.node_id not in entries or len(entries) != 1):
                    raise ValueError(
                        f"Execution {xid} has no predecessor history."
                    )
                branch = None
            self.branches_by_execution[xid] = branch
            if self._is_cancelled(branch):
                continue
            definition = self.nodes[execution.node_id]
            if execution.status == ExecutionStatus.COMPLETED or (
                isinstance(definition, RegionNode)
                and execution.status
                in (ExecutionStatus.RUNNING, ExecutionStatus.SUSPENDED)
            ):
                successors = self._select_successors(execution, branch)
                self._follow_edges(execution.node_id, successors, {xid}, branch)
        return {
            node_id: list(predecessors)
            for (node_id, predecessors), branch in self.ready.items()
            if self.current.xid in predecessors
            and not self._is_cancelled(branch)
        }


class WorkflowEngine(WorkflowEngineServicer):
    """Coordinate node dispatch and result commits over gRPC."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Store `sessions` as the database session factory and create empty caches.

        Do not load records or connect executors during initialization.
        """
        self._sessions = sessions
        # The engine owns one cache per workflow execution ID.
        self._executions: dict[UUID, WorkflowExecutionCache] = {}
        # The engine keeps one snapshot per workflow ID and revision.
        self._workflow_snapshots: dict[WorkflowSnapshotID, WorkflowSnapshot] = (
            {}
        )
        # Executor ID to connection mapping for active executors.
        self._executors: dict[UUID, _ExecutorConnection] = {}
        self._running_node_xids: dict[UUID, UUID] = {}

    async def start(self) -> None:
        """Recover unfinished executions before the server starts."""
        await self._load_executions()

    def get_next_node_ids(
        self,
        workflow_xid: UUID,
        node_xid: UUID,
        *,
        branch: str | None = None,
    ) -> dict[UUID, list[UUID]]:
        """Map ready successor node IDs to predecessor execution IDs.

        Read the snapshot and execution history without changing either.
        Previous scheduling transitions must be fully recorded in prev_xids
        before this query. The caller records returned executions in the same
        serialized checkpoint as the completion. Each predecessor list is
        sorted and has no duplicates. The scheduler resolves regions and their
        boundaries without returning control IDs or changing their state.

        Raise KeyError for unknown executions or missing predecessors. Raise
        ValueError for incomplete history, an invalid decision branch, an
        ambiguous join, or optional convergence before an inferred join.
        """
        cache = self._executions[workflow_xid]
        current = cache.nodes[node_xid]
        if cache.workflow.status in TerminalExecutionStatus:
            return {}
        snapshot = self._workflow_snapshots[
            (cache.workflow.workflow_id, cache.workflow.workflow_revision)
        ]
        definition = snapshot.get_node(current.node_id)
        if current.status != ExecutionStatus.COMPLETED and not (
            isinstance(definition, RegionNode)
            and current.status == ExecutionStatus.RUNNING
        ):
            return {}
        return _NodeScheduler(
            snapshot.get_root(), cache.nodes, current, branch
        ).get_next_node_ids()

    async def create_workflow_execution(
        self, workflow_id: UUID, inputs: DataMap
    ) -> WorkflowExecution:
        """Create and return a running execution of the current workflow revision.

        Raise LookupError if the workflow does not exist. Raise
        ValidationError if the inputs do not match its state schema.
        Cache the execution and its snapshot after the transaction commits.
        """
        states = deepcopy(inputs)
        xid = uuid4()
        async with (
            self._sessions(expire_on_commit=False) as session,
            session.begin(),
        ):
            # The workflow lock prevents two starts from creating the same snapshot.
            workflow = await session.get(
                Workflow, workflow_id, with_for_update=True
            )
            if workflow is None:
                raise LookupError(f"Workflow {workflow_id} does not exist.")

            snapshot_id = (workflow_id, workflow.revision)
            snapshot = self._workflow_snapshots.get(snapshot_id)
            if snapshot is None:
                snapshot = await session.get(
                    WorkflowSnapshot, snapshot_id, with_for_update=True
                )
            if snapshot is None:
                snapshot = WorkflowSnapshot(
                    workflow_id=workflow_id,
                    revision=workflow.revision,
                    root=workflow.root_json,
                    states=workflow.states,
                )
                session.add(snapshot)

            snapshot.validate_states(states)
            # Insert the snapshot before the execution trigger references it.
            await session.flush()
            execution = WorkflowExecution(
                xid=xid,
                workflow_id=workflow_id,
                workflow_revision=snapshot.revision,
                status=ExecutionStatus.RUNNING,
                started_at=datetime.now(UTC),
                states=states,
            )
            session.add(execution)
            root = snapshot.get_root()
            root_execution = self._add_node_execution(
                session,
                xid,
                root.id,
                None,
                None,
                status=ExecutionStatus.RUNNING,
            )
            nodes = {root_execution.xid: root_execution}
            successors = _NodeScheduler(
                root, nodes, root_execution, None
            ).get_next_node_ids()
            for node_id, prev_xids in successors.items():
                successor = self._add_node_execution(
                    session,
                    xid,
                    node_id,
                    None,
                    None,
                    prev_xids=prev_xids,
                    status=ExecutionStatus.SUSPENDED,
                )
                nodes[successor.xid] = successor

        self._workflow_snapshots[snapshot_id] = snapshot
        if execution.status not in TerminalExecutionStatus:
            self._executions[xid] = WorkflowExecutionCache(execution, nodes)
            for executor_id in list(self._executors):
                await self._execute_suspended_nodes(executor_id)
        return execution

    async def create_node_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        node_id: UUID,
        executor_id: UUID | None,
        inputs: DataMap | None,
        *,
        prev_xids: list[UUID] | None = None,
        status: ExecutionStatus = ExecutionStatus.RUNNING,
    ) -> NodeExecution:
        """Add and return a node execution in the caller's transaction.

        Raise KeyError if the workflow execution is absent from the cache.
        Copy the inputs and predecessor IDs, then return the execution. The
        cache receives the execution after the caller commits.
        """
        if workflow_xid not in self._executions:
            raise KeyError(workflow_xid)

        return self._add_node_execution(
            session,
            workflow_xid,
            node_id,
            executor_id,
            inputs,
            prev_xids=prev_xids,
            status=status,
        )

    def _add_node_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        node_id: UUID,
        executor_id: UUID | None,
        inputs: DataMap | None,
        *,
        prev_xids: list[UUID] | None = None,
        status: ExecutionStatus = ExecutionStatus.RUNNING,
    ) -> NodeExecution:
        """Add a visit to the caller's transaction without changing the cache."""
        xid = uuid4()
        node = NodeExecution(
            xid=xid,
            workflow_xid=workflow_xid,
            node_id=node_id,
            prev_xids=list(prev_xids) if prev_xids is not None else [],
            executor_id=executor_id,
            status=status,
            started_at=datetime.now(UTC),
            inputs=deepcopy(inputs),
        )
        node.set_status(status)
        session.sync_session.expire_on_commit = False
        session.add(node)
        self._register_execution_commit_handler(session)
        return node

    def _register_execution_commit_handler(self, session: AsyncSession) -> None:
        """Cache execution records and notify executors after `session` commits.

        Capture records before flush so new executions remain available after commit.
        Remove terminal workflows and notify executors of interrupted visits.
        The caller owns the transaction.
        """
        # Install these hooks once for this session.
        if self in session.info:
            return
        session.info[self] = True
        records: list[object] = []
        interrupted: set[UUID] = set()

        def capture_records(committing_session: Session) -> None:
            # Hold pending inserts through the commit flush until they enter the cache.
            nonlocal records
            records = list(committing_session.identity_map.values())
            records.extend(committing_session.new)
            capture_interruptions(committing_session)

        def capture_interruptions(
            committing_session: Session, _context=None, _instances=None
        ) -> None:
            for record in committing_session.dirty:
                if (
                    isinstance(record, NodeExecution)
                    and record.status == ExecutionStatus.INTERRUPTED
                    and inspect(record).attrs.status.history.has_changes()
                ):
                    interrupted.add(record.xid)

        def finish_after_commit(_session: Session) -> None:
            # Remove terminal workflows before considering their node records.
            for record in records:
                if (
                    isinstance(record, WorkflowExecution)
                    and record.status in TerminalExecutionStatus
                ):
                    self._executions.pop(record.xid, None)
            for record in records:
                if not isinstance(record, NodeExecution):
                    continue
                # Cache executions only while their workflow remains active.
                cache = self._executions.get(record.workflow_xid)
                if cache is not None:
                    cache.nodes[record.xid] = record
                # Send each interruption only after its status change commits.
                if record.xid in interrupted:
                    executor_id = self._running_node_xids.get(record.xid)
                    if executor_id is not None:
                        self._sned_interrupt_node_request(
                            executor_id, record.xid
                        )
            records.clear()
            interrupted.clear()

        def discard_after_rollback(_session: Session) -> None:
            records.clear()
            interrupted.clear()

        event.listen(
            session.sync_session, "after_rollback", discard_after_rollback
        )
        event.listen(
            session.sync_session, "before_flush", capture_interruptions
        )
        event.listen(session.sync_session, "before_commit", capture_records)
        event.listen(session.sync_session, "after_commit", finish_after_commit)

    async def _load_executions(self) -> None:
        """Restore execution caches using the configured session factory.

        In one transaction, change RUNNING workflows and executions to SUSPENDED.
        After commit, cache suspended workflows, their snapshots, and all their
        executions, including completed executions.
        """
        async with (
            self._sessions(expire_on_commit=False) as session,
            session.begin(),
        ):
            for execution in (WorkflowExecution, NodeExecution):
                await session.execute(
                    update(execution)
                    .where(execution.status == ExecutionStatus.RUNNING)
                    .values(status=ExecutionStatus.SUSPENDED)
                )
            workflows = (
                await session.scalars(
                    select(WorkflowExecution).where(
                        WorkflowExecution.status == ExecutionStatus.SUSPENDED
                    )
                )
            ).all()
            snapshots = (
                await session.scalars(
                    select(WorkflowSnapshot).where(
                        tuple_(
                            WorkflowSnapshot.workflow_id,
                            WorkflowSnapshot.revision,
                        ).in_(
                            select(
                                WorkflowExecution.workflow_id,
                                WorkflowExecution.workflow_revision,
                            ).where(
                                WorkflowExecution.status
                                == ExecutionStatus.SUSPENDED
                            )
                        )
                    )
                )
            ).all()
            nodes = (
                await session.scalars(
                    select(NodeExecution)
                    .join(WorkflowExecution)
                    .where(
                        WorkflowExecution.status == ExecutionStatus.SUSPENDED,
                    )
                )
            ).all()

        self._workflow_snapshots.update(
            ((snapshot.workflow_id, snapshot.revision), snapshot)
            for snapshot in snapshots
        )
        for workflow in workflows:
            self._executions[workflow.xid] = WorkflowExecutionCache(workflow)
        for node in nodes:
            self._executions[node.workflow_xid].nodes[node.xid] = node

    async def _get_workflow_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        *,
        cache: WorkflowExecutionCache | None = None,
    ) -> WorkflowExecution:
        """Bind and return `workflow_xid`'s shared record in `session`.

        Use `cache` when supplied; otherwise look it up by `workflow_xid`.
        Refresh expired attributes and keep them loaded after commit. The caller
        owns the transaction; rollback can expire the shared record. A missing
        `cache` entry raises KeyError.
        """
        if cache is None:
            cache = self._executions[workflow_xid]
        workflow = cache.workflow
        session.sync_session.expire_on_commit = False
        session.add(workflow)
        self._register_execution_commit_handler(session)
        if inspect(workflow).expired:
            await session.refresh(workflow)
        return workflow

    async def _get_node_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        node_xid: UUID,
        *,
        cache: WorkflowExecutionCache | None = None,
    ) -> NodeExecution:
        """Bind and return `node_xid`'s shared execution in `session`.

        Use `cache` when supplied; otherwise look it up by `workflow_xid`.
        Refresh expired attributes and keep them loaded after commit. The caller
        owns the transaction; rollback can expire the record. A missing workflow
        or execution raises KeyError.
        """
        if cache is None:
            cache = self._executions[workflow_xid]
        node = cache.nodes[node_xid]
        session.sync_session.expire_on_commit = False
        session.add(node)
        self._register_execution_commit_handler(session)
        if inspect(node).expired:
            await session.refresh(node)
        return node

    async def register_executor(
        self,
        request: messages.RegisterExecutorRequest,
        context: grpc.aio.ServicerContext,
    ) -> messages.RegisterExecutorResponse:
        """Create an executor or verify credentials for a disconnected one."""
        has_id = request.HasField("executor_id")
        has_token = request.HasField("token")
        if has_id != has_token:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Provide both the executor ID and token, or neither.",
            )
        if not has_id:
            executor_id = uuid4()
            token = token_urlsafe(32)
            await self._create_registration(executor_id, token)
        else:
            try:
                executor_id = UUID(request.executor_id)
            except ValueError:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid executor ID."
                )
                raise
            token = request.token
            token_hash = await self._get_token_hash(executor_id)
            if token_hash is None or not compare_digest(
                token_hash, self._hash_token(token)
            ):
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Invalid executor credentials.",
                )
            if executor_id in self._executors:
                await context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    "Executor already connected.",
                )
        return messages.RegisterExecutorResponse(
            executor_id=str(executor_id), token=token
        )

    async def _create_registration(self, executor_id: UUID, token: str) -> None:
        """Commit a registration for `executor_id`, storing `token`'s hash only."""
        async with self._sessions.begin() as session:
            session.add(
                WorkflowExecutorRegistration(
                    id=executor_id,
                    token_hash=self._hash_token(token),
                    created_at=datetime.now(UTC),
                )
            )

    async def _get_token_hash(self, executor_id: UUID) -> str | None:
        """Return `executor_id`'s stored token hash, or None if unregistered."""
        async with self._sessions() as session:
            return await session.scalar(
                select(WorkflowExecutorRegistration.token_hash).where(
                    WorkflowExecutorRegistration.id == executor_id
                )
            )

    async def _find_executor(self, token_hash: str) -> UUID | None:
        """Return the executor UUID matching `token_hash`, or None."""
        async with self._sessions() as session:
            return await session.scalar(
                select(WorkflowExecutorRegistration.id).where(
                    WorkflowExecutorRegistration.token_hash == token_hash
                )
            )

    async def _record_connection(
        self, executor_id: UUID, connected_at: datetime
    ) -> bool:
        """Save `connected_at` as `executor_id`'s last connection time.

        Commit the update and return True, or return False if unregistered.
        """
        async with self._sessions.begin() as session:
            registration = await session.get(
                WorkflowExecutorRegistration, executor_id
            )
            if registration is None:
                return False
            registration.last_connected_at = connected_at
        return True

    @staticmethod
    def _hash_token(token: str) -> str:
        """Return the SHA-256 hex digest of `token` encoded as UTF-8."""
        return sha256(token.encode()).hexdigest()

    async def _authenticate_executor(
        self, context: grpc.aio.ServicerContext
    ) -> UUID:
        """Return the executor UUID authenticated by `context`'s Bearer token.

        Missing, repeated, malformed, or unknown credentials abort the RPC
        with UNAUTHENTICATED.
        """
        metadata = context.invocation_metadata()
        credential = None
        if metadata is not None:
            for key, value in metadata:
                if key == "authorization":
                    if credential is not None:
                        await context.abort(
                            grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
                        )
                    credential = value
        executor_id = None
        if isinstance(credential, str) and credential.startswith("Bearer "):
            token = credential.removeprefix("Bearer ")
            executor_id = await self._find_executor(self._hash_token(token))
        if executor_id is None:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
            )
        assert executor_id is not None
        return executor_id

    async def connect_executor(
        self,
        request_iterator: AsyncIterator[messages.ExecutorMessage],
        context: grpc.aio.ServicerContext,
    ) -> None:
        """Read `request_iterator` and write executor messages through `context`.

        Authenticate and reserve one connection per executor. On exit, stop
        both stream tasks, clear assignments, and release the connection.
        """
        executor_id = await self._authenticate_executor(context)
        if executor_id in self._executors:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS, "Executor already connected."
            )
        # Reserve the connection before database work can yield to another RPC.
        connection = self._executors[executor_id] = _ExecutorConnection(
            executor_id
        )
        accepted = False
        stream_tasks: list[asyncio.Task] = []
        try:
            accepted = await self._record_connection(
                executor_id, datetime.now(UTC)
            )
            if not accepted:
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
                )
            await self._execute_suspended_nodes(executor_id)
            # End the connection when either side of the stream stops.
            stream_tasks.append(
                asyncio.create_task(
                    self._receive_executor_messages(
                        connection, request_iterator
                    )
                )
            )
            stream_tasks.append(
                asyncio.create_task(
                    self._send_executor_messages(connection, context)
                )
            )
            tasks, _ = await asyncio.wait(
                stream_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in tasks:
                task.result()
        except _InvalidExecutorMessage as error:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        finally:
            # Finish in-flight result processing before clearing its assignments.
            connection.closing = True
            for task in stream_tasks:
                task.cancel()
            await asyncio.gather(*stream_tasks, return_exceptions=True)
            for node_xid in connection.running_node_xids:
                del self._running_node_xids[node_xid]
            connection.running_node_xids.clear()
            try:
                if accepted:
                    await self._handle_executor_disconnected(executor_id)
            finally:
                del self._executors[executor_id]

    def _send_execute_node_request(
        self, executor_id: UUID, request: messages.ExecuteNodeRequest
    ) -> None:
        """Track `request`'s saved node execution and queue it for `executor_id`.

        Do not write to the database or stream. An unknown executor raises
        KeyError; a closing connection or duplicate assignment raises RuntimeError.
        """
        connection = self._executors[executor_id]
        if connection.closing:
            raise RuntimeError("Executor is closing.")
        node_xid = UUID(request.node_xid)
        if node_xid in self._running_node_xids:
            raise RuntimeError(
                "Node execution already has an assigned executor."
            )
        message = messages.EngineMessage(execute_node=request)
        message.execute_node.node_xid = str(node_xid)
        self._running_node_xids[node_xid] = executor_id
        connection.running_node_xids.add(node_xid)
        connection.queue.put_nowait(message)

    def _sned_interrupt_node_request(
        self, executor_id: UUID, node_xid: UUID
    ) -> None:
        """Queue an interrupt for `node_xid` on `executor_id`'s connection.

        Ignore missing or closing connections and untracked executions. Keep the
        stored status and running tracking unchanged.
        """
        connection = self._executors.get(executor_id)
        if (
            connection is not None
            and not connection.closing
            and node_xid in connection.running_node_xids
        ):
            connection.queue.put_nowait(
                messages.EngineMessage(
                    interrupt_node=messages.InterruptNodeRequest(
                        node_xid=str(node_xid)
                    )
                )
            )

    def _notify_engine_shutdown(self, executor_id: UUID) -> None:
        """Mark `executor_id`'s connection closing and queue shutdown.

        The send task writes shutdown before returning. Ignore missing
        connections and those already closing.
        """
        connection = self._executors.get(executor_id)
        if connection is not None and not connection.closing:
            connection.closing = True
            connection.queue.put_nowait(
                messages.EngineMessage(shutdown=messages.EngineShutdown())
            )

    async def _execute_suspended_nodes(self, executor_id: UUID) -> None:
        """Commit suspended node assignments, then queue execution on `executor_id`."""
        connection = self._executors.get(executor_id)
        if connection is None or connection.closing:
            return
        for workflow_xid, cache in list(self._executions.items()):
            # Serialize assignments with result processing for this workflow.
            async with cache.result_lock:
                if workflow_xid not in self._executions:
                    continue
                for node_xid in list(cache.nodes):
                    async with self._sessions.begin() as session:
                        # Bind cached records and check whether execution can resume.
                        workflow = await self._get_workflow_execution(
                            session, workflow_xid, cache=cache
                        )
                        node_execution = await self._get_node_execution(
                            session, workflow_xid, node_xid, cache=cache
                        )
                        if (
                            workflow.status in TerminalExecutionStatus
                            or node_execution.status
                            != ExecutionStatus.SUSPENDED
                            or node_xid in self._running_node_xids
                        ):
                            continue
                        snapshot = self._workflow_snapshots[
                            (workflow.workflow_id, workflow.workflow_revision)
                        ]
                        try:
                            node = snapshot.get_node(node_execution.node_id)
                        except KeyError:
                            continue
                        if not isinstance(node, (ActionNode, DecisionNode)):
                            continue
                        if (
                            self._executors.get(executor_id) is not connection
                            or connection.closing
                        ):
                            return
                        # Preserve saved inputs when resuming a suspended execution.
                        if node_execution.inputs is None:
                            node_execution.inputs = node.extract_input_states(
                                workflow.states
                            )
                        request_node_xid = str(node_xid)
                        inputs = json.dumps(
                            node_execution.inputs, allow_nan=False
                        ).encode()
                        request = messages.ExecuteNodeRequest(
                            node_xid=request_node_xid,
                            inputs=inputs,
                            node=node.model_dump(mode="json"),
                        )
                        request.started_at.FromDatetime(
                            node_execution.started_at
                        )
                        previous_executor_id = node_execution.executor_id
                        previous_workflow_status = workflow.status
                        # Commit the assignment before the executor can receive it.
                        node_execution.executor_id = executor_id
                        node_execution.set_status(ExecutionStatus.RUNNING)
                        workflow.set_status(ExecutionStatus.RUNNING)
                    # A disconnect during commit must leave the execution available.
                    if (
                        self._executors.get(executor_id) is not connection
                        or connection.closing
                    ):
                        async with self._sessions.begin() as session:
                            workflow = await self._get_workflow_execution(
                                session, workflow_xid, cache=cache
                            )
                            node_execution = await self._get_node_execution(
                                session, workflow_xid, node_xid, cache=cache
                            )
                            node_execution.executor_id = previous_executor_id
                            node_execution.set_status(ExecutionStatus.SUSPENDED)
                            workflow.set_status(previous_workflow_status)
                        return
                    self._send_execute_node_request(executor_id, request)

    async def _process_node_result(
        self,
        executor_id: UUID,
        node_xid: UUID,
        result: messages.ExecuteNodeResponse,
    ) -> None:
        """Commit `result` for `node_xid` assigned to `executor_id`.

        Serialize results per workflow. Update shared records, validate outputs,
        and save unassigned successors as SUSPENDED. Invalid outputs fail the
        node and workflow instead. Remove running tracking after commit; late
        results for uncached workflows only remove tracking. Database errors
        propagate, leaving tracking intact and cached records possibly expired.
        """
        # Find the workflow that owns this node execution.
        workflow_xid = None
        for xid, cache in self._executions.items():
            if node_xid in cache.nodes:
                workflow_xid = xid
                break
        # A late result only clears tracking after workflow cache removal.
        if workflow_xid is None:
            self._remove_running_node(self._executors[executor_id], node_xid)
            return
        cache = self._executions[workflow_xid]
        # Process results for the same workflow one at a time.
        async with cache.result_lock:
            # Another result can end the workflow while this result waits.
            if workflow_xid not in self._executions:
                self._remove_running_node(
                    self._executors[executor_id], node_xid
                )
                return
            async with (
                self._sessions(expire_on_commit=False) as session,
                session.begin(),
            ):
                # Bind the cached execution records to this transaction.
                workflow = await self._get_workflow_execution(
                    session, workflow_xid, cache=cache
                )
                for node_execution_xid in cache.nodes:
                    await self._get_node_execution(
                        session, workflow_xid, node_execution_xid, cache=cache
                    )
                # Record the reported status and terminal end time.
                node = cache.nodes[node_xid]
                if node.status == ExecutionStatus.INTERRUPTED:
                    # A late result cannot restart an exited region's path.
                    status = ExecutionStatus.INTERRUPTED
                else:
                    status = ExecutionStatus.from_proto(result.status)
                    node.set_status(status)
                    if status in TerminalExecutionStatus and result.HasField(
                        "ended_at"
                    ):
                        node.ended_at = result.ended_at.ToDatetime(tzinfo=UTC)
                if (
                    status == ExecutionStatus.FAILED
                    and workflow.status not in TerminalExecutionStatus
                ):
                    await self._fail_workflow_execution(session, workflow_xid)
                if (
                    status == ExecutionStatus.COMPLETED
                    and workflow.status not in TerminalExecutionStatus
                ):
                    snapshot = self._workflow_snapshots[
                        (workflow.workflow_id, workflow.workflow_revision)
                    ]
                    try:
                        definition = snapshot.get_node(node.node_id)
                    except KeyError:
                        definition = None
                    # Validate state writes or the decision branch before scheduling.
                    try:
                        branch = self._apply_node_outputs(
                            workflow,
                            snapshot,
                            definition,
                            result,
                        )
                    except _InvalidNodeOutputs:
                        # Fail the node and workflow; send interrupts after commit.
                        node.set_status(ExecutionStatus.FAILED)
                        await self._fail_workflow_execution(
                            session, workflow_xid
                        )
                    else:
                        if isinstance(definition, DecisionNode):
                            exec_metadata = ExecMetadata.model_validate(
                                node.exec_metadata or {}
                            )
                            exec_metadata.branch = branch
                            node.exec_metadata = exec_metadata.model_dump(
                                mode="json"
                            )
                        successors = self.get_next_node_ids(
                            workflow_xid, node_xid, branch=branch
                        )
                        for node_id, prev_xids in successors.items():
                            await self.create_node_execution(
                                session,
                                workflow_xid,
                                node_id,
                                None,
                                None,
                                prev_xids=prev_xids,
                                status=ExecutionStatus.SUSPENDED,
                            )
            # Clear running-node tracking only after the transaction commits.
            self._remove_running_node(self._executors[executor_id], node_xid)

    @staticmethod
    def _apply_node_outputs(
        workflow: WorkflowExecution,
        snapshot: WorkflowSnapshot,
        node: BaseNode | None,
        result: messages.ExecuteNodeResponse,
    ) -> str | None:
        """Validate `result`'s outputs against `node` and `snapshot`; update `workflow` state.

        Return a decision branch without changing state, or None for other nodes.
        Actions and waits copy only output_states paths, then validate the entire
        state before replacing `workflow`.states. A boundary uses `node=None`.
        Invalid data raises _InvalidNodeOutputs without changing state. Do not commit.
        """
        try:
            outputs = (
                decode_data_map(result.outputs)
                if result.HasField("outputs")
                else {}
            )
            if isinstance(node, DecisionNode):
                branch = outputs["branch"]
                if not isinstance(branch, str) or branch not in [
                    *node.branches,
                    "default",
                ]:
                    raise ValueError("Invalid decision branch.")
                return branch
            paths = (
                node.output_states or []
                if isinstance(node, (ActionNode, WaitNode))
                else []
            )
            states = deepcopy(workflow.states)
            for path in sorted(paths, key=lambda value: value.count(".")):
                source = outputs
                target = states
                keys = path.split(".")
                for index, key in enumerate(keys):
                    if not isinstance(source, dict) or not isinstance(
                        target, dict
                    ):
                        raise ValueError(
                            f"State path {path!r} requires an object."
                        )
                    source = source[key]
                    if index == len(keys) - 1:
                        target[key] = deepcopy(source)
                    else:
                        target = target.setdefault(key, {})
            snapshot.validate_states(states)
        except (
            KeyError,
            TypeError,
            ValueError,
            ValidationError,
            RecursionError,
        ) as error:
            raise _InvalidNodeOutputs(str(error)) from error
        workflow.states = states
        return None

    async def _fail_workflow_execution(
        self, session: AsyncSession, xid: UUID
    ) -> None:
        """Mark workflow `xid` FAILED in `session` without committing.

        Change RUNNING and SUSPENDED execution to INTERRUPTED; preserve other
        statuses. Outer commit removes the workflow cache and queues interrupts
        for assigned executions.
        """
        cache = self._executions[xid]
        workflow = await self._get_workflow_execution(session, xid, cache=cache)
        workflow.set_status(ExecutionStatus.FAILED)
        for node_xid in cache.nodes:
            node = await self._get_node_execution(
                session, xid, node_xid, cache=cache
            )
            if node.status in (
                ExecutionStatus.RUNNING,
                ExecutionStatus.SUSPENDED,
            ):
                node.set_status(ExecutionStatus.INTERRUPTED)

    async def _handle_executor_disconnected(self, executor_id: UUID) -> None:
        """Notify the scheduler when the executor stream ends."""

    async def _receive_executor_messages(
        self,
        connection: _ExecutorConnection,
        requests: AsyncIterator[messages.ExecutorMessage],
    ) -> None:
        """Process `requests` using `connection`'s executor and tracked executions.

        Ignore invalid UUIDs, unassigned results, and results during closure.
        Reject invalid message types, statuses, or output presence with
        _InvalidExecutorMessage. Return on stream completion or shutdown;
        shutdown also marks the `connection` closing. Cancellation waits for
        in-flight result processing before propagating.
        """
        async for request in requests:
            payload = request.WhichOneof("payload")
            if payload == "shutdown":
                connection.closing = True
                return
            if payload != "node_result":
                raise _InvalidExecutorMessage("Expected an executor message.")
            result = request.node_result
            try:
                node_xid = UUID(result.node_xid)
            except ValueError:
                continue
            if (
                connection.closing
                or node_xid not in connection.running_node_xids
            ):
                continue
            if result.status not in (
                messages.FAILED,
                messages.SUSPENDED,
                messages.INTERRUPTED,
                messages.COMPLETED,
            ):
                raise _InvalidExecutorMessage("Invalid node result status.")
            if (
                result.HasField("outputs")
                and result.status != messages.COMPLETED
            ):
                raise _InvalidExecutorMessage("Invalid node result outputs.")
            task = asyncio.create_task(
                self._process_node_result(
                    connection.executor_id, node_xid, result
                )
            )
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Finish processing before the scheduler handles disconnect.
                await task
                raise
            await self._execute_suspended_nodes(connection.executor_id)

    async def _send_executor_messages(
        self, connection: _ExecutorConnection, context: grpc.aio.ServicerContext
    ) -> None:
        """Write `connection`'s queued messages through `context`'s RPC stream.

        gRPC permits only one write at a time. This method serializes writes
        so producers can queue messages without waiting for network I/O.
        Discard ordinary messages while closing. Write shutdown even while
        closing, then return.
        """
        while True:
            message = await connection.queue.get()
            if message.HasField("shutdown"):
                await context.write(message)
                return
            if not connection.closing:
                await context.write(message)

    def _remove_running_node(
        self, connection: _ExecutorConnection, node_xid: UUID
    ) -> None:
        """Remove `node_xid` from global assignments and `connection`'s execution set.

        A missing entry raises KeyError. Do not change execution records or
        send an interrupt.
        """
        del self._running_node_xids[node_xid]
        connection.running_node_xids.remove(node_xid)


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""


class _InvalidNodeOutputs(ValueError):
    """Identify outputs that cannot update workflow state."""
