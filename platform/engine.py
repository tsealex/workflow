import asyncio
from collections.abc import AsyncIterator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime
from hashlib import sha256
from secrets import compare_digest, token_urlsafe
from uuid import UUID, uuid4

import grpc
from sqlalchemy import event, inspect, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session, SessionTransaction

import execution_pb2 as messages
from execution_pb2_grpc import WorkflowEngineServicer
from models import (
    ExecutionStatus,
    NodeExecution,
    Workflow,
    WorkflowExecution,
    WorkflowExecutorRegistration,
    WorkflowSnapshot,
    WorkflowSnapshotID,
)
from nodes import DataMap, decode_data_map

_TERMINAL_EXECUTION_STATUSES = frozenset(
    (
        ExecutionStatus.COMPLETED,
        ExecutionStatus.FAILED,
        ExecutionStatus.INTERRUPTED,
    )
)


@dataclass
class _ExecutionCacheChanges:
    """Hold cache changes for one transaction or savepoint."""

    # New visits by workflow and node execution ID.
    nodes: dict[tuple[UUID, UUID], NodeExecution] = field(default_factory=dict)
    # Final requested statuses by workflow and optional node execution ID.
    statuses: dict[tuple[UUID, UUID | None], ExecutionStatus] = field(
        default_factory=dict
    )


@dataclass
class WorkflowExecutionCache:
    """Group the records that the engine keeps for one workflow execution."""

    # The workflow execution record shared by all visits in this cache.
    workflow: WorkflowExecution
    # Node visits by execution ID. Startup loads suspended visits.
    nodes: dict[UUID, NodeExecution] = field(default_factory=dict)


@dataclass
class _ExecutorConnection:
    """Track outgoing messages and the executor's active node visit."""

    outgoing: asyncio.Queue[messages.EngineMessage] = field(
        default_factory=asyncio.Queue
    )
    node_xid: UUID | None = None
    closing: bool = False


class WorkflowEngine(WorkflowEngineServicer):
    """Coordinate node dispatch and result commits over gRPC."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        """Initialize the engine with the factory for its database sessions."""
        self._sessions = sessions
        # The engine owns one cache per workflow execution ID.
        self._executions: dict[UUID, WorkflowExecutionCache] = {}
        # The engine keeps one snapshot per workflow ID and revision.
        self._workflow_snapshots: dict[WorkflowSnapshotID, WorkflowSnapshot] = (
            {}
        )
        # Executor ID to connection mapping for active executors.
        self._executors: dict[UUID, _ExecutorConnection] = {}
        self._assigned_nodes: set[UUID] = set()

    async def start(self) -> None:
        """Recover unfinished executions before the server starts."""
        await self._load_executions()

    async def create_workflow_execution(
        self, workflow_id: UUID, inputs: DataMap
    ) -> UUID:
        """Create a running execution of the current workflow revision.

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

            snapshot = await session.get(
                WorkflowSnapshot,
                (workflow_id, workflow.revision),
                with_for_update=True,
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
            await session.flush()
            # The execution insert trigger updates the snapshot reference count.
            await session.refresh(snapshot, attribute_names=["ref_count"])

        self._workflow_snapshots[(snapshot.workflow_id, snapshot.revision)] = (
            snapshot
        )
        self._cache_workflow_execution(execution)
        return xid

    async def create_node_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        node_id: UUID,
        executor_id: UUID | None,
        inputs: DataMap,
    ) -> UUID:
        """Add a running node visit to the caller's transaction.

        Raise KeyError if the workflow execution is absent from the cache.
        Copy the inputs and return the visit ID. The cache receives the visit
        after the caller commits.
        """
        if workflow_xid not in self._executions:
            raise KeyError(workflow_xid)

        xid = uuid4()
        node = NodeExecution(
            xid=xid,
            workflow_xid=workflow_xid,
            node_id=node_id,
            executor_id=executor_id,
            status=ExecutionStatus.RUNNING,
            started_at=datetime.now(UTC),
            inputs=deepcopy(inputs),
        )
        session.sync_session.expire_on_commit = False
        session.add(node)
        changes = self._stage_execution_cache_changes(session)
        changes.nodes[(workflow_xid, xid)] = node
        return xid

    async def update_node_execution_status(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        node_xid: UUID,
        status: ExecutionStatus,
    ) -> None:
        """Set the node visit status and end time in the caller's transaction.

        Raise KeyError if the visit is absent. Remove a terminal visit from
        the cache after the caller commits.
        """
        node = await self._get_node_execution(session, workflow_xid, node_xid)
        self._set_execution_status(node, status)
        changes = self._stage_execution_cache_changes(session)
        changes.statuses[(workflow_xid, node_xid)] = status

    async def update_workflow_execution_status(
        self, session: AsyncSession, workflow_xid: UUID, status: ExecutionStatus
    ) -> None:
        """Set workflow status and end time in the caller's transaction.

        Raise KeyError if the workflow execution is absent. Remove the cache
        for a terminal workflow execution after the caller commits.
        """
        workflow = await self._get_workflow_execution(session, workflow_xid)
        self._set_execution_status(workflow, status)
        changes = self._stage_execution_cache_changes(session)
        changes.statuses[(workflow_xid, None)] = status

    @staticmethod
    def _set_execution_status(
        execution: NodeExecution | WorkflowExecution, status: ExecutionStatus
    ) -> None:
        """Set the status and keep the first end time until execution resumes."""
        execution.status = status
        if status in _TERMINAL_EXECUTION_STATUSES:
            if execution.ended_at is None:
                execution.ended_at = datetime.now(UTC)
        else:
            execution.ended_at = None

    def _stage_execution_cache_changes(
        self, session: AsyncSession
    ) -> _ExecutionCacheChanges:
        """Get the cache changes for the caller's open transaction or savepoint."""
        # SQLAlchemy sends AsyncSession events through its synchronous session.
        sync_session = session.sync_session
        pending = session.info.get(self)
        if pending is None:
            pending = session.info[self] = {}

            def apply_on_commit(committing_session: Session) -> None:
                transaction = (
                    committing_session.get_nested_transaction()
                    or committing_session.get_transaction()
                )
                assert transaction is not None
                changes = pending.pop(transaction, None)
                if changes is None:
                    return
                if transaction.parent is None:
                    self._apply_execution_cache_changes(changes)
                else:
                    # A savepoint release leaves its changes in the parent.
                    parent = pending.setdefault(
                        transaction.parent, _ExecutionCacheChanges()
                    )
                    parent.nodes.update(changes.nodes)
                    parent.statuses.update(changes.statuses)

            def discard_on_end(
                _session: Session, transaction: SessionTransaction
            ) -> None:
                # A rollback ends the transaction without a commit event.
                pending.pop(transaction, None)

            event.listen(sync_session, "after_commit", apply_on_commit)
            event.listen(sync_session, "after_transaction_end", discard_on_end)

        transaction = (
            sync_session.get_nested_transaction()
            or sync_session.get_transaction()
        )
        assert transaction is not None
        return pending.setdefault(transaction, _ExecutionCacheChanges())

    def _apply_execution_cache_changes(
        self, changes: _ExecutionCacheChanges
    ) -> None:
        """Add unfinished new visits and remove terminal records after commit."""
        for (workflow_xid, node_xid), status in changes.statuses.items():
            if status not in _TERMINAL_EXECUTION_STATUSES:
                continue
            if node_xid is None:
                self._executions.pop(workflow_xid, None)
            elif (cache := self._executions.get(workflow_xid)) is not None:
                cache.nodes.pop(node_xid, None)

        for (workflow_xid, node_xid), node in changes.nodes.items():
            status = changes.statuses.get((workflow_xid, node_xid))
            if (
                workflow_xid in self._executions
                and status not in _TERMINAL_EXECUTION_STATUSES
            ):
                self._cache_node_execution(workflow_xid, node_xid, node)

    async def _load_executions(self) -> None:
        """Recover unfinished executions in one database transaction.

        Mark running workflows and node visits as suspended. Load suspended
        workflows, their snapshots, and their suspended node visits. Add
        these records to the caches after the transaction commits and the
        session closes.
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
                        NodeExecution.status == ExecutionStatus.SUSPENDED,
                        WorkflowExecution.status == ExecutionStatus.SUSPENDED,
                    )
                )
            ).all()

        self._workflow_snapshots.update(
            ((snapshot.workflow_id, snapshot.revision), snapshot)
            for snapshot in snapshots
        )
        for workflow in workflows:
            self._cache_workflow_execution(workflow)
        for node in nodes:
            self._cache_node_execution(node.workflow_xid, node.xid, node)

    def _cache_workflow_execution(
        self, workflow: WorkflowExecution
    ) -> WorkflowExecutionCache:
        """Create and return the cache for a workflow execution.

        Store the given record with an empty map of node visits. Replace any
        cache for the same workflow execution ID.
        """
        cache = WorkflowExecutionCache(workflow)
        self._executions[workflow.xid] = cache
        return cache

    def _cache_node_execution(
        self, workflow_xid: UUID, node_xid: UUID, node: NodeExecution
    ) -> None:
        """Add a new node visit to the cache for its workflow execution.

        Raise KeyError if the workflow execution is absent from the cache.
        The caller supplies the IDs in case a rollback expired the visit's
        attributes.
        """
        self._executions[workflow_xid].nodes[node_xid] = node

    async def _get_workflow_execution(
        self, session: AsyncSession, workflow_xid: UUID
    ) -> WorkflowExecution:
        """Attach the cached workflow record to the session and return it.

        Raise KeyError if the cache has no entry for the execution ID.
        Refresh the record if a rollback expired its attributes. Disable
        expiration on commit to keep the record readable after commit.
        The caller controls the transaction.
        """
        workflow = self._executions[workflow_xid].workflow
        session.sync_session.expire_on_commit = False
        session.add(workflow)
        if inspect(workflow).expired:
            await session.refresh(workflow)
        return workflow

    async def _get_node_execution(
        self, session: AsyncSession, workflow_xid: UUID, node_xid: UUID
    ) -> NodeExecution:
        """Attach the cached node visit to the session and return it.

        Raise KeyError if the workflow execution or node visit is absent
        from the cache. Refresh the visit if a rollback expired its
        attributes. Disable expiration on commit to keep the visit readable
        after commit. The caller controls the transaction.
        """
        cache = self._executions[workflow_xid]
        pending = session.info.get(self, {})
        transaction = (
            session.sync_session.get_nested_transaction()
            or session.sync_session.get_transaction()
        )
        while transaction is not None:
            changes = pending.get(transaction)
            if changes is not None:
                node = changes.nodes.get((workflow_xid, node_xid))
                if node is not None:
                    if inspect(node).expired:
                        await session.refresh(node)
                    return node
            transaction = transaction.parent

        node = cache.nodes[node_xid]
        session.sync_session.expire_on_commit = False
        session.add(node)
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
        """Commit a new executor registration with a hash of its token."""
        async with self._sessions.begin() as session:
            session.add(
                WorkflowExecutorRegistration(
                    id=executor_id,
                    token_hash=self._hash_token(token),
                    created_at=datetime.now(UTC),
                )
            )

    async def _get_token_hash(self, executor_id: UUID) -> str | None:
        """Get the token hash for a registered executor, if it exists."""
        async with self._sessions() as session:
            return await session.scalar(
                select(WorkflowExecutorRegistration.token_hash).where(
                    WorkflowExecutorRegistration.id == executor_id
                )
            )

    async def _find_executor(self, token_hash: str) -> UUID | None:
        """Get the executor ID for a stored token hash, if it exists."""
        async with self._sessions() as session:
            return await session.scalar(
                select(WorkflowExecutorRegistration.id).where(
                    WorkflowExecutorRegistration.token_hash == token_hash
                )
            )

    async def _record_connection(
        self, executor_id: UUID, connected_at: datetime
    ) -> bool:
        """Commit the connection time and report if the executor exists."""
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
        return sha256(token.encode()).hexdigest()

    async def _authenticate_executor(
        self, context: grpc.aio.ServicerContext
    ) -> UUID:
        """Get the authenticated executor ID or abort the RPC."""
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
        """Exchange messages with an authenticated executor."""
        executor_id = await self._authenticate_executor(context)
        if executor_id in self._executors:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS, "Executor already connected."
            )

        connection = self._executors[executor_id] = _ExecutorConnection()
        connected = False
        try:
            record = asyncio.create_task(
                self._record_connection(executor_id, datetime.now(UTC))
            )
            try:
                accepted = await asyncio.shield(record)
            except asyncio.CancelledError:
                await record
                raise
            if not accepted:
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
                )
            connected = True
            await self.executor_available(executor_id)
            receive = asyncio.create_task(
                self._receive_executor_messages(
                    executor_id, connection, request_iterator
                )
            )
            send = asyncio.create_task(
                self._send_executor_messages(connection, context)
            )
            try:
                finished, _ = await asyncio.wait(
                    (receive, send), return_when=asyncio.FIRST_COMPLETED
                )
                for task in finished:
                    task.result()
            except _InvalidExecutorMessage as error:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, str(error)
                )
            finally:
                receive.cancel()
                send.cancel()
                await asyncio.gather(receive, send, return_exceptions=True)
        finally:
            connection.closing = True
            self._release_assignment(connection)
            try:
                if connected:
                    await self.executor_disconnected(executor_id)
            finally:
                del self._executors[executor_id]

    def execute_node(
        self, executor_id: UUID, request: messages.ExecuteNodeRequest
    ) -> None:
        """Send a recorded visit to an idle executor."""
        connection = self._executors[executor_id]
        if connection.closing or connection.node_xid is not None:
            raise RuntimeError("Executor is closing or busy.")
        node_xid = UUID(request.node_xid)
        if node_xid in self._assigned_nodes:
            raise RuntimeError("Node visit already has an assigned executor.")
        message = messages.EngineMessage(execute_node=request)
        message.execute_node.node_xid = str(node_xid)
        self._assigned_nodes.add(node_xid)
        connection.node_xid = node_xid
        connection.outgoing.put_nowait(message)

    def interrupt_node(self, executor_id: UUID, node_xid: UUID | str) -> None:
        """Ask the assigned executor to stop a visit."""
        if isinstance(node_xid, str):
            try:
                node_xid = UUID(node_xid)
            except ValueError:
                return
        connection = self._executors.get(executor_id)
        if (
            connection is not None
            and not connection.closing
            and connection.node_xid == node_xid
        ):
            connection.outgoing.put_nowait(
                messages.EngineMessage(
                    interrupt_node=messages.InterruptNodeRequest(
                        node_xid=str(node_xid)
                    )
                )
            )

    def shutdown_executor(self, executor_id: UUID) -> None:
        """Send shutdown and end the executor stream."""
        connection = self._executors.get(executor_id)
        if connection is not None and not connection.closing:
            connection.closing = True
            connection.outgoing.put_nowait(
                messages.EngineMessage(shutdown=messages.EngineShutdown())
            )

    async def executor_available(self, executor_id: UUID) -> None:
        """Notify the scheduler when the executor can accept a visit."""

    async def commit_node_result(
        self, executor_id: UUID, result: messages.ExecuteNodeResponse
    ) -> None:
        """Validate and commit a result before the next dispatch."""
        raise NotImplementedError

    async def executor_disconnected(self, executor_id: UUID) -> None:
        """Notify the scheduler when the executor stream ends."""

    async def _receive_executor_messages(
        self,
        executor_id: UUID,
        connection: _ExecutorConnection,
        requests: AsyncIterator[messages.ExecutorMessage],
    ) -> None:
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
            if connection.closing or node_xid != connection.node_xid:
                continue
            if result.status not in (
                messages.FAILED,
                messages.SUSPENDED,
                messages.INTERRUPTED,
                messages.COMPLETED,
            ):
                raise _InvalidExecutorMessage("Invalid node result status.")
            if result.HasField("outputs") and (
                result.status != messages.COMPLETED
                or not self._has_valid_outputs(result.outputs)
            ):
                raise _InvalidExecutorMessage("Invalid node result outputs.")
            result.node_xid = str(node_xid)
            commit = asyncio.create_task(
                self.commit_node_result(executor_id, result)
            )
            try:
                await asyncio.shield(commit)
            except asyncio.CancelledError:
                # Finish the commit before the scheduler handles disconnect.
                await commit
                raise
            self._release_assignment(connection)
            if not connection.closing:
                await self.executor_available(executor_id)

    @staticmethod
    def _has_valid_outputs(outputs: bytes) -> bool:
        try:
            return bool(decode_data_map(outputs))
        except (ValueError, RecursionError):
            return False

    async def _send_executor_messages(
        self, connection: _ExecutorConnection, context: grpc.aio.ServicerContext
    ) -> None:
        while True:
            message = await connection.outgoing.get()
            if message.HasField("shutdown"):
                await context.write(message)
                return
            if not connection.closing:
                await context.write(message)

    def _release_assignment(self, connection: _ExecutorConnection) -> None:
        if connection.node_xid is not None:
            self._assigned_nodes.remove(connection.node_xid)
            connection.node_xid = None


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""
