import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, UTC
from hashlib import sha256
from hmac import compare_digest
from secrets import token_urlsafe
from typing import AsyncIterator, Collection, Generic, TypeVar
from uuid import UUID, uuid4

import grpc
from sqlalchemy import select, update, tuple_
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm.util import object_state

import execution_pb2 as messages
from execution_pb2_grpc import WorkflowEngineServicer
from models import (
    WorkflowExecutorRegistration,
    WorkflowExecution,
    NodeExecution,
    WorkflowSnapshot,
    ExecutionStatus,
    TerminalExecutionStatus,
)
from nodes import ExecutorNodeTypes


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""

    pass


class _WorkflowExecutorConnection:

    def __init__(self, executor_id: UUID, engine: "WorkflowEngine"):
        self._id = executor_id
        self._engine = engine
        self.lock = asyncio.Lock()
        self.queue = asyncio.Queue[messages.EngineMessage]()
        # self.running_node_xids: set[UUID] = set()
        self._closing = False

    async def close(self):
        """Acquire the executor lock and mark the connection as closing."""
        async with self.lock:
            self._closing = True

    @property
    def closing(self) -> bool:
        """Return True if the executor connection is closing."""
        return self._closing

    async def loop_recv_messages(
        self,
        requests: AsyncIterator[messages.ExecutorMessage],
    ):
        """Pass executor messages to the engine until shutdown or stream closure."""
        async for request in requests:
            with suppress(Exception):
                await self._engine.handle_executor_message(self._id, request)
            if request.HasField("shutdown"):  # Executor shutdown
                # The receive task waits for any locked assignment to finish.
                await self.close()
            if self._closing:  # Executor shutdown
                return

    async def loop_send_messages(self, context: grpc.aio.ServicerContext):
        """Write the executor's queued messages through `context`'s RPC stream.

        gRPC permits only one write at a time. This method serializes writes
        so producers can queue messages without waiting for network I/O.
        Discard ordinary messages while closing. Write shutdown even while
        closing, then return.
        """
        while True:
            message = await self.queue.get()
            payload = message.WhichOneof("payload")
            if self._closing and payload != "shutdown":
                continue
            await context.write(message)
            if payload == "execute_node":
                # node_xid = UUID(message.execute_node.node_xid)
                # self.running_node_xids.add(node_xid)
                pass
            elif payload == "interrupt_node":
                # node_xid = UUID(message.interrupt_node.node_xid)
                # self.running_node_xids.discard(node_xid)
                pass
            elif payload == "shutdown":
                # The send task waits for any locked assignment to finish.
                await self.close()
                return


_Execution = TypeVar("_Execution", WorkflowExecution, NodeExecution)


@dataclass
class _CachedExecution(Generic[_Execution]):
    """Store one execution and provide its shared checkout behavior."""

    _execution: _Execution

    async def checkout_execution(self, session: AsyncSession) -> _Execution:
        """Return the execution and refresh it if its attributes expired.

        Checkout adds the execution to the session and keeps its
        attributes loaded after commit. The caller owns the transaction.
        """
        execution = self._execution
        session.sync_session.expire_on_commit = False
        session.add(execution)
        if object_state(execution).expired:
            await session.refresh(execution)
        return execution


@dataclass
class _CachedWorkflowExecution(_CachedExecution[WorkflowExecution]):
    """Group one workflow execution and its node executions for the engine."""

    node_xids: set[UUID] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _CachedNodeExecution(_CachedExecution[NodeExecution]):
    """Group one node execution and its workflow execution for the engine."""

    workflow_xid: UUID = field(init=False)

    def __post_init__(self) -> None:
        """Save the workflow ID before the node execution can expire."""
        self.workflow_xid = self._execution.workflow_xid


class WorkflowEngine(WorkflowEngineServicer):
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self._sessions = sessions
        # Executor connections by executor UUID.
        self._executors: dict[UUID, _WorkflowExecutorConnection] = {}
        # Workflow execution caches by workflow execution ID.
        self._workflow_executions: dict[UUID, _CachedWorkflowExecution] = {}
        # Node execution caches by node execution ID.
        self._node_executions: dict[UUID, _CachedNodeExecution] = {}
        # Workflow snapshot caches by (workflow ID, reversion).
        self._workflow_snapshots: dict[tuple[UUID, int], WorkflowSnapshot] = {}
        # Suspended node execution IDs.
        self._suspended_node_xids: set[UUID] = set()
        # Running node execution IDs by executor ID.
        self._running_node_xids: dict[UUID, UUID] = {}
        self._started = False

    async def start(self):
        """Recover unfinished executions before the server starts."""
        await self._load_executions()
        self._started = True

    async def stop(self):
        """Stop the workflow engine."""
        await self._notify_engine_shutdown()
        self._started = False

    async def _notify_engine_shutdown(self):
        """Mark all executor connections closing and queue shutdown."""
        for executor in self._executors.values():
            with suppress(Exception):
                await executor.queue.put(
                    messages.EngineMessage(shutdown=messages.EngineShutdown())
                )

    async def _load_executions(self):
        """Restore execution caches using the configured session factory.

        In one transaction, change RUNNING workflows and executions to SUSPENDED.
        After commit, cache suspended workflows, their snapshots, and all their
        executions, including completed executions.
        """
        async with (
            self._sessions(expire_on_commit=False) as session,
            session.begin(),
        ):
            # Mark all (previously) RUNNING workflows and executions as SUSPENDED, so we can
            # resume them.
            for execution in (WorkflowExecution, NodeExecution):
                await session.execute(
                    update(execution)
                    .where(execution.status == ExecutionStatus.RUNNING)
                    .values(status=ExecutionStatus.SUSPENDED)
                )
            # Load all SUSPENDED workflows, their snapshots, and all their
            # executions.
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
            cache = _CachedWorkflowExecution(workflow)
            self._workflow_executions[workflow.xid] = cache
        for node in nodes:
            xid = node.xid
            self._node_executions[xid] = _CachedNodeExecution(node)
            self._workflow_executions[node.workflow_xid].node_xids.add(xid)
            assert node.status != ExecutionStatus.RUNNING
            if node.status == ExecutionStatus.SUSPENDED:
                self._suspended_node_xids.add(xid)

    async def _load_workflow_snapshot(
        self,
        session: AsyncSession,
        workflow_id: UUID,
        revision: int,
    ) -> WorkflowSnapshot:
        """Return the cached snapshot or load it from the database.

        Detach a loaded snapshot so the session cannot expire its cached values.
        Raise KeyError if the workflow revision has no snapshot.
        """
        snapshot_id = workflow_id, revision
        snapshot = self._workflow_snapshots.get(snapshot_id)
        if snapshot is None:
            snapshot = await session.get(WorkflowSnapshot, snapshot_id)
            if snapshot is None:
                raise KeyError(snapshot_id)
            session.expunge(snapshot)
            self._workflow_snapshots[snapshot_id] = snapshot
        return snapshot

    @staticmethod
    def _hash_token(token: str) -> str:
        """Return the SHA-256 hex digest of `token` encoded as UTF-8."""
        return sha256(token.encode()).hexdigest()

    async def _checkout_workflow_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        *,
        cache: _CachedWorkflowExecution | None = None,
    ) -> WorkflowExecution:
        """Add the shared workflow execution to the session and return it.

        Use `cache` when supplied; otherwise look it up by `workflow_xid`.
        Checkout refreshes expired attributes and keeps them loaded after
        commit. The caller owns the transaction; rollback can
        expire the workflow execution.
        A missing `cache` entry raises KeyError.
        """
        # The workflow cache owns checkout for its workflow execution.
        if cache is None:
            cache = self._workflow_executions[workflow_xid]
        return await cache.checkout_execution(session)

    async def _checkout_node_execution(
        self,
        session: AsyncSession,
        node_xid: UUID,
        *,
        cache: _CachedNodeExecution | None = None,
    ) -> NodeExecution:
        """Add the shared node execution to the session and return it.

        Use `cache` when supplied; otherwise look it up by `node_xid`.
        Checkout refreshes expired attributes and keeps them loaded after
        commit. The caller owns the transaction; rollback can
        expire the node execution. A missing cache entry raises KeyError.
        """
        # The node cache owns checkout for its node execution.
        if cache is None:
            cache = self._node_executions[node_xid]
        return await cache.checkout_execution(session)

    async def _suspend_running_nodes(self, node_xids: Collection[UUID]) -> None:
        """Suspend the specified running nodes and update their workflows.

        Process each workflow under one lock and in one session. After its
        nodes are saved, suspend the workflow if it has no running nodes.
        Keep terminal node and workflow statuses unchanged.
        """
        # Group the node execution IDs by workflow execution ID.
        workflow_node_xids: dict[UUID, set[UUID]] = {}
        for node_xid in node_xids:
            node_cache = self._node_executions[node_xid]
            workflow_xid = node_cache.workflow_xid
            if workflow_xid not in workflow_node_xids:
                workflow_node_xids[workflow_xid] = set()
            workflow_node_xids[workflow_xid].add(node_xid)

        for workflow_xid, node_xids in workflow_node_xids.items():
            workflow_cache = self._workflow_executions[workflow_xid]
            async with workflow_cache.lock, self._sessions() as session:
                # Suspend the specified nodes in this workflow.
                for node_xid in node_xids:
                    node_exec = await self._checkout_node_execution(
                        session, node_xid
                    )
                    if node_exec.status != ExecutionStatus.RUNNING:
                        continue
                    node_exec.set_status(ExecutionStatus.SUSPENDED)
                    await session.commit()
                    # Remove the node from the running node list and add it
                    # to the suspended node list.
                    self._suspended_node_xids.add(node_xid)
                    self._running_node_xids.pop(node_xid, None)

                # Suspend the workflow if it has no running nodes.
                workflow_exec = await self._checkout_workflow_execution(
                    session, workflow_xid, cache=workflow_cache
                )
                if workflow_exec.status in TerminalExecutionStatus:
                    continue
                has_running_nodes = (
                    set(self._running_node_xids.keys())
                    & workflow_cache.node_xids
                )
                if (
                    not has_running_nodes
                    and workflow_exec.status != ExecutionStatus.SUSPENDED
                ):
                    workflow_exec.set_status(ExecutionStatus.SUSPENDED)
                    await session.commit()

    async def _interrupt_nodes(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        region_id: UUID | None = None,
    ) -> None:
        """Interrupt unfinished cached node executions of the workflow.

        If region_id is supplied, select nodes with that region ID. Save all
        selected nodes before sending interrupt requests to their executors.
        Keep terminal node statuses and the workflow status unchanged.
        """
        workflow_cache = self._workflow_executions[workflow_xid]
        async with workflow_cache.lock:
            interrupted_node_xids = []
            running_nodes = []
            # The workflow cache identifies its node executions without a query.
            for node_xid in workflow_cache.node_xids:
                node_exec = await self._checkout_node_execution(
                    session, node_xid
                )
                if node_exec.status in TerminalExecutionStatus:
                    continue
                if region_id is not None and node_exec.region_id != region_id:
                    continue
                if node_exec.status == ExecutionStatus.RUNNING:
                    # The engine keeps executor assignments for interrupt requests.
                    executor_id = self._running_node_xids.get(
                        node_xid, node_exec.executor_id
                    )
                    running_nodes.append((node_xid, executor_id))
                node_exec.set_status(ExecutionStatus.INTERRUPTED)
                interrupted_node_xids.append(node_xid)

            if not interrupted_node_xids:
                return
            # The engine saves interruption statuses before it notifies executors.
            await session.commit()
            # The engine stops tracking interrupted nodes as running or suspended.
            for node_xid in interrupted_node_xids:
                self._running_node_xids.pop(node_xid, None)
                self._suspended_node_xids.discard(node_xid)
            # The engine requests a stop only for nodes that were running.
            for node_xid, executor_id in running_nodes:
                executor = self._executors.get(executor_id)
                if executor is None or executor.closing:
                    continue
                request_node_xid = str(node_xid)
                request = messages.InterruptNodeRequest(
                    node_xid=request_node_xid
                )
                message = messages.EngineMessage(interrupt_node=request)
                await executor.queue.put(message)

    async def _execute_suspended_nodes(
        self, executor_id: UUID, *, node_xids: set[UUID] | None = None
    ):
        executor = self._executors[executor_id]
        if not executor or executor.closing:
            return
        if node_xids is None:
            node_xids = set(self._suspended_node_xids)

        for node_xid in node_xids:
            node_cache = self._node_executions[node_xid]
            # The cache retains the workflow ID when node attributes expire.
            workflow_xid = node_cache.workflow_xid
            workflow_cache = self._workflow_executions.get(workflow_xid)
            if not workflow_cache:
                continue
            # Lock the workflow cache to prevent concurrent modifications
            # while executing the node.
            async with workflow_cache.lock, self._sessions() as session:
                # Checkout the workflow and node executions in the session.
                node_exec = await self._checkout_node_execution(
                    session, node_xid, cache=node_cache
                )
                if node_exec.status != ExecutionStatus.SUSPENDED:
                    continue
                workflow_exec = await self._checkout_workflow_execution(
                    session, workflow_xid, cache=workflow_cache
                )
                assert workflow_exec.status not in TerminalExecutionStatus
                snapshot = await self._load_workflow_snapshot(
                    session,
                    workflow_exec.workflow_id,
                    workflow_exec.workflow_revision,
                )
                node = snapshot.get_node(node_exec.node_id)
                # Reject node types that cannot be executed by the executor
                # e.g. wait, signal, etc.
                if node.type not in ExecutorNodeTypes:
                    # TODO: Should execute them here in the engine instead
                    continue
                # Prepare the execute_node message to send to the executor.
                request_node_xid = str(node_xid)
                request_workflow_xid = str(workflow_xid)
                inputs = (
                    json.dumps(node_exec.inputs, allow_nan=False).encode()
                    if node_exec.inputs is not None
                    else None
                )
                request = messages.ExecuteNodeRequest(
                    node_xid=request_node_xid,
                    workflow_xid=request_workflow_xid,
                    inputs=inputs,
                    node=node.model_dump(mode="json"),
                )
                request.started_at.FromDatetime(node_exec.started_at)
                message = messages.EngineMessage(execute_node=request)
                # The engine saves the node execution before it queues the
                # request.
                async with executor.lock:
                    if executor.closing:
                        return
                    # Update the node execution status to RUNNING.
                    node_exec.executor_id = executor_id
                    node_exec.set_status(ExecutionStatus.RUNNING)
                    session.add(node_exec)
                    # Update the workflow execution status to RUNNING if it
                    # was SUSPENDED.
                    if workflow_exec.status == ExecutionStatus.SUSPENDED:
                        workflow_exec.status = ExecutionStatus.RUNNING
                        session.add(workflow_exec)
                    await session.commit()
                    self._suspended_node_xids.discard(node_xid)
                    self._running_node_xids[node_xid] = executor_id
                    executor.queue.put_nowait(message)

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
            async with self._sessions() as session:
                executor_id = await session.scalar(
                    select(WorkflowExecutorRegistration.id).where(
                        WorkflowExecutorRegistration.token_hash
                        == self._hash_token(token)
                    )
                )
        if executor_id is None:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
            )
        assert executor_id is not None
        return executor_id

    async def handle_executor_message(
        self, executor_id: UUID, message: messages.ExecutorMessage
    ):
        """Mark the executor connection as closing when it sends shutdown."""
        payload = message.WhichOneof("payload")
        if payload == "node_result":
            pass

    async def register_executor(
        self,
        request: messages.RegisterExecutorRequest,
        context: messages.RegisterExecutorResponse,
    ):
        """Create an executor or verify credentials for a disconnected one."""
        has_id = request.HasField("executor_id")
        has_token = request.HasField("token")
        if has_id != has_token:
            await context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Provide both the executor ID and token, or neither.",
            )
        async with self._sessions() as session:
            if not has_id:
                # Create a new executor registration with a random UUID if
                # executor ID and token are not provided. Return the new ID and
                # token to the caller.
                executor_id = uuid4()
                token = token_urlsafe(32)
                executor_reg = WorkflowExecutorRegistration(
                    id=executor_id,
                    token_hash=self._hash_token(token),
                    created_at=datetime.now(UTC),
                )
                session.add(executor_reg)
                await session.commit()
            else:
                # Verify the provided executor ID and token against the database.
                try:
                    executor_id = UUID(request.executor_id)
                except ValueError:
                    await context.abort(
                        grpc.StatusCode.INVALID_ARGUMENT, "Invalid executor ID."
                    )
                    raise
                # Verify the executor registration and token hash
                executor_reg = await session.get(
                    WorkflowExecutorRegistration, executor_id
                )
                token = request.token
                if not executor_reg or not compare_digest(
                    executor_reg.token_hash, self._hash_token(token)
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
        # Return the executor ID and token to the caller. The caller must
        # store the token securely, as it is not retrievable from the database.
        return messages.RegisterExecutorResponse(
            executor_id=str(executor_id), token=token
        )

    async def connect_executor(
        self,
        request_iterator: AsyncIterator[messages.ExecutorMessage],
        context: grpc.aio.ServicerContext,
    ):
        """Authenticate an executor and run its message tasks until either stops.

        For engine shutdown, an EngineShutdown message must be queued on
        executor.queue. The send task writes that message to the executor
        and returns.

        For executor shutdown, the executor sends an ExecutorShutdown message.
        The message handler calls executor.close() to mark it as closing,
        and the receive task returns.

        When either task stops, this method marks the executor as closing,
        cancels and waits for both tasks, and removes the executor from
        self._executors. The RPC ends when this method returns. The stored
        executor registration remains in the database.
        """
        executor_id = await self._authenticate_executor(context)
        if executor_id in self._executors:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS, "Executor already connected."
            )
        # Register the connection before database waits allow another call.
        executor = _WorkflowExecutorConnection(executor_id, self)
        self._executors[executor_id] = executor
        # Start the receive and send tasks for the executor.
        tasks = (
            asyncio.create_task(executor.loop_recv_messages(request_iterator)),
            asyncio.create_task(executor.loop_send_messages(context)),
        )
        try:
            # Save the connection time before the message tasks start.
            async with self._sessions() as session:
                executor_reg = await session.get(
                    WorkflowExecutorRegistration, executor_id
                )
                if not executor_reg:
                    await context.abort(
                        grpc.StatusCode.UNAUTHENTICATED, "Unknown executor."
                    )
                executor_reg.last_connected_at = datetime.now(UTC)
                session.add(executor_reg)
                await session.commit()
            # If one task completes, wait for the other to finish and raise
            # any exceptions.
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
        finally:
            # Cleanup waits for the executor lock before it cancels both tasks.
            await executor.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # Update the nodes that were running on this executor to
            # SUSPENDED, and remove them from the running node cache.
            running_node_xids = [
                node_xid
                for node_xid, assigner_id in self._running_node_xids.items()
                if assigner_id == executor_id
            ]
            with suppress(Exception):
                await self._suspend_running_nodes(running_node_xids)
            del self._executors[executor_id]
