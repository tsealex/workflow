import asyncio
import json
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, UTC
from hashlib import sha256
from hmac import compare_digest
from secrets import token_urlsafe
from typing import AsyncIterator, Collection
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


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""

    pass


class _WorkflowExecutorConnection:

    def __init__(self, executor_id: UUID, engine: "WorkflowEngine"):
        self._id = executor_id
        self._engine = engine
        self.queue = asyncio.Queue[messages.EngineMessage]()
        self.running_node_xids: set[UUID] = set()
        self._closing = False

    def close(self):
        """Mark the executor connection as closing."""
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
                self.close()
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
                node_xid = UUID(message.execute_node.node_xid)
                self.running_node_xids.add(node_xid)
            elif payload == "interrupt_node":
                node_xid = UUID(message.interrupt_node.node_xid)
                self.running_node_xids.discard(node_xid)
            elif payload == "shutdown":
                self.close()
                return


@dataclass
class _CachedWorkflowExecution:
    """Group one workflow execution and its node executions for the engine."""

    execution: WorkflowExecution
    node_xids: set[UUID] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _CachedNodeExecution:
    """Group one node execution and its workflow execution for the engine."""

    execution: NodeExecution


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

    async def _checkout_execution(
        self,
        session: AsyncSession,
        cache_map: dict,
        xid: UUID,
        *,
        cache=None,
        read_only: bool = False,
    ):
        """Return the cached execution without session changes when read-only."""
        if cache is None:
            cache = cache_map[xid]
        execution = cache.execution
        if not read_only:
            session.sync_session.expire_on_commit = False
            session.add(execution)
        if object_state(execution).expired:
            await session.refresh(execution)
        return execution

    async def checkout_workflow_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        *,
        cache: _CachedWorkflowExecution | None = None,
        read_only: bool = False,
    ) -> WorkflowExecution:
        """Return the shared workflow execution, optionally without session changes.

        Use `cache` when supplied; otherwise look it up by `workflow_xid`.
        If `read_only` is true, return the cached workflow execution without
        adding it to the session or refreshing expired attributes.
        Otherwise, bind it to the session, refresh expired attributes, and keep
        them loaded after commit. The caller owns the transaction; rollback can
        expire the workflow execution.
        A missing `cache` entry raises KeyError.
        """
        return await self._checkout_execution(
            session,
            self._workflow_executions,
            workflow_xid,
            cache=cache,
            read_only=read_only,
        )

    async def checkout_node_execution(
        self,
        session: AsyncSession,
        node_xid: UUID,
        *,
        cache: _CachedNodeExecution | None = None,
        read_only: bool = False,
    ) -> NodeExecution:
        """Return the shared node execution, optionally without session changes.

        Use `cache` when supplied; otherwise look it up by `node_xid`.
        If `read_only` is true, return the cached node execution without
        adding it to the session or refreshing expired attributes.
        Otherwise, bind it to the session, refresh expired attributes, and keep
        them loaded after commit. The caller owns the transaction; rollback can
        expire the node execution. A missing cache entry raises KeyError.
        """
        return await self._checkout_execution(
            session,
            self._node_executions,
            node_xid,
            cache=cache,
            read_only=read_only,
        )

    async def _suspend_running_nodes(
        self, session: AsyncSession, node_xids: Collection[UUID]
    ) -> None:
        """Suspend the specified running nodes and update their workflows.

        Save changes through the supplied session. After all nodes are saved,
        suspend affected workflows that have no running nodes. Keep terminal
        node and workflow statuses unchanged.
        """
        session.sync_session.expire_on_commit = False
        workflow_xids = set()
        # Suspend the specified nodes.
        for node_xid in node_xids:
            node_execution = self._node_executions[node_xid].execution
            workflow_cache = self._workflow_executions[
                node_execution.workflow_xid
            ]
            async with workflow_cache.lock:
                session.add(node_execution)
                if object_state(node_execution).expired:
                    await session.refresh(node_execution)
                workflow_xids.add(node_execution.workflow_xid)
                if node_execution.status != ExecutionStatus.RUNNING:
                    continue
                node_execution.set_status(ExecutionStatus.SUSPENDED)
                await session.commit()
                # Remove the node from the running node list and add it to
                # the suspended node list.
                self._suspended_node_xids.add(node_xid)
                self._running_node_xids.pop(node_xid, None)
        # After all nodes are suspended, update the workflows that have no
        # running nodes to SUSPENDED.
        for workflow_xid in workflow_xids:
            workflow_cache = self._workflow_executions[workflow_xid]
            async with workflow_cache.lock:
                workflow_execution = await self.checkout_workflow_execution(
                    session, workflow_xid, cache=workflow_cache
                )
                if workflow_execution.status in TerminalExecutionStatus:
                    continue
                has_running_nodes = (
                    set(self._running_node_xids.keys())
                    & workflow_cache.node_xids
                )
                if (
                    not has_running_nodes
                    and workflow_execution.status != ExecutionStatus.SUSPENDED
                ):
                    workflow_execution.set_status(ExecutionStatus.SUSPENDED)
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
                node_execution = await self.checkout_node_execution(
                    session, node_xid
                )
                if node_execution.status in TerminalExecutionStatus:
                    continue
                if (
                    region_id is not None
                    and node_execution.region_id != region_id
                ):
                    continue
                if node_execution.status == ExecutionStatus.RUNNING:
                    # The engine keeps executor assignments for interrupt requests.
                    executor_id = self._running_node_xids.get(
                        node_xid, node_execution.executor_id
                    )
                    running_nodes.append((node_xid, executor_id))
                node_execution.set_status(ExecutionStatus.INTERRUPTED)
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
            node_xids = set(self._node_executions)

        for node_xid in node_xids:
            node_cache = self._node_executions[node_xid]
            workflow_xid = node_cache.execution.workflow_xid
            workflow_cache = self._workflow_executions.get(workflow_xid)
            if not workflow_cache:
                continue
            # Lock the workflow cache to prevent concurrent modifications
            # while executing the node.
            async with workflow_cache.lock:
                async with self._sessions() as session:
                    # Checkout the workflow and node executions in the session.
                    node_execution = await self.checkout_node_execution(
                        session, node_xid, cache=node_cache
                    )
                    if node_execution.status != ExecutionStatus.SUSPENDED:
                        continue
                    workflow_execution = await self.checkout_workflow_execution(
                        session, workflow_xid, cache=workflow_cache
                    )
                    assert (
                        workflow_execution.status not in TerminalExecutionStatus
                    )
                    snapshot = await self._load_workflow_snapshot(
                        session,
                        workflow_execution.workflow_id,
                        workflow_execution.workflow_revision,
                    )
                    if executor.closing:
                        return
                    # Update the node execution status to RUNNING.
                    node_execution.set_status(ExecutionStatus.RUNNING)
                    session.add(node_execution)
                    # Update the workflow execution status to RUNNING if it
                    # was SUSPENDED.
                    if workflow_execution.status == ExecutionStatus.SUSPENDED:
                        workflow_execution.status = ExecutionStatus.RUNNING
                        session.add(workflow_execution)
                    # Send the execute_node message to the executor.
                    request_node_xid = str(node_xid)
                    request_workflow_xid = str(workflow_xid)
                    inputs = json.dumps(
                        node_execution.inputs, allow_nan=False
                    ).encode()
                    node = snapshot.get_node(node_execution.node_id)
                    request = messages.ExecuteNodeRequest(
                        node_xid=request_node_xid,
                        workflow_xid=request_workflow_xid,
                        inputs=inputs,
                        node=node.model_dump(mode="json"),
                    )
                    request.started_at.FromDatetime(node_execution.started_at)
                    message = messages.EngineMessage(execute_node=request)
                    await executor.queue.put(message)
                    await session.commit()
                    self._suspended_node_xids.discard(node_xid)
                    self._running_node_xids[node_xid] = executor_id

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
        # Update the executor registration's last_connected_at timestamp in
        # the database.
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
        # Create a new executor connection and run its receive and send tasks
        # ∏concurrently.
        executor = _WorkflowExecutorConnection(executor_id, self)
        self._executors[executor_id] = executor
        tasks = (
            asyncio.create_task(executor.loop_recv_messages(request_iterator)),
            asyncio.create_task(executor.loop_send_messages(context)),
        )
        try:
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            # If one task completes, wait for the other to finish and raise
            # any exceptions.
            for task in done:
                task.result()
        finally:
            # Mark the executor connection as closing, cancel both tasks,
            # and wait for them to finish.
            executor.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # Update the nodes that were running on this executor to
            # SUSPENDED, and remove them from the running node cache.
            async with self._sessions() as session:
                await self._suspend_running_nodes(
                    session, executor.running_node_xids
                )
            del self._executors[executor_id]
