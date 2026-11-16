import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, UTC
from hashlib import sha256
from hmac import compare_digest
from secrets import token_urlsafe
from typing import AsyncIterator
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
)


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""

    pass


class _WorkflowExecutorConnection:

    def __init__(self, executor_id: UUID, engine: "WorkflowEngine"):
        self._id = executor_id
        self._engine = engine
        self.queue = asyncio.Queue[messages.EngineMessage]()
        self._closing = False

    def close(self):
        """Mark the executor connection as closing."""
        self._closing = True

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
            if message.HasField("shutdown"):  # Engine shutdown
                await context.write(message)
                return
            if not self._closing:
                await context.write(message)


@dataclass
class _CachedWorkflowExecution:
    """Group one workflow execution and its node executions for the engine."""

    workflow: WorkflowExecution
    node_xids: set[UUID] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _CachedNodeExecution:
    """Group one node execution and its workflow execution for the engine."""

    node: NodeExecution


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

    @staticmethod
    def _hash_token(token: str) -> str:
        """Return the SHA-256 hex digest of `token` encoded as UTF-8."""
        return sha256(token.encode()).hexdigest()

    async def _checkout_execution(
        self, session: AsyncSession, cache_map: dict, xid: UUID, *, cache=None
    ):
        if cache is None:
            cache = cache_map[xid]
        execution = cache.workflow
        session.sync_session.expire_on_commit = False
        session.add(execution)
        # self._register_execution_commit_handler(session)
        if object_state(execution).expired:
            await session.refresh(execution)
        return execution

    async def checkout_workflow_execution(
        self,
        session: AsyncSession,
        workflow_xid: UUID,
        *,
        cache: _CachedWorkflowExecution | None = None,
    ) -> WorkflowExecution:
        """Bind and return the shared workflow execution in `session`.

        Use `cache` when supplied; otherwise look it up by `workflow_xid`.
        Refresh expired attributes and keep them loaded after commit. The caller
        owns the transaction; rollback can expire the workflow execution.
        A missing `cache` entry raises KeyError.
        """
        return await self._checkout_execution(
            session, self._workflow_executions, workflow_xid, cache=cache
        )

    async def checkout_node_execution(
        self,
        session: AsyncSession,
        node_xid: UUID,
        *,
        cache: _CachedNodeExecution | None = None,
    ) -> NodeExecution:
        """Bind and return the shared node execution in `session`.

        Use `cache` when supplied; otherwise look it up by `node_xid`.
        Refresh expired attributes and keep them loaded after commit. The caller
        owns the transaction; rollback can expire the node execution. A missing
        workflow execution or node execution raises KeyError.
        """
        return await self._checkout_execution(
            session, self._node_executions, node_xid, cache=cache
        )

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
            del self._executors[executor_id]
