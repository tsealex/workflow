import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from hashlib import sha256
from secrets import compare_digest, token_urlsafe
from uuid import UUID, uuid4

import grpc
from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    String,
    column,
    select,
)
from sqlalchemy.orm import (
    Mapped,
    Session,
    mapped_column,
    sessionmaker,
    validates,
)

import execution_pb2 as messages
from database import Base
from execution_pb2_grpc import WorkflowEngineServicer
from nodes import decode_data_map


class ExecutionStatus(StrEnum):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUSPENDED = "SUSPENDED"
    INTERRUPTED = "INTERRUPTED"
    COMPLETED = "COMPLETED"


execution_status_type = Enum(
    ExecutionStatus,
    name=None,
    native_enum=False,
    create_constraint=True,
    validate_strings=True,
)


class WorkflowSnapshot(Base):
    __tablename__ = "workflow_snapshot"
    __table_args__ = (CheckConstraint(column("ref_count") >= 0),)

    workflow_id: Mapped[UUID] = mapped_column(primary_key=True)
    revision: Mapped[int] = mapped_column(primary_key=True)
    root: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    states: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    ref_count: Mapped[int] = mapped_column(default=0, server_default="0")


class WorkflowExecution(Base):
    __tablename__ = "workflow_execution"
    __table_args__ = (
        CheckConstraint(
            "status <> 'INTERRUPTED'",
            name="workflow_execution_status_check1",
        ),
    )

    xid: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_id: Mapped[UUID]
    workflow_revision: Mapped[int]
    workflow_snapshot: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_type)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    states: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    @validates("status")
    def validate_status(
        self, key: str, value: ExecutionStatus | str
    ) -> ExecutionStatus:
        """Convert the status to ExecutionStatus and reject INTERRUPTED."""
        status = ExecutionStatus(value)
        if status == ExecutionStatus.INTERRUPTED:
            raise ValueError("INTERRUPTED applies only to node executions.")
        return status


class NodeExecution(Base):
    __tablename__ = "node_execution"

    xid: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_xid: Mapped[UUID] = mapped_column(
        ForeignKey(WorkflowExecution.xid)
    )
    node_id: Mapped[UUID]
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_type)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    inputs: Mapped[dict | None] = mapped_column(JSON(none_as_null=True))
    executor_id: Mapped[UUID | None]
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# SQLAlchemy reserves the metadata name during declarative class setup.
NodeExecution.metadata = mapped_column(JSON(none_as_null=True), nullable=True)


class WorkflowExecutorRegistration(Base):
    """Store an executor identity and the hash of its token."""

    __tablename__ = "workflow_executor"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_connected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )


@dataclass
class _ExecutorConnection:
    """Track the outgoing messages and the active attempt for an executor."""

    outgoing: asyncio.Queue[messages.EngineMessage] = field(
        default_factory=asyncio.Queue
    )
    node_xid: str | None = None
    attempt_id: str | None = None
    closing: bool = False


class WorkflowEngine(WorkflowEngineServicer):
    """Coordinate node dispatch and result commits over gRPC."""

    def __init__(self, sessions: sessionmaker[Session]) -> None:
        self._sessions = sessions
        self._executors: dict[str, _ExecutorConnection] = {}
        self._assigned_nodes: set[str] = set()

    async def RegisterExecutor(
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
            await asyncio.to_thread(
                self._create_registration, executor_id, token
            )
        else:
            try:
                executor_id = UUID(request.executor_id)
            except ValueError:
                await context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT, "Invalid executor ID."
                )
            token = request.token
            token_hash = await asyncio.to_thread(
                self._get_token_hash, executor_id
            )
            if token_hash is None or not compare_digest(
                token_hash, self._hash_token(token)
            ):
                await context.abort(
                    grpc.StatusCode.UNAUTHENTICATED,
                    "Invalid executor credentials.",
                )
            if str(executor_id) in self._executors:
                await context.abort(
                    grpc.StatusCode.ALREADY_EXISTS,
                    "Executor already connected.",
                )
        return messages.RegisterExecutorResponse(
            executor_id=str(executor_id), token=token
        )

    def _create_registration(self, executor_id: UUID, token: str) -> None:
        with self._sessions.begin() as session:
            session.add(
                WorkflowExecutorRegistration(
                    id=executor_id,
                    token_hash=self._hash_token(token),
                    created_at=datetime.now(UTC),
                )
            )

    def _get_token_hash(self, executor_id: UUID) -> str | None:
        with self._sessions() as session:
            return session.scalar(
                select(WorkflowExecutorRegistration.token_hash).where(
                    WorkflowExecutorRegistration.id == executor_id
                )
            )

    def _find_executor(self, token_hash: str) -> UUID | None:
        with self._sessions() as session:
            return session.scalar(
                select(WorkflowExecutorRegistration.id).where(
                    WorkflowExecutorRegistration.token_hash == token_hash
                )
            )

    def _record_connection(
        self, executor_id: UUID, connected_at: datetime
    ) -> bool:
        with self._sessions.begin() as session:
            registration = session.get(
                WorkflowExecutorRegistration, executor_id
            )
            if registration is None:
                return False
            registration.last_connected_at = connected_at
        return True

    @staticmethod
    def _hash_token(token: str) -> str:
        return sha256(token.encode()).hexdigest()

    async def ConnectExecutor(
        self,
        request_iterator: AsyncIterator[messages.ExecutorMessage],
        context: grpc.aio.ServicerContext,
    ) -> None:
        """Exchange messages with an authenticated executor."""
        authorization = [
            value
            for key, value in context.invocation_metadata()
            if key == "authorization"
        ]
        registered_id = None
        if len(authorization) == 1 and authorization[0].startswith("Bearer "):
            token = authorization[0].removeprefix("Bearer ")
            registered_id = await asyncio.to_thread(
                self._find_executor, self._hash_token(token)
            )
        if registered_id is None:
            await context.abort(
                grpc.StatusCode.UNAUTHENTICATED, "Invalid token."
            )
        executor_id = str(registered_id)
        if executor_id in self._executors:
            await context.abort(
                grpc.StatusCode.ALREADY_EXISTS, "Executor already connected."
            )

        connection = self._executors[executor_id] = _ExecutorConnection()
        connected = False
        try:
            record = asyncio.create_task(
                asyncio.to_thread(
                    self._record_connection, registered_id, datetime.now(UTC)
                )
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
        self, executor_id: str, request: messages.ExecuteNodeRequest
    ) -> str:
        """Send a recorded visit to an idle executor and return its attempt ID."""
        connection = self._executors[executor_id]
        if connection.closing or connection.node_xid is not None:
            raise RuntimeError("Executor is closing or busy.")
        node_xid = str(UUID(request.node_xid))
        if node_xid in self._assigned_nodes:
            raise RuntimeError("Node visit already has an active attempt.")
        message = messages.EngineMessage(execute_node=request)
        attempt_id = str(uuid4())
        message.execute_node.node_xid = node_xid
        message.execute_node.attempt_id = attempt_id
        self._assigned_nodes.add(node_xid)
        connection.node_xid = node_xid
        connection.attempt_id = attempt_id
        connection.outgoing.put_nowait(message)
        return attempt_id

    def interrupt_node(self, executor_id: str, node_xid: str) -> None:
        """Ask the assigned executor to stop a visit."""
        connection = self._executors.get(executor_id)
        if (
            connection is not None
            and not connection.closing
            and connection.node_xid == node_xid
        ):
            connection.outgoing.put_nowait(
                messages.EngineMessage(
                    interrupt_node=messages.InterruptNodeRequest(
                        node_xid=node_xid
                    )
                )
            )

    def shutdown_executor(self, executor_id: str) -> None:
        """Send shutdown and end the executor stream."""
        connection = self._executors.get(executor_id)
        if connection is not None and not connection.closing:
            connection.closing = True
            connection.outgoing.put_nowait(
                messages.EngineMessage(shutdown=messages.EngineShutdown())
            )

    async def executor_available(self, executor_id: str) -> None:
        """Notify the scheduler when the executor can accept a visit."""

    async def commit_node_result(
        self, executor_id: str, result: messages.ExecuteNodeResponse
    ) -> None:
        """Validate and commit a result before the next dispatch."""
        raise NotImplementedError

    async def executor_disconnected(self, executor_id: str) -> None:
        """Notify the scheduler when the executor stream ends."""

    async def _receive_executor_messages(
        self,
        executor_id: str,
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
            if (
                connection.closing
                or result.node_xid != connection.node_xid
                or result.attempt_id != connection.attempt_id
            ):
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
            connection.attempt_id = None


class _InvalidExecutorMessage(Exception):
    """Identify a malformed executor message."""
