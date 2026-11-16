import asyncio
import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import TypeVar
from uuid import UUID

import grpc
import keyring
from cryptography.fernet import Fernet, InvalidToken
from filelock import AsyncFileLock
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from keyring.backend import KeyringBackend
from keyring.backends.chainer import ChainerBackend
from pydantic import TypeAdapter

import execution_pb2 as messages
from execution_pb2_grpc import WorkflowEngineStub
from nodes import DataMap, Node, decode_data_map
from processes import execute_in_child_process
from runtime import ExecContext as ExecContext, Executable as Executable

logger = logging.getLogger(__name__)
_CredentialResult = TypeVar("_CredentialResult")

_NODE_ADAPTER = TypeAdapter(Node)
_KEYRING_SERVICE = "workflow.executor"
_SYSTEM_KEYRINGS = {
    "keyring.backends.SecretService",
    "keyring.backends.Windows",
    "keyring.backends.kwallet",
    "keyring.backends.macOS",
}


@dataclass(frozen=True)
class ExecutorCredentials:
    """Store the executor ID and token used to authenticate with the engine."""

    executor_id: UUID
    token: str


def _get_system_keyring() -> KeyringBackend:
    backend = keyring.get_keyring()
    candidates = (
        backend.backends if isinstance(backend, ChainerBackend) else [backend]
    )
    for candidate in candidates:
        if type(candidate).__module__ in _SYSTEM_KEYRINGS:
            return candidate
    raise RuntimeError("Executor credentials require an OS keyring.")


def _load_executor_credentials(
    path: Path,
) -> tuple[Fernet, ExecutorCredentials | None]:
    """Load credentials, or prepare an encryption key for first registration."""
    try:
        encrypted = path.read_bytes()
    except FileNotFoundError:
        encrypted = None

    backend = _get_system_keyring()
    account = str(path)
    key = backend.get_password(_KEYRING_SERVICE, account)
    if key is None:
        if encrypted is not None:
            raise RuntimeError("The executor credential key is missing.")
        key = Fernet.generate_key().decode("ascii")
        backend.set_password(_KEYRING_SERVICE, account, key)

    try:
        cipher = Fernet(key.encode("ascii"))
        if encrypted is None:
            return cipher, None
        response = messages.RegisterExecutorResponse.FromString(
            cipher.decrypt(encrypted)
        )
        if not response.executor_id or not response.token:
            raise RuntimeError("The executor credential file is incomplete.")
        credentials = ExecutorCredentials(
            UUID(response.executor_id), response.token
        )
    except (ValueError, InvalidToken, DecodeError) as error:
        raise RuntimeError(
            "Cannot read the executor credential file."
        ) from error

    return cipher, credentials


def _save_executor_credentials(
    path: Path, cipher: Fernet, credentials: ExecutorCredentials
) -> None:
    """Write credentials atomically with access limited to the local user."""
    response = messages.RegisterExecutorResponse(
        executor_id=str(credentials.executor_id), token=credentials.token
    )
    encrypted = cipher.encrypt(response.SerializeToString())
    with NamedTemporaryFile(dir=path.parent, delete=False) as file:
        temporary_path = Path(file.name)
        try:
            file.write(encrypted)
            file.flush()
            os.fsync(file.fileno())
            file.close()
            temporary_path.replace(path)
        finally:
            temporary_path.unlink(missing_ok=True)


class Machine:
    """Physical or virtual computer with its own execution environment."""


async def _complete_credential_io(
    operation: Callable[..., _CredentialResult], *arguments: object
) -> _CredentialResult:
    """Finish local IO before the caller releases the credential lock."""
    task = asyncio.create_task(asyncio.to_thread(operation, *arguments))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await task
        raise


@dataclass
class _NodeAttempt:
    """Track the request, start time, and execution task."""

    request: messages.ExecuteNodeRequest
    started_at: datetime
    execution: asyncio.Task[DataMap]

    def cancel(self) -> None:
        """Request cancellation once so cleanup can finish."""
        if not self.execution.cancelling():
            self.execution.cancel()

    async def stop(self) -> None:
        """Cancel the task and wait for cleanup."""
        self.cancel()
        await asyncio.gather(self.execution, return_exceptions=True)


class WorkflowExecutor:
    """Receive work from the engine and report node results."""

    def __init__(
        self,
        channel: grpc.aio.Channel,
        *,
        node_timeout: float = 30,
        node_output_limit: int = 1024 * 1024,
    ) -> None:
        self._engine = WorkflowEngineStub(channel)
        self._node_timeout = node_timeout
        self._node_output_limit = node_output_limit
        self._stop = asyncio.Event()
        self._running = False
        self._credentials_path = (
            Path.home() / ".workflow" / "executor.credentials"
        )

    async def register(self) -> ExecutorCredentials:
        """Reuse saved credentials or register and encrypt new credentials."""
        path = self._credentials_path
        await asyncio.to_thread(
            path.parent.mkdir, mode=0o700, parents=True, exist_ok=True
        )
        async with AsyncFileLock(
            path.with_name(f"{path.name}.lock"), mode=0o600
        ):
            cipher, saved = await _complete_credential_io(
                _load_executor_credentials, path
            )
            request = messages.RegisterExecutorRequest()
            if saved is not None:
                request.executor_id = str(saved.executor_id)
                request.token = saved.token
            response = await self._engine.register_executor(request)
            if not response.executor_id or not response.token:
                raise ValueError(
                    "The engine returned incomplete executor credentials."
                )
            registration = ExecutorCredentials(
                UUID(response.executor_id), response.token
            )
            if saved is None:
                await _complete_credential_io(
                    _save_executor_credentials, path, cipher, registration
                )
            elif registration != saved:
                raise RuntimeError(
                    "The engine returned different executor credentials."
                )
            return registration

    async def run(self) -> None:
        """Register with the engine and handle messages until the stream stops."""
        if self._running:
            raise RuntimeError("Executor is already running.")
        if self._stop.is_set():
            return
        self._running = True
        try:
            registration = await self.register()
            await self._run(registration)
        finally:
            self._running = False

    async def _run(self, registration: ExecutorCredentials) -> None:
        call = self._engine.connect_executor(
            metadata=(("authorization", f"Bearer {registration.token}"),)
        )
        receive = asyncio.create_task(call.read())
        stop = asyncio.create_task(self._stop.wait())
        attempts: dict[str, _NodeAttempt] = {}
        try:
            while True:
                pending = [receive, stop]
                pending.extend(
                    attempt.execution for attempt in attempts.values()
                )
                finished, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                if stop in finished:
                    await asyncio.gather(
                        *(attempt.stop() for attempt in attempts.values())
                    )
                    attempts.clear()
                    if not call.done():
                        await call.write(
                            messages.ExecutorMessage(
                                shutdown=messages.ExecutorShutdown()
                            )
                        )
                        await call.done_writing()
                    return
                if receive in finished:
                    message = receive.result()
                    if message is grpc.aio.EOF:
                        return
                    payload = message.WhichOneof("payload")
                    if payload == "shutdown":
                        return
                    if payload == "execute_node":
                        request = message.execute_node
                        if request.node_xid in attempts:
                            raise RuntimeError(
                                "Received a duplicate node dispatch."
                            )
                        attempts[request.node_xid] = _NodeAttempt(
                            request,
                            datetime.now(UTC),
                            asyncio.create_task(self._execute_node(request)),
                        )
                    elif payload == "interrupt_node":
                        attempt = attempts.get(message.interrupt_node.node_xid)
                        if attempt is not None:
                            attempt.cancel()
                    else:
                        raise RuntimeError(
                            "Received an invalid engine message."
                        )
                    receive = asyncio.create_task(call.read())
                for node_xid, attempt in list(attempts.items()):
                    if attempt.execution in finished:
                        await call.write(
                            messages.ExecutorMessage(
                                node_result=self._node_result(attempt)
                            )
                        )
                        del attempts[node_xid]
        finally:
            call.cancel()
            receive.cancel()
            stop.cancel()
            await asyncio.gather(
                receive,
                stop,
                *(attempt.stop() for attempt in attempts.values()),
                return_exceptions=True,
            )

    def shutdown(self) -> None:
        """Ask the active stream to stop work and report executor shutdown."""
        self._stop.set()

    async def execute_node(
        self,
        request: messages.ExecuteNodeRequest,
        inputs: DataMap | None = None,
    ) -> DataMap:
        """Run the generated code in a separate Python process.

        The stream supplies validated inputs. Decode inputs from the request
        if the caller supplies no inputs.
        """
        node = _NODE_ADAPTER.validate_python(MessageToDict(request.node))
        if not node.has_code():
            raise ValueError(
                "The executor only runs action and decision nodes."
            )
        code: str = getattr(node, "code")
        if not code.strip():
            raise ValueError("The node needs Python code.")
        if inputs is None:
            inputs = decode_data_map(request.inputs) if request.inputs else {}
        return await execute_in_child_process(
            code,
            inputs,
            timeout=self._node_timeout,
            output_limit=self._node_output_limit,
        )

    async def _execute_node(
        self, request: messages.ExecuteNodeRequest
    ) -> DataMap:
        """Validate the request inputs before dispatch."""
        inputs = decode_data_map(request.inputs) if request.inputs else {}
        return await self.execute_node(request, inputs)

    @staticmethod
    def _node_result(attempt: _NodeAttempt) -> messages.ExecuteNodeResponse:
        request = attempt.request
        result = messages.ExecuteNodeResponse(
            node_xid=request.node_xid,
            status=messages.COMPLETED,
        )
        result.started_at.FromDatetime(attempt.started_at)
        result.ended_at.FromDatetime(datetime.now(UTC))
        try:
            outputs = attempt.execution.result()
            if not isinstance(outputs, dict):
                raise TypeError("Expected a DataMap result.")
            if outputs:
                result.outputs = json.dumps(outputs, allow_nan=False).encode()
        except asyncio.CancelledError:
            result.status = messages.INTERRUPTED
        except Exception:
            logger.exception("Node execution failed for %s", request.node_xid)
            result.status = messages.FAILED
        return result
