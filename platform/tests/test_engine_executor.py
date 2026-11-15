import asyncio
import json
import unittest
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch
from uuid import uuid4

import grpc
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

import execution_pb2 as messages
from database import Base
from engine import WorkflowEngine, WorkflowExecutorRegistration
from execution_pb2_grpc import (
    WorkflowEngineStub,
    add_WorkflowEngineServicer_to_server,
)
from executors import WorkflowExecutor


class RecordingEngine(WorkflowEngine):
    def __init__(self, sessions):
        super().__init__(sessions)
        self.available = asyncio.Queue()
        self.results = asyncio.Queue()
        self.disconnected = asyncio.Queue()
        self.allow_commit = asyncio.Event()
        self.allow_commit.set()

    async def executor_available(self, executor_id):
        await self.available.put(executor_id)

    async def commit_node_result(self, executor_id, result):
        await self.results.put(result)
        await self.allow_commit.wait()

    async def executor_disconnected(self, executor_id):
        await self.disconnected.put(executor_id)


class RecordingExecutor(WorkflowExecutor):
    def __init__(self, channel):
        super().__init__(channel)
        self.dispatched = asyncio.Queue()
        self.finish = asyncio.Queue()
        self.stopped = asyncio.Event()

    async def execute_node(self, request):
        await self.dispatched.put(request)
        try:
            result = await self.finish.get()
            if isinstance(result, Exception):
                raise result
            return result
        finally:
            self.stopped.set()


class EngineExecutorTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.home = Path(temporary.name)
        self.database = create_engine(f"sqlite:///{self.home / 'workflow.db'}")
        self.addCleanup(self.database.dispose)
        Base.metadata.create_all(self.database)
        self.sessions = sessionmaker(self.database)
        self.keys = {}
        native_keyring = Mock()
        native_keyring.get_password.side_effect = (
            lambda service, user: self.keys.get((service, user))
        )
        native_keyring.set_password.side_effect = lambda service, user, key: (
            self.keys.__setitem__((service, user), key)
        )
        patcher = patch(
            "executors._get_system_keyring",
            return_value=native_keyring,
        )
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.start_engine()
        self.registration = await self.executor.register()
        self.tasks = []

    async def start_engine(self):
        self.engine = RecordingEngine(self.sessions)
        self.server = grpc.aio.server()
        add_WorkflowEngineServicer_to_server(self.engine, self.server)
        port = self.server.add_insecure_port("127.0.0.1:0")
        await self.server.start()
        self.channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        with patch("executors.Path.home", return_value=self.home):
            self.executor = RecordingExecutor(self.channel)

    async def asyncTearDown(self):
        self.engine.allow_commit.set()
        await self.channel.close()
        await self.server.stop(None)
        await asyncio.gather(*self.tasks, return_exceptions=True)

    @staticmethod
    def new_request(**kwargs):
        request = messages.ExecuteNodeRequest(node_xid=str(uuid4()), **kwargs)
        request.started_at.FromDatetime(datetime.now(UTC))
        request.node.update({"code": "unused test fixture"})
        return request

    async def connect_executor(self):
        task = asyncio.create_task(self.executor.run())
        self.tasks.append(task)
        self.assertEqual(
            await self.engine.available.get(), self.registration.executor_id
        )
        return task

    async def test_dispatch_commit_and_reconnect(self):
        async with asyncio.timeout(5):
            task = await self.connect_executor()
            executor_id = self.registration.executor_id
            self.engine.allow_commit.clear()
            request = self.new_request(inputs=b'{"amount": 9007199254740993}')
            attempt_id = self.engine.execute_node(executor_id, request)
            dispatched = await self.executor.dispatched.get()
            self.assertEqual(dispatched.attempt_id, attempt_id)
            self.assertEqual(dispatched.inputs, request.inputs)
            self.assertEqual(request.attempt_id, "")
            await self.executor.finish.put({"amount": 9007199254740993})

            result = await self.engine.results.get()
            self.assertEqual(result.status, messages.COMPLETED)
            self.assertEqual(result.node_xid, request.node_xid)
            self.assertEqual(result.attempt_id, attempt_id)
            self.assertEqual(
                json.loads(result.outputs), {"amount": 9007199254740993}
            )
            with self.assertRaises(RuntimeError):
                self.engine.execute_node(executor_id, self.new_request())

            self.engine.allow_commit.set()
            self.assertEqual(await self.engine.available.get(), executor_id)
            self.engine.shutdown_executor(executor_id)
            await task
            self.assertEqual(await self.engine.disconnected.get(), executor_id)

            task = await self.connect_executor()
            self.executor.shutdown()
            await task
            self.assertEqual(await self.engine.disconnected.get(), executor_id)

    async def test_live_interrupt_and_failure_results(self):
        async with asyncio.timeout(5):
            task = await self.connect_executor()
            executor_id = self.registration.executor_id
            request = self.new_request()

            self.engine.execute_node(executor_id, request)
            await self.executor.dispatched.get()
            self.engine.interrupt_node(executor_id, request.node_xid)
            result = await self.engine.results.get()
            self.assertEqual(result.status, messages.INTERRUPTED)
            self.assertFalse(result.HasField("outputs"))
            self.assertTrue(self.executor.stopped.is_set())
            await self.engine.available.get()

            self.engine.execute_node(executor_id, request)
            await self.executor.dispatched.get()
            with self.assertLogs("executors", level="ERROR"):
                await self.executor.finish.put(RuntimeError("fixture failure"))
                self.assertEqual(
                    (await self.engine.results.get()).status, messages.FAILED
                )
            await self.engine.available.get()

            self.engine.execute_node(executor_id, request)
            await self.executor.dispatched.get()
            with self.assertLogs("executors", level="ERROR"):
                await self.executor.finish.put([])
                self.assertEqual(
                    (await self.engine.results.get()).status, messages.FAILED
                )
            await self.engine.available.get()

            request.inputs = b'{"nested": [1e400]}'
            self.engine.execute_node(executor_id, request)
            with self.assertLogs("executors", level="ERROR"):
                self.assertEqual(
                    (await self.engine.results.get()).status, messages.FAILED
                )
            self.assertTrue(self.executor.dispatched.empty())
            await self.engine.available.get()

            self.executor.shutdown()
            await task

    async def test_engine_rejects_unauthorized_and_malformed_results(self):
        async with asyncio.timeout(5):
            stub = WorkflowEngineStub(self.channel)
            with self.assertRaises(grpc.aio.AioRpcError) as unauthorized:
                await stub.ConnectExecutor().read()
            self.assertEqual(
                unauthorized.exception.code(), grpc.StatusCode.UNAUTHENTICATED
            )

            call = stub.ConnectExecutor(
                metadata=(
                    ("authorization", f"Bearer {self.registration.token}"),
                )
            )
            executor_id = await self.engine.available.get()
            request = self.new_request()
            self.engine.execute_node(executor_id, request)
            first = (await call.read()).execute_node
            suspended = messages.ExecuteNodeResponse(
                node_xid=first.node_xid,
                attempt_id=first.attempt_id,
                status=messages.SUSPENDED,
            )
            suspended.started_at.CopyFrom(first.started_at)
            suspended.ended_at.FromDatetime(datetime.now(UTC))
            await call.write(messages.ExecutorMessage(node_result=suspended))
            self.assertEqual(await self.engine.results.get(), suspended)
            await self.engine.available.get()

            self.engine.execute_node(executor_id, request)
            retry = (await call.read()).execute_node
            self.assertNotEqual(retry.attempt_id, first.attempt_id)
            await call.write(messages.ExecutorMessage(node_result=suspended))
            invalid = messages.ExecuteNodeResponse(
                node_xid=retry.node_xid,
                attempt_id=retry.attempt_id,
                status=messages.COMPLETED,
                outputs=b'{"nested": [1e400]}',
            )
            invalid.started_at.CopyFrom(retry.started_at)
            invalid.ended_at.FromDatetime(datetime.now(UTC))
            await call.write(messages.ExecutorMessage(node_result=invalid))

            with self.assertRaises(grpc.aio.AioRpcError) as rejected:
                await call.read()
            self.assertEqual(
                rejected.exception.code(), grpc.StatusCode.INVALID_ARGUMENT
            )
            self.assertTrue(self.engine.results.empty())

    async def test_engine_shutdown_stops_active_work_before_run_returns(self):
        async with asyncio.timeout(5):
            task = await self.connect_executor()
            executor_id = self.registration.executor_id
            self.engine.execute_node(executor_id, self.new_request())
            await self.executor.dispatched.get()

            self.engine.shutdown_executor(executor_id)
            await task
            self.assertTrue(self.executor.stopped.is_set())
            self.assertEqual(await self.engine.disconnected.get(), executor_id)
            self.assertTrue(self.engine.results.empty())

    async def test_registration_and_encrypted_credentials_survive_restart(self):
        async with asyncio.timeout(5):
            credential_file = self.home / ".workflow" / "executor.credentials"
            original_ciphertext = credential_file.read_bytes()
            self.assertNotIn(
                self.registration.token.encode(), original_ciphertext
            )
            self.assertNotIn(
                self.registration.executor_id.encode(), original_ciphertext
            )
            self.assertEqual(credential_file.stat().st_mode & 0o777, 0o600)

            def read_registration():
                with self.sessions() as session:
                    return session.scalar(select(WorkflowExecutorRegistration))

            original = read_registration()
            self.assertEqual(str(original.id), self.registration.executor_id)
            self.assertEqual(
                original.token_hash,
                sha256(self.registration.token.encode()).hexdigest(),
            )
            self.assertIsNone(original.last_connected_at)
            task = await self.connect_executor()
            first_connection = read_registration().last_connected_at
            self.assertIsNotNone(first_connection)
            self.executor.shutdown()
            await task
            await self.channel.close()
            await self.server.stop(None)

            await self.start_engine()
            task = await self.connect_executor()
            recovered = read_registration()
            self.assertEqual(recovered.id, original.id)
            self.assertEqual(recovered.created_at, original.created_at)
            self.assertGreater(recovered.last_connected_at, first_connection)
            self.assertEqual(credential_file.read_bytes(), original_ciphertext)
            self.executor.shutdown()
            await task

    async def test_rejects_partial_invalid_and_duplicate_registration(self):
        async with asyncio.timeout(5):
            stub = WorkflowEngineStub(self.channel)
            executor_id = self.registration.executor_id
            token = self.registration.token
            for request, status in (
                (
                    messages.RegisterExecutorRequest(executor_id=executor_id),
                    grpc.StatusCode.INVALID_ARGUMENT,
                ),
                (
                    messages.RegisterExecutorRequest(token=token),
                    grpc.StatusCode.INVALID_ARGUMENT,
                ),
                (
                    messages.RegisterExecutorRequest(
                        executor_id=executor_id, token="bad"
                    ),
                    grpc.StatusCode.UNAUTHENTICATED,
                ),
            ):
                with self.assertRaises(grpc.aio.AioRpcError) as rejected:
                    await stub.RegisterExecutor(request)
                self.assertEqual(rejected.exception.code(), status)

            credentials = messages.RegisterExecutorRequest(
                executor_id=executor_id, token=token
            )
            self.assertEqual(
                await stub.RegisterExecutor(credentials), self.registration
            )
            task = await self.connect_executor()
            with self.sessions() as session:
                connected_at = session.scalar(
                    select(WorkflowExecutorRegistration.last_connected_at)
                )
            with self.assertRaises(grpc.aio.AioRpcError) as registration:
                await stub.RegisterExecutor(credentials)
            self.assertEqual(
                registration.exception.code(), grpc.StatusCode.ALREADY_EXISTS
            )
            with self.assertRaises(grpc.aio.AioRpcError) as stream:
                await stub.ConnectExecutor(
                    metadata=(("authorization", f"Bearer {token}"),)
                ).read()
            self.assertEqual(
                stream.exception.code(), grpc.StatusCode.ALREADY_EXISTS
            )
            with self.sessions() as session:
                self.assertEqual(
                    session.scalar(
                        select(WorkflowExecutorRegistration.last_connected_at)
                    ),
                    connected_at,
                )
            self.executor.shutdown()
            await task

    async def test_missing_key_or_corrupt_file_does_not_create_identity(self):
        async with asyncio.timeout(5):
            path = self.home / ".workflow" / "executor.credentials"
            encrypted = path.read_bytes()
            saved_keys = self.keys.copy()
            self.keys.clear()
            with self.assertRaisesRegex(
                RuntimeError, "credential key is missing"
            ):
                await self.executor.register()
            self.assertEqual(path.read_bytes(), encrypted)

            self.keys.update(saved_keys)
            path.write_bytes(b"corrupt test fixture")
            with self.assertRaisesRegex(
                RuntimeError, "Cannot read the executor credential file"
            ):
                await self.executor.register()
            self.assertEqual(path.read_bytes(), b"corrupt test fixture")
            with self.sessions() as session:
                self.assertEqual(
                    session.scalar(
                        select(func.count()).select_from(
                            WorkflowExecutorRegistration
                        )
                    ),
                    1,
                )
