import asyncio
import os
import signal
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from processes import execute_in_child_process, _start_node

_RUNNER = """
from executors import ExecContext, Executable

class Probe(Executable):
    async def execute(self, context: ExecContext):
        return await probe(context.input)

    async def test_execute(self):
        pass
"""


class NodeProcessTest(unittest.IsolatedAsyncioTestCase):
    async def test_node_uses_same_environment_in_a_different_interpreter(self):
        code = _RUNNER + """
import os

async def probe(inputs):
    return {"pid": os.getpid(), "environment": os.environ["WORKFLOW_TEST_PROCESS"]}
"""
        with patch.dict(
            os.environ, {"WORKFLOW_TEST_PROCESS": "synthetic fixture"}
        ):
            result = await execute_in_child_process(
                code, {}, timeout=5, output_limit=4096
            )
        self.assertNotEqual(result["pid"], os.getpid())
        self.assertEqual(result["environment"], "synthetic fixture")

    async def test_empty_output_from_node_printing_a_log(self):
        result = await execute_in_child_process(
            _RUNNER + """
async def probe(inputs):
    assert inputs == {}
    print("fixture log")
    return {}
""",
            {},
            timeout=5,
            output_limit=4096,
        )
        self.assertEqual(result, {})

    async def test_runtime_rejects_request_without_input(self):
        runtime = Path(__file__).resolve().parents[1] / "runtime.py"
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            str(runtime),
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(b'{"code": "pass"}'), timeout=5
            )
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        self.assertNotEqual(process.returncode, 0)
        self.assertEqual(stdout, b"")
        self.assertIn(b"NodeExecutionRequest", stderr)
        self.assertIn(b"Field required", stderr)

    async def test_executor_rejects_invalid_child_response(self):
        fixtures = (
            (b"{}", "Field required"),
            (b'{"output": {"nested": [1e400]}}', "must be finite"),
        )
        for response, error in fixtures:
            with self.subTest(response=response):
                code = _RUNNER + f"""
import os

async def probe(inputs):
    os.write(1, {response!r})
    os._exit(0)
"""
                with self.assertRaisesRegex(ValueError, error):
                    await execute_in_child_process(
                        code, {}, timeout=5, output_limit=4096
                    )

    async def test_loader_reports_invalid_node_code(self):
        with self.assertRaisesRegex(
            RuntimeError, "Expected one concrete Executable"
        ):
            await execute_in_child_process(
                "class NotAnExecutable: pass", {}, timeout=5, output_limit=4096
            )

    async def test_output_limit_reaps_the_process(self):
        processes = []
        launch = _start_node

        async def track(*args, **kwargs):
            process = await launch(*args, **kwargs)
            processes.append(process[0])
            return process

        with patch("processes._start_node", track):
            with self.assertRaisesRegex(RuntimeError, "output limit"):
                await execute_in_child_process(
                    _RUNNER + """
async def probe(inputs):
    while True:
        print("fixture log" * 1024, flush=True)
""",
                    {},
                    timeout=5,
                    output_limit=4096,
                )
        self.assertIsNotNone(processes[0].get_returncode())

    async def test_cancel_and_timeout_reap_the_process(self):
        code = _RUNNER + """
import asyncio

async def probe(inputs):
    await asyncio.sleep(30)
    return {}
"""
        for cancel in (True, False):
            with self.subTest(cancel=cancel):
                started = asyncio.Event()
                processes = []
                launch = _start_node

                async def track(*args, **kwargs):
                    process = await launch(*args, **kwargs)
                    processes.append(process[0])
                    started.set()
                    return process

                with patch("processes._start_node", track):
                    task = asyncio.create_task(
                        execute_in_child_process(
                            code, {}, timeout=0.5, output_limit=4096
                        )
                    )
                    if cancel:
                        await asyncio.wait_for(started.wait(), timeout=5)
                        task.cancel()
                    failure = asyncio.CancelledError if cancel else TimeoutError
                    with self.assertRaises(failure):
                        await task
                self.assertIsNotNone(processes[0].get_returncode())

    @unittest.skipUnless(os.name == "posix", "Requires a POSIX process session")
    async def test_timeout_closes_pipes_inherited_by_a_detached_child(self):
        code = _RUNNER + """
from pathlib import Path
import subprocess
import sys

async def probe(inputs):
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        start_new_session=True,
    )
    Path(inputs["pid_file"]).write_text(str(child.pid))
    return {}
"""
        with TemporaryDirectory() as directory:
            path = Path(directory) / "detached.pid"
            try:
                with self.assertRaisesRegex(
                    TimeoutError, "Node process exceeded"
                ):
                    async with asyncio.timeout(4):
                        await execute_in_child_process(
                            code,
                            {"pid_file": str(path)},
                            timeout=1.5,
                            output_limit=4096,
                        )
                self.assertTrue(path.is_file())
            finally:
                if path.is_file():
                    try:
                        os.kill(int(path.read_text()), signal.SIGKILL)
                    except ProcessLookupError:
                        pass
