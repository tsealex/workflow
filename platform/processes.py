"""Run node code in a separate Python process."""

import asyncio
import json
import math
import os
import signal
import subprocess
import sys
from pathlib import Path

from nodes import DataMap, decode_data_map
from runtime import NodeExecutionRequest, NodeExecutionResponse

_RUNTIME = Path(__file__).with_name("runtime.py")


class _NodeProtocol(asyncio.SubprocessProtocol):
    """Collect bounded output and track the process exit separately from pipe EOF."""

    def __init__(self, output_limit: int) -> None:
        loop = asyncio.get_running_loop()
        self.exit = loop.create_future()
        self.finished = loop.create_future()
        self.output = {1: bytearray(), 2: bytearray()}
        self._output_limit = output_limit
        self._open_pipes = {1, 2}

    def pipe_data_received(self, fd: int, data: bytes) -> None:
        if self.finished.done():
            return
        output = self.output[fd]
        if len(output) + len(data) > self._output_limit:
            self.finished.set_exception(
                RuntimeError("Node process exceeded its output limit.")
            )
        else:
            output.extend(data)

    def pipe_connection_lost(self, fd: int, exc: Exception | None) -> None:
        self._open_pipes.discard(fd)
        self._check_finished()

    def process_exited(self) -> None:
        self.exit.set_result(None)
        self._check_finished()

    def _check_finished(self) -> None:
        if (
            self.exit.done()
            and not self._open_pipes
            and not self.finished.done()
        ):
            self.finished.set_result(None)


async def _start_node(
    output_limit: int,
) -> tuple[asyncio.SubprocessTransport, _NodeProtocol]:
    return await asyncio.get_running_loop().subprocess_exec(
        lambda: _NodeProtocol(output_limit),
        sys.executable,
        "-u",
        str(_RUNTIME),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=os.name == "posix",
    )


async def execute_in_child_process(
    code: str, inputs: DataMap, *, timeout: float, output_limit: int
) -> DataMap:
    """Run a node with a wall-clock timeout and bounded stdout and stderr."""
    if not math.isfinite(timeout) or timeout <= 0 or output_limit <= 0:
        raise ValueError("The timeout and output limit must be positive.")
    request = NodeExecutionRequest(code=code, input=inputs)
    payload = json.dumps(request.model_dump(), allow_nan=False).encode()
    launch = asyncio.create_task(_start_node(output_limit))
    child = None
    collected = False
    try:
        async with asyncio.timeout(timeout):
            child = await asyncio.shield(launch)
            transport, protocol = child
            stdin = transport.get_pipe_transport(0)
            stdin.write(payload)
            stdin.close()
            await protocol.finished
            collected = True
            status = transport.get_returncode()
            if status:
                diagnostic = (
                    protocol.output[2][-2048:].decode(errors="replace").strip()
                )
                raise RuntimeError(
                    f"Node process exited with status {status}: {diagnostic!r}"
                )
            response = NodeExecutionResponse.model_validate(
                decode_data_map(bytes(protocol.output[1]))
            )
            return response.output
    except TimeoutError as error:
        raise TimeoutError(
            f"Node process exceeded its {timeout:g}-second timeout."
        ) from error
    finally:
        if child is None:
            child = (await asyncio.gather(launch, return_exceptions=True))[0]
        if isinstance(child, tuple):
            transport, protocol = child
            if not collected and os.name == "posix":
                try:
                    os.killpg(transport.get_pid(), signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await protocol.exit
            # Close our pipes even when another process inherited the far end.
            transport.close()
            await protocol.exit
