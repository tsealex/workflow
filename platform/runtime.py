"""Run one generated executable in a child process."""

import asyncio
import inspect
import sys
import types
from abc import ABC, abstractmethod
from contextlib import redirect_stdout
from json import dumps

from pydantic import BaseModel

from nodes import DataMap, decode_data_map


class NodeExecutionRequest(BaseModel):
    """Send node source and input to the child process."""

    code: str
    input: DataMap


class NodeExecutionResponse(BaseModel):
    """Return the node output from the child process."""

    output: DataMap


class ExecContext(BaseModel):
    """Supply the selected input values for a node execution."""

    input: DataMap


class Executable(ABC):
    @abstractmethod
    async def execute(self, context: ExecContext) -> DataMap: ...

    @abstractmethod
    async def test_execute(self) -> None: ...


async def _run_node(request: NodeExecutionRequest) -> NodeExecutionResponse:
    """Load the single concrete executable and run it with its input."""
    module = types.ModuleType("workflow_node")
    sys.modules[module.__name__] = module
    module.__dict__.update(
        Executable=Executable, ExecContext=ExecContext, DataMap=DataMap
    )
    exec(compile(request.code, "<workflow node>", "exec"), module.__dict__)
    candidates = {
        value
        for value in module.__dict__.values()
        if inspect.isclass(value)
        and value.__module__ == module.__name__
        and issubclass(value, Executable)
        and not inspect.isabstract(value)
    }
    if len(candidates) != 1:
        raise ValueError("Expected one concrete Executable class.")
    executable = candidates.pop()()
    result = await executable.execute(ExecContext(input=request.input))
    if not isinstance(result, dict):
        raise TypeError("Expected a DataMap result.")
    return NodeExecutionResponse(output=result)


def _main() -> None:
    """Read the request and write one JSON object to standard output."""
    sys.modules["runtime"] = sys.modules[__name__]
    request = NodeExecutionRequest.model_validate(
        decode_data_map(sys.stdin.buffer.read())
    )
    with redirect_stdout(sys.stderr):
        result = asyncio.run(_run_node(request))
    sys.stdout.write(dumps(result.model_dump(), allow_nan=False))


if __name__ == "__main__":
    _main()
