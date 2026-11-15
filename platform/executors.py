from abc import abstractmethod, ABC
from datetime import datetime
from typing import Self
from uuid import UUID

from pydantic import BaseModel, model_validator

from engine import ExecutionStatus
from nodes import DataMap


class Machine:
    """Physical or virtual computer with its own execution environment."""


class WorkflowExecutor:
    """Runs dispatched node executions on a machine and reports their outcomes."""


class RegisterExecutorRequest(BaseModel):
    pass


class RegisterExecutorResponse(BaseModel):
    executor_id: UUID
    token: str


class ExecuteNodeRequest(BaseModel):
    node_xid: UUID
    started_at: datetime
    node: DataMap
    inputs: DataMap | None = None


class InterruptNodeRequest(BaseModel):
    node_xid: UUID


class ExecuteNodeResponse(BaseModel):
    node_xid: UUID
    status: ExecutionStatus
    started_at: datetime
    ended_at: datetime
    outputs: DataMap | None = None

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Allow outputs only when nonempty and the node has completed."""
        if self.outputs is not None and (
                self.status != ExecutionStatus.COMPLETED or not self.outputs
        ):
            raise ValueError(
                "Include outputs only for a completed node with a nonempty result."
            )
        return self


class EngineShutdown(BaseModel):
    pass


class ExecutorShutdown(BaseModel):
    pass


class EngineMessage(BaseModel):
    execute_node: ExecuteNodeRequest | None = None
    interrupt_node: InterruptNodeRequest | None = None
    shutdown: EngineShutdown | None = None

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require exactly one engine message payload."""
        payloads = (self.execute_node, self.interrupt_node, self.shutdown)
        if sum(payload is not None for payload in payloads) != 1:
            raise ValueError("EngineMessage must contain exactly one payload.")
        return self


class ExecutorMessage(BaseModel):
    node_result: ExecuteNodeResponse | None = None
    shutdown: ExecutorShutdown | None = None

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require exactly one executor message payload."""
        if (self.node_result is None) == (self.shutdown is None):
            raise ValueError(
                "ExecutorMessage must contain exactly one payload."
            )
        return self


class Executable(ABC):
    @abstractmethod
    async def execute(self, inputs: DataMap) -> DataMap: ...

    @abstractmethod
    async def test_execute(self) -> None: ...
