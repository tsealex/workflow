from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from typing import Annotated, Literal, Self, TypeAlias, cast
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)

DataMap: TypeAlias = dict[str, object]


def _decode_number(value: str) -> float:
    number = float(value)
    if not isfinite(number):
        raise ValueError("DataMap numbers must be finite.")
    return number


def decode_data_map(value: bytes) -> DataMap:
    """Read a UTF-8 JSON object with finite numbers."""
    data = json.loads(
        value.decode(),
        parse_float=_decode_number,
        parse_constant=_decode_number,
    )
    if not isinstance(data, dict):
        raise ValueError("Expected a DataMap object.")
    return data


class FilterOperator(StrEnum):
    """Identify a comparison in an event filter."""

    EQ = "eq"
    NE = "ne"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    NOT_IN = "not_in"


FilterMap: TypeAlias = dict[str, dict[FilterOperator, object]]


class Edge(BaseModel):
    """Connect two nodes in a workflow."""

    model_config = ConfigDict(validate_by_name=True, serialize_by_alias=True)

    from_: UUID = Field(alias="from")
    to: UUID
    branch: str | None = None

    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler):
        """Omit the branch key for edges without a branch label."""
        data = handler(self)
        if self.branch is None:
            data.pop("branch", None)
        return data


class EventSelector(BaseModel):
    event_id: UUID
    filter: FilterMap | None = None


class NodeType(StrEnum):
    ACTION = "action"
    DECISION = "decision"
    SIGNAL = "signal"
    WAIT = "wait"
    REGION = "region"


# Define the node types that can be executed by an executor. Other node types
# are executed by the engine itself.
ExecutorNodeTypes = {NodeType.ACTION, NodeType.DECISION}


class BaseNode(BaseModel):
    """Provide the identity and name shared by workflow nodes."""

    id: UUID
    name: str

    def has_code(self) -> bool:
        """Report whether this node type has a Python code field."""
        return False

    def extract_input_states(self, states: DataMap) -> DataMap:
        """Return an empty input map for this node."""
        return {}


class _InputStateNode(BaseNode):
    """Read selected workflow state paths."""

    input_states: list[str]

    def extract_input_states(self, states: DataMap) -> DataMap:
        """Copy selected values and use None for missing final keys.

        Raise KeyError if an intermediate value is missing or is not an object.
        """
        inputs: DataMap = {}
        # Copy parent paths before adding any missing child keys.
        for path in sorted(self.input_states, key=lambda path: path.count(".")):
            keys = path.split(".")
            source = states
            for key in keys[:-1]:
                child = source.get(key)
                if not isinstance(child, dict):
                    raise KeyError(path)
                source = child

            target = inputs
            for key in keys[:-1]:
                target = cast(DataMap, target.setdefault(key, {}))
            target[keys[-1]] = deepcopy(source.get(keys[-1]))
        return inputs


class ActionNode(_InputStateNode):
    description: str
    code: str
    output_states: list[str]
    type: Literal[NodeType.ACTION] = Field(default=NodeType.ACTION, frozen=True)

    def has_code(self) -> bool:
        """Report that this node type has a Python code field."""
        return True


class DecisionNode(_InputStateNode):
    description: str
    branches: list[str]
    code: str
    type: Literal[NodeType.DECISION] = Field(
        default=NodeType.DECISION, frozen=True
    )

    def has_code(self) -> bool:
        """Report that this node type has a Python code field."""
        return True

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require unique branch labels and reserve the default label."""
        if "default" in self.branches or len(set(self.branches)) != len(
            self.branches
        ):
            raise ValueError(
                "Decision branch labels must be unique and cannot include 'default'."
            )
        return self


class SignalNode(_InputStateNode):
    event_id: UUID
    type: Literal[NodeType.SIGNAL] = Field(default=NodeType.SIGNAL, frozen=True)


class WaitNode(BaseNode):
    duration: timedelta | None = None
    until: datetime | None = None
    event: EventSelector | None = None
    output_states: list[str] | None = None
    type: Literal[NodeType.WAIT] = Field(default=NodeType.WAIT, frozen=True)

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require one wait mode and allow output states only for event waits."""
        if (
            sum(
                value is not None
                for value in (self.duration, self.until, self.event)
            )
            != 1
        ):
            raise ValueError("Set exactly one of duration, until, or event.")
        if self.event is None and self.output_states is not None:
            raise ValueError("Only event waits can specify output_states.")
        return self


class ExecMetadata(BaseModel):
    """Store the data needed to resume a node execution."""

    pass


class RegionExecMetadata(ExecMetadata):
    """Store the index of the current collection item for a region execution."""

    item_idx: int = Field(default=0, ge=0, strict=True)


class RegionNode(BaseNode):
    nodes: list[Node]
    edges: list[Edge]
    collection_state: str | None = None
    item_state: str | None = None
    type: Literal[NodeType.REGION] = Field(default=NodeType.REGION, frozen=True)

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require an item name for collections.

        An item name must be nonempty and contain no dots.
        """
        if self.collection_state is not None and self.item_state is None:
            raise ValueError("collection_state requires item_state.")
        if self.item_state is not None and (
            not self.item_state or "." in self.item_state
        ):
            raise ValueError("item_state must be nonempty and contain no dots.")
        return self


Node: TypeAlias = Annotated[
    ActionNode | DecisionNode | SignalNode | WaitNode | RegionNode,
    Field(discriminator="type"),
]

RegionNode.model_rebuild()
