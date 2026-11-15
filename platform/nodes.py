from __future__ import annotations

import json
from datetime import datetime, timedelta
from enum import StrEnum
from math import isfinite
from typing import Annotated, Literal, Self, TypeAlias
from uuid import UUID, uuid4

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


class ActionNode(BaseModel):
    id: UUID
    name: str
    description: str
    code: str
    input_states: list[str]
    output_states: list[str]
    type: Literal["action"] = Field(default="action", frozen=True)


class DecisionNode(BaseModel):
    id: UUID
    name: str
    description: str
    branches: list[str]
    code: str
    input_states: list[str]
    type: Literal["decision"] = Field(default="decision", frozen=True)

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


class SignalNode(BaseModel):
    id: UUID
    name: str
    event_id: UUID
    input_states: list[str]
    type: Literal["signal"] = Field(default="signal", frozen=True)


class WaitNode(BaseModel):
    id: UUID
    name: str
    duration: timedelta | None = None
    until: datetime | None = None
    event: EventSelector | None = None
    output_states: list[str] | None = None
    type: Literal["wait"] = Field(default="wait", frozen=True)

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


class _RegionBoundary(BaseModel):
    id: UUID = Field(default_factory=uuid4)


class RegionNode(BaseModel):
    id: UUID
    name: str
    nodes: list[Node]
    edges: list[Edge]
    initial: _RegionBoundary = Field(default_factory=_RegionBoundary)
    final: _RegionBoundary = Field(default_factory=_RegionBoundary)
    collection_state: str | None = None
    item_state: str | None = None
    type: Literal["region"] = Field(default="region", frozen=True)

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require distinct boundary IDs and an item name for collections.

        An item name must be nonempty and contain no dots.
        """
        if self.initial.id == self.final.id:
            raise ValueError("Initial and final nodes must have distinct IDs.")
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
