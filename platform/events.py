from datetime import datetime, timedelta
from typing import Self
from uuid import UUID

from pydantic import BaseModel, model_validator
from pydantic.json_schema import JsonSchemaValue

from nodes import DataMap


class Event(BaseModel):
    id: UUID
    name: str
    payload: JsonSchemaValue
    description: str | None = None

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require the payload schema to declare an object at its root."""
        if self.payload.get("type") != "object":
            raise ValueError(
                "Event.payload must declare an object at its root."
            )
        return self


class EventInstance(BaseModel):
    event_id: UUID
    instance_id: UUID
    xid: UUID
    notified_at: datetime
    payload: DataMap

    @model_validator(mode="after")
    def validate_constraints(self) -> Self:
        """Require a notification time with a UTC offset of zero."""
        if self.notified_at.utcoffset() != timedelta(0):
            raise ValueError("notified_at must be in UTC.")
        return self
