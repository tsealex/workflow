from uuid import UUID

from pydantic.json_schema import JsonSchemaValue
from sqlalchemy import JSON
from sqlalchemy.orm import Mapped, mapped_column, validates

from database import Base
from nodes import RegionNode


class Workflow(Base):
    __tablename__ = "workflow"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    root_json: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    states: Mapped[JsonSchemaValue] = mapped_column(JSON(none_as_null=True))
    revision: Mapped[int]
    name: Mapped[str | None]
    description: Mapped[str | None]

    def get_root(self) -> RegionNode:
        return RegionNode.model_validate(self.root_json)

    def set_root(self, root: RegionNode) -> None:
        self.root_json = root.model_dump(mode="json")

    @validates("states")
    def validate_states(
        self, key: str, states: JsonSchemaValue
    ) -> JsonSchemaValue:
        """Require the state schema to declare an object at its root."""
        if states.get("type") != "object":
            raise ValueError(
                "Workflow.states must declare an object at its root."
            )
        return states

    @validates("root_json")
    def validate_root_json(self, key: str, root_json: dict) -> dict:
        """Validate the root as a RegionNode and return JSON-compatible data."""
        root = RegionNode.model_validate(root_json)
        return root.model_dump(mode="json")
