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
        """Require an object schema and check region item names.

        New state names can conflict with item names in the stored root.
        Both state and root assignment check this rule, in either order.
        """
        if states.get("type") != "object":
            raise ValueError(
                "Workflow.states must declare an object at its root."
            )
        if self.root_json is not None:
            self._validate_item_states(
                self.get_root(), set(states.get("properties", {}))
            )
        return states

    @validates("root_json")
    def validate_root_json(self, key: str, root_json: dict) -> dict:
        """Parse the root, check item names against states, and return JSON."""
        root = RegionNode.model_validate(root_json)
        if self.states is not None:
            self._validate_item_states(
                root, set(self.states.get("properties", {}))
            )
        return root.model_dump(mode="json")

    @staticmethod
    def _validate_item_states(
        region: RegionNode, names_in_scope: set[str]
    ) -> None:
        """Reject item names that match workflow states or outer region items."""
        if region.item_state is not None:
            if region.item_state in names_in_scope:
                raise ValueError(
                    f"item_state {region.item_state!r} conflicts with a name in scope."
                )
            names_in_scope = names_in_scope | {region.item_state}

        for node in region.nodes:
            if isinstance(node, RegionNode):
                Workflow._validate_item_states(node, names_in_scope)
