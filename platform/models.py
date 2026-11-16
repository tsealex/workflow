"""Define the database models and PostgreSQL triggers."""

from datetime import UTC, datetime
from enum import StrEnum
from typing import Self, TypeAlias
from uuid import UUID

from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
from jsonschema import Draft202012Validator
from pydantic.json_schema import JsonSchemaValue
from sqlalchemy import (
    JSON,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    MetaData,
    String,
    column,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, validates
from sqlalchemy.ext.mutable import MutableList
from sqlalchemy.types import TypeDecorator

import execution_pb2 as messages
from nodes import DataMap, Node, RegionNode


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={"ck": "%(table_name)s_%(column_0_name)s_check"}
    )


class UUIDList(TypeDecorator[list[UUID]]):
    """Store UUID lists as JSON and restore UUID values on load."""

    impl = JSON(none_as_null=True)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        """Convert UUID values to JSON strings."""
        return [str(item) for item in value] if value is not None else None

    def process_result_value(self, value, dialect):
        """Restore UUID values from JSON strings."""
        return [UUID(item) for item in value] if value is not None else None


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

    def validate_states(self, inputs: DataMap) -> None:
        """Raise ValidationError if the inputs do not match the state schema."""
        Draft202012Validator(self.states).validate(inputs)


class ExecutionStatus(StrEnum):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUSPENDED = "SUSPENDED"
    INTERRUPTED = "INTERRUPTED"
    COMPLETED = "COMPLETED"

    @classmethod
    def from_proto(cls, value: messages.ExecutionStatus.ValueType) -> Self:
        """Convert a protobuf status; raise ValueError for an unsupported value."""
        return cls(messages.ExecutionStatus.Name(value))


TerminalExecutionStatus: set[ExecutionStatus] = {
    ExecutionStatus.COMPLETED,
    ExecutionStatus.FAILED,
    ExecutionStatus.INTERRUPTED,
}


execution_status_type = Enum(
    ExecutionStatus,
    name=None,
    native_enum=False,
    create_constraint=True,
    validate_strings=True,
)


WorkflowSnapshotID: TypeAlias = tuple[UUID, int]


class WorkflowSnapshot(Base):
    __tablename__ = "workflow_snapshot"
    __table_args__ = (CheckConstraint(column("ref_count") >= 0),)
    __allow_unmapped__ = True

    workflow_id: Mapped[UUID] = mapped_column(primary_key=True)
    revision: Mapped[int] = mapped_column(primary_key=True)
    root: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    states: Mapped[JsonSchemaValue] = mapped_column(JSON(none_as_null=True))
    ref_count: Mapped[int] = mapped_column(default=0, server_default="0")
    _root: RegionNode | None = None
    _node_map: dict[UUID, Node]

    def get_root(self) -> RegionNode:
        """Parse and index the stored nodes on first use."""
        if self._root is None:
            root = RegionNode.model_validate(self.root)
            node_map: dict[UUID, Node] = {}
            pending: list[Node] = [root]
            while pending:
                node = pending.pop()
                node_map.setdefault(node.id, node)
                if isinstance(node, RegionNode):
                    pending.extend(node.nodes)
            self._node_map = node_map
            self._root = root
        return self._root

    def get_node(self, node_id: UUID) -> Node:
        """Return the node with `node_id` or raise KeyError."""
        self.get_root()
        return self._node_map[node_id]

    def validate_states(self, inputs: DataMap) -> None:
        """Raise ValidationError if the inputs do not match the state schema."""
        Draft202012Validator(self.states).validate(inputs)


snapshot_ref_count_function = PGFunction(
    schema="public",
    signature="update_workflow_snapshot_ref_count()",
    definition="""
    RETURNS trigger
    LANGUAGE plpgsql
    AS $$
    DECLARE
        old_active boolean := false;
        new_active boolean := false;
        snapshot_workflow_id uuid;
        snapshot_revision integer;
        reference_change integer;
    BEGIN
        IF TG_OP <> 'INSERT' THEN
            old_active := OLD.status IN ('RUNNING', 'SUSPENDED');
        END IF;
        IF TG_OP <> 'DELETE' THEN
            new_active := NEW.status IN ('RUNNING', 'SUSPENDED');
        END IF;

        IF old_active = new_active THEN
            RETURN NULL;
        END IF;

        IF old_active THEN
            snapshot_workflow_id := OLD.workflow_id;
            snapshot_revision := OLD.workflow_revision;
            reference_change := -1;
        ELSE
            snapshot_workflow_id := NEW.workflow_id;
            snapshot_revision := NEW.workflow_revision;
            reference_change := 1;
        END IF;

        UPDATE public.workflow_snapshot
        SET ref_count = ref_count + reference_change
        WHERE workflow_id = snapshot_workflow_id AND revision = snapshot_revision;

        IF NOT FOUND THEN
            RAISE foreign_key_violation USING MESSAGE = format(
                'Workflow snapshot %s at revision %s does not exist.',
                snapshot_workflow_id, snapshot_revision
            );
        END IF;
        RETURN NULL;
    END;
    $$;
    """,
)


snapshot_ref_count_trigger = PGTrigger(
    schema="public",
    signature="workflow_execution_snapshot_ref_count",
    on_entity="public.workflow_execution",
    definition=(
        "AFTER INSERT OR UPDATE OF status OR DELETE "
        "ON public.workflow_execution FOR EACH ROW "
        "EXECUTE FUNCTION public.update_workflow_snapshot_ref_count()"
    ),
)


class WorkflowExecution(Base):
    __tablename__ = "workflow_execution"
    __table_args__ = (
        CheckConstraint(
            "status <> 'INTERRUPTED'",
            name="workflow_execution_status_check1",
        ),
    )

    xid: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_id: Mapped[UUID]
    workflow_revision: Mapped[int]
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_type)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    states: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def set_status(self, status: ExecutionStatus) -> None:
        """Set the status and keep the first end time until execution resumes."""
        self.status = status
        if status in TerminalExecutionStatus:
            if self.ended_at is None:
                self.ended_at = datetime.now(UTC)
        else:
            self.ended_at = None

    @validates("status")
    def validate_status(
        self, key: str, value: ExecutionStatus | str
    ) -> ExecutionStatus:
        """Convert the status to ExecutionStatus and reject INTERRUPTED."""
        status = ExecutionStatus(value)
        if status == ExecutionStatus.INTERRUPTED:
            raise ValueError("INTERRUPTED applies only to node executions.")
        return status


class NodeExecution(Base):
    __tablename__ = "node_execution"

    xid: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_xid: Mapped[UUID] = mapped_column(
        ForeignKey(WorkflowExecution.xid)
    )
    node_id: Mapped[UUID]
    prev_xids: Mapped[list[UUID]] = mapped_column(
        MutableList.as_mutable(UUIDList()),
        default=list,
        server_default=text("'[]'"),
    )
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_type)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    inputs: Mapped[dict | None] = mapped_column(JSON(none_as_null=True))
    exec_metadata: Mapped[dict | None] = mapped_column(
        "metadata", JSON(none_as_null=True)
    )
    executor_id: Mapped[UUID | None]
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    def set_status(self, status: ExecutionStatus) -> None:
        """Set the status and keep the first end time until execution resumes."""
        self.status = status
        if status in TerminalExecutionStatus:
            if self.ended_at is None:
                self.ended_at = datetime.now(UTC)
        else:
            self.ended_at = None


class WorkflowExecutorRegistration(Base):
    """Store an executor identity and the hash of its token."""

    __tablename__ = "workflow_executor"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_connected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
