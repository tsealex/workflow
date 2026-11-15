"""Define the database models and PostgreSQL triggers."""

from datetime import datetime
from enum import StrEnum
from typing import TypeAlias
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
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, validates

from nodes import DataMap, RegionNode


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={"ck": "%(table_name)s_%(column_0_name)s_check"}
    )


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

    def get_root(self) -> RegionNode:
        """Parse the stored root on first use and return the cached node."""
        if self._root is None:
            self._root = RegionNode.model_validate(self.root)
        assert self._root is not None
        return self._root

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
    status: Mapped[ExecutionStatus] = mapped_column(execution_status_type)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    inputs: Mapped[dict | None] = mapped_column(JSON(none_as_null=True))
    executor_id: Mapped[UUID | None]
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# SQLAlchemy reserves the metadata name during declarative class setup.
NodeExecution.metadata = mapped_column(JSON(none_as_null=True), nullable=True)


class WorkflowExecutorRegistration(Base):
    """Store an executor identity and the hash of its token."""

    __tablename__ = "workflow_executor"

    id: Mapped[UUID] = mapped_column(primary_key=True)
    token_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_connected_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
