from datetime import datetime
from enum import StrEnum
from uuid import UUID

from sqlalchemy import JSON, CheckConstraint, DateTime, Enum, ForeignKey, column
from sqlalchemy.orm import Mapped, mapped_column, validates

from database import Base
from nodes import DataMap


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


class WorkflowSnapshot(Base):
    __tablename__ = "workflow_snapshot"
    __table_args__ = (CheckConstraint(column("ref_count") >= 0),)

    workflow_id: Mapped[UUID] = mapped_column(primary_key=True)
    revision: Mapped[int] = mapped_column(primary_key=True)
    root: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    states: Mapped[dict] = mapped_column(JSON(none_as_null=True))
    ref_count: Mapped[int] = mapped_column(default=0, server_default="0")


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
    workflow_snapshot: Mapped[dict] = mapped_column(JSON(none_as_null=True))
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


class WorkflowEngine:
    """Orchestrates workflow executions and checkpoints their progress."""
