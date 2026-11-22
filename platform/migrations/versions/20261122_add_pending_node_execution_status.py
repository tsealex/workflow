"""add pending node execution status

Revision ID: c02d51ebcfb2
Revises: 37b9ce9a53d7
Create Date: 2026-11-22 10:33:43.328216

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c02d51ebcfb2"
down_revision: Union[str, Sequence[str], None] = "37b9ce9a53d7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Allow new node executions to start pending."""
    op.drop_constraint("executionstatus", "node_execution", type_="check")
    op.create_check_constraint(
        "executionstatus",
        "node_execution",
        "status IN ('PENDING', 'RUNNING', 'FAILED', 'SUSPENDED', "
        "'INTERRUPTED', 'COMPLETED')",
    )
    op.alter_column("node_execution", "status", server_default="PENDING")


def downgrade() -> None:
    """Restore the previous initial status for pending node executions."""
    op.alter_column("node_execution", "status", server_default=None)
    op.execute(
        "UPDATE node_execution SET status = 'SUSPENDED' WHERE status = 'PENDING'"
    )
    op.drop_constraint("executionstatus", "node_execution", type_="check")
    op.create_check_constraint(
        "executionstatus",
        "node_execution",
        "status IN ('RUNNING', 'FAILED', 'SUSPENDED', 'INTERRUPTED', 'COMPLETED')",
    )
