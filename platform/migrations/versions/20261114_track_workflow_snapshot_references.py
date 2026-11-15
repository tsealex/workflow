"""Track shared workflow snapshot references

Revision ID: d5d60b6b5547
Revises: 24c10ad877c2
Create Date: 2026-11-14 23:41:09.448924

"""

from typing import Sequence, Union

from alembic import op
from alembic_utils.pg_function import PGFunction
from alembic_utils.pg_trigger import PGTrigger
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d5d60b6b5547"
down_revision: Union[str, Sequence[str], None] = "24c10ad877c2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


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
    $$
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


def upgrade() -> None:
    """Add the reference count trigger and remove the per-execution snapshot."""
    op.create_entity(snapshot_ref_count_function)
    op.create_entity(snapshot_ref_count_trigger)
    op.drop_column("workflow_execution", "workflow_snapshot")


def downgrade() -> None:
    """Restore the per-execution snapshot column on an empty execution table."""
    op.add_column(
        "workflow_execution",
        sa.Column(
            "workflow_snapshot",
            postgresql.JSON(astext_type=sa.Text()),
            nullable=False,
        ),
    )
    op.drop_entity(snapshot_ref_count_trigger)
    op.drop_entity(snapshot_ref_count_function)
