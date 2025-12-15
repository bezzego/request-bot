"""Ensure apartment column exists on requests."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "ensure_apartment_column"
down_revision: Union[str, Sequence[str], None] = "add_database_indexes"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # idempotent: add column only if it is missing
    op.execute("ALTER TABLE requests ADD COLUMN IF NOT EXISTS apartment VARCHAR(50)")


def downgrade() -> None:
    # safe drop
    op.drop_column("requests", "apartment")

