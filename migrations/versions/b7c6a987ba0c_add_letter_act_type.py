"""add letter act type

Revision ID: b7c6a987ba0c
Revises: e979945ece44
Create Date: 2025-02-08 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "b7c6a987ba0c"
down_revision: Union[str, Sequence[str], None] = "e979945ece44"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TYPE acttype ADD VALUE IF NOT EXISTS 'LETTER'")


def downgrade() -> None:
    op.execute("DELETE FROM acts WHERE type = 'LETTER'")
    op.execute("ALTER TYPE acttype RENAME TO acttype_old")
    op.execute("CREATE TYPE acttype AS ENUM ('INSPECTION', 'COMPLETION')")
    op.execute("ALTER TABLE acts ALTER COLUMN type TYPE acttype USING type::text::acttype")
    op.execute("DROP TYPE acttype_old")
