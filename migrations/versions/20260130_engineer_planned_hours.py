"""Add requests.engineer_planned_hours for manual planned hours by engineer."""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "eng_planned_hrs_20260130"
down_revision: Union[str, Sequence[str], None] = "req_title_1024_20260121"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "requests",
        sa.Column("engineer_planned_hours", sa.Numeric(10, 2), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("requests", "engineer_planned_hours")
