"""add apartment field to requests

Revision ID: add_apartment_requests
Revises: b7c6a987ba0c
Create Date: 2025-11-16 15:50:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "add_apartment_requests"
down_revision: Union[str, Sequence[str], None] = "b7c6a987ba0c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column('requests', sa.Column('apartment', sa.String(length=50), nullable=True))


def downgrade() -> None:
    op.drop_column('requests', 'apartment')

