"""Increase requests.title length to 1024.

Это нужно, чтобы в заголовке заявки могли помещаться длинные формулировки дефектов.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
# Важно: длина значения должна быть <= 32 символов,
# так как столбец alembic_version.version_num имеет тип VARCHAR(32).
revision: str = "req_title_1024_20260121"
down_revision: Union[str, Sequence[str], None] = "ensure_apartment_column"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Увеличиваем длину столбца title в таблице requests
    op.alter_column(
        "requests",
        "title",
        existing_type=sa.String(length=255),
        type_=sa.String(length=1024),
        existing_nullable=False,
    )


def downgrade() -> None:
    # Возвращаемся к прежнему ограничению 255 символов
    op.alter_column(
        "requests",
        "title",
        existing_type=sa.String(length=1024),
        type_=sa.String(length=255),
        existing_nullable=False,
    )

