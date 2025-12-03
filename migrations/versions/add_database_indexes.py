"""add database indexes

Revision ID: add_database_indexes
Revises: add_apartment_requests
Create Date: 2025-01-20 12:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "add_database_indexes"
down_revision: Union[str, Sequence[str], None] = "add_apartment_requests"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Индексы для таблицы requests
    op.create_index("ix_requests_status", "requests", ["status"])
    op.create_index("ix_requests_specialist_id", "requests", ["specialist_id"])
    op.create_index("ix_requests_engineer_id", "requests", ["engineer_id"])
    op.create_index("ix_requests_master_id", "requests", ["master_id"])
    op.create_index("ix_requests_created_at", "requests", ["created_at"])
    op.create_index("ix_requests_due_at", "requests", ["due_at"])
    
    # Составные индексы для оптимизации частых запросов
    op.create_index("ix_requests_specialist_created", "requests", ["specialist_id", "created_at"])
    op.create_index("ix_requests_engineer_status", "requests", ["engineer_id", "status"])
    op.create_index("ix_requests_master_status", "requests", ["master_id", "status"])
    op.create_index("ix_requests_status_created", "requests", ["status", "created_at"])
    op.create_index("ix_requests_due_at_status", "requests", ["due_at", "status"])
    
    # Индекс для таблицы users (роль)
    op.create_index("ix_users_role", "users", ["role"])


def downgrade() -> None:
    # Удаляем индексы в обратном порядке
    op.drop_index("ix_users_role", table_name="users")
    op.drop_index("ix_requests_due_at_status", table_name="requests")
    op.drop_index("ix_requests_status_created", table_name="requests")
    op.drop_index("ix_requests_master_status", table_name="requests")
    op.drop_index("ix_requests_engineer_status", table_name="requests")
    op.drop_index("ix_requests_specialist_created", table_name="requests")
    op.drop_index("ix_requests_due_at", table_name="requests")
    op.drop_index("ix_requests_created_at", table_name="requests")
    op.drop_index("ix_requests_master_id", table_name="requests")
    op.drop_index("ix_requests_engineer_id", table_name="requests")
    op.drop_index("ix_requests_specialist_id", table_name="requests")
    op.drop_index("ix_requests_status", table_name="requests")

