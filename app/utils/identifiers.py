from __future__ import annotations

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import Request


async def generate_request_number(session: AsyncSession) -> str:
    """Генерирует уникальный номер заявки формата RQ-YYYYMMDD-XXXX."""
    today = datetime.now().strftime("%Y%m%d")
    stmt = (
        select(func.count(Request.id))
        .where(Request.number.like(f"RQ-{today}-%"))
    )
    result = await session.execute(stmt)
    count_for_today = result.scalar_one()
    return f"RQ-{today}-{count_for_today + 1:04d}"
