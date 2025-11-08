from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import Request
from app.utils.timezone import now_moscow


async def generate_request_number(session: AsyncSession) -> str:
    """Генерирует уникальный номер заявки формата RQ-YYYYMMDD-XXXX."""
    today = now_moscow().strftime("%Y%m%d")
    stmt = (
        select(func.count(Request.id))
        .where(Request.number.like(f"RQ-{today}-%"))
    )
    result = await session.execute(stmt)
    count_for_today = result.scalar_one()
    return f"RQ-{today}-{count_for_today + 1:04d}"
