"""Утилиты для модулей мастера."""
from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, User
from app.infrastructure.db.session import async_session


async def get_master(session, telegram_id: int) -> User | None:
    """Получить мастера по telegram_id."""
    return await session.scalar(
        select(User).where(
            User.telegram_id == str(telegram_id),
            User.role == "master",
        )
    )


async def load_request(session, master_id: int, request_id: int) -> Request | None:
    """Загрузить заявку мастера с полными связями."""
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.work_sessions),
            selectinload(Request.photos),
            selectinload(Request.engineer),
        )
        .where(Request.id == request_id, Request.master_id == master_id)
    )
