from __future__ import annotations

import re

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import Request
from app.utils.timezone import now_moscow


async def generate_request_number(session: AsyncSession) -> str:
    """Генерирует уникальный номер заявки формата RQ-YYYYMMDD-XXXX.
    
    Находит максимальный номер за сегодня и инкрементирует его.
    Если номер уже существует (race condition), повторяет попытку.
    """
    today = now_moscow().strftime("%Y%m%d")
    prefix = f"RQ-{today}-"
    
    # Находим максимальный номер за сегодня
    stmt = (
        select(Request.number)
        .where(Request.number.like(f"{prefix}%"))
        .order_by(Request.number.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    max_number = result.scalar_one_or_none()
    
    if max_number:
        # Извлекаем число из номера (последние 4 цифры после последнего дефиса)
        match = re.search(rf"{re.escape(prefix)}(\d+)$", max_number)
        if match:
            last_num = int(match.group(1))
            next_num = last_num + 1
        else:
            # Если формат неожиданный, используем подсчет
            count_stmt = select(func.count(Request.id)).where(Request.number.like(f"{prefix}%"))
            count_result = await session.execute(count_stmt)
            next_num = count_result.scalar_one() + 1
    else:
        # Нет заявок за сегодня, начинаем с 0001
        next_num = 1
    
    # Проверяем, что номер уникален (защита от race condition)
    max_attempts = 100
    for attempt in range(max_attempts):
        candidate = f"{prefix}{next_num:04d}"
        
        # Проверяем существование номера
        check_stmt = select(Request.id).where(Request.number == candidate).limit(1)
        check_result = await session.execute(check_stmt)
        if check_result.scalar_one_or_none() is None:
            return candidate
        
        # Номер существует, пробуем следующий
        next_num += 1
    
    # Если все попытки исчерпаны (крайне маловероятно), используем timestamp
    import time
    timestamp_suffix = str(int(time.time()))[-4:]
    return f"{prefix}{timestamp_suffix}"
