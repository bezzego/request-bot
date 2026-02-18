"""Общие утилиты для обработчиков инженера."""
from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, User
from app.infrastructure.db.session import async_session
from app.utils.advanced_filters import build_filter_conditions, format_filter_label
from app.utils.request_filters import format_date_range_label
from datetime import datetime

REQUESTS_PAGE_SIZE = 10


async def get_engineer(session, telegram_id: int) -> User | None:
    """Получает пользователя, который может быть инженером (ENGINEER, SPECIALIST или MANAGER с is_super_admin)."""
    user = await session.scalar(
        select(User)
        .options(selectinload(User.leader_profile))
        .where(User.telegram_id == telegram_id)
    )
    if not user:
        return None
    
    from app.infrastructure.db.models import UserRole
    is_engineer = user.role == UserRole.ENGINEER
    is_specialist = user.role == UserRole.SPECIALIST
    is_super_admin = (
        user.role == UserRole.MANAGER 
        and user.leader_profile 
        and user.leader_profile.is_super_admin
    )
    
    if is_engineer or is_specialist or is_super_admin:
        return user
    
    return None


def engineer_filter_conditions(filter_payload: dict[str, Any] | None) -> list:
    """Строит условия фильтрации для заявок инженера."""
    if not filter_payload:
        return []
    
    # Поддержка старого формата фильтра для обратной совместимости
    if "mode" in filter_payload:
        mode = (filter_payload.get("mode") or "").strip().lower()
        value = (filter_payload.get("value") or "").strip()
        conditions: list = []
        if mode == "адрес" and value:
            conditions.append(func.lower(Request.address).like(f"%{value.lower()}%"))
        elif mode == "дата":
            start = filter_payload.get("start")
            end = filter_payload.get("end")
            if start and end:
                try:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                    conditions.append(Request.created_at.between(start_dt, end_dt))
                except ValueError:
                    pass
        return conditions
    
    # Новый формат фильтра
    return build_filter_conditions(filter_payload)


def engineer_filter_label(filter_payload: dict[str, Any] | None) -> str:
    """Форматирует описание фильтра для отображения."""
    if not filter_payload:
        return ""
    
    # Поддержка старого формата фильтра для обратной совместимости
    if "mode" in filter_payload:
        mode = (filter_payload.get("mode") or "").strip().lower()
        if mode == "адрес":
            value = (filter_payload.get("value") or "").strip()
            return f"адрес: {value}" if value else ""
        if mode == "дата":
            start = filter_payload.get("start")
            end = filter_payload.get("end")
            if start and end:
                try:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                    return f"дата: {format_date_range_label(start_dt, end_dt)}"
                except ValueError:
                    return ""
        return ""
    
    # Новый формат фильтра
    return format_filter_label(filter_payload)


def engineer_filter_menu_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура меню фильтра инженера."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🏠 По адресу", callback_data="eng:flt:mode:address")
    builder.button(text="📅 По дате", callback_data="eng:flt:mode:date")
    builder.button(text="🗓 Сегодня", callback_data="eng:flt:quick:today")
    builder.button(text="7 дней", callback_data="eng:flt:quick:7d")
    builder.button(text="30 дней", callback_data="eng:flt:quick:30d")
    builder.button(text="Этот месяц", callback_data="eng:flt:quick:this_month")
    builder.button(text="Прошлый месяц", callback_data="eng:flt:quick:prev_month")
    builder.button(text="♻️ Сбросить фильтр", callback_data="eng:flt:clear")
    builder.button(text="✖️ Отмена", callback_data="eng:flt:cancel")
    builder.adjust(2)
    return builder.as_markup()


def engineer_filter_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура отмены фильтра."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✖️ Отмена", callback_data="eng:flt:cancel")
    builder.adjust(1)
    return builder.as_markup()
