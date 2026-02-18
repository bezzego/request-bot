"""Модуль списка заявок специалиста."""
from __future__ import annotations

import html
import logging
from typing import Any

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestStatus
from app.infrastructure.db.session import async_session
from app.utils.pagination import clamp_page, total_pages_for
from app.utils.request_formatters import STATUS_TITLES, format_request_label
from app.handlers.specialist.utils import (
    get_specialist,
    is_super_admin,
    REQUESTS_PAGE_SIZE,
)
# Импортируем функции фильтрации из модуля filters
from app.handlers.specialist.filters.utils import (
    specialist_filter_conditions as _specialist_filter_conditions,
    specialist_filter_label as _specialist_filter_label,
)

logger = logging.getLogger(__name__)

router = Router()


async def fetch_specialist_requests_page(
    session,
    specialist_id: int,
    page: int,
    filter_payload: dict[str, Any] | None = None,
    is_super_admin: bool = False,
    filter_scope: str | None = None,
) -> tuple[list[Request], int, int, int]:
    """Получает страницу заявок специалиста."""
    logger.info(f"[FETCH REQUESTS] Fetching page {page} for specialist_id {specialist_id}, is_super_admin: {is_super_admin}, filter_scope: {filter_scope}")
    
    base_conditions = []
    if is_super_admin:
        if filter_scope == "all":
            logger.info(f"[FETCH REQUESTS] Super admin mode - showing ALL requests")
        else:
            base_conditions.append(Request.specialist_id == specialist_id)
            logger.info(f"[FETCH REQUESTS] Super admin mode - showing OWN requests")
    else:
        base_conditions.append(Request.specialist_id == specialist_id)
    
    conditions = _specialist_filter_conditions(filter_payload)
    all_conditions = base_conditions + conditions
    
    count_query = select(func.count()).select_from(Request).where(*all_conditions)
    total = await session.scalar(count_query)
    total = int(total or 0)
    
    total_pages = total_pages_for(total, REQUESTS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    
    select_query = (
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.engineer),
            selectinload(Request.master),
            selectinload(Request.work_items),
        )
        .where(*all_conditions)
        .order_by(Request.created_at.desc())
        .limit(REQUESTS_PAGE_SIZE)
        .offset(page * REQUESTS_PAGE_SIZE)
    )
    
    result = await session.execute(select_query)
    requests = list(result.scalars().all())
    
    return requests, page, total_pages, total


async def show_specialist_requests_list(
    message: Message,
    session,
    specialist_id: int,
    page: int,
    *,
    context: str = "list",
    filter_payload: dict[str, Any] | None = None,
    edit: bool = False,
    is_super_admin: bool = False,
    filter_scope: str | None = None,
) -> None:
    """Показывает список заявок специалиста."""
    requests, page, total_pages, total = await fetch_specialist_requests_page(
        session,
        specialist_id,
        page,
        filter_payload=filter_payload,
        is_super_admin=is_super_admin,
        filter_scope=filter_scope,
    )

    if not requests:
        text = (
            "Заявок по заданному фильтру не найдено."
            if context == "filter"
            else "У вас пока нет заявок. Создайте первую через «➕ Создать заявку»."
        )
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    builder = InlineKeyboardBuilder()
    ctx_key = "filter" if context == "filter" else "list"
    start_index = page * REQUESTS_PAGE_SIZE
    for idx, req in enumerate(requests, start=start_index + 1):
        status = STATUS_TITLES.get(req.status, req.status.value)
        if context == "filter":
            detail_cb = f"spec:detail:{req.id}:f:{page}"
        else:
            detail_cb = f"spec:detail:{req.id}:{page}"
        builder.button(
            text=f"{idx}. {format_request_label(req)} · {status}",
            callback_data=detail_cb,
        )
        if req.status != RequestStatus.CLOSED:
            builder.button(text="🗑", callback_data=f"spec:delete:{req.id}:{ctx_key}:{page}")
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"spec:{'filter' if context == 'filter' else 'list'}:{page - 1}",
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:noop"))
        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"spec:{'filter' if context == 'filter' else 'list'}:{page + 1}",
                )
            )
        builder.row(*nav)

    if context == "filter":
        label = _specialist_filter_label(filter_payload)
        header = "Результаты фильтрации. Выберите заявку:"
        if label:
            header = f"{header}\n\n<b>Фильтр:</b>\n{html.escape(label)}"
    else:
        header = "Выберите заявку, чтобы посмотреть подробности и актуальный статус."
    footer = f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    text = f"{header}{footer}"

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


@router.message(F.text == "📄 Мои заявки")
async def specialist_requests(message: Message, state: FSMContext):
    """Обработчик команды просмотра списка заявок."""
    async with async_session() as session:
        specialist = await get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        is_super = is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        await show_specialist_requests_list(
            message, session, specialist.id, page=0, is_super_admin=is_super, filter_scope=filter_scope
        )


@router.callback_query(F.data.startswith("spec:list:"))
async def specialist_requests_page(callback: CallbackQuery, state: FSMContext):
    """Навигация по страницам списка заявок (без фильтра)."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    
    await state.update_data(spec_filter=None)
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        await show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=page,
            context="list",
            filter_payload=None,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:filter:"))
async def specialist_filter_page(callback: CallbackQuery, state: FSMContext):
    """Навигация по страницам отфильтрованного списка заявок."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    
    data = await state.get_data()
    filter_payload = data.get("spec_filter")
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = is_super_admin(specialist)
        filter_scope = data.get("filter_scope") if is_super else None
        await show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=page,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.callback_query(F.data == "spec:noop")
async def specialist_noop(callback: CallbackQuery):
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()


