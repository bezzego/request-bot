"""Модуль списка заявок инженера."""
from __future__ import annotations

import html
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
from app.handlers.engineer.utils import (
    get_engineer,
    engineer_filter_conditions,
    engineer_filter_label,
    REQUESTS_PAGE_SIZE,
)

router = Router()


async def fetch_engineer_requests_page(
    session,
    engineer_id: int,
    page: int,
    filter_payload: dict[str, Any] | None = None,
) -> tuple[list[Request], int, int, int]:
    """Получает страницу заявок инженера."""
    base_conditions = [Request.engineer_id == engineer_id]
    conditions = engineer_filter_conditions(filter_payload)
    all_conditions = base_conditions + conditions
    total = await session.scalar(select(func.count()).select_from(Request).where(*all_conditions))
    total = int(total or 0)
    total_pages = total_pages_for(total, REQUESTS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    requests = (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.master),
                )
                .where(*all_conditions)
                .order_by(Request.created_at.desc())
                .limit(REQUESTS_PAGE_SIZE)
                .offset(page * REQUESTS_PAGE_SIZE)
            )
        )
        .scalars()
        .all()
    )
    return requests, page, total_pages, total


async def show_engineer_requests_list(
    message: Message,
    session,
    engineer_id: int,
    page: int,
    *,
    context: str = "list",
    filter_payload: dict[str, Any] | None = None,
    edit: bool = False,
) -> None:
    """Показывает список заявок инженера."""
    requests, page, total_pages, total = await fetch_engineer_requests_page(
        session,
        engineer_id,
        page,
        filter_payload=filter_payload,
    )

    if not requests:
        text = (
            "Заявок по заданному фильтру не найдено."
            if context == "filter"
            else "У вас пока нет назначенных заявок. Ожидайте распределения."
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
        status_text = STATUS_TITLES.get(req.status, req.status.value)
        detail_cb = (
            f"eng:detail:{req.id}:f:{page}" if context == "filter" else f"eng:detail:{req.id}:{page}"
        )
        builder.button(
            text=f"{idx}. {format_request_label(req)} · {status_text}",
            callback_data=detail_cb,
        )
        if req.status != RequestStatus.CLOSED:
            builder.button(text="🗑", callback_data=f"eng:delete:{req.id}:{ctx_key}:{page}")
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"eng:{'filter' if context == 'filter' else 'list'}:{page - 1}",
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="eng:noop"))
        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"eng:{'filter' if context == 'filter' else 'list'}:{page + 1}",
                )
            )
        builder.row(*nav)

    if context == "filter":
        label = engineer_filter_label(filter_payload)
        header = "Результаты фильтрации. Выберите заявку:"
        if label:
            header = f"{header}\n\n<b>Фильтр:</b>\n{html.escape(label)}"
    else:
        header = "Выберите заявку, чтобы управлять этапами и бюджетом."
    footer = f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    text = f"{header}{footer}"

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


@router.message(F.text == "📋 Мои заявки")
async def engineer_requests(message: Message):
    """Обработчик команды просмотра списка заявок."""
    async with async_session() as session:
        engineer = await get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам, специалистам и суперадминам.")
            return

        await show_engineer_requests_list(message, session, engineer.id, page=0)


@router.callback_query(F.data.startswith("eng:list:"))
async def engineer_requests_page(callback: CallbackQuery):
    """Навигация по страницам списка заявок."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        await show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=page,
            context="list",
            filter_payload=None,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:filter:"))
async def engineer_filter_page(callback: CallbackQuery, state: FSMContext):
    """Навигация по страницам отфильтрованного списка заявок."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    
    data = await state.get_data()
    filter_payload = data.get("eng_filter")
    
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        await show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=page,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "eng:noop")
async def engineer_noop(callback: CallbackQuery):
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()
