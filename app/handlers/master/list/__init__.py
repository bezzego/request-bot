"""Модуль списка заявок мастера."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request
from app.infrastructure.db.session import async_session
from app.utils.pagination import clamp_page, total_pages_for
from app.utils.request_formatters import format_request_label, STATUS_TITLES
from app.handlers.master.utils import get_master

router = Router()
REQUESTS_PAGE_SIZE = 10


async def fetch_master_requests_page(
    session,
    master_id: int,
    page: int,
) -> tuple[list[Request], int, int, int]:
    """Получить страницу заявок мастера."""
    conditions = [Request.master_id == master_id]
    total = await session.scalar(select(func.count()).select_from(Request).where(*conditions))
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
                    selectinload(Request.work_sessions),
                    selectinload(Request.photos),
                    selectinload(Request.engineer),
                )
                .where(*conditions)
                .order_by(Request.created_at.desc())
                .limit(REQUESTS_PAGE_SIZE)
                .offset(page * REQUESTS_PAGE_SIZE)
            )
        )
        .scalars()
        .all()
    )
    return requests, page, total_pages, total


async def show_master_requests_list(
    message: Message,
    session,
    master_id: int,
    page: int,
    *,
    edit: bool = False,
) -> None:
    """Показать список заявок мастера."""
    requests, page, total_pages, total = await fetch_master_requests_page(session, master_id, page)

    if not requests:
        text = "У вас пока нет назначенных заявок. Ожидайте задач от инженера."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    builder = InlineKeyboardBuilder()
    start_index = page * REQUESTS_PAGE_SIZE
    for idx, req in enumerate(requests, start=start_index + 1):
        builder.button(
            text=f"{idx}. {format_request_label(req)} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"master:detail:{req.id}:{page}",
        )
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"master:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="master:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"master:list:{page + 1}"))
        builder.row(*nav)

    text = (
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт."
        f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    )

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


@router.message(F.text == "📥 Мои заявки")
async def master_requests(message: Message):
    """Обработчик команды просмотра списка заявок."""
    async with async_session() as session:
        master = await get_master(session, message.from_user.id)
        if not master:
            await message.answer("Эта функция доступна только мастерам.")
            return

        await show_master_requests_list(message, session, master.id, page=0)


@router.callback_query(F.data.startswith("master:list:"))
async def master_requests_page(callback: CallbackQuery):
    """Обработчик пагинации списка заявок."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await show_master_requests_list(
            callback.message,
            session,
            master.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "master:noop")
async def master_noop(callback: CallbackQuery):
    """Пустой обработчик для кнопки пагинации."""
    await callback.answer()
