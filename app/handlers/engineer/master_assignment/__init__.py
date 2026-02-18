"""Модуль назначения мастера на заявку инженером."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select

from app.infrastructure.db.models import User, UserRole
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.utils.request_formatters import format_request_label
from app.handlers.engineer.utils import get_engineer
from app.handlers.engineer.detail import load_request, refresh_request_detail

router = Router()


@router.callback_query(F.data.startswith("eng:assign_master:"))
async def engineer_assign_master(callback: CallbackQuery):
    """Показать список мастеров для назначения на заявку."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        masters = (
            (
                await session.execute(
                    select(User).where(User.role == UserRole.MASTER).order_by(User.full_name)
                )
            )
            .scalars()
            .all()
        )

    if not masters:
        await callback.answer("Активных мастеров нет. Обратитесь к руководителю.", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    for master in masters:
        builder.button(
            text=f"{master.full_name}",
            callback_data=f"eng:pick_master:{request_id}:{master.id}",
        )
    builder.button(text="⬅️ Назад", callback_data=f"eng:detail:{request_id}")
    builder.adjust(1)

    await callback.message.edit_text("Выберите мастера для заявки:", reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("eng:pick_master:"))
async def engineer_pick_master(callback: CallbackQuery):
    """Назначить выбранного мастера на заявку."""
    _, _, request_id_str, master_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    master_id = int(master_id_str)

    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        master = await session.scalar(select(User).where(User.id == master_id, User.role == UserRole.MASTER))
        if not master:
            await callback.answer("Мастер не найден.", show_alert=True)
            return

        await RequestService.assign_master(
            session,
            request,
            master_id=master.id,
            assigned_by=engineer.id,
        )
        await session.commit()

    try:
        await callback.bot.send_message(
            chat_id=master.telegram_id,
            text=(
                f"Вам назначена заявка {format_request_label(request)}.\n"
                f"Объект: {request.object.name if request.object else request.address}."
            ),
        )
    except Exception:
        # Игнорируем ошибки отправки уведомления
        pass

    await callback.answer("Мастер назначен.")
    if callback.bot:
        await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
