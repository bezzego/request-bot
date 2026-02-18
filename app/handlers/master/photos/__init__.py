"""Модуль фотоотчетов мастера."""
from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Photo, PhotoType, Request, WorkSession
from app.infrastructure.db.session import async_session
from app.keyboards.master_kb import master_kb
from app.utils.request_formatters import format_request_label
from app.handlers.master.utils import get_master, load_request
from app.handlers.master.work.utils import notify_engineer

logger = logging.getLogger(__name__)

router = Router()


async def get_request_for_master(session, master_id: int, number: str) -> Request | None:
    """Получить заявку мастера по номеру."""
    return await session.scalar(
        select(Request)
        .options(selectinload(Request.engineer))
        .where(Request.number == number, Request.master_id == master_id)
    )


@router.message(F.text == "📸 Инструкция по фотоотчёту")
async def master_photo_instruction(message: Message):
    """Инструкция по фотоотчету для мастера."""
    await message.answer(
        "Для фиксации хода работ отправляйте фото с подписью вида:\n"
        "<code>RQ-123 описание фотографии</code>\n"
        "Бот автоматически сохранит фото в карточке заявки. Перед завершением работ\n"
        "обязательно приложите фото «до/после» и акт выполненных работ."
    )


@router.message(F.photo)
async def master_photo(message: Message):
    """Обработка фото в обычном режиме (не в процессе завершения работы)."""
    caption = (message.caption or "").strip()
    logger.debug("Master photo handler start: user=%s caption=%r", message.from_user.id, caption)

    async with async_session() as session:
        master = await get_master(session, message.from_user.id)
        if not master:
            logger.warning("Master photo: user %s is not a master", message.from_user.id)
            return

        request: Request | None = None
        comment: str | None = None
        number_hint: str | None = None

        # 1. Try caption RQ-... pattern
        if caption:
            parts = caption.split()
            number_hint = parts[0]
            if number_hint.upper().startswith("RQ-"):
                comment = " ".join(parts[1:]) if len(parts) > 1 else None
                request = await get_request_for_master(session, master.id, number_hint)
                if not request and number_hint[3:].isdigit():
                    alt = number_hint[3:]
                    logger.debug("Master photo: caption lookup failed, trying alt=%s", alt)
                    request = await get_request_for_master(session, master.id, alt)

        # 2. Try reply-to message (if user replied to card)
        if not request and message.reply_to_message:
            replied_text = message.reply_to_message.text or ""
            logger.debug("Master photo: reply_to text=%r", replied_text)
            for token in replied_text.split():
                if token.upper().startswith("RQ-"):
                    number_hint = token
                    break
                if token.isdigit():
                    number_hint = token
                    break
            if number_hint:
                request = await get_request_for_master(session, master.id, number_hint)
                if not request and number_hint.isdigit():
                    alt = f"RQ-{number_hint}"
                    request = await get_request_for_master(session, master.id, alt)

        # 3. Try active work session
        if not request:
            active_session = await session.scalar(
                select(WorkSession)
                .where(
                    WorkSession.master_id == master.id,
                    WorkSession.finished_at.is_(None),
                )
                .order_by(WorkSession.started_at.desc())
            )
            if active_session:
                request = await load_request(session, master.id, active_session.request_id)
                logger.debug("Master photo: using active session request_id=%s", active_session.request_id)

        # 4. Fallback to most recent assigned/in-progress request
        if not request:
            request = await session.scalar(
                select(Request)
                .options(selectinload(Request.engineer))
                .where(Request.master_id == master.id)
                .order_by(Request.updated_at.desc())
            )
            if request:
                logger.debug("Master photo: fallback to latest request %s", request.number)

        if not request:
            await message.answer(
                "Не удалось определить заявку. Добавьте подпись с номером вида «RQ-123 описание» "
                "или отправьте фото в ответ на карточку заявки."
            )
            logger.warning("Master photo: request not resolved for user=%s caption=%r", message.from_user.id, caption)
            return

        photo = message.photo[-1]
        new_photo = Photo(
            request_id=request.id,
            type=PhotoType.PROCESS,
            file_id=photo.file_id,
            caption=comment,
        )
        session.add(new_photo)
        await session.commit()
        logger.info(
            "Master photo saved: request_id=%s user=%s file_id=%s caption=%s",
            request.id,
            message.from_user.id,
            photo.file_id,
            comment,
        )

    label = format_request_label(request)
    await message.answer(f"Фото добавлено к заявке {label}.")
    await notify_engineer(
        message.bot,
        request,
        text=f"📸 Мастер {master.full_name} добавил фото к заявке {label}.",
    )


@router.message(F.location)
async def master_location(message: Message, state: FSMContext):
    """Обработка геопозиции в обычном режиме (не в процессе начала/завершения работы)."""
    # Пропускаем обработку, если мастер находится в специальных состояниях
    # (специфичные обработчики для этих состояний обработают геопозицию)
    current_state = await state.get_state()
    if current_state:
        state_str = str(current_state)
        if (
            "waiting_start_location" in state_str
            or "waiting_finish_location" in state_str
        ):
            # Геопозиция будет обработана специфичными обработчиками для этих состояний
            return
    
    async with async_session() as session:
        master = await get_master(session, message.from_user.id)
        if not master:
            return

        work_session = await session.scalar(
            select(WorkSession)
            .where(WorkSession.master_id == master.id, WorkSession.finished_at.is_(None))
            .order_by(WorkSession.started_at.desc())
        )

        if work_session:
            work_session.started_latitude = message.location.latitude
            work_session.started_longitude = message.location.longitude
            await session.commit()
            request = await load_request(session, master.id, work_session.request_id)
            if request:
                label = format_request_label(request)
                await notify_engineer(
                    message.bot,
                    request,
                    text=(
                        f"📍 Мастер {master.full_name} обновил геопозицию старта по заявке {label}: "
                        f"https://www.google.com/maps?q={message.location.latitude},{message.location.longitude}"
                    ),
                    location=(message.location.latitude, message.location.longitude),
                )
            await message.answer("Геопозиция старта работ сохранена.", reply_markup=master_kb)
            return

        last_session = await session.scalar(
            select(WorkSession)
            .where(
                WorkSession.master_id == master.id,
                WorkSession.finished_at.isnot(None),
                WorkSession.finished_latitude.is_(None),
            )
            .order_by(WorkSession.finished_at.desc())
        )

        if last_session:
            last_session.finished_latitude = message.location.latitude
            last_session.finished_longitude = message.location.longitude
            await session.commit()
            request = await load_request(session, master.id, last_session.request_id)
            if request:
                label = format_request_label(request)
                await notify_engineer(
                    message.bot,
                    request,
                    text=(
                        f"📍 Мастер {master.full_name} обновил геопозицию завершения по заявке {label}: "
                        f"https://www.google.com/maps?q={message.location.latitude},{message.location.longitude}"
                    ),
                    location=(message.location.latitude, message.location.longitude),
                )
            await message.answer("Геопозиция завершения работ сохранена.", reply_markup=master_kb)
            return
