from __future__ import annotations

from datetime import datetime
from typing import Optional

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import select

from app.infrastructure.db.models import (
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
)
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService, load_request


router = Router()


async def _get_master(session, telegram_id: int) -> Optional[User]:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.MASTER)
    )


@router.message(F.text == "📥 Мои заявки")
async def master_requests(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Эта команда доступна только мастерам.")
            return

        stmt = (
            select(Request)
            .where(
                Request.master_id == master.id,
                Request.status.notin_([RequestStatus.CLOSED, RequestStatus.CANCELLED]),
            )
            .order_by(Request.created_at)
        )
        requests = (await session.execute(stmt)).scalars().all()

    if not requests:
        await message.answer("У вас нет активных заявок.")
        return

    lines = ["📄 <b>Назначенные заявки:</b>"]
    for req in requests:
        status_line = f"Статус: {req.status.value}"
        work_started = req.work_started_at.strftime("%d.%m.%Y %H:%M") if req.work_started_at else "—"
        lines.append(
            f"\n#{req.number} — {req.title}\n"
            f"{status_line}\n"
            f"Осмотр инженером: "
            f"{req.inspection_completed_at.strftime('%d.%m.%Y %H:%M') if req.inspection_completed_at else 'не завершён'}\n"
            f"Старт работ: {work_started}"
        )
    lines.append(
        "\nКоманды:\n"
        "• /start_work <номер> [место]\n"
        "• /finish_work <номер> [отработанные_часы] [комментарий]\n"
        "• /update_budget <номер>;название;факт_кол-во;факт_часы;факт_стоимость\n"
        "Отправьте фото с подписью «RQ-XXXX описание» для фиксации процессов."
    )
    await message.answer("\n".join(lines))


@router.message(Command("start_work"))
async def master_start_work(message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /start_work RQ-20250101-0001 [место]")
        return
    _, number, *place = parts
    place_text = place[0] if place else None

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Команда доступна только мастерам.")
            return

        request = await load_request(session, number)
        if not request or request.master_id != master.id:
            await message.answer("Заявка не найдена или не назначена вам.")
            return

        await RequestService.start_work(
            session,
            request,
            master_id=master.id,
            address=place_text,
        )
        await session.commit()

    await message.answer(f"✅ Начало работ по заявке {number} зафиксировано.")


@router.message(Command("finish_work"))
async def master_finish_work(message: Message):
    parts = message.text.split(maxsplit=3)
    if len(parts) < 2:
        await message.answer("Использование: /finish_work <номер> [часы] [комментарий]")
        return
    _, number, *rest = parts
    hours = None
    comment = None
    if rest:
        try:
            hours = float(rest[0].replace(",", "."))
            if len(rest) > 1:
                comment = " ".join(rest[1:])
        except ValueError:
            comment = " ".join(rest)

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Команда доступна только мастерам.")
            return

        request = await load_request(session, number)
        if not request or request.master_id != master.id:
            await message.answer("Заявка не найдена или не назначена вам.")
            return

        await RequestService.finish_work(
            session,
            request,
            master_id=master.id,
            hours_reported=hours,
            completion_notes=comment,
        )
        await session.commit()

    await message.answer(f"✅ Работы по заявке {number} завершены. Не забудьте загрузить фото и акт.")


@router.message(Command("update_budget"))
async def master_update_budget(message: Message):
    try:
        _, payload = message.text.split(maxsplit=1)
    except ValueError:
        await message.answer(
            "Использование: /update_budget номер;название;факт_кол-во;факт_часы;факт_стоимость"
        )
        return

    number, name, actual_qty, actual_hours, actual_cost = [
        part.strip() for part in payload.split(";")
    ]

    def _float(value: str) -> float | None:
        return float(value.replace(",", ".")) if value else None

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Команда доступна только мастерам.")
            return

        request = await load_request(session, number)
        if not request or request.master_id != master.id:
            await message.answer("Заявка не найдена или не назначена вам.")
            return

        try:
            await RequestService.update_work_item_actual(
                session,
                request,
                name=name,
                actual_quantity=_float(actual_qty),
                actual_hours=_float(actual_hours),
                actual_cost=_float(actual_cost),
                author_id=master.id,
            )
            await session.commit()
        except ValueError as exc:
            await message.answer(str(exc))
            return

    await message.answer(f"Фактические данные по «{name}» обновлены.")


@router.message(F.photo)
async def handle_photo(message: Message):
    caption = message.caption or ""
    if "RQ-" not in caption:
        return  # игнорируем фото без номера заявки
    number = caption.split()[0]
    comment = " ".join(caption.split()[1:]) if len(caption.split()) > 1 else None

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            return

        request = await load_request(session, number)
        if not request or request.master_id != master.id:
            await message.answer("Не удалось определить заявку по подписи фото.")
            return

        file_id = message.photo[-1].file_id
        photo = Photo(
            request_id=request.id,
            type=PhotoType.PROCESS,
            file_id=file_id,
            caption=comment,
        )
        session.add(photo)
        await session.commit()

    await message.answer(f"📸 Фото добавлено к заявке {number}.")
