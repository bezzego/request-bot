from __future__ import annotations

from datetime import datetime

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import select

from app.infrastructure.db.models import Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService, WorkItemData, load_request


router = Router()


@router.message(F.text == "🧾 Отчёты")
async def engineer_help(message: Message):
    await message.answer(
        "Доступные команды:\n"
        "/inspection_schedule <номер> <ДД.ММ.ГГГГ> <ЧЧ:ММ> — назначить осмотр\n"
        "/inspection_done <номер> [комментарий] — завершить осмотр\n"
        "/add_budget <номер>;позиция;категория;ед.;план_кол-во;план_часы;план_стоимость — добавить позицию\n"
        "/update_budget <номер>;позиция;факт_кол-во;факт_часы;факт_стоимость — актуализировать данные\n"
        "/ready_for_sign <номер> — отправить на подписание\n"
        "/assign_master <номер> <telegram_id> — назначить мастера"
    )

async def _get_engineer(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.ENGINEER)
    )


@router.message(F.text == "📋 Назначенные заявки")
async def engineer_requests(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта команда доступна только инженерам.")
            return

        stmt = (
            select(Request)
            .where(
                Request.engineer_id == engineer.id,
                Request.status.notin_([RequestStatus.CLOSED, RequestStatus.CANCELLED]),
            )
            .order_by(Request.created_at)
        )
        requests = (await session.execute(stmt)).scalars().all()

    if not requests:
        await message.answer("У вас нет активных заявок.")
        return

    lines = ["📋 <b>Ваши заявки:</b>"]
    for req in requests:
        inspection_text = (
            req.inspection_scheduled_at.strftime("%d.%m.%Y %H:%M")
            if req.inspection_scheduled_at
            else "не назначен"
        )
        lines.append(
            f"\n#{req.number} — {req.title}\n"
            f"Статус: {req.status.value}\n"
            f"Осмотр: {inspection_text}"
        )
    lines.append(
        "\nКоманды:\n"
        "• /inspection_schedule <номер> <ДД.ММ.ГГГГ> <ЧЧ:ММ>\n"
        "• /inspection_done <номер> [комментарий]\n"
        "• /add_budget <номер>;название;категория;ед.;план_кол-во;план_часы;план_стоимость\n"
        "  (факт укажите позднее через /update_budget)\n"
        "• /ready_for_sign <номер>\n"
    )
    await message.answer("\n".join(lines))


@router.message(Command("inspection_schedule"))
async def schedule_inspection(message: Message):
    parts = message.text.split()
    if len(parts) < 4:
        await message.answer(
            "Использование: /inspection_schedule RQ-20250101-0001 25.10.2025 10:00"
        )
        return
    _, number, date_part, time_part, *location = parts
    try:
        inspection_dt = datetime.strptime(f"{date_part} {time_part}", "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("Неверный формат даты. Ожидается ДД.ММ.ГГГГ ЧЧ:ММ.")
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Команда доступна только инженерам.")
            return

        request = await load_request(session, number)
        if not request or request.engineer_id != engineer.id:
            await message.answer("Заявка не найдена или не закреплена за вами.")
            return

        location_value = " ".join(location) if location else request.inspection_location
        await RequestService.assign_engineer(
            session,
            request,
            engineer_id=engineer.id,
            inspection_datetime=inspection_dt,
            inspection_location=location_value,
        )
        await session.commit()

    await message.answer(f"Осмотр по заявке {number} назначен на {inspection_dt:%d.%m.%Y %H:%M}.")


@router.message(Command("inspection_done"))

@router.message(Command("assign_master"))
async def assign_master(message: Message):
    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /assign_master <номер> <telegram_id_мастера>")
        return
    _, number, master_telegram = parts

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Команда доступна только инженерам.")
            return

        master_user = await session.scalar(
            select(User).where(User.telegram_id == int(master_telegram), User.role == UserRole.MASTER)
        )
        if not master_user:
            await message.answer("Мастер с указанным Telegram ID не найден.")
            return

        request = await load_request(session, number)
        if not request or request.engineer_id != engineer.id:
            await message.answer("Заявка не найдена или не закреплена за вами.")
            return

        await RequestService.assign_master(session, request, master_id=master_user.id, assigned_by=engineer.id)
        object_name = request.object.name if request.object else request.title
        master_name = master_user.full_name
        master_chat_id = master_user.telegram_id
        await session.commit()

    try:
        await message.bot.send_message(
            chat_id=master_chat_id,
            text=(
                f"Вам назначена заявка {number}.\n"
                f"Объект: {object_name}."
            ),
        )
    except Exception:
        pass

    await message.answer(f"Мастер {master_name} назначен на заявку {number}.")

async def inspection_done(message: Message):
    parts = message.text.split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Использование: /inspection_done <номер> [комментарий]")
        return
    _, number, *comment = parts
    comment_text = comment[0] if comment else None

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Команда доступна только инженерам.")
            return

        request = await load_request(session, number)
        if not request or request.engineer_id != engineer.id:
            await message.answer("Заявка не найдена или не закреплена за вами.")
            return

        await RequestService.record_inspection(
            session,
            request,
            engineer_id=engineer.id,
            notes=comment_text,
        )
        await session.commit()

    await message.answer(f"Заявка {number}: осмотр завершён.")


@router.message(Command("add_budget"))
async def add_budget_item(message: Message):
    try:
        _, payload = message.text.split(maxsplit=1)
    except ValueError:
        await message.answer(
            "Использование: /add_budget номер;название;категория;ед.;план_кол-во;план_часы;план_стоимость"
        )
        return

    parts = [part.strip() for part in payload.split(";")]
    if len(parts) < 7:
        await message.answer("Недостаточно данных. Проверьте формат команды.")
        return

    number, name, category, unit, planned_qty, planned_hours, planned_cost = parts[:7]

    def _float_or_none(value: str) -> float | None:
        return float(value.replace(",", ".")) if value else None

    item = WorkItemData(
        name=name,
        category=category or None,
        unit=unit or None,
        planned_quantity=_float_or_none(planned_qty),
        planned_hours=_float_or_none(planned_hours),
        planned_cost=_float_or_none(planned_cost),
    )

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Команда доступна только инженерам.")
            return

        request = await load_request(session, number)
        if not request or request.engineer_id != engineer.id:
            await message.answer("Заявка не найдена или не закреплена за вами.")
            return

        await RequestService.add_work_item(session, request, item, author_id=engineer.id)
        await session.commit()

    await message.answer(f"Позиция {item.name} добавлена в бюджет заявки {number}.")


@router.message(Command("ready_for_sign"))
async def ready_for_sign(message: Message):
    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /ready_for_sign <номер>")
        return
    _, number = parts

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Команда доступна только инженерам.")
            return

        request = await load_request(session, number)
        if not request or request.engineer_id != engineer.id:
            await message.answer("Заявка не найдена или не закреплена за вами.")
            return

        await RequestService.mark_ready_for_sign(session, request, user_id=engineer.id)
        await session.commit()

    await message.answer(f"Заявка {number} переведена в статус ожидания подписания актов.")
