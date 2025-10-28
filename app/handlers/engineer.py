from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestStatus, User, UserRole, WorkItem
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService, WorkItemData

router = Router()


class EngineerStates(StatesGroup):
    schedule_datetime = State()
    inspection_comment = State()
    budget_plan = State()
    budget_fact = State()


STATUS_TITLES = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначен мастер",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


@router.message(F.text == "📋 Мои заявки")
async def engineer_requests(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("У вас пока нет назначенных заявок. Ожидайте распределения.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы управлять этапами и бюджетом.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("eng:detail:"))
async def engineer_request_detail(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    await _show_request_detail(callback.message, request, edit=True)
    await callback.answer()


@router.callback_query(F.data == "eng:back")
async def engineer_back_to_list(callback: CallbackQuery):
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await callback.message.edit_text("Активных заявок нет. Ожидайте распределения.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите заявку, чтобы управлять этапами и бюджетом.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:schedule:"))
async def engineer_schedule(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(EngineerStates.schedule_datetime)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Введите дату и время осмотра в формате «ДД.ММ.ГГГГ ЧЧ:ММ».\n"
        "Можно добавить место осмотра после точки с запятой: 25.10.2025 10:00; Склад №3."
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.schedule_datetime))
async def engineer_schedule_datetime(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    data = await state.get_data()
    request_id = data.get("request_id")

    parts = [part.strip() for part in message.text.split(";")]
    datetime_part = parts[0]
    location_part = parts[1] if len(parts) > 1 else None
    try:
        inspection_dt = datetime.strptime(datetime_part, "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ ЧЧ:ММ.")
        return

    inspection_dt = inspection_dt.replace(tzinfo=timezone.utc)

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.assign_engineer(
            session,
            request,
            engineer_id=engineer.id,
            inspection_datetime=inspection_dt,
            inspection_location=location_part or request.inspection_location,
        )
        await session.commit()

    await message.answer(
        f"Осмотр по заявке {request.number} назначен на {inspection_dt.strftime('%d.%m.%Y %H:%M')}."
    )
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:inspect:"))
async def engineer_inspection(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(EngineerStates.inspection_comment)
    await state.update_data(request_id=request_id)
    await callback.message.answer("Добавьте комментарий по результатам осмотра (или отправьте «-»).")
    await callback.answer()


@router.message(StateFilter(EngineerStates.inspection_comment))
async def engineer_inspection_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    request_id = data.get("request_id")
    comment = None if message.text.strip() == "-" else message.text.strip()

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.record_inspection(
            session,
            request,
            engineer_id=engineer.id,
            notes=comment,
            completed_at=datetime.now(timezone.utc),
        )
        await session.commit()

    await message.answer(f"Осмотр по заявке {request.number} отмечен как выполненный.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:add_plan:"))
async def engineer_add_plan(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(EngineerStates.budget_plan)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Введите данные плановой позиции через «;»:\n"
        "Название;Категория;Ед.;План кол-во;План часы;План стоимость;Материалы в руб.\n"
        "Например: Окраска стен;Работы;м²;120;32;55000;8000"
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.budget_plan))
async def engineer_add_plan_data(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in message.text.split(";")]
    if len(parts) < 6:
        await message.answer("Недостаточно данных. Укажите минимум 6 значений через «;».")
        return

    (name, category, unit, planned_qty, planned_hours, planned_cost, *rest) = parts
    planned_material = rest[0] if rest else None

    def _float(value: str | None) -> float | None:
        if not value:
            return None
        return float(value.replace(",", "."))

    item = WorkItemData(
        name=name,
        category=category or None,
        unit=unit or None,
        planned_quantity=_float(planned_qty),
        planned_hours=_float(planned_hours),
        planned_cost=_float(planned_cost),
        planned_material_cost=_float(planned_material),
    )

    data = await state.get_data()
    request_id = data.get("request_id")

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.add_work_item(session, request, item, author_id=engineer.id)
        await session.commit()

    await message.answer(f"Позиция «{item.name}» добавлена в план заявки {request.number}.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:update_fact:"))
async def engineer_update_fact(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(EngineerStates.budget_fact)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Введите фактические данные через «;»:\n"
        "Название;Факт кол-во;Факт часы;Факт стоимость;Материалы;Комментарий\n"
        "Например: Окраска стен;118;30;53000;7500;Подкорректировали объём"
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.budget_fact))
async def engineer_update_fact_data(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in message.text.split(";")]
    if len(parts) < 5:
        await message.answer("Недостаточно данных. Нужны минимум 5 значений через «;».")
        return

    name, actual_qty, actual_hours, actual_cost, actual_material, *comment = parts

    def _float(value: str | None) -> float | None:
        if not value:
            return None
        return float(value.replace(",", "."))

    comment_text = comment[0] if comment else None

    data = await state.get_data()
    request_id = data.get("request_id")

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        try:
            await RequestService.update_work_item_actual(
                session,
                request,
                name=name,
                actual_quantity=_float(actual_qty),
                actual_hours=_float(actual_hours),
                actual_cost=_float(actual_cost),
                actual_material_cost=_float(actual_material),
                notes=comment_text,
                author_id=engineer.id,
            )
            await session.commit()
        except ValueError as exc:
            await message.answer(str(exc))
            return

    await message.answer(f"Фактические данные по «{name}» обновлены.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:assign_master:"))
async def engineer_assign_master(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
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
    _, _, request_id_str, master_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    master_id = int(master_id_str)

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
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
                f"Вам назначена заявка {request.number}.\n"
                f"Объект: {request.object.name if request.object else request.address}."
            ),
        )
    except Exception:
        # Игнорируем ошибки отправки уведомления
        pass

    await callback.answer("Мастер назначен.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:ready:"))
async def engineer_ready_for_sign(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        await RequestService.mark_ready_for_sign(session, request, user_id=engineer.id)
        await session.commit()

    await callback.answer("Статус обновлён.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.message(F.text == "📊 Аналитика")
async def engineer_analytics(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Ожидайте назначенных заявок.")
        return

    summary = _build_engineer_analytics(requests)
    await message.answer(summary)


# --- служебные функции ---


async def _get_engineer(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.ENGINEER)
    )


async def _load_engineer_requests(session, engineer_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.master),
                )
                .where(Request.engineer_id == engineer_id)
                .order_by(Request.created_at.desc())
                .limit(20)
            )
        )
        .scalars()
        .all()
    )


async def _load_request(session, engineer_id: int, request_id: int) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.master),
            selectinload(Request.photos),
            selectinload(Request.acts),
        )
        .where(Request.id == request_id, Request.engineer_id == engineer_id)
    )


async def _refresh_request_detail(bot, chat_id: int, engineer_telegram_id: int, request_id: int) -> None:
    async with async_session() as session:
        engineer = await _get_engineer(session, engineer_telegram_id)
        if not engineer:
            return
        request = await _load_request(session, engineer.id, request_id)

    if not request:
        return

    if not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=_format_request_detail(request),
            reply_markup=_detail_keyboard(request.id),
        )
    except Exception:
        pass


async def _show_request_detail(message: Message, request: Request, *, edit: bool = False) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(request_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request_id}")
    builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request_id}")
    builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request_id}")
    builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request_id}")
    builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data="eng:back")
    builder.adjust(1)
    return builder.as_markup()


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    master = request.master.full_name if request.master else "не назначен"
    object_name = request.object.name if request.object else request.address
    due_text = request.due_at.strftime("%d.%m.%Y %H:%M") if request.due_at else "не задан"
    inspection = (
        request.inspection_scheduled_at.strftime("%d.%m.%Y %H:%M")
        if request.inspection_scheduled_at
        else "не назначен"
    )
    work_end = (
        request.work_completed_at.strftime("%d.%m.%Y %H:%M")
        if request.work_completed_at
        else "—"
    )

    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)

    lines = [
        f"📄 <b>{request.number}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Объект: {object_name}",
        f"Мастер: {master}",
        f"Осмотр: {inspection}",
        f"Работы завершены: {work_end}",
        f"Срок устранения: {due_text}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if request.contract:
        lines.append(f"Договор: {request.contract.number}")
    if request.defect_type:
        lines.append(f"Тип дефекта: {request.defect_type.name}")

    if request.work_items:
        lines.append("")
        lines.append("📦 <b>Позиции бюджета</b>")
        for item in request.work_items:
            lines.append(
                f"• {item.name} — план {_format_currency(item.planned_cost)} ₽ / "
                f"факт {_format_currency(item.actual_cost)} ₽"
            )
            if item.actual_hours is not None:
                lines.append(
                    f"  Часы: {_format_hours(item.planned_hours)} → {_format_hours(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    return "\n".join(lines)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"


def _build_engineer_analytics(requests: Sequence[Request]) -> str:
    from collections import Counter

    now = datetime.now(timezone.utc)
    counter = Counter(req.status for req in requests)
    total = len(requests)
    scheduled = counter.get(RequestStatus.INSPECTION_SCHEDULED, 0)
    in_progress = counter.get(RequestStatus.IN_PROGRESS, 0) + counter.get(RequestStatus.ASSIGNED, 0)
    completed = counter.get(RequestStatus.COMPLETED, 0) + counter.get(RequestStatus.READY_FOR_SIGN, 0)
    closed = counter.get(RequestStatus.CLOSED, 0)
    overdue = sum(
        1
        for req in requests
        if req.due_at and req.due_at < now and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
    )

    planned_budget = float(sum(req.planned_budget or 0 for req in requests))
    actual_budget = float(sum(req.actual_budget or 0 for req in requests))
    planned_hours = float(sum(req.planned_hours or 0 for req in requests))
    actual_hours = float(sum(req.actual_hours or 0 for req in requests))

    upcoming = [
        req
        for req in requests
        if req.due_at
        and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
        and 0 <= (req.due_at - now).total_seconds() <= 72 * 3600
    ]

    lines = [
        "📊 <b>Аналитика по вашим заявкам</b>",
        f"Всего: {total}",
        f"Назначен осмотр: {scheduled}",
        f"В работе: {in_progress}",
        f"Завершены: {completed}",
        f"Закрыты: {closed}",
        f"Просрочено: {overdue}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {_format_currency(actual_budget - planned_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if upcoming:
        lines.append("")
        lines.append("⚠️ Срок устранения в ближайшие 72 часа:")
        for req in upcoming:
            lines.append(f"• {req.number} — до {req.due_at.strftime('%d.%m.%Y %H:%M')}")

    return "\n".join(lines)
