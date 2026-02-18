from __future__ import annotations

import html
import logging
from collections.abc import Sequence
from datetime import date, datetime, time
from typing import Any

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.handlers.common.work_fact_view import (
    build_category_keyboard,
    build_quantity_keyboard,
    decode_quantity,
    format_category_message,
    format_quantity_message,
)
from app.infrastructure.db.models import (
    ActType,
    Leader,
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
    WorkItem,
)
from app.infrastructure.db.session import async_session
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month
from app.services.request_service import RequestCreateData, RequestService
from app.services.work_catalog import get_work_catalog
from app.utils.pagination import clamp_page, total_pages_for
from app.utils.request_filters import format_date_range_label, parse_date_range, quick_date_range
from app.utils.request_formatters import format_hours_minutes, format_request_label, STATUS_TITLES
from app.utils.timezone import combine_moscow, format_moscow, now_moscow
from app.utils.advanced_filters import (
    build_filter_conditions,
    format_filter_label,
    get_available_objects,
    DateFilterMode,
)
from typing import Any

router = Router()
ENGINEER_CALENDAR_PREFIX = "eng_schedule"
REQUESTS_PAGE_SIZE = 10

logger = logging.getLogger(__name__)


class EngineerStates(StatesGroup):
    # Состояния для ввода плановых часов
    planned_hours_input = State()  # Ввод плановых часов (число)


# Состояния EngineerCreateStates перенесены в app/handlers/engineer/create/
# Импортируем из нового модуля
from app.handlers.engineer.create import EngineerCreateStates


# Состояния EngineerFilterStates перенесены в app/handlers/engineer/filters/
# Импортируем из нового модуля
from app.handlers.engineer.filters import EngineerFilterStates

# Состояния для осмотра перенесены в app/handlers/engineer/inspection/
# Импортируем из нового модуля
from app.handlers.engineer.inspection import EngineerInspectionStates

# Состояния для управления бюджетом перенесены в app/handlers/engineer/budget/
# Импортируем из нового модуля
from app.handlers.engineer.budget import EngineerBudgetStates


# Функции фильтрации и списка перенесены в app/handlers/engineer/utils.py и app/handlers/engineer/list/
# Импортируем из новых модулей
from app.handlers.engineer.utils import (
    engineer_filter_conditions as _engineer_filter_conditions,
    engineer_filter_label as _engineer_filter_label,
    engineer_filter_menu_keyboard as _engineer_filter_menu_keyboard,
    engineer_filter_cancel_keyboard as _engineer_filter_cancel_keyboard,
)
from app.handlers.engineer.list import (
    fetch_engineer_requests_page as _fetch_engineer_requests_page,
    show_engineer_requests_list as _show_engineer_requests_list,
)


# Обработчики создания заявок перенесены в app/handlers/engineer/create/
# Они автоматически подключаются через router в __init__.py


# Функции и обработчики осмотра перенесены в app/handlers/engineer/inspection/
# Они автоматически подключаются через router в __init__.py


@router.message(F.text == "📋 Мои заявки")
async def engineer_requests(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам, специалистам и суперадминам.")
            return

        await _show_engineer_requests_list(message, session, engineer.id, page=0)


@router.callback_query(F.data.startswith("eng:list:"))
async def engineer_requests_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=page,
            edit=True,
        )
    await callback.answer()


# Обработчики фильтрации перенесены в app/handlers/engineer/filters/
# Импортируем из нового модуля
from app.handlers.engineer.filters import (
    engineer_filter_start,
    engineer_filter_mode,
    engineer_filter_mode_callback,
    engineer_filter_quick,
    engineer_filter_clear,
    engineer_filter_cancel,
    engineer_filter_apply,
    engineer_filter_page,
)


# Обработчики просмотра деталей, удаления и просмотра фото перенесены в app/handlers/engineer/detail/
# Они автоматически подключаются через router в __init__.py


# Обработчики осмотра перенесены в app/handlers/engineer/inspection/
# Они автоматически подключаются через router в __init__.py

# Обработчики управления бюджетом перенесены в app/handlers/engineer/budget/
# Они автоматически подключаются через router в __init__.py


# Обработчики назначения мастера перенесены в app/handlers/engineer/master_assignment/
# Они автоматически подключаются через router в __init__.py


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
            await message.answer("Эта функция доступна только инженерам, специалистам и суперадминам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Ожидайте назначенных заявок.")
        return

    summary = _build_engineer_analytics(requests)
    await message.answer(summary)


# Обработчики фото/видео во время осмотра перенесены в app/handlers/engineer/inspection/
# Они автоматически подключаются через router в __init__.py




# Функция _get_engineer перенесена в app/handlers/engineer/utils.py
# Импортируем из нового модуля
from app.handlers.engineer.utils import get_engineer as _get_engineer




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


# Функции загрузки и отображения деталей заявки перенесены в app/handlers/engineer/detail/
# Импортируем из нового модуля для использования в других модулях
from app.handlers.engineer.detail import (
    load_request as _load_request,
    show_request_detail as _show_request_detail,
    send_all_photos as _send_all_photos,
)
from app.handlers.engineer.detail.keyboards import build_detail_keyboard as _detail_keyboard
from app.handlers.engineer.detail.formatters import format_engineer_request_detail as _format_request_detail


async def _refresh_request_detail(bot, chat_id: int, engineer_telegram_id: int, request_id: int) -> None:
    """Обновляет детали заявки через бота (для внешних вызовов)."""
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
        from app.handlers.engineer.detail.formatters import format_engineer_request_detail
        await bot.send_message(
            chat_id=chat_id,
            text=format_engineer_request_detail(request),
            reply_markup=_detail_keyboard(request.id, request),
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("eng:warranty_yes:"))
async def engineer_warranty_yes(callback: CallbackQuery, state: FSMContext):
    """Гарантия: заявка продолжается как обычно."""
    request_id = int(callback.data.split(":")[2])
    await callback.answer("Заявка в гарантии. Продолжайте работу по заявке.")
    # Обновляем карточку (кнопки «Гарантия»/«Не гарантия» остаются до смены статуса)
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            return
        request = await _load_request(session, engineer.id, request_id)
    if request:
        await _show_request_detail(callback.message, request, edit=True, list_context="list", list_page=0)


@router.callback_query(F.data.startswith("eng:warranty_no:"))
async def engineer_warranty_no(callback: CallbackQuery, state: FSMContext):
    """Не гарантия: заявка переводится в статус «Отменена»."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        if request.status in (RequestStatus.CLOSED, RequestStatus.CANCELLED):
            await callback.answer("Заявка уже закрыта или отменена.", show_alert=True)
            return
        await RequestService.cancel_request(
            session,
            request,
            cancelled_by=engineer.id,
            reason="Не гарантия (указал инженер)",
        )
        await session.commit()
    await callback.answer("Заявка отменена (не гарантия).", show_alert=True)
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if engineer:
            request = await _load_request(session, engineer.id, request_id)
            if request:
                await _show_request_detail(callback.message, request, edit=True, list_context="list", list_page=0)


@router.callback_query(F.data.startswith("eng:set_planned_hours:"))
async def engineer_set_planned_hours_start(callback: CallbackQuery, state: FSMContext):
    """Старт ввода плановых часов: просим ввести число часов."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        current = format_hours_minutes(float(request.engineer_planned_hours or 0))

    await state.set_state(EngineerStates.planned_hours_input)
    await state.update_data(planned_hours_request_id=request_id)
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.message.answer(
        f"Введите плановые часы (число, например 2 или 2.5).\n"
        f"Сейчас указано: {current}\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.planned_hours_input))
async def engineer_planned_hours_input(message: Message, state: FSMContext):
    """Обработка введённых плановых часов."""
    text = (message.text or "").strip()
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Ввод отменён.")
        return

    try:
        hours = float(text.replace(",", "."))
    except ValueError:
        await message.answer("Введите число (например 2 или 2.5). Для отмены — «Отмена».")
        return

    if hours < 0:
        await message.answer("Число часов не может быть отрицательным. Введите число ≥ 0.")
        return

    data = await state.get_data()
    request_id = data.get("planned_hours_request_id")
    if not request_id:
        await state.clear()
        await message.answer("Сессия истекла. Откройте карточку заявки снова.")
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await state.clear()
            await message.answer("Нет доступа к заявке.")
            return
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await state.clear()
            await message.answer("Заявка не найдена.")
            return

        await RequestService.set_engineer_planned_hours(session, request, hours)
        await session.commit()
        label = format_request_label(request)

    await state.clear()
    await message.answer(
        f"Плановые часы для заявки {label} установлены: {format_hours_minutes(hours)}."
    )
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


ENGINEER_TERM_CALENDAR_PREFIX = "eng_term"


@router.callback_query(F.data.startswith("eng:set_term:"))
async def engineer_set_remedy_term(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        current_text = format_moscow(request.due_at, "%d.%m.%Y") if request.due_at else "не задан"

    prefix = f"{ENGINEER_TERM_CALENDAR_PREFIX}_{request_id}"
    await callback.message.answer(
        f"Выберите срок устранения (дату). Сейчас: {current_text}",
        reply_markup=build_calendar(prefix),
    )
    await callback.answer()


@router.callback_query(F.data.startswith(f"cal:{ENGINEER_TERM_CALENDAR_PREFIX}_"))
async def engineer_set_term_calendar(callback: CallbackQuery):
    """Обработка календаря выбора срока устранения (инженер/менеджер)."""
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    try:
        request_id = int(payload.prefix.split("_")[-1])
    except (ValueError, IndexError):
        await callback.answer("Ошибка.", show_alert=True)
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar(payload.prefix, year=new_year, month=new_month)
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        async with async_session() as session:
            engineer = await _get_engineer(session, callback.from_user.id)
            if not engineer:
                await callback.answer("Нет доступа к заявке.", show_alert=True)
                return
            request = await _load_request(session, engineer.id, request_id)
            if not request:
                await callback.answer("Заявка не найдена.", show_alert=True)
                return

            selected = date(payload.year, payload.month, payload.day)
            due_at = combine_moscow(selected, time(23, 59, 59))
            await RequestService.set_due_date(session, request, due_at)
            await session.commit()
            label = format_request_label(request)

        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.answer("Срок сохранён.")
        await callback.message.answer(
            f"Срок устранения для заявки {label} установлен: {selected.strftime('%d.%m.%Y')}."
        )
        await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
        return

    await callback.answer()


# Функции форматирования и отправки фото перенесены в app/handlers/engineer/detail/formatters.py
# Используются через импорты выше


def _format_hours(value: float | None) -> str:
    return format_hours_minutes(value)


def _build_engineer_analytics(requests: Sequence[Request]) -> str:
    from collections import Counter

    now = now_moscow()
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
        f"Плановые часы: {format_hours_minutes(planned_hours)}",
        f"Фактические часы: {format_hours_minutes(actual_hours)}",
    ]

    if upcoming:
        lines.append("")
        lines.append("⚠️ Срок устранения в ближайшие 72 часа:")
        for req in upcoming:
            due_text = format_moscow(req.due_at) or "не задан"
            lines.append(f"• {format_request_label(req)} — до {due_text}")

    return "\n".join(lines)


# --- служебные функции для каталога ---


async def _update_catalog_message(message: Message, text: str, markup) -> None:
    try:
        await message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            await message.edit_reply_markup(reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)


async def _get_work_item(session, request_id: int, name: str) -> WorkItem | None:
    return await session.scalar(
        select(WorkItem).where(
            WorkItem.request_id == request_id,
            func.lower(WorkItem.name) == name.lower(),
        )
    )


def _catalog_header(request: Request) -> str:
    return f"Заявка {format_request_label(request)} · {request.title}"
