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
    schedule_date = State()
    schedule_time = State()
    # Состояния для завершения осмотра
    inspection_waiting_photos = State()  # Ожидание отправки фото
    inspection_waiting_comment = State()  # Ожидание комментария
    inspection_final_confirm = State()  # Финальное подтверждение завершения осмотра
    # Состояния для ввода количества вручную
    quantity_input_plan = State()  # Ввод количества для плана
    quantity_input_fact = State()  # Ввод количества для факта
    planned_hours_input = State()  # Ввод плановых часов (число)


class EngineerCreateStates(StatesGroup):
    title = State()
    object_name = State()
    address = State()
    apartment = State()
    description = State()
    phone = State()
    confirmation = State()


class EngineerFilterStates(StatesGroup):
    mode = State()
    value = State()


def _engineer_filter_conditions(filter_payload: dict[str, Any] | None) -> list:
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


def _engineer_filter_label(filter_payload: dict[str, Any] | None) -> str:
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


def _engineer_filter_menu_keyboard() -> InlineKeyboardMarkup:
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
    # Кнопки фильтра показываем столбиком, чтобы длинные подписи не обрезались
    builder.adjust(1)
    return builder.as_markup()


def _engineer_filter_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✖️ Отмена", callback_data="eng:flt:cancel")
    builder.adjust(1)
    return builder.as_markup()


async def _fetch_engineer_requests_page(
    session,
    engineer_id: int,
    page: int,
    filter_payload: dict[str, Any] | None = None,
) -> tuple[list[Request], int, int, int]:
    base_conditions = [Request.engineer_id == engineer_id]
    conditions = _engineer_filter_conditions(filter_payload)
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


async def _show_engineer_requests_list(
    message: Message,
    session,
    engineer_id: int,
    page: int,
    *,
    context: str = "list",
    filter_payload: dict[str, Any] | None = None,
    edit: bool = False,
) -> None:
    requests, page, total_pages, total = await _fetch_engineer_requests_page(
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
    list_lines = []
    for idx, req in enumerate(requests, start=start_index + 1):
        status_text = STATUS_TITLES.get(req.status, req.status.value)
        detail_cb = (
            f"eng:detail:{req.id}:f:{page}" if context == "filter" else f"eng:detail:{req.id}:{page}"
        )
        label = format_request_label(req)
        list_lines.append(f"{idx}. {html.escape(label)}\n<b>{html.escape(status_text)}</b>")
        builder.button(
            text=f"{idx}. {label} · {status_text}",
            callback_data=detail_cb,
        )
        # Под кнопкой заявки — корзинка удаления (безвозвратно из БД)
        if req.status != RequestStatus.CLOSED:
            builder.button(text="🗑", callback_data=f"eng:delete:{req.id}:{ctx_key}:{page}")
    builder.adjust(1)  # заявка — строка, под ней корзинка

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
        label = _engineer_filter_label(filter_payload)
        header = "Результаты фильтрации. Выберите заявку:"
        if label:
            header = f"{header}\n\n<b>Фильтр:</b>\n{html.escape(label)}"
    else:
        header = "Выберите заявку, чтобы управлять этапами и бюджетом."
    requests_list = "\n\n".join(list_lines)
    footer = f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    text = f"{header}\n\n{requests_list}{footer}"

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


@router.message(F.text == "➕ Новая заявка")
async def engineer_create_request(message: Message, state: FSMContext):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Создание доступно только инженерам.")
            return

    await state.clear()
    await state.update_data(
        engineer_id=engineer.id,
        contact_person=engineer.full_name,
        contact_phone=engineer.phone,
    )
    await state.set_state(EngineerCreateStates.title)
    await message.answer(
        "Начинаем упрощённое создание заявки.\n"
        "1️⃣ Введите короткий заголовок (до 120 символов).\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.title))
async def engineer_create_title(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    title = (message.text or "").strip()
    if not title:
        await message.answer("Заголовок не может быть пустым. Попробуйте снова.")
        return
    if len(title) > 120:
        await message.answer("Сократите заголовок до 120 символов.")
        return

    await state.update_data(title=title)
    await state.set_state(EngineerCreateStates.object_name)
    await message.answer(
        "2️⃣ Укажите объект или ЖК (например, «ЖК Сириус, корпус 3»).\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.object_name))
async def engineer_create_object(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    object_name = (message.text or "").strip()
    if not object_name:
        await message.answer("Название объекта обязательно. Введите его ещё раз.")
        return

    await state.update_data(object_name=object_name)
    await state.set_state(EngineerCreateStates.address)
    await message.answer(
        "3️⃣ Введите адрес (улица, дом, подъезд). Без квартиры — её спросим отдельно.\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.address))
async def engineer_create_address(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    address = (message.text or "").strip()
    if not address:
        await message.answer("Адрес обязателен. Введите его ещё раз.")
        return

    await state.update_data(address=address)
    await state.set_state(EngineerCreateStates.apartment)
    await message.answer(
        "4️⃣ Укажите квартиру/помещение или отправьте «-», если не нужно.\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.apartment))
async def engineer_create_apartment(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    apartment = (message.text or "").strip()
    await state.update_data(apartment=None if apartment == "-" else apartment)
    await state.set_state(EngineerCreateStates.description)
    await message.answer(
        "5️⃣ Коротко опишите проблему или отправьте «-», если достаточно заголовка.\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.description))
async def engineer_create_description(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    description = (message.text or "").strip()
    await state.update_data(description=None if description == "-" else description)
    await state.set_state(EngineerCreateStates.phone)
    await message.answer(
        "6️⃣ Оставьте телефон для связи или «-», чтобы использовать номер из профиля.\n"
        "Для отмены напишите «Отмена».",
    )


@router.message(StateFilter(EngineerCreateStates.phone))
async def engineer_create_phone(message: Message, state: FSMContext):
    if await _maybe_cancel_engineer_creation(message, state):
        return
    phone_text = (message.text or "").strip()
    data = await state.get_data()

    phone_value = phone_text
    if phone_text == "-":
        phone_value = data.get("contact_phone")
        if not phone_value:
            await message.answer("В профиле нет телефона. Введите номер вручную.")
            return
    if not phone_value:
        await message.answer("Телефон обязателен. Введите его ещё раз.")
        return

    await state.update_data(contact_phone=phone_value)
    await _send_engineer_creation_summary(message, state)


@router.callback_query(F.data == "eng:confirm_create", StateFilter(EngineerCreateStates.confirmation))
async def engineer_create_confirm(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.message.answer("Нет доступа к созданию заявки.")
            await state.clear()
            await callback.answer()
            return

        create_data = RequestCreateData(
            title=data["title"],
            description=data.get("description") or data["title"],
            object_name=data["object_name"],
            address=data["address"],
            apartment=data.get("apartment"),
            contact_person=data.get("contact_person") or engineer.full_name,
            contact_phone=data["contact_phone"],
            specialist_id=engineer.id,
            engineer_id=engineer.id,
            remedy_term_days=14,
        )
        request = await RequestService.create_request(session, create_data)
        await session.commit()

    label = format_request_label(request)
    await callback.message.answer(
        f"✅ Заявка {label} создана. Вы назначены ответственным инженером.\n"
        "Следите за статусом в разделе «📋 Мои заявки».",
    )
    await state.clear()
    await callback.answer("Заявка создана")


@router.callback_query(F.data == "eng:cancel_create", StateFilter(EngineerCreateStates.confirmation))
async def engineer_create_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Создание заявки отменено.")
    await callback.answer()


async def _maybe_cancel_engineer_creation(message: Message, state: FSMContext) -> bool:
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Создание заявки отменено.")
        return True
    return False


async def _send_engineer_creation_summary(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    summary = _build_engineer_creation_summary(data)
    await state.set_state(EngineerCreateStates.confirmation)
    
    # Создаем кнопки для подтверждения
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data="eng:confirm_create")
    builder.button(text="❌ Отменить", callback_data="eng:cancel_create")
    builder.adjust(1)
    
    await message.answer(summary, reply_markup=builder.as_markup())


def _build_engineer_creation_summary(data: dict) -> str:
    apartment = data.get("apartment") or "—"
    description = data.get("description") or data.get("title")
    phone = data.get("contact_phone") or "—"
    return (
        "Проверьте данные заявки:\n"
        f"• Заголовок: {data.get('title')}\n"
        f"• Объект: {data.get('object_name')}\n"
        f"• Адрес: {data.get('address')}\n"
        f"• Квартира: {apartment}\n"
        f"• Описание: {description}\n"
        f"• Контакт: {data.get('contact_person')} / {phone}\n\n"
        "Нажмите кнопку ниже для подтверждения или отмены создания заявки."
    )


async def _prompt_schedule_calendar(message: Message):
    await message.answer(
        "Когда назначить комиссионный осмотр?\n"
        "Выберите дату через календарь или отправьте «-» (или «-; новое место»), если дата пока не определена.\n"
        "Для отмены напишите «Отмена».",
        reply_markup=build_calendar(ENGINEER_CALENDAR_PREFIX),
    )


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


@router.callback_query(F.data.startswith("eng:filter:"))
async def engineer_filter_page(callback: CallbackQuery, state: FSMContext):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    data = await state.get_data()
    filter_payload = data.get("eng_filter")
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
            context="filter",
            filter_payload=filter_payload,
            edit=True,
        )
    await callback.answer()


@router.message(F.text == "🔍 Фильтр заявок")
async def engineer_filter_start(message: Message, state: FSMContext):
    await state.set_state(EngineerFilterStates.mode)
    await message.answer(
        "🔍 <b>Фильтр заявок</b>\n\n"
        "Выберите способ фильтрации или быстрый период:",
        reply_markup=_engineer_filter_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(StateFilter(EngineerFilterStates.mode))
async def engineer_filter_mode(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Фильтр отменён.")
        return
    if text not in {"адрес", "дата"}:
        await message.answer("Введите «Адрес» или «Дата», либо нажмите «Отмена».")
        return
    await state.update_data(mode=text)
    await state.set_state(EngineerFilterStates.value)
    if text == "адрес":
        await message.answer(
            "Введите часть адреса (улица, дом и т.п.).",
            reply_markup=_engineer_filter_cancel_keyboard(),
        )
    else:
        await message.answer(
            "Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.\n"
            "Можно одну дату (ДД.ММ.ГГГГ) — покажем заявки за этот день.",
            reply_markup=_engineer_filter_cancel_keyboard(),
        )


@router.callback_query(F.data.startswith("eng:flt:mode:"))
async def engineer_filter_mode_callback(callback: CallbackQuery, state: FSMContext):
    mode = callback.data.split(":")[3]
    if mode == "address":
        await state.update_data(mode="адрес")
        await state.set_state(EngineerFilterStates.value)
        await callback.message.edit_text(
            "Введите часть адреса (улица, дом и т.п.).",
            reply_markup=_engineer_filter_cancel_keyboard(),
        )
    elif mode == "date":
        await state.update_data(mode="дата")
        await state.set_state(EngineerFilterStates.value)
        await callback.message.edit_text(
            "Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.\n"
            "Можно одну дату (ДД.ММ.ГГГГ) — покажем заявки за этот день.",
            reply_markup=_engineer_filter_cancel_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:flt:quick:"))
async def engineer_filter_quick(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":")[3]
    quick = quick_date_range(code)
    if not quick:
        await callback.answer("Неизвестный период.", show_alert=True)
        return
    start, end, label = quick
    filter_payload = {
        "mode": "дата",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "value": "",
        "label": label,
    }
    await state.update_data(eng_filter=filter_payload)
    await state.set_state(None)

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=0,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "eng:flt:clear")
async def engineer_filter_clear(callback: CallbackQuery, state: FSMContext):
    await state.update_data(eng_filter=None)
    await state.set_state(None)
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=0,
            context="list",
            edit=True,
        )
    await callback.answer("Фильтр сброшен.")


@router.callback_query(F.data == "eng:flt:cancel")
async def engineer_filter_cancel(callback: CallbackQuery, state: FSMContext):
    await state.set_state(None)
    await callback.message.edit_text("Фильтр отменён.")
    await callback.answer()


@router.message(StateFilter(EngineerFilterStates.value))
async def engineer_filter_apply(message: Message, state: FSMContext):
    data = await state.get_data()
    mode = data.get("mode")
    value = (message.text or "").strip()
    if value.lower() == "отмена":
        await state.clear()
        await message.answer("Фильтр отменён.")
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await state.clear()
            await message.answer("Нет доступа.")
            return

        filter_payload: dict[str, str] = {"mode": mode or "", "value": value}
        if mode == "адрес":
            if not value:
                await message.answer("Адрес не может быть пустым. Введите часть адреса.")
                return
            filter_payload["value"] = value
        elif mode == "дата":
            start, end, error = parse_date_range(value)
            if error:
                await message.answer(error)
                return
            filter_payload["start"] = start.isoformat()
            filter_payload["end"] = end.isoformat()

        await state.update_data(eng_filter=filter_payload)
        await state.set_state(None)

        await _show_engineer_requests_list(
            message,
            session,
            engineer.id,
            page=0,
            context="filter",
            filter_payload=filter_payload,
        )


@router.callback_query(F.data.startswith("eng:detail:"))
async def engineer_request_detail(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    request_id = int(parts[2])
    context = "list"
    page = 0
    if len(parts) >= 4:
        if parts[3] == "f":
            context = "filter"
            if len(parts) >= 5:
                try:
                    page = int(parts[4])
                except ValueError:
                    page = 0
        else:
            try:
                page = int(parts[3])
            except ValueError:
                page = 0
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

    # Save the last viewed request id into FSM so subsequent photos (even without
    # captions) can be associated correctly when the user is working with this card.
    await state.update_data(request_id=request.id)

    await _show_request_detail(callback.message, request, edit=True, list_context=context, list_page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("eng:back"))
async def engineer_back_to_list(callback: CallbackQuery):
    parts = callback.data.split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
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


@router.callback_query(F.data == "eng:noop")
async def engineer_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("eng:delete:"))
async def engineer_delete_prompt(callback: CallbackQuery):
    """Показывает подтверждение безвозвратного удаления заявки из БД."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    from_detail = len(parts) >= 4 and parts[3] == "detail"
    if from_detail:
        cancel_cb = f"eng:detail:{request_id}"
        confirm_cb = f"eng:delete_confirm:{request_id}"
        ctx_key, page = "list", 0
    else:
        ctx_key = parts[3] if len(parts) >= 4 else "list"
        page = int(parts[4]) if len(parts) >= 5 else 0
        cancel_cb = f"eng:{ctx_key}:{page}"
        confirm_cb = f"eng:delete_confirm:{request_id}:{ctx_key}:{page}"

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await _load_request(session, engineer.id, request_id)
    if not request:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    if request.status == RequestStatus.CLOSED:
        await callback.answer("Заявка уже закрыта.", show_alert=True)
        return
    label = format_request_label(request)
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить безвозвратно", callback_data=confirm_cb)
    builder.button(text="❌ Отмена", callback_data=cancel_cb)
    builder.adjust(1)
    await callback.message.edit_text(
        f"⚠️ <b>Удалить заявку {label}?</b>\n\n"
        "Заявка будет удалена из базы безвозвратно. Это действие нельзя отменить.",
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:delete_confirm:"))
async def engineer_delete_confirm(callback: CallbackQuery, state: FSMContext):
    """Безвозвратное удаление заявки из БД; при необходимости возврат к списку."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    return_to_list = len(parts) >= 5
    ctx_key = parts[3] if return_to_list else "list"
    page = int(parts[4]) if return_to_list else 0

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        if request.status == RequestStatus.CLOSED:
            await callback.answer("Заявка уже закрыта.", show_alert=True)
            return
        await RequestService.delete_request(session, request)
        await session.commit()

        if return_to_list:
            context = "filter" if ctx_key == "filter" else "list"
            filter_payload = (await state.get_data()).get("eng_filter") if context == "filter" else None
            _, _, total_pages, _ = await _fetch_engineer_requests_page(session, engineer.id, 0, filter_payload=filter_payload)
            safe_page = min(page, max(0, total_pages - 1)) if total_pages else 0
            await _show_engineer_requests_list(
                callback.message,
                session,
                engineer.id,
                page=safe_page,
                context=context,
                filter_payload=filter_payload,
                edit=True,
            )
            await callback.answer("Заявка удалена из базы")
            return

    await callback.message.edit_text("✅ Заявка удалена из базы.")
    await callback.answer("Заявка удалена")


@router.callback_query(F.data.startswith("eng:schedule:"))
async def engineer_schedule(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    
    # Проверяем доступ к заявке
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена или больше не закреплена за вами.", show_alert=True)
            return
    
    await state.set_state(EngineerStates.schedule_date)
    await state.update_data(request_id=request_id)
    await _prompt_schedule_calendar(callback.message)
    await callback.answer()


@router.message(StateFilter(EngineerStates.schedule_date))
async def engineer_schedule_date_text(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    if text.startswith("-"):
        location = None
        if ";" in text:
            _, location_part = text.split(";", 1)
            location = location_part.strip() or None
        await _complete_engineer_schedule(
            message,
            state,
            inspection_dt=None,
            location=location,
        )
        return

    await message.answer(
        "Дата выбирается через календарь. Нажмите на нужный день или отправьте «-», если дата пока неизвестна."
    )


@router.callback_query(
    StateFilter(EngineerStates.schedule_date),
    F.data.startswith(f"cal:{ENGINEER_CALENDAR_PREFIX}:"),
)
async def engineer_schedule_calendar(callback: CallbackQuery, state: FSMContext):
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar(ENGINEER_CALENDAR_PREFIX, year=new_year, month=new_month)
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        selected = date(payload.year, payload.month, payload.day)
        await state.update_data(schedule_date=selected.isoformat())
        await state.set_state(EngineerStates.schedule_time)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(
            f"Дата осмотра: {selected.strftime('%d.%m.%Y')}.\n"
            "Введите время в формате ЧЧ:ММ или «-», если время пока не определено.\n"
            "Можно добавить место после точки с запятой: 10:00; Склад №3."
        )
        await callback.answer(f"Выбрано {selected.strftime('%d.%m.%Y')}")
        return

    await callback.answer()


@router.message(StateFilter(EngineerStates.schedule_time))
async def engineer_schedule_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in text.split(";")]
    time_part = parts[0] if parts else ""
    location_part = parts[1] if len(parts) > 1 else None

    if time_part == "-":
        await _complete_engineer_schedule(
            message,
            state,
            inspection_dt=None,
            location=location_part,
        )
        return

    try:
        time_value = datetime.strptime(time_part, "%H:%M").time()
    except ValueError:
        await message.answer("Не удалось распознать время. Используйте формат ЧЧ:ММ.")
        return

    data = await state.get_data()
    date_str = data.get("schedule_date")
    if not date_str:
        await message.answer("Сначала выберите дату через календарь.")
        await state.set_state(EngineerStates.schedule_date)
        await _prompt_schedule_calendar(message)
        return

    selected_date = date.fromisoformat(date_str)
    inspection_dt = combine_moscow(selected_date, time_value)
    await _complete_engineer_schedule(
        message,
        state,
        inspection_dt=inspection_dt,
        location=location_part,
    )


async def _complete_engineer_schedule(
    message: Message,
    state: FSMContext,
    *,
    inspection_dt: datetime | None,
    location: str | None,
) -> None:
    data = await state.get_data()
    request_id = data.get("request_id")
    if not request_id:
        await message.answer("Не удалось определить заявку. Начните процесс заново.")
        await state.clear()
        return

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
            inspection_location=location or request.inspection_location,
        )
        await session.commit()
        request_label = format_request_label(request)

    if inspection_dt:
        inspection_text = format_moscow(inspection_dt) or "—"
        main_line = f"Осмотр по заявке {request_label} назначен на {inspection_text}."
    else:
        main_line = f"Информация об осмотре заявки {request_label} обновлена."
    if location:
        main_line += f"\nМесто осмотра: {location}"

    await message.answer(main_line)
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:inspect:"))
async def engineer_inspection(callback: CallbackQuery, state: FSMContext):
    """Начало процесса завершения осмотра."""
    request_id = int(callback.data.split(":")[2])
    
    # Проверяем доступ к заявке
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        
        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена или больше не закреплена за вами.", show_alert=True)
            return
    
    # Сохраняем request_id и очищаем временные данные
    await state.set_state(EngineerStates.inspection_waiting_photos)
    await state.update_data(
        request_id=request_id,
        photos=[],
        videos=[],
        photo_file_ids=[],
        status_message_id=None,
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="📷 Отправить фото/видео",
        callback_data=f"eng:inspection:start_photos:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    
    await callback.message.answer(
        "Для завершения осмотра отправьте фото или видео дефектов.\n"
        "Нажмите кнопку «📷 Отправить фото/видео», чтобы начать загрузку.\n"
        "Можно отправить несколько фото/видео подряд, затем подтвердить все сразу.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:start_photos:")
)
async def engineer_inspection_start_photos(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки фото."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    await state.set_state(EngineerStates.inspection_waiting_photos)
    status_msg = await callback.message.edit_text(
        "📷 Жду ваши фотографии и видео.\n"
        "Отправьте все необходимые фото/видео дефектов подряд.\n"
        "После отправки всех файлов нажмите «✅ Подтвердить».",
        reply_markup=_waiting_photos_keyboard(request_id, photo_count=0, video_count=0),
    )
    await state.update_data(status_message_id=status_msg.message_id)
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:confirm_photos:")
)
async def engineer_inspection_confirm_photos(callback: CallbackQuery, state: FSMContext):
    """Подтверждение отправленных фото."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    photos = data.get("photos", [])
    videos = data.get("videos", [])
    total_files = len(photos) + len(videos)
    
    if total_files == 0:
        await callback.answer("Сначала отправьте хотя бы одно фото или видео.", show_alert=True)
        return

    # Сохраняем фото и видео в БД
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await state.clear()
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        # Сохраняем все фото
        for photo_data in photos:
            new_photo = Photo(
                request_id=request.id,
                type=PhotoType.BEFORE,
                file_id=photo_data["file_id"],
                caption=photo_data.get("caption"),
            )
            session.add(new_photo)
        
        # Сохраняем все видео (как фото с типом BEFORE)
        for video_data in videos:
            new_photo = Photo(
                request_id=request.id,
                type=PhotoType.BEFORE,
                file_id=video_data["file_id"],
                caption=video_data.get("caption"),
            )
            session.add(new_photo)
        
        await session.commit()
        logger.info(
            "Saved %s photos and %s videos for request_id=%s user=%s",
            len(photos),
            len(videos),
            request.id,
            callback.from_user.id,
        )
    
    # Переходим к вводу комментария
    await state.set_state(EngineerStates.inspection_waiting_comment)
    files_text = []
    if len(photos) > 0:
        files_text.append(f"{len(photos)} фото")
    if len(videos) > 0:
        files_text.append(f"{len(videos)} видео")
    files_summary = " и ".join(files_text) if files_text else "файлы"
    
    await callback.message.edit_text(
        f"✅ Сохранено: {files_summary}.\n\n"
        "Напишите комментарий к осмотру (или отправьте «-», если комментарий не требуется).",
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.inspection_waiting_comment))
async def engineer_inspection_comment(message: Message, state: FSMContext):
    """Обработка комментария к осмотру."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    if not text:
        await message.answer("Введите комментарий или «-», либо отправьте «Отмена».")
        return
    
    comment = None if text == "-" else text
    data = await state.get_data()
    request_id = data.get("request_id")
    
    await state.update_data(comment=comment)
    await state.set_state(EngineerStates.inspection_final_confirm)
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Завершить осмотр",
        callback_data=f"eng:inspection:final_confirm:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    
    await message.answer(
        "Комментарий сохранён.\n\n"
        "Нажмите «✅ Завершить осмотр», чтобы завершить процесс.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(
    StateFilter(EngineerStates.inspection_final_confirm),
    F.data.startswith("eng:inspection:final_confirm:")
)
async def engineer_inspection_final_confirm(callback: CallbackQuery, state: FSMContext):
    """Финальное завершение осмотра."""
    request_id = int(callback.data.split(":")[3])

    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await state.clear()
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        comment = data.get("comment")
        await RequestService.record_inspection(
            session,
            request,
            engineer_id=engineer.id,
            notes=comment,
            completed_at=now_moscow(),
        )
        await session.commit()

    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.answer("Осмотр завершён.")
    await callback.message.answer(f"✅ Осмотр по заявке {format_request_label(request)} отмечен как выполненный.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.callback_query(F.data == "eng:inspection:cancel")
async def engineer_inspection_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена процесса завершения осмотра."""
    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer("Действие отменено.")
    await callback.message.answer("Действие отменено.")


@router.callback_query(F.data.startswith("eng:add_plan:"))
async def engineer_add_plan(callback: CallbackQuery):
    """Старт добавления плана: сразу показываем виды работ (материалы автоподсчёт)."""
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

        header = _catalog_header(request)

    catalog = get_work_catalog()
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="ep",
        request_id=request_id,
    )
    text = f"{header}\n\n{format_category_message(None, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:ep:"))
async def engineer_work_catalog_plan(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "ep":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    catalog = get_work_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="ep",
                request_id=request_id,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            new_quantity = current_quantity or 1.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.add_plan_from_catalog(
                session,
                request,
                catalog_item=catalog_item,
                planned_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
                quantity_page=page,
            )
            await state.set_state(EngineerStates.quantity_input_plan)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.callback_query(F.data.startswith("eng:update_fact:"))
async def engineer_update_fact(callback: CallbackQuery):
    """Старт обновления факта: сразу показываем виды работ (материалы автоподсчёт)."""
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

        header = _catalog_header(request)

    catalog = get_work_catalog()
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="e",
        request_id=request_id,
    )
    text = f"{header}\n\n{format_category_message(None, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("material:epm:"))
async def engineer_material_catalog_plan(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога материалов для добавления в план инженером."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "epm":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    from app.services.material_catalog import get_material_catalog
    catalog = get_material_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="epm",
                request_id=request_id,
                is_material=True,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, is_material=True, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            new_quantity = current_quantity or 1.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.add_plan_from_material_catalog(
                session,
                request,
                catalog_item=catalog_item,
                planned_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=True,
                quantity_page=page,
            )
            await state.set_state(EngineerStates.quantity_input_plan)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.callback_query(F.data.startswith("material:em:"))
async def engineer_material_catalog_fact(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога материалов для обновления факта инженером."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "em":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    from app.services.material_catalog import get_material_catalog
    catalog = get_material_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="em",
                request_id=request_id,
                is_material=True,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, is_material=True, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            new_quantity = current_quantity or 0.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.update_actual_from_material_catalog(
                session,
                request,
                catalog_item=catalog_item,
                actual_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=True,
                quantity_page=page,
            )
            await state.set_state(EngineerStates.quantity_input_fact)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.callback_query(F.data.startswith("work:e:"))
async def engineer_work_catalog(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "e":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    catalog = get_work_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="e",
                request_id=request_id,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            new_quantity = current_quantity or 0.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.update_actual_from_catalog(
                session,
                request,
                catalog_item=catalog_item,
                actual_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
                quantity_page=page,
            )
            await state.set_state(EngineerStates.quantity_input_fact)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.message(StateFilter(EngineerStates.quantity_input_plan))
async def engineer_quantity_input_plan(message: Message, state: FSMContext):
    """Обработка ручного ввода количества для плана."""
    try:
        quantity = float(message.text.strip().replace(",", "."))
        if quantity < 0:
            await message.answer("Количество не может быть отрицательным. Введите положительное число.")
            return
    except ValueError:
        await message.answer("Неверный формат. Введите число (можно с десятичной частью, например: 2.5).")
        return
    
    data = await state.get_data()
    request_id = data.get("quantity_request_id")
    item_id = data.get("quantity_item_id")
    role_key = data.get("quantity_role_key")
    is_material = data.get("quantity_is_material", False)
    page = data.get("quantity_page")
    
    if not request_id or not item_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    from app.services.work_catalog import get_work_catalog
    from app.services.material_catalog import get_material_catalog
    
    catalog = get_material_catalog() if is_material else get_work_catalog()
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        await message.answer("Элемент каталога не найден.")
        await state.clear()
        return
    
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
        
        header = _catalog_header(request)
        work_item = await _get_work_item(session, request.id, catalog_item.name)
        current_quantity = (
            float(work_item.planned_quantity)
            if work_item and work_item.planned_quantity is not None
            else None
        )
        
        text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=quantity, current_quantity=current_quantity, is_material=is_material)}"
        markup = build_quantity_keyboard(
            catalog_item=catalog_item,
            role_key=role_key,
            request_id=request_id,
            new_quantity=quantity,
            is_material=is_material,
            page=page,
        )
        await message.answer(text, reply_markup=markup)
        await state.clear()


@router.message(StateFilter(EngineerStates.quantity_input_fact))
async def engineer_quantity_input_fact(message: Message, state: FSMContext):
    """Обработка ручного ввода количества для факта."""
    try:
        quantity = float(message.text.strip().replace(",", "."))
        if quantity < 0:
            await message.answer("Количество не может быть отрицательным. Введите положительное число.")
            return
    except ValueError:
        await message.answer("Неверный формат. Введите число (можно с десятичной частью, например: 2.5).")
        return
    
    data = await state.get_data()
    request_id = data.get("quantity_request_id")
    item_id = data.get("quantity_item_id")
    role_key = data.get("quantity_role_key")
    is_material = data.get("quantity_is_material", False)
    page = data.get("quantity_page")
    
    if not request_id or not item_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    from app.services.work_catalog import get_work_catalog
    from app.services.material_catalog import get_material_catalog
    
    catalog = get_material_catalog() if is_material else get_work_catalog()
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        await message.answer("Элемент каталога не найден.")
        await state.clear()
        return
    
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
        
        header = _catalog_header(request)
        work_item = await _get_work_item(session, request.id, catalog_item.name)
        current_quantity = (
            float(work_item.actual_quantity)
            if work_item and work_item.actual_quantity is not None
            else None
        )
        
        text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=quantity, current_quantity=current_quantity, is_material=is_material)}"
        markup = build_quantity_keyboard(
            catalog_item=catalog_item,
            role_key=role_key,
            request_id=request_id,
            new_quantity=quantity,
            is_material=is_material,
            page=page,
        )
        await message.answer(text, reply_markup=markup)
        await state.clear()


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
                f"Вам назначена заявка {format_request_label(request)}.\n"
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
            await message.answer("Эта функция доступна только инженерам, специалистам и суперадминам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Ожидайте назначенных заявок.")
        return

    summary = _build_engineer_analytics(requests)
    await message.answer(summary)


@router.message(StateFilter(EngineerStates.inspection_waiting_photos), F.photo)
async def engineer_inspection_photo(message: Message, state: FSMContext):
    """Обработка фото во время завершения осмотра."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем фото
    photo = message.photo[-1]
    caption = (message.caption or "").strip() or None
    
    # Добавляем фото в список
    photos = data.get("photos", [])
    photos.append({
        "file_id": photo.file_id,
        "caption": caption,
        "is_video": False,
    })
    
    videos = data.get("videos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(photos=photos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


@router.message(StateFilter(EngineerStates.inspection_waiting_photos), F.video)
async def engineer_inspection_video(message: Message, state: FSMContext):
    """Обработка видео во время завершения осмотра."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем видео
    video = message.video
    caption = (message.caption or "").strip() or None
    
    # Добавляем видео в список
    videos = data.get("videos", [])
    videos.append({
        "file_id": video.file_id,
        "caption": caption,
        "is_video": True,
    })
    
    photos = data.get("photos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(videos=videos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


@router.message(StateFilter(EngineerStates.inspection_waiting_photos), F.document)
async def engineer_inspection_document(message: Message, state: FSMContext):
    """Обработка документов-изображений во время завершения осмотра."""
    doc = message.document
    mime_type = doc.mime_type or ""
    
    # Поддерживаем только изображения
    if not mime_type.startswith("image/"):
        return

    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем документ как фото
    caption = (message.caption or "").strip() or None
    
    # Добавляем фото в список
    photos = data.get("photos", [])
    photos.append({
        "file_id": doc.file_id,
        "caption": caption,
        "is_video": False,
    })
    
    videos = data.get("videos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(photos=photos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


# --- служебные функции ---


def _waiting_photos_keyboard(request_id: int, photo_count: int = 0, video_count: int = 0):
    """Клавиатура во время ожидания фото."""
    builder = InlineKeyboardBuilder()
    total = photo_count + video_count
    if total > 0:
        builder.button(
            text=f"✅ Подтвердить ({total})",
            callback_data=f"eng:inspection:confirm_photos:{request_id}",
        )
    builder.button(
        text="🔄 Отправить заново",
        callback_data=f"eng:inspection:restart_photos:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:restart_photos:")
)
async def engineer_inspection_restart_photos(callback: CallbackQuery, state: FSMContext):
    """Начать загрузку фото заново."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return
    
    await state.update_data(photos=[], videos=[], photo_file_ids=[], status_message_id=None)
    status_msg = await callback.message.edit_text(
        "🔄 Список очищен. Отправьте фото/видео заново.\n"
        "Отправьте все необходимые фото/видео подряд, затем подтвердите все сразу.",
        reply_markup=_waiting_photos_keyboard(request_id, photo_count=0, video_count=0),
    )
    await state.update_data(status_message_id=status_msg.message_id)
    await callback.answer("Начните отправку фото/видео заново.")




async def _get_engineer(session, telegram_id: int) -> User | None:
    """Получает пользователя, который может быть инженером (ENGINEER, SPECIALIST или MANAGER с is_super_admin)."""
    user = await session.scalar(
        select(User)
        .options(selectinload(User.leader_profile))
        .where(User.telegram_id == telegram_id)
    )
    if not user:
        return None
    
    # Инженеры всегда имеют доступ
    if user.role == UserRole.ENGINEER:
        return user
    
    # Специалисты могут быть назначены как инженеры
    if user.role == UserRole.SPECIALIST:
        return user
    
    # Суперадмины (менеджеры с is_super_admin) могут быть назначены как инженеры
    if user.role == UserRole.MANAGER:
        # Проверяем через загруженный профиль leader_profile
        if user.leader_profile and user.leader_profile.is_super_admin:
            return user
    
    return None




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
            selectinload(Request.work_sessions),
            selectinload(Request.master),
            selectinload(Request.engineer),
            selectinload(Request.specialist),
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
            reply_markup=_detail_keyboard(request.id, request),
        )
    except Exception:
        pass


async def _show_request_detail(
    message: Message,
    request: Request,
    *,
    edit: bool = False,
    list_context: str = "list",
    list_page: int = 0,
) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id, request, list_context=list_context, list_page=list_page)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(
    request_id: int,
    request: Request | None = None,
    *,
    list_context: str = "list",
    list_page: int = 0,
):
    builder = InlineKeyboardBuilder()
    # После осмотра: гарантия / не гарантия (не гарантия → отмена заявки)
    if request and request.status == RequestStatus.INSPECTED and request.inspection_completed_at:
        builder.button(text="✅ Гарантия", callback_data=f"eng:warranty_yes:{request_id}")
        builder.button(text="❌ Не гарантия", callback_data=f"eng:warranty_no:{request_id}")
    builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request_id}")
    if request and not request.inspection_completed_at:
        builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request_id}")
    builder.button(text="⏱ Плановые часы", callback_data=f"eng:set_planned_hours:{request_id}")
    builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request_id}")
    builder.button(text="⏱ Срок устранения", callback_data=f"eng:set_term:{request_id}")
    builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request_id}")
    builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request_id}")
    if request and request.photos:
        builder.button(text="📷 Просмотреть фото", callback_data=f"eng:photos:{request_id}")
    if request and request.status != RequestStatus.CLOSED:
        builder.button(text="🗑 Удалить", callback_data=f"eng:delete:{request_id}:detail")
    back_cb = f"eng:list:{list_page}" if list_context == "list" else f"eng:filter:{list_page}"
    builder.button(text="⬅️ Назад к списку", callback_data=back_cb)
    builder.adjust(1)
    return builder.as_markup()


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


@router.callback_query(F.data.startswith("eng:photos:"))
async def engineer_view_photos(callback: CallbackQuery):
    """Просмотр всех фото заявки для инженера."""
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

        # Загружаем все фото заявки
        photos = (
            await session.execute(
                select(Photo)
                .where(Photo.request_id == request.id)
                .order_by(Photo.created_at.asc())
            )
        ).scalars().all()

    if not photos:
        await callback.answer("Фото не найдены.", show_alert=True)
        return

    await _send_all_photos(callback.message, photos)
    await callback.answer()


async def _send_all_photos(message: Message, photos: list[Photo]) -> None:
    """Отправка всех фото заявки, разделённых по типам (BEFORE, PROCESS, AFTER)."""
    if not photos:
        return
    
    # Разделяем фото по типам
    before_photos = [p for p in photos if p.type == PhotoType.BEFORE]
    process_photos = [p for p in photos if p.type == PhotoType.PROCESS]
    after_photos = [p for p in photos if p.type == PhotoType.AFTER]
    
    # Отправляем фото по типам
    if before_photos:
        await message.answer("📷 <b>Фото дефектов (до работ)</b>")
        await _send_photos_by_type(message, before_photos)
    
    if process_photos:
        await message.answer("📷 <b>Фото в процессе работ</b>")
        await _send_photos_by_type(message, process_photos)
    
    if after_photos:
        await message.answer("📷 <b>Фото после работ</b>")
        await _send_photos_by_type(message, after_photos)


# Максимум фото одного типа за раз, чтобы не перегружать чат и не упираться в лимиты Telegram
MAX_PHOTOS_PER_TYPE = 100


async def _send_photos_by_type(message: Message, photos: list[Photo]) -> None:
    """Отправка фото одного типа пачками по 10 (media_group). Фото и видео не тестируем отправкой — шлём пачкой, при ошибке «video» шлём по одному."""
    if not photos:
        return
    total = len(photos)
    to_send = photos[:MAX_PHOTOS_PER_TYPE]
    if total > MAX_PHOTOS_PER_TYPE:
        await message.answer(f"Показано {MAX_PHOTOS_PER_TYPE} из {total} (остальные сохранены в заявке).")

    # Пачки по 10 (лимит media_group в Telegram)
    chunk_size = 10
    i = 0
    while i < len(to_send):
        chunk = to_send[i : i + chunk_size]
        i += chunk_size
        media_list: list[InputMediaPhoto] = [
            InputMediaPhoto(media=p.file_id, caption=p.caption or None) for p in chunk
        ]
        try:
            if len(media_list) == 1:
                await message.answer_photo(media_list[0].media, caption=media_list[0].caption)
            else:
                await message.answer_media_group(media_list)
        except TelegramBadRequest as e:
            if "Video" in str(e) or "video" in str(e):
                # В пачке есть видео — отправляем по одному
                for p in chunk:
                    try:
                        await message.answer_photo(p.file_id, caption=p.caption or None)
                    except TelegramBadRequest:
                        try:
                            await message.answer_video(p.file_id, caption=p.caption or None)
                        except Exception:
                            pass
                    except Exception:
                        pass
            else:
                for p in chunk:
                    try:
                        await message.answer_photo(p.file_id, caption=p.caption or None)
                    except Exception:
                        try:
                            await message.answer_video(p.file_id, caption=p.caption or None)
                        except Exception:
                            pass
        except Exception:
            for p in chunk:
                try:
                    await message.answer_photo(p.file_id, caption=p.caption or None)
                except Exception:
                    try:
                        await message.answer_video(p.file_id, caption=p.caption or None)
                    except Exception:
                        pass


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    master = request.master.full_name if request.master else "не назначен"
    object_name = request.object.name if request.object else request.address
    due_text = format_moscow(request.due_at) or "не задан"
    inspection = format_moscow(request.inspection_scheduled_at) or "не назначен"
    work_end = format_moscow(request.work_completed_at) or "—"
    label = format_request_label(request)

    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    hours_delta = actual_hours - planned_hours
    
    # Рассчитываем разбивку стоимостей
    cost_breakdown = _calculate_cost_breakdown(request.work_items or [])

    lines = [
        f"📄 <b>{label}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Объект: {object_name}",
        f"Адрес: {request.address}",
        f"Квартира: {request.apartment or '—'}",
        f"Контактное лицо: {request.contact_person}",
        f"Телефон: {request.contact_phone}",
        f"Мастер: {master}",
        f"Осмотр: {inspection}",
        f"Работы завершены: {work_end}",
        f"Срок устранения: {due_text}",
        "",
        f"Плановая стоимость видов работ: {_format_currency(cost_breakdown['planned_work_cost'])} ₽",
        f"Плановая стоимость материалов: {_format_currency(cost_breakdown['planned_material_cost'])} ₽",
        f"Плановая общая стоимость: {_format_currency(cost_breakdown['planned_total_cost'])} ₽",
        f"Фактическая стоимость видов работ: {_format_currency(cost_breakdown['actual_work_cost'])} ₽",
        f"Фактическая стоимость материалов: {_format_currency(cost_breakdown['actual_material_cost'])} ₽",
        f"Фактическая общая стоимость: {_format_currency(cost_breakdown['actual_total_cost'])} ₽",
        f"Плановые часы: {format_hours_minutes(planned_hours)}",
        f"Фактические часы: {format_hours_minutes(actual_hours)}",
        f"Δ Часы: {format_hours_minutes(hours_delta, signed=True)}",
    ]

    if request.work_sessions:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        for session in sorted(request.work_sessions, key=lambda ws: ws.started_at):
            start = format_moscow(session.started_at, "%d.%m %H:%M") or "—"
            finish = format_moscow(session.finished_at, "%d.%m %H:%M") if session.finished_at else "в работе"
            duration_h = (
                float(session.hours_reported)
                if session.hours_reported is not None
                else (float(session.hours_calculated) if session.hours_calculated is not None else None)
            )
            if duration_h is None and session.started_at and session.finished_at:
                delta = session.finished_at - session.started_at
                duration_h = delta.total_seconds() / 3600
            duration_str = format_hours_minutes(duration_h) if duration_h is not None else "—"
            lines.append(f"• {start} — {finish} · {duration_str}")
            if session.notes:
                lines.append(f"  → {session.notes}")
    elif (request.actual_hours or 0) > 0:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        lines.append(f"• Суммарно: {format_hours_minutes(float(request.actual_hours or 0))} (учёт до внедрения сессий)")

    if request.contract:
        lines.append(f"Договор: {request.contract.number}")
    if request.defect_type:
        lines.append(f"Тип дефекта: {request.defect_type.name}")

    if request.work_items:
        lines.append("")
        lines.append("📦 <b>Позиции бюджета</b>")
        for item in request.work_items:
            is_material = bool(
                item.planned_material_cost
                or item.actual_material_cost
                or ("материал" in (item.category or "").lower())
            )
            emoji = "📦" if is_material else "🛠"
            planned_cost = item.planned_cost
            actual_cost = item.actual_cost
            if planned_cost in (None, 0):
                planned_cost = item.planned_material_cost
            if actual_cost in (None, 0):
                actual_cost = item.actual_material_cost
            unit = item.unit or ""
            qty_part = ""
            if item.planned_quantity is not None or item.actual_quantity is not None:
                pq = item.planned_quantity if item.planned_quantity is not None else 0
                aq = item.actual_quantity if item.actual_quantity is not None else 0
                qty_part = f" | объём: {pq:.2f} → {aq:.2f} {unit}".rstrip()
            lines.append(
                f"{emoji} {item.name} — план {_format_currency(planned_cost)} ₽ / "
                f"факт {_format_currency(actual_cost)} ₽{qty_part}"
            )
            if item.actual_hours is not None:
                lines.append(
                    f"  Часы: {format_hours_minutes(item.planned_hours)} → {format_hours_minutes(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        if letter_count:
            lines.append("")
            lines.append("✉️ Письмо специалиста: приложено")

    return "\n".join(lines)


def _calculate_cost_breakdown(work_items) -> dict[str, float]:
    """Рассчитывает разбивку стоимостей по работам и материалам."""
    planned_work_cost = 0.0
    planned_material_cost = 0.0
    actual_work_cost = 0.0
    actual_material_cost = 0.0
    
    for item in work_items:
        # Плановая стоимость работ
        if item.planned_cost is not None:
            planned_work_cost += float(item.planned_cost)
        
        # Плановая стоимость материалов
        if item.planned_material_cost is not None:
            planned_material_cost += float(item.planned_material_cost)
        
        # Фактическая стоимость работ
        if item.actual_cost is not None:
            actual_work_cost += float(item.actual_cost)
        
        # Фактическая стоимость материалов
        if item.actual_material_cost is not None:
            actual_material_cost += float(item.actual_material_cost)
    
    return {
        "planned_work_cost": planned_work_cost,
        "planned_material_cost": planned_material_cost,
        "planned_total_cost": planned_work_cost + planned_material_cost,
        "actual_work_cost": actual_work_cost,
        "actual_material_cost": actual_material_cost,
        "actual_total_cost": actual_work_cost + actual_material_cost,
    }


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


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
