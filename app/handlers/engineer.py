from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
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
from app.utils.request_formatters import format_request_label
from app.utils.timezone import combine_moscow, format_moscow, now_moscow

router = Router()
ENGINEER_CALENDAR_PREFIX = "eng_schedule"

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


@router.message(StateFilter(EngineerCreateStates.confirmation), F.text.lower() == "подтвердить")
async def engineer_create_confirm(message: Message, state: FSMContext):
    data = await state.get_data()
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа к созданию заявки.")
            await state.clear()
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
    await message.answer(
        f"✅ Заявка {label} создана. Вы назначены ответственным инженером.\n"
        "Следите за статусом в разделе «📋 Мои заявки».",
    )
    await state.clear()


@router.message(StateFilter(EngineerCreateStates.confirmation), F.text.lower() == "отмена")
async def engineer_create_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Создание заявки отменено.")


@router.message(StateFilter(EngineerCreateStates.confirmation))
async def engineer_create_help(message: Message):
    await message.answer("Отправьте «Подтвердить» для сохранения или «Отмена» для отмены.")


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
    await message.answer(summary)


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
        "Отправьте «Подтвердить» для создания или «Отмена», чтобы прервать."
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

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("У вас пока нет назначенных заявок. Ожидайте распределения.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{format_request_label(req)} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы управлять этапами и бюджетом.",
        reply_markup=builder.as_markup(),
    )


@router.message(F.text == "🔍 Фильтр заявок")
async def engineer_filter_start(message: Message, state: FSMContext):
    await state.set_state(EngineerFilterStates.mode)
    await message.answer(
        "Выберите режим фильтрации:\n"
        "• отправьте «Адрес» — для поиска по адресу\n"
        "• отправьте «Дата» — для фильтра по диапазону дат создания (формат 01.01.2025-31.01.2025)"
    )


@router.message(StateFilter(EngineerFilterStates.mode))
async def engineer_filter_mode(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text not in {"адрес", "дата"}:
        await message.answer("Введите «Адрес» или «Дата».")
        return
    await state.update_data(mode=text)
    await state.set_state(EngineerFilterStates.value)
    if text == "адрес":
        await message.answer("Введите часть адреса (улица, дом и т.п.).")
    else:
        await message.answer("Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.")


@router.message(StateFilter(EngineerFilterStates.value))
async def engineer_filter_apply(message: Message, state: FSMContext):
    from datetime import datetime
    data = await state.get_data()
    mode = data.get("mode")
    value = (message.text or "").strip()

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await state.clear()
            await message.answer("Нет доступа.")
            return

        query = (
            select(Request)
            .options(
                selectinload(Request.master),
            )
            .where(Request.engineer_id == engineer.id)
            .order_by(Request.created_at.desc())
        )

        if mode == "адрес":
            query = query.where(func.lower(Request.address).like(f"%{value.lower()}%"))
        elif mode == "дата":
            try:
                start_str, end_str = [p.strip() for p in value.split("-", 1)]
                start = datetime.strptime(start_str, "%d.%m.%Y")
                end = datetime.strptime(end_str, "%d.%m.%Y")
                end = end.replace(hour=23, minute=59, second=59)
            except Exception:
                await message.answer("Неверный формат. Используйте ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.")
                return
            query = query.where(Request.created_at.between(start, end))

        requests = (
            (await session.execute(query.limit(30)))
            .scalars()
            .all()
        )

    await state.clear()

    if not requests:
        await message.answer("Заявок по заданному фильтру не найдено.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{format_request_label(req)} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Результаты фильтрации. Выберите заявку:",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("eng:detail:"))
async def engineer_request_detail(callback: CallbackQuery, state: FSMContext):
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

    # Проверяем, что пользователь действительно назначен как инженер на эту заявку
    if request.engineer_id != engineer.id:
        await callback.answer("Нет доступа к заявке.", show_alert=True)
        return

    # Save the last viewed request id into FSM so subsequent photos (even without
    # captions) can be associated correctly when the user is working with this card.
    await state.update_data(request_id=request.id)

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
            text=f"{format_request_label(req)} · {STATUS_TITLES.get(req.status, req.status.value)}",
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
    text = f"{header}\n\n{format_category_message(None)}"
    markup = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="ep",
        request_id=request_id,
    )
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

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="ep",
                request_id=request_id,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"План обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
            )
            await state.set_state(EngineerStates.quantity_input_plan)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
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
    text = f"{header}\n\n{format_category_message(None)}"
    markup = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="e",
        request_id=request_id,
    )
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

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category, is_material=True)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="epm",
                request_id=request_id,
                is_material=True,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"План обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=True,
            )
            await state.set_state(EngineerStates.quantity_input_plan)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
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

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category, is_material=True)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="em",
                request_id=request_id,
                is_material=True,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Факт обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=True,
            )
            await state.set_state(EngineerStates.quantity_input_fact)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
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

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="e",
                request_id=request_id,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
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

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Факт обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
            )
            await state.set_state(EngineerStates.quantity_input_fact)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
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
        select(User).where(User.telegram_id == telegram_id)
    )
    if not user:
        return None
    
    # Инженеры всегда имеют доступ
    if user.role == UserRole.ENGINEER:
        return user
    
    # Специалисты и суперадмины могут быть назначены как инженеры
    if user.role == UserRole.SPECIALIST:
        return user
    
    # Суперадмины (менеджеры с is_super_admin)
    if user.role == UserRole.MANAGER:
        leader = await session.scalar(
            select(Leader).where(Leader.user_id == user.id, Leader.is_super_admin == True)
        )
        if leader:
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
    builder.button(text="⏱ Срок устранения", callback_data=f"eng:set_term:{request_id}")
    builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request_id}")
    builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data="eng:back")
    builder.adjust(1)
    return builder.as_markup()


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
        current = request.remedy_term_days

    builder = InlineKeyboardBuilder()
    for days in (14, 30):
        builder.button(text=f"{days} дней", callback_data=f"eng:set_term_value:{request_id}:{days}")
    builder.button(text="⬅️ Назад", callback_data=f"eng:detail:{request_id}")
    builder.adjust(1)

    await callback.message.answer(
        f"Выберите срок устранения (сейчас {current} дней):",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:set_term_value:"))
async def engineer_set_remedy_term_value(callback: CallbackQuery):
    _, _, request_id_str, days_str = callback.data.split(":")
    try:
        request_id = int(request_id_str)
        days = int(days_str)
    except ValueError:
        await callback.answer("Некорректный срок.", show_alert=True)
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        await RequestService.set_remedy_term(session, request, days)
        await session.commit()
        label = format_request_label(request)

    await callback.answer("Срок сохранён.")
    await callback.message.answer(f"Срок устранения для заявки {label} установлен: {days} дней.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    master = request.master.full_name if request.master else "не назначен"
    object_name = request.object.name if request.object else request.address
    due_text = format_moscow(request.due_at) or "не задан"
    inspection = format_moscow(request.inspection_scheduled_at) or "не назначен"
    work_end = format_moscow(request.work_completed_at) or "—"
    label = format_request_label(request)

    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)

    lines = [
        f"📄 <b>{label}</b>",
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
                    f"  Часы: {_format_hours(item.planned_hours)} → {_format_hours(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        if letter_count:
            lines.append("")
            lines.append("✉️ Письмо специалиста: приложено")

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
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
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
