"""Модуль создания заявки инженером."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.session import async_session
from app.services.request_service import RequestCreateData, RequestService
from app.utils.request_formatters import format_request_label
from app.handlers.engineer.utils import get_engineer

router = Router()


class EngineerCreateStates(StatesGroup):
    """Состояния для создания новой заявки инженером."""
    title = State()
    object_name = State()
    address = State()
    apartment = State()
    description = State()
    phone = State()
    confirmation = State()


async def maybe_cancel_engineer_creation(message: Message, state: FSMContext) -> bool:
    """Проверяет, была ли отменена операция создания заявки."""
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Создание заявки отменено.")
        return True
    return False


def build_engineer_creation_summary(data: dict) -> str:
    """Строит сводку данных заявки для подтверждения."""
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


async def send_engineer_creation_summary(message: Message, state: FSMContext) -> None:
    """Отправляет сводку данных заявки и запрашивает подтверждение."""
    data = await state.get_data()
    summary = build_engineer_creation_summary(data)
    await state.set_state(EngineerCreateStates.confirmation)
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data="eng:confirm_create")
    builder.button(text="❌ Отменить", callback_data="eng:cancel_create")
    builder.adjust(1)
    
    await message.answer(summary, reply_markup=builder.as_markup())


@router.message(F.text == "➕ Новая заявка")
async def engineer_create_request(message: Message, state: FSMContext):
    """Начало создания новой заявки инженером."""
    async with async_session() as session:
        engineer = await get_engineer(session, message.from_user.id)
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
    """Обработка ввода заголовка заявки."""
    if await maybe_cancel_engineer_creation(message, state):
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
    """Обработка ввода объекта."""
    if await maybe_cancel_engineer_creation(message, state):
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
    """Обработка ввода адреса."""
    if await maybe_cancel_engineer_creation(message, state):
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
    """Обработка ввода квартиры."""
    if await maybe_cancel_engineer_creation(message, state):
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
    """Обработка ввода описания."""
    if await maybe_cancel_engineer_creation(message, state):
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
    """Обработка ввода телефона."""
    if await maybe_cancel_engineer_creation(message, state):
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
    await send_engineer_creation_summary(message, state)


@router.callback_query(F.data == "eng:confirm_create", StateFilter(EngineerCreateStates.confirmation))
async def engineer_create_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждение создания заявки."""
    data = await state.get_data()
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
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
    """Отмена создания заявки."""
    await state.clear()
    await callback.message.answer("Создание заявки отменено.")
    await callback.answer()
