"""Модуль создания заявки специалистом."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import User, UserRole
from app.infrastructure.db.session import async_session
from app.handlers.specialist.utils import get_specialist

router = Router()


class NewRequestStates(StatesGroup):
    """Состояния для создания новой заявки."""
    title = State()
    description = State()
    object_name = State()
    address = State()
    apartment = State()
    contact_person = State()
    contact_phone = State()
    contract_number = State()
    defect_type = State()
    inspection_datetime = State()
    inspection_time = State()
    inspection_location = State()
    engineer = State()
    due_date = State()
    letter = State()
    confirmation = State()  # Состояние подтверждения создания заявки


@router.message(F.text == "➕ Создать заявку")
async def start_new_request(message: Message, state: FSMContext):
    """Начало создания новой заявки."""
    async with async_session() as session:
        user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not user:
            await message.answer("Пользователь не найден.")
            return
        
        # Проверяем, является ли пользователь специалистом или суперадмином
        is_specialist = user.role == UserRole.SPECIALIST
        is_super_admin = (
            user.role == UserRole.MANAGER 
            and user.leader_profile 
            and user.leader_profile.is_super_admin
        )
        
        if not (is_specialist or is_super_admin):
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return
        
        await state.set_state(NewRequestStates.title)
        await state.update_data(specialist_id=user.id)

    await message.answer("Введите короткий заголовок заявки (до 255 символов).")


@router.message(StateFilter(NewRequestStates.title))
async def handle_title(message: Message, state: FSMContext):
    """Обработка ввода заголовка заявки."""
    title = message.text.strip()
    if not title:
        await message.answer("Заголовок не может быть пустым. Попробуйте снова.")
        return
    await state.update_data(title=title)
    await state.set_state(NewRequestStates.description)
    await message.answer("Опишите суть дефекта и требуемые работы.")


# Остальные обработчики создания заявки остаются в legacy файле
# TODO: Постепенно перенести все обработчики в этот модуль
# Они автоматически подключаются через legacy_router в главном __init__.py
