from aiogram import F, Router
from aiogram.types import Message
from sqlalchemy import select

from app.infrastructure.db.models.user import User, UserRole
from app.infrastructure.db.session import async_session
from app.keyboards import client_kb, engineer_kb, manager_kb, master_kb, specialist_kb

router = Router()


@router.message(F.text == "/start")
async def start_handler(message: Message):
    telegram_id = message.from_user.id
    full_name = message.from_user.full_name
    username = message.from_user.username or "Нет"

    async with async_session() as session:
        user = await session.scalar(select(User).where(User.telegram_id == telegram_id))

        # Если пользователь новый — создаём
        if not user:
            user = User(
                telegram_id=telegram_id,
                full_name=full_name,
                username=username,
                role=UserRole.CLIENT,  # по умолчанию клиент
            )
            session.add(user)
            await session.commit()
            await message.answer(
                f"👋 Привет, {full_name}!\n\n"
                "Вы зарегистрированы как клиент.\n"
                "Ожидайте подтверждения роли от руководителя.",
                reply_markup=client_kb,
            )
            print(f"[+] Новый пользователь: {full_name} ({telegram_id}) — роль CLIENT")
            return

        # Если пользователь уже есть — подгружаем клавиатуру по роли
        role_keyboards = {
            UserRole.CLIENT: client_kb,
            UserRole.SPECIALIST: specialist_kb,
            UserRole.ENGINEER: engineer_kb,
            UserRole.MASTER: master_kb,
            UserRole.MANAGER: manager_kb,
        }

        kb = role_keyboards.get(user.role, client_kb)
        await message.answer(
            f"👋 С возвращением, {user.full_name}!\n" f"Ваша роль: <b>{user.role}</b>.",
            reply_markup=kb,
        )
