from aiogram import F, Router
from aiogram.types import Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.config.settings import settings
from app.infrastructure.db.models.user import User, UserRole
from app.services.user_service import UserRoleService
from app.infrastructure.db.session import async_session
from app.keyboards import client_kb, engineer_kb, manager_kb, master_kb, specialist_kb

router = Router()


@router.message(F.text == "/start")
async def start_handler(message: Message):
    telegram_id = message.from_user.id
    full_name = message.from_user.full_name
    username = message.from_user.username or "Нет"
    is_super_admin = telegram_id in settings.SUPER_ADMIN_IDS

    async with async_session() as session:
        user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == telegram_id)
        )

        # Если пользователь новый — создаём
        if not user:
            default_role = UserRole.MANAGER if is_super_admin else UserRole.CLIENT
            user = User(
                telegram_id=telegram_id,
                full_name=full_name,
                username=username,
                role=default_role,
            )
            session.add(user)
            await session.flush()
            await UserRoleService.ensure_profile(session, user)
            if is_super_admin:
                await UserRoleService.set_super_admin(session, user, True)
            await session.commit()
            kb = manager_kb if is_super_admin else client_kb
            role_label = "супер-админ" if is_super_admin else "клиент"
            await message.answer(
                f"👋 Привет, {full_name}!\n\n"
                f"Вы зарегистрированы как {role_label}.\n"
                "Добро пожаловать!",
                reply_markup=kb,
            )
            print(
                f"[+] Новый пользователь: {full_name} ({telegram_id}) — роль {user.role.value.upper()}"
            )
            return

        if is_super_admin and user.role != UserRole.MANAGER:
            await UserRoleService.assign_role(session, user, UserRole.MANAGER)
        await UserRoleService.ensure_profile(session, user)
        await UserRoleService.set_super_admin(session, user, is_super_admin)
        await session.commit()

        # Если пользователь уже есть — подгружаем клавиатуру по роли
        role_keyboards = {
            UserRole.CLIENT: client_kb,
            UserRole.SPECIALIST: specialist_kb,
            UserRole.ENGINEER: engineer_kb,
            UserRole.MASTER: master_kb,
            UserRole.MANAGER: manager_kb,
        }

        kb = role_keyboards.get(user.role, client_kb)
        role_label = "super-admin" if is_super_admin else user.role.value
        await message.answer(
            f"👋 С возвращением, {user.full_name}!\n" f"Ваша роль: <b>{role_label}</b>.",
            reply_markup=kb,
        )
