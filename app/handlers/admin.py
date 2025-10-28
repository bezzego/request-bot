from aiogram import F, Router
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models.user import User, UserRole
from app.infrastructure.db.session import async_session
from app.services.user_service import UserRoleService

router = Router()

# Клавиатура только для лидера (manager)
admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="👥 Список пользователей")],
        [KeyboardButton(text="🛠 Назначить роль")],
        [KeyboardButton(text="⬅️ Назад")],
    ],
    resize_keyboard=True,
)


def _is_super_admin(user: User | None) -> bool:
    return (
        user is not None
        and user.role == UserRole.MANAGER
        and user.leader_profile is not None
        and user.leader_profile.is_super_admin
    )


@router.message(F.text == "👥 Список пользователей")
async def list_users(message: Message):
    """Показывает всех пользователей бота"""
    async with async_session() as session:
        manager = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not _is_super_admin(manager):
            await message.answer("⚠️ Доступ только для супер-администраторов.")
            return

        result = await session.execute(select(User))
        users = result.scalars().all()

        if not users:
            await message.answer("Пока нет зарегистрированных пользователей.")
            return

        text = "📋 <b>Список пользователей:</b>\n\n"
        for u in users:
            text += f"🧾 <b>{u.full_name}</b> — {u.role}\n"
            text += f"   Telegram ID: <code>{u.telegram_id}</code>\n"
            text += f"   Username: @{u.username or 'Нет'}\n\n"

        await message.answer(text)


@router.message(F.text == "🛠 Назначить роль")
async def start_assign_role(message: Message):
    async with async_session() as session:
        manager = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not _is_super_admin(manager):
            await message.answer("⚠️ Доступ только для супер-администраторов.")
            return

    await message.answer(
        "Введите Telegram ID пользователя, которому хотите изменить роль.\n"
        "Формат: <code>/setrole [telegram_id] [роль]</code>\n\n"
        "Пример: <code>/setrole 123456789 specialist</code>\n\n"
        "Доступные роли:\n"
        f"• {UserRole.SPECIALIST}\n"
        f"• {UserRole.ENGINEER}\n"
        f"• {UserRole.MASTER}\n"
        f"• {UserRole.MANAGER}\n"
        f"• {UserRole.CLIENT}"
    )


@router.message(F.text.startswith("/setrole"))
async def assign_role(message: Message):
    """Команда: /setrole 123456789 specialist"""
    parts = message.text.split()

    if len(parts) != 3:
        await message.answer("⚠️ Формат неверный. Используйте: /setrole [telegram_id] [роль]")
        return

    _, telegram_id, role_name = parts

    # Проверим корректность роли
    try:
        new_role = UserRole(role_name)
    except ValueError:
        await message.answer(f"⚠️ Некорректная роль: {role_name}")
        return

    async with async_session() as session:
        manager = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not _is_super_admin(manager):
            await message.answer("⚠️ Доступ только для супер-администраторов.")
            return

        user = await session.scalar(select(User).where(User.telegram_id == int(telegram_id)))
        if not user:
            await message.answer(f"❌ Пользователь с ID {telegram_id} не найден.")
            return

        old_role = user.role
        await UserRoleService.assign_role(session, user, new_role)
        await session.commit()

        await message.answer(
            f"✅ Роль пользователя <b>{user.full_name}</b> изменена:\n"
            f"<b>{old_role}</b> → <b>{new_role}</b>"
        )
        print(
            f"[+] {message.from_user.full_name} изменил роль {user.full_name} с {old_role} на {new_role}"
        )
