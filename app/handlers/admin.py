from aiogram import F, Router
from aiogram.types import CallbackQuery, InlineKeyboardButton, KeyboardButton, Message, ReplyKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models.user import User, UserRole
from app.infrastructure.db.session import async_session
from app.services.user_service import UserRoleService
from app.utils.pagination import clamp_page, total_pages_for

router = Router()
USERS_PAGE_SIZE = 10

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


async def _fetch_users_page(session, page: int) -> tuple[list[User], int, int, int]:
    total = await session.scalar(select(func.count()).select_from(User))
    total = int(total or 0)
    total_pages = total_pages_for(total, USERS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    users = (
        (
            await session.execute(
                select(User)
                .order_by(User.created_at.desc())
                .limit(USERS_PAGE_SIZE)
                .offset(page * USERS_PAGE_SIZE)
            )
        )
        .scalars()
        .all()
    )
    return users, page, total_pages, total


async def _show_users_list(message: Message, session, page: int, *, edit: bool = False) -> None:
    users, page, total_pages, total = await _fetch_users_page(session, page)
    if not users:
        text = "Пока нет зарегистрированных пользователей."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    text = "📋 <b>Список пользователей:</b>\n\n"
    start_index = page * USERS_PAGE_SIZE
    for idx, u in enumerate(users, start=start_index + 1):
        text += f"🧾 <b>{idx}. {u.full_name}</b> — {u.role}\n"
        text += f"   Telegram ID: <code>{u.telegram_id}</code>\n"
        text += f"   Username: @{u.username or 'Нет'}\n\n"
    text += f"Страница {page + 1}/{total_pages} · Всего: {total}"

    builder = InlineKeyboardBuilder()
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin:users_page:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="admin:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"admin:users_page:{page + 1}"))
        builder.row(*nav)

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


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
        await _show_users_list(message, session, page=0)


@router.callback_query(F.data.startswith("admin:users_page:"))
async def admin_users_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        manager = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == callback.from_user.id)
        )
        if not _is_super_admin(manager):
            await callback.answer("⚠️ Доступ только для супер-администраторов.", show_alert=True)
            return
        await _show_users_list(callback.message, session, page=page, edit=True)
    await callback.answer()


@router.callback_query(F.data == "admin:noop")
async def admin_noop(callback: CallbackQuery):
    await callback.answer()


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
