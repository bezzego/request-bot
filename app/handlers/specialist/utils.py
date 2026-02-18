"""Общие утилиты для обработчиков специалиста."""
from __future__ import annotations

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Contract,
    DefectType,
    Object,
    Request,
    User,
    UserRole,
)
from app.infrastructure.db.session import async_session

DEFECT_TYPES_PAGE_SIZE = 12
OBJECTS_PAGE_SIZE = 12
ADDRESSES_PAGE_SIZE = 12
CONTRACTS_PAGE_SIZE = 12
REQUESTS_PAGE_SIZE = 10


async def get_specialist(session, telegram_id: int) -> User | None:
    """Получает специалиста или суперадмина."""
    user = await session.scalar(
        select(User)
        .options(selectinload(User.leader_profile))
        .where(User.telegram_id == telegram_id)
    )
    if not user:
        return None
    
    # Проверяем, является ли пользователь специалистом
    if user.role == UserRole.SPECIALIST:
        return user
    
    # Проверяем, является ли пользователь суперадмином
    if user.role == UserRole.MANAGER and user.leader_profile and user.leader_profile.is_super_admin:
        return user
    
    return None


def is_super_admin(user: User | None) -> bool:
    """Проверяет, является ли пользователь суперадмином."""
    return (
        user is not None
        and user.role == UserRole.MANAGER
        and user.leader_profile is not None
        and user.leader_profile.is_super_admin
    )


async def get_defect_types_page(
    session, page: int = 0, page_size: int = DEFECT_TYPES_PAGE_SIZE
) -> tuple[list[DefectType], int, int]:
    """Возвращает (список типов дефектов для страницы, текущая страница, всего страниц)."""
    total = await session.scalar(select(func.count()).select_from(DefectType))
    total = int(total or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    offset = page * page_size
    items = (
        (
            await session.execute(
                select(DefectType)
                .order_by(DefectType.name.asc())
                .limit(page_size)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )
    return items, page, total_pages


async def get_objects_page(
    session, page: int = 0, page_size: int = OBJECTS_PAGE_SIZE
) -> tuple[list[Object], int, int]:
    """Возвращает (список объектов для страницы, текущая страница, всего страниц)."""
    total = await session.scalar(select(func.count()).select_from(Object))
    total = int(total or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    offset = page * page_size
    items = (
        (
            await session.execute(
                select(Object)
                .order_by(Object.name.asc())
                .limit(page_size)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )
    return items, page, total_pages


async def get_saved_objects(session, limit: int = 10) -> list[Object]:
    """Получает список ранее использованных объектов (ЖК). Оставлено для обратной совместимости."""
    return (
        (
            await session.execute(
                select(Object)
                .order_by(Object.created_at.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def get_saved_addresses(session, object_name: str | None = None, limit: int = 10) -> list[str]:
    """Получает список ранее использованных адресов (из заявок)."""
    if object_name:
        name_normalized = object_name.strip().lower()
        if not name_normalized:
            object_name = None
        else:
            query = (
                select(Request.address, func.max(Request.created_at).label('max_created_at'))
                .join(Object, Request.object_id == Object.id)
                .where(
                    Request.address.isnot(None),
                    func.lower(Object.name) == name_normalized,
                )
                .group_by(Request.address)
                .order_by(func.max(Request.created_at).desc())
                .limit(limit)
            )
            result = await session.execute(query)
            return [row[0] for row in result.all() if row[0]]
    if object_name is None or not (object_name or "").strip():
        query = (
            select(Request.address, func.max(Request.created_at).label('max_created_at'))
            .where(Request.address.isnot(None))
            .group_by(Request.address)
            .order_by(func.max(Request.created_at).desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return [row[0] for row in result.all() if row[0]]
    return []


async def get_addresses_page(
    session, object_name: str | None = None, page: int = 0, page_size: int = ADDRESSES_PAGE_SIZE
) -> tuple[list[str], int, int]:
    """Возвращает (список адресов для страницы, текущая страница, всего страниц)."""
    if object_name:
        name_normalized = object_name.strip().lower()
        if name_normalized:
            base_query = (
                select(Request.address)
                .join(Object, Request.object_id == Object.id)
                .where(
                    Request.address.isnot(None),
                    func.lower(Object.name) == name_normalized,
                )
            )
        else:
            object_name = None
    
    if not object_name:
        base_query = select(Request.address).where(Request.address.isnot(None))
    
    count_subquery = (
        base_query.group_by(Request.address).subquery()
    )
    total = await session.scalar(select(func.count()).select_from(count_subquery))
    total = int(total or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    offset = page * page_size
    
    query = (
        base_query
        .group_by(Request.address)
        .order_by(func.max(Request.created_at).desc())
        .limit(page_size)
        .offset(offset)
    )
    result = await session.execute(query)
    addresses = [row[0] for row in result.all() if row[0]]
    return addresses, page, total_pages


async def get_addresses_for_keyboard(session, object_name: str | None, limit: int = 15) -> list[str]:
    """Адреса для кнопок: сначала по текущему объекту, затем недавние по всем объектам."""
    seen = set()
    result: list[str] = []
    name = (object_name or "").strip() or None
    for addr in await get_saved_addresses(session, object_name=name, limit=limit):
        if addr and addr not in seen:
            seen.add(addr)
            result.append(addr)
    if len(result) >= limit:
        return result
    for addr in await get_saved_addresses(session, object_name=None, limit=limit * 2):
        if addr and addr not in seen:
            seen.add(addr)
            result.append(addr)
            if len(result) >= limit:
                break
    return result


async def get_contracts_page(
    session, page: int = 0, page_size: int = CONTRACTS_PAGE_SIZE
) -> tuple[list[Contract], int, int]:
    """Возвращает (список договоров для страницы, текущая страница, всего страниц)."""
    total = await session.scalar(select(func.count()).select_from(Contract))
    total = int(total or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    offset = page * page_size
    items = (
        (
            await session.execute(
                select(Contract)
                .order_by(Contract.number.asc())
                .limit(page_size)
                .offset(offset)
            )
        )
        .scalars()
        .all()
    )
    return items, page, total_pages


async def get_saved_contracts(session, limit: int = 10) -> list[Contract]:
    """Возвращает последние использованные договоры."""
    return (
        (
            await session.execute(
                select(Contract).order_by(Contract.created_at.desc()).limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def load_specialist_requests(session, specialist_id: int) -> list[Request]:
    """Загружает заявки специалиста для аналитики."""
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.engineer),
                    selectinload(Request.master),
                    selectinload(Request.work_items),
                )
                .where(Request.specialist_id == specialist_id)
                .order_by(Request.created_at.desc())
                .limit(15)
            )
        )
        .scalars()
        .all()
    )


def object_keyboard(
    objects: list[Object],
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    """Клавиатура выбора объекта (ЖК) с пагинацией."""
    builder = InlineKeyboardBuilder()
    for obj in objects:
        name = obj.name[:40] + "…" if len(obj.name) > 40 else obj.name
        builder.button(
            text=name,
            callback_data=f"spec:object:{obj.id}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data="spec:object:manual")
    
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"spec:object:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:object:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"spec:object:p:{page + 1}"))
        builder.row(*nav)
    
    builder.adjust(1)
    return builder.as_markup()


def address_keyboard(
    addresses: list[str],
    page: int = 0,
    total_pages: int = 1,
    prefix: str = "spec:address",
) -> InlineKeyboardMarkup:
    """Клавиатура выбора адреса с пагинацией."""
    builder = InlineKeyboardBuilder()
    for idx, addr in enumerate(addresses):
        addr_text = addr[:50] + "…" if len(addr) > 50 else addr
        builder.button(
            text=addr_text,
            callback_data=f"{prefix}_idx:{idx}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data=f"{prefix}:manual")
    
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{prefix}:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"{prefix}:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{prefix}:p:{page + 1}"))
        builder.row(*nav)
    
    builder.adjust(1)
    return builder.as_markup()


def contract_keyboard(
    contracts: list[Contract],
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    """Клавиатура выбора договора с пагинацией."""
    builder = InlineKeyboardBuilder()
    for contract in contracts:
        contract_text = contract.number or f"Договор {contract.id}"
        if contract.description:
            contract_text = f"{contract.number} — {contract.description[:30]}"
        contract_text = contract_text[:40] + "…" if len(contract_text) > 40 else contract_text
        builder.button(
            text=contract_text,
            callback_data=f"spec:contract:{contract.id}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data="spec:contract:manual")
    
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"spec:contract:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:contract:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"spec:contract:p:{page + 1}"))
        builder.row(*nav)
    
    builder.adjust(1)
    return builder.as_markup()


def defect_type_keyboard(
    defect_types: list[DefectType],
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    """Клавиатура выбора типа дефекта с пагинацией."""
    builder = InlineKeyboardBuilder()
    for defect in defect_types:
        name = defect.name[:40] + "…" if len(defect.name) > 40 else defect.name
        builder.button(
            text=name,
            callback_data=f"spec:defect:{defect.id}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data="spec:defect:manual")
    
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"spec:defect:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:defect:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"spec:defect:p:{page + 1}"))
        builder.row(*nav)
    
    builder.adjust(1)
    return builder.as_markup()
