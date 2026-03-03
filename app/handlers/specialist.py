from __future__ import annotations

import html
import logging
from datetime import date, datetime, time
from typing import Any

logger = logging.getLogger(__name__)

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InputMediaPhoto, InputMediaVideo, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Act,
    ActType,
    DefectType,
    Leader,
    Object,
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
    Contract,
)
from app.infrastructure.db.session import async_session
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month
from app.services.request_service import RequestCreateData, RequestService
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

router = Router()

SPEC_CALENDAR_PREFIX = "spec_inspection"
SPEC_DUE_CALENDAR_PREFIX = "spec_due"
REQUESTS_PAGE_SIZE = 10


async def _get_specialist(session, telegram_id: int) -> User | None:
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


def _is_super_admin(user: User | None) -> bool:
    """Проверяет, является ли пользователь суперадмином."""
    return (
        user is not None
        and user.role == UserRole.MANAGER
        and user.leader_profile is not None
        and user.leader_profile.is_super_admin
    )


DEFECT_TYPES_PAGE_SIZE = 12


async def _get_defect_types_page(
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


OBJECTS_PAGE_SIZE = 12


async def _get_objects_page(
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


async def _get_saved_objects(session, limit: int = 10) -> list[Object]:
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


async def _get_saved_addresses(session, object_name: str | None = None, limit: int = 10) -> list[str]:
    """Получает список ранее использованных адресов (из заявок). Вручную введённые сохраняются в заявке и попадают сюда."""
    # Используем GROUP BY вместо DISTINCT, чтобы можно было сортировать по created_at
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


ADDRESSES_PAGE_SIZE = 12


async def _get_addresses_page(
    session, object_name: str | None = None, page: int = 0, page_size: int = ADDRESSES_PAGE_SIZE
) -> tuple[list[str], int, int]:
    """Возвращает (список адресов для страницы, текущая страница, всего страниц)."""
    # Строим базовые условия для запроса
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
    
    # Подсчитываем общее количество уникальных адресов
    count_subquery = (
        base_query.group_by(Request.address).subquery()
    )
    total = await session.scalar(select(func.count()).select_from(count_subquery))
    total = int(total or 0)
    total_pages = max(1, (total + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    offset = page * page_size
    
    # Получаем адреса для текущей страницы
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


async def _get_addresses_for_keyboard(session, object_name: str | None, limit: int = 15) -> list[str]:
    """Адреса для кнопок: сначала по текущему объекту, затем недавние по всем объектам (в т.ч. введённые вручную). Оставлено для обратной совместимости."""
    seen = set()
    result: list[str] = []
    name = (object_name or "").strip() or None
    for addr in await _get_saved_addresses(session, object_name=name, limit=limit):
        if addr and addr not in seen:
            seen.add(addr)
            result.append(addr)
    if len(result) >= limit:
        return result
    for addr in await _get_saved_addresses(session, object_name=None, limit=limit * 2):
        if addr and addr not in seen:
            seen.add(addr)
            result.append(addr)
            if len(result) >= limit:
                break
    return result


def _object_keyboard(
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
    
    # Навигация пагинации в одну строку
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


def _address_keyboard(
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
    
    # Навигация пагинации в одну строку
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


def _contract_keyboard(
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
    
    # Навигация пагинации в одну строку
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


def _defect_type_keyboard(
    defect_types: list[DefectType],
    page: int = 0,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for defect in defect_types:
        # Обрезаем длинные названия для кнопки (лимит Telegram ~64 байта на callback_data, текст кнопки можно длиннее)
        name = defect.name[:40] + "…" if len(defect.name) > 40 else defect.name
        builder.button(
            text=name,
            callback_data=f"spec:defect:{defect.id}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data="spec:defect:manual")
    
    # Навигация пагинации в одну строку
    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"spec:defect:p:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:defect:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"spec:defect:p:{page + 1}"))
        builder.row(*nav)  # Навигация в одну строку
    
    builder.adjust(1)  # Кнопки дефектов в один столбец
    return builder.as_markup()


async def _prompt_inspection_calendar(message: Message):
    await message.answer(
        "Когда планируется комиссионный осмотр?\n"
        "Выберите дату через календарь или отправьте «-», если дата пока не определена.",
        reply_markup=build_calendar(SPEC_CALENDAR_PREFIX),
    )


CONTRACTS_PAGE_SIZE = 12


async def _get_contracts_page(
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


async def _get_saved_contracts(session, limit: int = 10) -> list[Contract]:
    """Возвращает последние использованные договоры. Оставлено для обратной совместимости."""
    return (
        (
            await session.execute(
                select(Contract).order_by(Contract.created_at.desc()).limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def _prompt_inspection_location(message: Message):
    await message.answer("Место осмотра (если отличается от адреса). Если совпадает — отправьте «-».")


class NewRequestStates(StatesGroup):
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
    confirmation = State()


class CloseRequestStates(StatesGroup):
    confirmation = State()
    comment = State()


class SpecialistFilterStates(StatesGroup):
    """Состояния для настройки фильтра заявок."""
    scope_selection = State()  # Выбор области фильтрации (для суперадминов: свои/все заявки)
    main_menu = State()  # Главное меню фильтра
    status_selection = State()  # Выбор статусов
    object_selection = State()  # Выбор объекта
    date_mode_selection = State()  # Выбор режима даты
    date_input = State()  # Ввод даты
    address_input = State()  # Ввод адреса
    contact_input = State()  # Ввод контактного лица
    engineer_selection = State()  # Выбор инженера
    master_selection = State()  # Выбор мастера
    number_input = State()  # Ввод номера заявки
    contract_selection = State()  # Выбор договора
    defect_selection = State()  # Выбор типа дефекта


@router.message(F.text == "📄 Мои заявки")
async def specialist_requests(message: Message, state: FSMContext):
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        is_super = _is_super_admin(specialist)
        # Для "Мои заявки" всегда показываем только свои: специалист — по specialist_id, суперадмин — по engineer_id
        filter_scope = None
        await _show_specialist_requests_list(message, session, specialist.id, page=0, is_super_admin=is_super, filter_scope=filter_scope)


@router.callback_query(F.data.startswith("spec:list:"))
async def specialist_requests_page(callback: CallbackQuery, state: FSMContext):
    """Навигация по страницам списка заявок (без фильтра)."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    # Очищаем фильтр при переходе на обычный список
    await state.update_data(spec_filter=None)
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = _is_super_admin(specialist)
        # Для "Мои заявки" всегда только свои: специалист — specialist_id, суперадмин — engineer_id
        filter_scope = None
        await _show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=page,
            context="list",
            filter_payload=None,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:filter:"))
async def specialist_filter_page(callback: CallbackQuery, state: FSMContext):
    """Навигация по страницам отфильтрованного списка заявок."""
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    # Восстанавливаем фильтр из state
    data = await state.get_data()
    filter_payload = data.get("spec_filter")
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = _is_super_admin(specialist)
        filter_scope = data.get("filter_scope") if is_super else None
        await _show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=page,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.message(F.text == "🔍 Фильтр заявок")
async def specialist_filter_start(message: Message, state: FSMContext):
    """Открывает новое расширенное меню фильтра для всех специалистов и супер-админов."""
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

    # Загружаем текущий фильтр из state
    data = await state.get_data()
    current_filter = data.get("spec_filter")
    
    is_super = _is_super_admin(specialist)
    
    # Для суперадминов проверяем, выбран ли уже scope (область фильтрации)
    if is_super:
        filter_scope = data.get("filter_scope")  # "mine" или "all"
        if not filter_scope:
            # Показываем выбор области фильтрации
            await state.set_state(SpecialistFilterStates.scope_selection)
            builder = InlineKeyboardBuilder()
            builder.button(text="📋 Только мои заявки", callback_data="spec:flt:scope:mine")
            builder.button(text="🌐 Все заявки", callback_data="spec:flt:scope:all")
            builder.adjust(1)
            
            await message.answer(
                "🔍 <b>Фильтр заявок</b>\n\n"
                "Выберите область фильтрации:",
                reply_markup=builder.as_markup(),
                parse_mode="HTML",
            )
            return
    
    # Для обычных специалистов или если scope уже выбран - показываем основное меню
    await state.set_state(SpecialistFilterStates.main_menu)
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and data.get("filter_scope") == "all") else "по вашим заявкам"
    filter_scope = data.get("filter_scope") if is_super else None
    await message.answer(
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


# Старые обработчики фильтра удалены - используется новый расширенный фильтр


@router.callback_query(F.data.startswith("spec:flt:scope:"))
async def specialist_filter_scope_select(callback: CallbackQuery, state: FSMContext):
    """Выбор области фильтрации для суперадмина (свои/все заявки)."""
    scope = callback.data.split(":")[3]  # "mine" или "all"
    
    await state.update_data(filter_scope=scope)
    await state.set_state(SpecialistFilterStates.main_menu)
    
    data = await state.get_data()
    current_filter = data.get("spec_filter")
    
    scope_text = "по всем заявкам" if scope == "all" else "по вашим заявкам"
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    await callback.message.edit_text(
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=scope),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:quick:"))
async def specialist_filter_quick(callback: CallbackQuery, state: FSMContext):
    """Быстрый выбор периода (использует новый формат фильтра)."""
    code = callback.data.split(":")[3]
    quick = quick_date_range(code)
    if not quick:
        await callback.answer("Неизвестный период.", show_alert=True)
        return
    start, end, label = quick
    # Используем новый формат фильтра
    filter_payload = {
        "date_mode": DateFilterMode.CREATED,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
    }
    await state.update_data(spec_filter=filter_payload)
    await state.set_state(None)

    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = _is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        await _show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=0,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:clear")
async def specialist_filter_clear(callback: CallbackQuery, state: FSMContext):
    await state.update_data(spec_filter=None)
    await state.set_state(None)
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = _is_super_admin(specialist)
        # При возврате к списку показываем "Мои заявки": специалист — specialist_id, суперадмин — engineer_id
        filter_scope = None
        await _show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=0,
            context="list",
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer("Фильтр сброшен.")


@router.callback_query(F.data == "spec:flt:cancel")
async def specialist_filter_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена настройки фильтра."""
    # Очищаем фильтр, но сохраняем filter_scope для суперадминов
    await state.update_data(spec_filter=None)
    await state.set_state(None)
    await callback.message.edit_text("Фильтр отменён.")
    await callback.answer()


# Новые обработчики расширенного фильтра

@router.callback_query(F.data == "spec:flt:back")
async def specialist_filter_back(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню фильтра."""
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        is_super = _is_super_admin(specialist)
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        filter_scope = data.get("filter_scope")
        
        await state.set_state(SpecialistFilterStates.main_menu)
        
        filter_info = ""
        if current_filter:
            filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
        
        scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
        await callback.message.edit_text(
            f"🔍 <b>Фильтр заявок</b>\n\n"
            f"Фильтрация {scope_text}.\n"
            f"Выберите параметры фильтрации:{filter_info}",
            reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:status")
async def specialist_filter_status_menu(callback: CallbackQuery, state: FSMContext):
    """Меню выбора статусов."""
    data = await state.get_data()
    current_filter = data.get("spec_filter")
    selected_statuses = current_filter.get("statuses") if current_filter else None
    
    await state.set_state(SpecialistFilterStates.status_selection)
    await callback.message.edit_text(
        "📊 <b>Выбор статусов</b>\n\n"
        "Выберите один или несколько статусов. Можно выбрать несколько.",
        reply_markup=_build_status_selection_keyboard(selected_statuses),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:status_toggle:"))
async def specialist_filter_status_toggle(callback: CallbackQuery, state: FSMContext):
    """Переключение выбора статуса."""
    status_key = callback.data.split(":")[3]
    
    # Маппинг ключей на названия из ТЗ
    status_mapping = {
        "new": "Новая",
        "assigned": "Принята в работу",
        "in_progress": "Приступили к выполнению",
        "completed": "Выполнена",
        "ready_for_sign": "Ожидает подписания",
        "cancelled": "Отмена",
    }
    
    status_name = status_mapping.get(status_key)
    if not status_name:
        await callback.answer("Неизвестный статус.", show_alert=True)
        return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    selected_statuses = current_filter.get("statuses") or []
    
    if status_name in selected_statuses:
        selected_statuses.remove(status_name)
    else:
        selected_statuses.append(status_name)
    
    if selected_statuses:
        current_filter["statuses"] = selected_statuses
    else:
        current_filter.pop("statuses", None)
    
    await state.update_data(spec_filter=current_filter)
    
    await callback.message.edit_text(
        "📊 <b>Выбор статусов</b>\n\n"
        "Выберите один или несколько статусов. Можно выбрать несколько.",
        reply_markup=_build_status_selection_keyboard(selected_statuses),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:object")
async def specialist_filter_object_menu(callback: CallbackQuery, state: FSMContext):
    """Меню выбора объекта."""
    async with async_session() as session:
        objects = await get_available_objects(session)
        
        if not objects:
            await callback.answer("Объекты не найдены.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        selected_object_id = current_filter.get("object_id") if current_filter else None
        
        await state.set_state(SpecialistFilterStates.object_selection)
        await callback.message.edit_text(
            "🏢 <b>Выбор объекта</b>\n\n"
            "Выберите объект для фильтрации:",
            reply_markup=_build_object_selection_keyboard(objects, selected_object_id),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:object_select:"))
async def specialist_filter_object_select(callback: CallbackQuery, state: FSMContext):
    """Выбор объекта."""
    object_id = int(callback.data.split(":")[3])
    
    async with async_session() as session:
        obj = await session.get(Object, object_id)
        if not obj:
            await callback.answer("Объект не найден.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter") or {}
        current_filter["object_id"] = object_id
        current_filter["object_name"] = obj.name
        await state.update_data(spec_filter=current_filter)
        
        objects = await get_available_objects(session)
        await callback.message.edit_text(
            "🏢 <b>Выбор объекта</b>\n\n"
            "Выберите объект для фильтрации:",
            reply_markup=_build_object_selection_keyboard(objects, object_id),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:object_remove")
async def specialist_filter_object_remove(callback: CallbackQuery, state: FSMContext):
    """Удаление фильтра по объекту."""
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter.pop("object_id", None)
    current_filter.pop("object_name", None)
    await state.update_data(spec_filter=current_filter)
    
    async with async_session() as session:
        objects = await get_available_objects(session)
        await callback.message.edit_text(
            "🏢 <b>Выбор объекта</b>\n\n"
            "Выберите объект для фильтрации:",
            reply_markup=_build_object_selection_keyboard(objects, None),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:date")
async def specialist_filter_date_mode_menu(callback: CallbackQuery, state: FSMContext):
    """Меню выбора режима фильтрации по дате."""
    await state.set_state(SpecialistFilterStates.date_mode_selection)
    await callback.message.edit_text(
        "📅 <b>Выбор режима фильтрации по дате</b>\n\n"
        "Выберите, по какой дате фильтровать заявки:",
        reply_markup=_build_date_mode_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:date_mode:"))
async def specialist_filter_date_mode_select(callback: CallbackQuery, state: FSMContext):
    """Выбор режима фильтрации по дате."""
    date_mode = callback.data.split(":")[3]
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter["date_mode"] = date_mode
    await state.update_data(spec_filter=current_filter)
    
    await state.set_state(SpecialistFilterStates.date_input)
    
    mode_labels = {
        "created": "дате создания",
        "planned": "плановой дате",
        "completed": "дате выполнения",
    }
    mode_label = mode_labels.get(date_mode, "дате")
    
    await callback.message.edit_text(
        f"📅 <b>Ввод периода</b>\n\n"
        f"Фильтрация по {mode_label}.\n\n"
        f"Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ\n"
        f"Или одну дату (ДД.ММ.ГГГГ) — покажем заявки за этот день.\n"
        f"Можно указать только начальную дату (с ДД.ММ.ГГГГ) или только конечную (до ДД.ММ.ГГГГ).",
        reply_markup=_specialist_filter_cancel_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(StateFilter(SpecialistFilterStates.date_input))
async def specialist_filter_date_input(message: Message, state: FSMContext):
    """Обработка ввода даты."""
    value = (message.text or "").strip()
    if value.lower() == "отмена":
        await state.set_state(SpecialistFilterStates.main_menu)
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        async with async_session() as session:
            specialist = await _get_specialist(session, message.from_user.id)
            is_super = _is_super_admin(specialist) if specialist else False
            filter_scope = data.get("filter_scope") if is_super else None
        filter_info = ""
        if current_filter:
            filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
        scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
        await message.answer(
            f"🔍 <b>Фильтр заявок</b>\n\n"
            f"Фильтрация {scope_text}.\n"
            f"Выберите параметры фильтрации:{filter_info}",
            reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
            parse_mode="HTML",
        )
        return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    date_mode = current_filter.get("date_mode", DateFilterMode.CREATED)
    
    # Парсим дату
    start, end, error = parse_date_range(value)
    if error:
        await message.answer(error)
        return
    
    if start:
        current_filter["date_start"] = start.isoformat()
    else:
        current_filter.pop("date_start", None)
    
    if end:
        current_filter["date_end"] = end.isoformat()
    else:
        current_filter.pop("date_end", None)
    
    await state.update_data(spec_filter=current_filter)
    await state.set_state(SpecialistFilterStates.main_menu)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        is_super = _is_super_admin(specialist) if specialist else False
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
    await message.answer(
        f"✅ Период сохранён.\n\n"
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


def _clean_filter_payload(filter_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Очищает фильтр от пустых значений и нормализует данные."""
    logger.info(f"[FILTER CLEAN] Input filter_payload: {filter_payload}")
    if not filter_payload:
        logger.info("[FILTER CLEAN] filter_payload is None or empty, returning None")
        return None
    
    cleaned = {}
    
    # Статусы
    statuses = filter_payload.get("statuses")
    if statuses and isinstance(statuses, list):
        cleaned_statuses = [s for s in statuses if s]
        if cleaned_statuses:
            cleaned["statuses"] = cleaned_statuses
    
    # ID поля - проверяем что это валидное число > 0
    for key in ["object_id", "engineer_id", "master_id", "contract_id", "defect_type_id"]:
        value = filter_payload.get(key)
        logger.info(f"[FILTER CLEAN] Processing {key}: {value} (type: {type(value)})")
        # Проверяем что значение существует и не пустое
        if value is not None:
            # Если это строка, проверяем что она не пустая
            if isinstance(value, str) and not value.strip():
                logger.info(f"[FILTER CLEAN] Skipping {key}: empty string")
                continue
            # Если это число 0 или отрицательное, пропускаем
            if isinstance(value, (int, float)) and value <= 0:
                logger.info(f"[FILTER CLEAN] Skipping {key}: <= 0")
                continue
            try:
                int_value = int(value)
                if int_value > 0:
                    logger.info(f"[FILTER CLEAN] Adding {key}: {int_value}")
                    cleaned[key] = int_value
                else:
                    logger.warning(f"[FILTER CLEAN] Skipping {key}: converted to {int_value} <= 0")
            except (ValueError, TypeError) as e:
                logger.warning(f"[FILTER CLEAN] Failed to convert {key} to int: {value}, error: {e}")
    
    # Строковые поля - проверяем что не пустые
    for key in ["address", "contact_person", "request_number"]:
        value = filter_payload.get(key)
        logger.info(f"[FILTER CLEAN] Processing string field {key}: {value}")
        if value and str(value).strip():
            cleaned_value = str(value).strip()
            logger.info(f"[FILTER CLEAN] Adding {key}: '{cleaned_value}'")
            cleaned[key] = cleaned_value
        else:
            logger.info(f"[FILTER CLEAN] Skipping {key}: empty or None")
    
    # Даты
    date_mode = filter_payload.get("date_mode")
    date_start = filter_payload.get("date_start")
    date_end = filter_payload.get("date_end")
    
    if date_start or date_end:
        cleaned["date_mode"] = date_mode or DateFilterMode.CREATED
        if date_start and str(date_start).strip():
            cleaned["date_start"] = str(date_start).strip()
        if date_end and str(date_end).strip():
            cleaned["date_end"] = str(date_end).strip()
    
    # Дополнительные поля для отображения
    for key in ["object_name", "engineer_name", "master_name", "contract_number", "defect_type_name"]:
        value = filter_payload.get(key)
        if value:
            logger.info(f"[FILTER CLEAN] Adding display field {key}: {value}")
            cleaned[key] = value
    
    logger.info(f"[FILTER CLEAN] Final cleaned filter: {cleaned}")
    result = cleaned if cleaned else None
    logger.info(f"[FILTER CLEAN] Returning: {result}")
    return result


@router.callback_query(F.data == "spec:flt:apply")
async def specialist_filter_apply(callback: CallbackQuery, state: FSMContext):
    """Применение фильтра."""
    logger.info("[FILTER APPLY] Starting filter apply")
    data = await state.get_data()
    filter_payload = data.get("spec_filter")
    logger.info(f"[FILTER APPLY] Raw filter_payload from state: {filter_payload}")
    
    # Проверяем, что есть хотя бы один параметр фильтра (до очистки)
    if not filter_payload:
        logger.warning("[FILTER APPLY] No filter_payload in state")
        await callback.answer("Выберите хотя бы один параметр фильтрации.", show_alert=True)
        return
    
    # Проверяем наличие хотя бы одного непустого параметра
    has_filter = (
        filter_payload.get("statuses")
        or filter_payload.get("object_id")
        or filter_payload.get("address")
        or filter_payload.get("contact_person")
        or filter_payload.get("engineer_id")
        or filter_payload.get("master_id")
        or filter_payload.get("request_number")
        or filter_payload.get("contract_id")
        or filter_payload.get("defect_type_id")
        or filter_payload.get("date_start")
        or filter_payload.get("date_end")
    )
    logger.info(f"[FILTER APPLY] has_filter check: {has_filter}")
    
    if not has_filter:
        logger.warning("[FILTER APPLY] No valid filter parameters found")
        await callback.answer("Выберите хотя бы один параметр фильтрации.", show_alert=True)
        return
    
    # Очищаем фильтр от пустых значений, но сохраняем все валидные данные
    cleaned_filter = _clean_filter_payload(filter_payload)
    logger.info(f"[FILTER APPLY] Cleaned filter: {cleaned_filter}")
    
    # Если после очистки фильтр стал пустым, значит все значения были невалидными
    if not cleaned_filter:
        logger.warning("[FILTER APPLY] Cleaned filter is empty")
        await callback.answer("Выберите хотя бы один валидный параметр фильтрации.", show_alert=True)
        return
    
    # Сохраняем очищенный фильтр обратно в state
    await state.update_data(spec_filter=cleaned_filter)
    await state.set_state(None)
    logger.info("[FILTER APPLY] Filter saved to state")
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        is_super = _is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        logger.info(f"[FILTER APPLY] Applying filter for specialist_id: {specialist.id}, is_super_admin: {is_super}, filter_scope: {filter_scope}")
        try:
            await _show_specialist_requests_list(
                callback.message,
                session,
                specialist.id,
                page=0,
                context="filter",
                filter_payload=cleaned_filter,
                edit=True,
                is_super_admin=is_super,
                filter_scope=filter_scope,
            )
            logger.info("[FILTER APPLY] Filter applied successfully")
            await callback.answer("Фильтр применён.")
        except Exception as e:
            logger.error(f"[FILTER APPLY] Error applying filter: {e}", exc_info=True)
            await callback.answer(f"Ошибка при применении фильтра: {str(e)}", show_alert=True)


@router.callback_query(F.data == "spec:flt:address")
async def specialist_filter_address(callback: CallbackQuery, state: FSMContext):
    """Фильтр по адресу."""
    await state.set_state(SpecialistFilterStates.address_input)
    await callback.message.edit_text(
        "🏠 <b>Фильтр по адресу</b>\n\n"
        "Введите часть адреса для поиска (улица, дом и т.п.):",
        reply_markup=_specialist_filter_cancel_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(StateFilter(SpecialistFilterStates.address_input))
async def specialist_filter_address_input(message: Message, state: FSMContext):
    """Обработка ввода адреса."""
    value = (message.text or "").strip()
    if value.lower() == "отмена":
        await state.set_state(SpecialistFilterStates.main_menu)
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        async with async_session() as session:
            specialist = await _get_specialist(session, message.from_user.id)
            is_super = _is_super_admin(specialist) if specialist else False
            filter_scope = data.get("filter_scope") if is_super else None
        filter_info = ""
        if current_filter:
            filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
        scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
        await message.answer(
            f"🔍 <b>Фильтр заявок</b>\n\n"
            f"Фильтрация {scope_text}.\n"
            f"Выберите параметры фильтрации:{filter_info}",
            reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
            parse_mode="HTML",
        )
        return
    
    if not value:
        await message.answer("Адрес не может быть пустым. Введите часть адреса.")
        return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter["address"] = value
    await state.update_data(spec_filter=current_filter)
    await state.set_state(SpecialistFilterStates.main_menu)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        is_super = _is_super_admin(specialist) if specialist else False
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
    await message.answer(
        f"✅ Адрес сохранён.\n\n"
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "spec:flt:contact")
async def specialist_filter_contact(callback: CallbackQuery, state: FSMContext):
    """Фильтр по контактному лицу."""
    await state.set_state(SpecialistFilterStates.contact_input)
    await callback.message.edit_text(
        "👤 <b>Фильтр по контактному лицу</b>\n\n"
        "Введите имя или часть имени контактного лица:",
        reply_markup=_specialist_filter_cancel_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(StateFilter(SpecialistFilterStates.contact_input))
async def specialist_filter_contact_input(message: Message, state: FSMContext):
    """Обработка ввода контактного лица."""
    value = (message.text or "").strip()
    if value.lower() == "отмена":
        await state.set_state(SpecialistFilterStates.main_menu)
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        async with async_session() as session:
            specialist = await _get_specialist(session, message.from_user.id)
            is_super = _is_super_admin(specialist) if specialist else False
            filter_scope = data.get("filter_scope") if is_super else None
        filter_info = ""
        if current_filter:
            filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
        scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
        await message.answer(
            f"🔍 <b>Фильтр заявок</b>\n\n"
            f"Фильтрация {scope_text}.\n"
            f"Выберите параметры фильтрации:{filter_info}",
            reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
            parse_mode="HTML",
        )
        return
    
    if not value:
        await message.answer("Имя контактного лица не может быть пустым.")
        return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter["contact_person"] = value
    await state.update_data(spec_filter=current_filter)
    await state.set_state(SpecialistFilterStates.main_menu)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        is_super = _is_super_admin(specialist) if specialist else False
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
    await message.answer(
        f"✅ Контактное лицо сохранено.\n\n"
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "spec:flt:engineer")
async def specialist_filter_engineer(callback: CallbackQuery, state: FSMContext):
    """Фильтр по инженеру."""
    async with async_session() as session:
        from app.infrastructure.db.models import UserRole
        engineers = await session.execute(
            select(User)
            .where(User.role == UserRole.ENGINEER)
            .order_by(User.full_name)
        )
        engineers_list = list(engineers.scalars().all())
        
        if not engineers_list:
            await callback.answer("Инженеры не найдены.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        selected_engineer_id = current_filter.get("engineer_id") if current_filter else None
        
        builder = InlineKeyboardBuilder()
        for engineer in engineers_list:
            prefix = "✅ " if selected_engineer_id and engineer.id == selected_engineer_id else ""
            builder.button(
                text=f"{prefix}{engineer.full_name}",
                callback_data=f"spec:flt:engineer_select:{engineer.id}"
            )
        
        if selected_engineer_id:
            builder.button(text="❌ Убрать инженера", callback_data="spec:flt:engineer_remove")
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await state.set_state(SpecialistFilterStates.engineer_selection)
        await callback.message.edit_text(
            "🔧 <b>Выбор инженера</b>\n\n"
            "Выберите инженера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:engineer_select:"))
async def specialist_filter_engineer_select(callback: CallbackQuery, state: FSMContext):
    """Выбор инженера."""
    engineer_id = int(callback.data.split(":")[3])
    
    async with async_session() as session:
        engineer = await session.get(User, engineer_id)
        if not engineer:
            await callback.answer("Инженер не найден.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter") or {}
        current_filter["engineer_id"] = engineer_id
        current_filter["engineer_name"] = engineer.full_name
        await state.update_data(spec_filter=current_filter)
        
        from app.infrastructure.db.models import UserRole
        engineers = await session.execute(
            select(User)
            .where(User.role == UserRole.ENGINEER)
            .order_by(User.full_name)
        )
        engineers_list = list(engineers.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for eng in engineers_list:
            prefix = "✅ " if eng.id == engineer_id else ""
            builder.button(
                text=f"{prefix}{eng.full_name}",
                callback_data=f"spec:flt:engineer_select:{eng.id}"
            )
        
        builder.button(text="❌ Убрать инженера", callback_data="spec:flt:engineer_remove")
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "🔧 <b>Выбор инженера</b>\n\n"
            "Выберите инженера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:engineer_remove")
async def specialist_filter_engineer_remove(callback: CallbackQuery, state: FSMContext):
    """Удаление фильтра по инженеру."""
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter.pop("engineer_id", None)
    current_filter.pop("engineer_name", None)
    await state.update_data(spec_filter=current_filter)
    
    async with async_session() as session:
        from app.infrastructure.db.models import UserRole
        engineers = await session.execute(
            select(User)
            .where(User.role == UserRole.ENGINEER)
            .order_by(User.full_name)
        )
        engineers_list = list(engineers.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for engineer in engineers_list:
            builder.button(
                text=f"{engineer.full_name}",
                callback_data=f"spec:flt:engineer_select:{engineer.id}"
            )
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "🔧 <b>Выбор инженера</b>\n\n"
            "Выберите инженера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:master")
async def specialist_filter_master(callback: CallbackQuery, state: FSMContext):
    """Фильтр по мастеру."""
    async with async_session() as session:
        from app.infrastructure.db.models import UserRole
        masters = await session.execute(
            select(User)
            .where(User.role == UserRole.MASTER)
            .order_by(User.full_name)
        )
        masters_list = list(masters.scalars().all())
        
        if not masters_list:
            await callback.answer("Мастера не найдены.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        selected_master_id = current_filter.get("master_id") if current_filter else None
        
        builder = InlineKeyboardBuilder()
        for master in masters_list:
            prefix = "✅ " if selected_master_id and master.id == selected_master_id else ""
            builder.button(
                text=f"{prefix}{master.full_name}",
                callback_data=f"spec:flt:master_select:{master.id}"
            )
        
        if selected_master_id:
            builder.button(text="❌ Убрать мастера", callback_data="spec:flt:master_remove")
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await state.set_state(SpecialistFilterStates.master_selection)
        await callback.message.edit_text(
            "👷 <b>Выбор мастера</b>\n\n"
            "Выберите мастера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:master_select:"))
async def specialist_filter_master_select(callback: CallbackQuery, state: FSMContext):
    """Выбор мастера."""
    master_id = int(callback.data.split(":")[3])
    
    async with async_session() as session:
        master = await session.get(User, master_id)
        if not master:
            await callback.answer("Мастер не найден.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter") or {}
        current_filter["master_id"] = master_id
        current_filter["master_name"] = master.full_name
        await state.update_data(spec_filter=current_filter)
        
        from app.infrastructure.db.models import UserRole
        masters = await session.execute(
            select(User)
            .where(User.role == UserRole.MASTER)
            .order_by(User.full_name)
        )
        masters_list = list(masters.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for m in masters_list:
            prefix = "✅ " if m.id == master_id else ""
            builder.button(
                text=f"{prefix}{m.full_name}",
                callback_data=f"spec:flt:master_select:{m.id}"
            )
        
        builder.button(text="❌ Убрать мастера", callback_data="spec:flt:master_remove")
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "👷 <b>Выбор мастера</b>\n\n"
            "Выберите мастера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:master_remove")
async def specialist_filter_master_remove(callback: CallbackQuery, state: FSMContext):
    """Удаление фильтра по мастеру."""
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter.pop("master_id", None)
    current_filter.pop("master_name", None)
    await state.update_data(spec_filter=current_filter)
    
    async with async_session() as session:
        from app.infrastructure.db.models import UserRole
        masters = await session.execute(
            select(User)
            .where(User.role == UserRole.MASTER)
            .order_by(User.full_name)
        )
        masters_list = list(masters.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for master in masters_list:
            builder.button(
                text=f"{master.full_name}",
                callback_data=f"spec:flt:master_select:{master.id}"
            )
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "👷 <b>Выбор мастера</b>\n\n"
            "Выберите мастера для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:number")
async def specialist_filter_number(callback: CallbackQuery, state: FSMContext):
    """Фильтр по номеру заявки."""
    await state.set_state(SpecialistFilterStates.number_input)
    await callback.message.edit_text(
        "🔢 <b>Фильтр по номеру заявки</b>\n\n"
        "Введите номер заявки или его часть (например, RQ-2026 или 20260211):",
        reply_markup=_specialist_filter_cancel_keyboard(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.message(StateFilter(SpecialistFilterStates.number_input))
async def specialist_filter_number_input(message: Message, state: FSMContext):
    """Обработка ввода номера заявки."""
    value = (message.text or "").strip().upper()
    if value.lower() == "отмена":
        await state.set_state(SpecialistFilterStates.main_menu)
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        async with async_session() as session:
            specialist = await _get_specialist(session, message.from_user.id)
            is_super = _is_super_admin(specialist) if specialist else False
            filter_scope = data.get("filter_scope") if is_super else None
        filter_info = ""
        if current_filter:
            filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
        scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
        await message.answer(
            f"🔍 <b>Фильтр заявок</b>\n\n"
            f"Фильтрация {scope_text}.\n"
            f"Выберите параметры фильтрации:{filter_info}",
            reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
            parse_mode="HTML",
        )
        return
    
    if not value:
        await message.answer("Номер заявки не может быть пустым.")
        return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter["request_number"] = value
    await state.update_data(spec_filter=current_filter)
    await state.set_state(SpecialistFilterStates.main_menu)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        is_super = _is_super_admin(specialist) if specialist else False
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
    await message.answer(
        f"✅ Номер заявки сохранён.\n\n"
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=_build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "spec:flt:contract")
async def specialist_filter_contract(callback: CallbackQuery, state: FSMContext):
    """Фильтр по договору."""
    async with async_session() as session:
        from app.infrastructure.db.models import Contract
        contracts = await session.execute(
            select(Contract)
            .order_by(Contract.number)
            .limit(50)
        )
        contracts_list = list(contracts.scalars().all())
        
        if not contracts_list:
            await callback.answer("Договоры не найдены.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        selected_contract_id = current_filter.get("contract_id") if current_filter else None
        
        builder = InlineKeyboardBuilder()
        for contract in contracts_list:
            prefix = "✅ " if selected_contract_id and contract.id == selected_contract_id else ""
            contract_text = contract.number or f"Договор {contract.id}"
            builder.button(
                text=f"{prefix}{contract_text}",
                callback_data=f"spec:flt:contract_select:{contract.id}"
            )
        
        if selected_contract_id:
            builder.button(text="❌ Убрать договор", callback_data="spec:flt:contract_remove")
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await state.set_state(SpecialistFilterStates.contract_selection)
        await callback.message.edit_text(
            "📄 <b>Выбор договора</b>\n\n"
            "Выберите договор для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:contract_select:"))
async def specialist_filter_contract_select(callback: CallbackQuery, state: FSMContext):
    """Выбор договора."""
    contract_id = int(callback.data.split(":")[3])
    
    async with async_session() as session:
        from app.infrastructure.db.models import Contract
        contract = await session.get(Contract, contract_id)
        if not contract:
            await callback.answer("Договор не найден.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter") or {}
        current_filter["contract_id"] = contract_id
        current_filter["contract_number"] = contract.number
        await state.update_data(spec_filter=current_filter)
        
        contracts = await session.execute(
            select(Contract)
            .order_by(Contract.number)
            .limit(50)
        )
        contracts_list = list(contracts.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for c in contracts_list:
            prefix = "✅ " if c.id == contract_id else ""
            contract_text = c.number or f"Договор {c.id}"
            builder.button(
                text=f"{prefix}{contract_text}",
                callback_data=f"spec:flt:contract_select:{c.id}"
            )
        
        builder.button(text="❌ Убрать договор", callback_data="spec:flt:contract_remove")
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "📄 <b>Выбор договора</b>\n\n"
            "Выберите договор для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:contract_remove")
async def specialist_filter_contract_remove(callback: CallbackQuery, state: FSMContext):
    """Удаление фильтра по договору."""
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter.pop("contract_id", None)
    current_filter.pop("contract_number", None)
    await state.update_data(spec_filter=current_filter)
    
    async with async_session() as session:
        from app.infrastructure.db.models import Contract
        contracts = await session.execute(
            select(Contract)
            .order_by(Contract.number)
            .limit(50)
        )
        contracts_list = list(contracts.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for contract in contracts_list:
            contract_text = contract.number or f"Договор {contract.id}"
            builder.button(
                text=f"{contract_text}",
                callback_data=f"spec:flt:contract_select:{contract.id}"
            )
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "📄 <b>Выбор договора</b>\n\n"
            "Выберите договор для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:defect")
async def specialist_filter_defect(callback: CallbackQuery, state: FSMContext):
    """Фильтр по типу дефекта."""
    async with async_session() as session:
        from app.infrastructure.db.models import DefectType
        defects = await session.execute(
            select(DefectType)
            .order_by(DefectType.name)
            .limit(50)
        )
        defects_list = list(defects.scalars().all())
        
        if not defects_list:
            await callback.answer("Типы дефектов не найдены.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter")
        selected_defect_id = current_filter.get("defect_type_id") if current_filter else None
        
        builder = InlineKeyboardBuilder()
        for defect in defects_list:
            prefix = "✅ " if selected_defect_id and defect.id == selected_defect_id else ""
            builder.button(
                text=f"{prefix}{defect.name}",
                callback_data=f"spec:flt:defect_select:{defect.id}"
            )
        
        if selected_defect_id:
            builder.button(text="❌ Убрать дефект", callback_data="spec:flt:defect_remove")
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await state.set_state(SpecialistFilterStates.defect_selection)
        await callback.message.edit_text(
            "⚠️ <b>Выбор типа дефекта</b>\n\n"
            "Выберите тип дефекта для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:defect_select:"))
async def specialist_filter_defect_select(callback: CallbackQuery, state: FSMContext):
    """Выбор типа дефекта."""
    defect_id = int(callback.data.split(":")[3])
    
    async with async_session() as session:
        from app.infrastructure.db.models import DefectType
        defect = await session.get(DefectType, defect_id)
        if not defect:
            await callback.answer("Тип дефекта не найден.", show_alert=True)
            return
        
        data = await state.get_data()
        current_filter = data.get("spec_filter") or {}
        current_filter["defect_type_id"] = defect_id
        current_filter["defect_type_name"] = defect.name
        await state.update_data(spec_filter=current_filter)
        
        defects = await session.execute(
            select(DefectType)
            .order_by(DefectType.name)
            .limit(50)
        )
        defects_list = list(defects.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for d in defects_list:
            prefix = "✅ " if d.id == defect_id else ""
            builder.button(
                text=f"{prefix}{d.name}",
                callback_data=f"spec:flt:defect_select:{d.id}"
            )
        
        builder.button(text="❌ Убрать дефект", callback_data="spec:flt:defect_remove")
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "⚠️ <b>Выбор типа дефекта</b>\n\n"
            "Выберите тип дефекта для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:defect_remove")
async def specialist_filter_defect_remove(callback: CallbackQuery, state: FSMContext):
    """Удаление фильтра по типу дефекта."""
    data = await state.get_data()
    current_filter = data.get("spec_filter") or {}
    current_filter.pop("defect_type_id", None)
    current_filter.pop("defect_type_name", None)
    await state.update_data(spec_filter=current_filter)
    
    async with async_session() as session:
        from app.infrastructure.db.models import DefectType
        defects = await session.execute(
            select(DefectType)
            .order_by(DefectType.name)
            .limit(50)
        )
        defects_list = list(defects.scalars().all())
        
        builder = InlineKeyboardBuilder()
        for defect in defects_list:
            builder.button(
                text=f"{defect.name}",
                callback_data=f"spec:flt:defect_select:{defect.id}"
            )
        
        builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
        builder.adjust(1)
        
        await callback.message.edit_text(
            "⚠️ <b>Выбор типа дефекта</b>\n\n"
            "Выберите тип дефекта для фильтрации:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    await callback.answer()


# Старый обработчик фильтра удален - используется новый расширенный фильтр


@router.callback_query(F.data.startswith("spec:detail:"))
async def specialist_request_detail(callback: CallbackQuery, state: FSMContext):
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
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
                selectinload(Request.work_items),
                selectinload(Request.work_sessions),
                selectinload(Request.photos),
                selectinload(Request.acts),
                selectinload(Request.feedback),
            )
            .where(Request.id == request_id)
        )
        
        # Если пользователь не суперадмин, ограничиваем заявки только теми, что закреплены за ним
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
        if not request:
            await callback.message.edit_text("Заявка не найдена или была удалена.")
            await callback.answer()
            return
        from app.handlers.engineer import _get_engineer
        engineer = await _get_engineer(session, callback.from_user.id)
        is_engineer = engineer and request.engineer_id == engineer.id

    detail_text = _format_specialist_request_detail(request)
    builder = InlineKeyboardBuilder()
    
    # Если специалист/суперадмин является инженером на этой заявке, показываем кнопки инженера
    if is_engineer:
        builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request.id}")
        if not request.inspection_completed_at:
            builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request.id}")
        builder.button(text="⏱ Плановые часы", callback_data=f"eng:set_planned_hours:{request.id}")
        builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request.id}")
        builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request.id}")
        builder.button(text="⏱ Срок устранения", callback_data=f"eng:set_term:{request.id}")
        builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request.id}")
        builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request.id}")
    
    # Добавляем кнопку просмотра фото
    if request.photos:
        builder.button(text="📷 Просмотреть фото", callback_data=f"spec:photos:{request.id}")
    
    # Добавляем кнопки для файлов (писем)
    letter_acts = [act for act in request.acts if act.type == ActType.LETTER]
    for act in letter_acts:
        file_name = act.file_name or f"Файл {act.id}"
        # Ограничиваем длину имени файла для кнопки
        button_text = file_name[:40] + "..." if len(file_name) > 40 else file_name
        builder.button(
            text=f"📎 {button_text}",
            callback_data=f"spec:file:{act.id}",
        )
    
    # Добавляем кнопку закрытия заявки, если можно закрыть
    can_close, reasons = await RequestService.can_close_request(request)
    if request.status == RequestStatus.CLOSED:
        builder.button(
            text="✅ Заявка закрыта",
            callback_data="spec:noop",
        )
    elif can_close:
        builder.button(
            text="✅ Закрыть заявку",
            callback_data=f"spec:close:{request.id}",
        )
    else:
        # Показываем, почему нельзя закрыть (только первую причину для краткости)
        reason_text = reasons[0][:35] + "..." if reasons and len(reasons[0]) > 35 else (reasons[0] if reasons else "не выполнены условия")
        builder.button(
            text=f"⚠️ {reason_text}",
            callback_data=f"spec:close_info:{request.id}",
        )
    
    # Кнопка удаления заявки (безвозвратно из БД); из карточки — возврат в карточку при отмене
    ctx_key = "filter" if context == "filter" else "list"
    if request.status != RequestStatus.CLOSED:
        builder.button(text="🗑 Удалить", callback_data=f"spec:delete:{request.id}:detail")

    back_callback = f"spec:list:{page}" if context == "list" else f"spec:filter:{page}"
    refresh_callback = (
        f"spec:detail:{request.id}:f:{page}" if context == "filter" else f"spec:detail:{request.id}:{page}"
    )
    builder.button(text="⬅️ Назад к списку", callback_data=back_callback)
    builder.button(text="🔄 Обновить", callback_data=refresh_callback)
    # Раскладываем все кнопки в один столбик, чтобы они не обрезались по ширине
    builder.adjust(1)
    
    # Сохраняем контекст фильтра в state для восстановления при возврате
    if context == "filter":
        data = await state.get_data()
        filter_payload = data.get("spec_filter")
        if not filter_payload:
            # Если фильтр был потерян, пытаемся восстановить из контекста
            # Но лучше просто сохранить пустой словарь, чтобы контекст сохранился
            await state.update_data(spec_filter={})
    
    await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("spec:delete:"))
async def specialist_delete_prompt(callback: CallbackQuery):
    """Показывает подтверждение безвозвратного удаления заявки из БД."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    from_detail = len(parts) >= 4 and parts[3] == "detail"  # spec:delete:id:detail
    if from_detail:
        cancel_cb = f"spec:detail:{request_id}"
        confirm_cb = f"spec:delete_confirm:{request_id}"
        ctx_key, page = "list", 0
    else:
        ctx_key = parts[3] if len(parts) >= 4 else "list"
        page = int(parts[4]) if len(parts) >= 5 else 0
        cancel_cb = f"spec:{ctx_key}:{page}"
        confirm_cb = f"spec:delete_confirm:{request_id}:{ctx_key}:{page}"

    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = select(Request).where(Request.id == request_id)
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
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


@router.callback_query(F.data.startswith("spec:delete_confirm:"))
async def specialist_delete_confirm(callback: CallbackQuery, state: FSMContext):
    """Безвозвратное удаление заявки из БД; при необходимости возврат к списку."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    return_to_list = len(parts) >= 5
    ctx_key = parts[3] if return_to_list else "list"
    page = int(parts[4]) if return_to_list else 0
    
    # Получаем фильтр из state для сохранения контекста
    data = await state.get_data()
    filter_payload = data.get("spec_filter") if ctx_key == "filter" else None

    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = select(Request).where(Request.id == request_id)
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
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
            data = await state.get_data()
            filter_payload = data.get("spec_filter") if context == "filter" else None
            is_super = _is_super_admin(specialist)
            filter_scope = data.get("filter_scope") if is_super else None
            _, _, total_pages, _ = await _fetch_specialist_requests_page(session, specialist.id, 0, filter_payload=filter_payload, is_super_admin=is_super, filter_scope=filter_scope)
            safe_page = min(page, max(0, total_pages - 1)) if total_pages else 0
            await _show_specialist_requests_list(
                callback.message,
                session,
                specialist.id,
                page=safe_page,
                context=context,
                filter_payload=filter_payload,
                edit=True,
                is_super_admin=is_super,
                filter_scope=filter_scope,
            )
            await callback.answer("Заявка удалена из базы")
            return

    await callback.message.edit_text("✅ Заявка удалена из базы.")
    await callback.answer("Заявка удалена")


@router.callback_query(F.data.startswith("spec:photos:"))
async def specialist_view_photos(callback: CallbackQuery):
    """Просмотр всех фото заявки для специалиста."""
    request_id = int(callback.data.split(":")[2])
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Request)
            .options(selectinload(Request.photos))
            .where(Request.id == request_id)
        )
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        photos = request.photos or []

    if not photos:
        await callback.answer("Фото не найдены.", show_alert=True)
        return

    from app.handlers.engineer import _send_all_photos
    await _send_all_photos(callback.message, photos)
    await callback.answer()


@router.callback_query(F.data.startswith("spec:close_info:"))
async def specialist_close_info(callback: CallbackQuery):
    """Показывает информацию о том, почему заявку нельзя закрыть."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
        )
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
        can_close, reasons = await RequestService.can_close_request(request)
        if can_close:
            await callback.answer("Заявку можно закрыть.", show_alert=True)
            return
        
        reasons_text = "\n".join(f"• {reason}" for reason in reasons)
        await callback.message.answer(
            f"⚠️ <b>Заявку нельзя закрыть</b>\n\n"
            f"Причины:\n{reasons_text}\n\n"
            f"Убедитесь, что все условия выполнены, и попробуйте снова.",
        )
        await callback.answer()


@router.callback_query(F.data.startswith("spec:close:"))
async def specialist_start_close(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс закрытия заявки."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
        )
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
        # Проверяем, можно ли закрыть
        can_close, reasons = await RequestService.can_close_request(request)
        if not can_close:
            reasons_text = "\n".join(f"• {reason}" for reason in reasons)
            await callback.message.answer(
                f"⚠️ <b>Заявку нельзя закрыть</b>\n\n"
                f"Причины:\n{reasons_text}",
            )
            await callback.answer()
            return
        
        if request.status == RequestStatus.CLOSED:
            await callback.answer("Заявка уже закрыта.", show_alert=True)
            return
        
        # Сохраняем данные в state
        request_label = format_request_label(request)
        await state.update_data(
            request_id=request_id,
            request_label=request_label,
        )
        await state.set_state(CloseRequestStates.comment)
        
        await callback.message.answer(
            f"📋 <b>Закрытие заявки {request_label}</b>\n\n"
            f"Заявка будет окончательно закрыта.\n\n"
            f"Введите комментарий к закрытию (или отправьте «-», чтобы пропустить):",
        )
        await callback.answer()


@router.message(StateFilter(CloseRequestStates.comment))
async def specialist_close_comment(message: Message, state: FSMContext):
    """Обрабатывает комментарий при закрытии заявки."""
    comment = message.text.strip() if message.text and message.text.strip() != "-" else None
    await state.update_data(comment=comment)
    await state.set_state(CloseRequestStates.confirmation)
    
    data = await state.get_data()
    request_label = data.get("request_label", "N/A")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить закрытие", callback_data="spec:close_confirm")
    builder.button(text="❌ Отменить", callback_data="spec:close_cancel")
    builder.adjust(1)
    
    comment_text = f"\n\nКомментарий: {comment}" if comment else "\n\nКомментарий не указан"
    await message.answer(
        f"📋 <b>Подтверждение закрытия заявки {request_label}</b>\n\n"
        f"Вы уверены, что хотите закрыть эту заявку?{comment_text}",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data == "spec:close_confirm", StateFilter(CloseRequestStates.confirmation))
async def specialist_close_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждает закрытие заявки."""
    data = await state.get_data()
    request_id = data.get("request_id")
    comment = data.get("comment")
    
    if not request_id:
        await callback.answer("Ошибка: не найден ID заявки.", show_alert=True)
        await state.clear()
        return
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            await state.clear()
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
        )
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        request = await session.scalar(query)
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            await state.clear()
            return
        
        # Проверяем ещё раз перед закрытием
        can_close, reasons = await RequestService.can_close_request(request)
        if not can_close:
            reasons_text = "\n".join(f"• {reason}" for reason in reasons)
            await callback.message.answer(
                f"⚠️ <b>Не удалось закрыть заявку</b>\n\n"
                f"Причины:\n{reasons_text}",
            )
            await callback.answer()
            await state.clear()
            return
        
        try:
            await RequestService.close_request(
                session,
                request,
                user_id=specialist.id,
                comment=comment,
            )
            await session.commit()
            
            label = format_request_label(request)
            await callback.message.answer(
                f"✅ <b>Заявка {label} успешно закрыта</b>\n\n"
                f"Все работы завершены, заявка закрыта.",
            )
            await callback.answer("Заявка закрыта")
            
            # Уведомляем инженера, если он назначен
            if request.engineer and request.engineer.telegram_id:
                try:
                    await callback.message.bot.send_message(
                        chat_id=int(request.engineer.telegram_id),
                        text=f"✅ Заявка {label} закрыта специалистом.",
                    )
                except Exception:
                    pass
            
        except ValueError as e:
            await callback.message.answer(
                f"❌ <b>Ошибка при закрытии заявки</b>\n\n{str(e)}",
            )
            await callback.answer("Ошибка", show_alert=True)
        except Exception as e:
            await callback.message.answer(
                f"❌ <b>Произошла ошибка</b>\n\n{str(e)}",
            )
            await callback.answer("Ошибка", show_alert=True)
    
    await state.clear()


@router.callback_query(F.data == "spec:close_cancel")
async def specialist_close_cancel(callback: CallbackQuery, state: FSMContext):
    """Отменяет закрытие заявки."""
    await state.clear()
    await callback.message.answer("Закрытие заявки отменено.")
    await callback.answer()


@router.callback_query(F.data == "spec:noop")
async def specialist_noop(callback: CallbackQuery):
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()


@router.callback_query(F.data.startswith("spec:file:"))
async def specialist_open_file(callback: CallbackQuery):
    """Отправляет прикреплённый файл пользователю."""
    _, _, act_id_str = callback.data.split(":")
    act_id = int(act_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        # Проверяем, является ли пользователь суперадмином
        is_super_admin = _is_super_admin(specialist)
        
        # Формируем запрос с учетом прав доступа
        query = (
            select(Act)
            .join(Request)
            .where(
                Act.id == act_id,
                Act.type == ActType.LETTER,
            )
        )
        if not is_super_admin:
            query = query.where(Request.specialist_id == specialist.id)
        
        act = await session.scalar(query)
        
        if not act:
            await callback.answer("Файл не найден.", show_alert=True)
            return
        
        try:
            # Отправляем файл пользователю
            await callback.message.bot.send_document(
                chat_id=callback.from_user.id,
                document=act.file_id,
                caption=f"📎 {act.file_name or 'Файл'}",
            )
            await callback.answer("Файл отправлен.")
        except Exception as e:
            await callback.answer(f"Ошибка при отправке файла: {str(e)}", show_alert=True)


@router.callback_query(F.data.startswith("spec:back"))
async def specialist_back_to_list(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    page = 0
    context = "list"
    
    # Определяем контекст и страницу из callback_data
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
    
    # Получаем фильтр из state
    data = await state.get_data()
    filter_payload = data.get("spec_filter")
    
    # Определяем контекст по наличию фильтра
    if filter_payload and any(filter_payload.values()):
        context = "filter"
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = _is_super_admin(specialist)
        filter_scope = data.get("filter_scope") if is_super else None
        await _show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=page,
            context=context,
            filter_payload=filter_payload,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.message(F.text == "📊 Аналитика")
async def specialist_analytics(message: Message):
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        requests = await _load_specialist_requests(session, specialist.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Создайте заявку, чтобы начать работу.")
        return

    summary_text = _build_specialist_analytics(requests)
    await message.answer(summary_text)


@router.message(F.text == "➕ Создать заявку")
async def start_new_request(message: Message, state: FSMContext):
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
    title = message.text.strip()
    if not title:
        await message.answer("Заголовок не может быть пустым. Попробуйте снова.")
        return
    await state.update_data(title=title)
    await state.set_state(NewRequestStates.description)
    await message.answer("Опишите суть дефекта и требуемые работы.")


@router.message(StateFilter(NewRequestStates.description))
async def handle_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    
    # Показываем сохранённые ЖК с пагинацией
    async with async_session() as session:
        objects, page, total_pages = await _get_objects_page(session, page=0)
    
    await state.set_state(NewRequestStates.object_name)
    await state.update_data(object_page=0)
    
    if objects:
        await message.answer(
            "Выберите ЖК из списка или введите вручную:",
            reply_markup=_object_keyboard(objects, page=page, total_pages=total_pages),
        )
    else:
        await message.answer("Укажите объект (например, ЖК «Север», корпус 3).")


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data == "spec:object:noop")
async def handle_object_noop(callback: CallbackQuery):
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data.startswith("spec:object:p:"))
async def handle_object_page(callback: CallbackQuery, state: FSMContext):
    """Переключение страницы списка объектов."""
    try:
        page = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return

    async with async_session() as session:
        objects, cur_page, total_pages = await _get_objects_page(session, page=page)

    await state.update_data(object_page=cur_page)

    if objects:
        await callback.message.edit_reply_markup(
            reply_markup=_object_keyboard(objects, page=cur_page, total_pages=total_pages),
        )
    await callback.answer()
    return


async def _handle_object_selection(callback: CallbackQuery, state: FSMContext):
    """Общая логика выбора объекта."""
    if callback.data == "spec:object:manual":
        await state.set_state(NewRequestStates.object_name)
        await callback.message.edit_reply_markup()
        await callback.message.answer("Укажите объект (например, ЖК «Север», корпус 3).")
        await callback.answer()
        return
    
    if callback.data.startswith("spec:object:"):
        try:
            object_id = int(callback.data.split(":")[2])
            async with async_session() as session:
                obj = await session.get(Object, object_id)
                if obj:
                    object_name = obj.name
                    await state.update_data(object_name=object_name)
                    await callback.message.edit_text(f"ЖК: {object_name}")
                    
                    # Показываем адреса с пагинацией
                    addresses, addr_page, addr_total_pages = await _get_addresses_page(session, object_name=object_name, page=0)
                    
                    if addresses:
                        await state.update_data(saved_addresses=addresses, address_page=0)
                        await state.set_state(NewRequestStates.object_name)  # Остаёмся в этом состоянии для обработки адреса
                        await callback.message.answer(
                            "Выберите адрес из списка или введите вручную:",
                            reply_markup=_address_keyboard(addresses, page=addr_page, total_pages=addr_total_pages),
                        )
                    else:
                        await state.set_state(NewRequestStates.address)
                        await callback.message.answer("Укажите адрес объекта.")
                    await callback.answer()
                    return
        except (ValueError, IndexError):
            pass
    
    await callback.answer("Ошибка выбора ЖК. Попробуйте снова.", show_alert=True)


@router.callback_query(StateFilter(NewRequestStates.description), F.data.startswith("spec:object"))
async def handle_object_choice(callback: CallbackQuery, state: FSMContext):
    await _handle_object_selection(callback, state)


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data.startswith("spec:object"))
async def handle_object_choice_from_object_state(callback: CallbackQuery, state: FSMContext):
    """Обработчик выбора объекта в состоянии object_name (для пагинации)."""
    await _handle_object_selection(callback, state)


@router.message(StateFilter(NewRequestStates.object_name))
async def handle_object(message: Message, state: FSMContext):
    object_name = message.text.strip()
    if not object_name:
        await message.answer("Название объекта не может быть пустым.")
        return
    
    # Сохраняем вручную введённый объект в справочник
    async with async_session() as session:
        try:
            await RequestService._get_or_create_object(session, object_name, None)
            await session.commit()
        except Exception:
            await session.rollback()
        # В любом случае продолжаем — объект попадёт в заявку при создании
        
        # Показываем адреса с пагинацией
        addresses, addr_page, addr_total_pages = await _get_addresses_page(session, object_name=object_name, page=0)
    
    await state.update_data(object_name=object_name, saved_addresses=addresses, address_page=0)
    
    if addresses:
        await message.answer(
            f"Объект «{object_name}» сохранён в справочник.\n\n"
            "Выберите адрес из списка или введите вручную:",
            reply_markup=_address_keyboard(addresses, page=addr_page, total_pages=addr_total_pages),
        )
    else:
        await state.set_state(NewRequestStates.address)
        await message.answer(f"Объект «{object_name}» сохранён в справочник.\n\nУкажите адрес объекта.")


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data == "spec:address:noop")
async def handle_address_noop(callback: CallbackQuery):
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data.startswith("spec:address:p:"))
async def handle_address_page(callback: CallbackQuery, state: FSMContext):
    """Переключение страницы списка адресов."""
    try:
        page = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return

    data = await state.get_data()
    object_name = data.get("object_name")

    async with async_session() as session:
        addresses, cur_page, total_pages = await _get_addresses_page(session, object_name=object_name, page=page)

    await state.update_data(saved_addresses=addresses, address_page=cur_page)

    if addresses:
        await callback.message.edit_reply_markup(
            reply_markup=_address_keyboard(addresses, page=cur_page, total_pages=total_pages),
        )
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data.startswith("spec:address"))
async def handle_address_choice(callback: CallbackQuery, state: FSMContext):
    if callback.data == "spec:address:manual":
        await state.set_state(NewRequestStates.address)
        await callback.message.edit_reply_markup()
        await callback.message.answer("Укажите адрес объекта.")
        await callback.answer()
        return
    
    if callback.data.startswith("spec:address_idx:"):
        data = await state.get_data()
        saved_addresses = data.get("saved_addresses", [])
        try:
            idx = int(callback.data.split(":")[2])
            if 0 <= idx < len(saved_addresses):
                address = saved_addresses[idx]
                await state.update_data(address=address, saved_addresses=None)
                await state.set_state(NewRequestStates.apartment)
                await callback.message.edit_text(f"Адрес: {address}")
                await callback.message.answer("Укажите номер квартиры (или отправьте «-», если не применимо).")
                await callback.answer()
                return
        except (ValueError, IndexError):
            pass
    
    await callback.answer("Ошибка выбора адреса. Попробуйте снова.", show_alert=True)


@router.message(StateFilter(NewRequestStates.address))
async def handle_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text.strip())
    await state.set_state(NewRequestStates.apartment)
    await message.answer("Укажите номер квартиры (или отправьте «-», если не применимо).")


@router.message(StateFilter(NewRequestStates.apartment))
async def handle_apartment(message: Message, state: FSMContext):
    apartment = message.text.strip()
    await state.update_data(apartment=None if apartment == "-" else apartment)
    await state.set_state(NewRequestStates.contact_person)
    await message.answer("Контактное лицо на объекте (ФИО).")


@router.message(StateFilter(NewRequestStates.contact_person))
async def handle_contact_person(message: Message, state: FSMContext):
    await state.update_data(contact_person=message.text.strip())
    await state.set_state(NewRequestStates.contact_phone)
    await message.answer("Телефон контактного лица.")


@router.message(StateFilter(NewRequestStates.contact_phone))
async def handle_contact_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if len(phone) < 6:
        await message.answer("Похоже, номер слишком короткий. Введите номер полностью.")
        return
    await state.update_data(contact_phone=phone)

    # Показываем сохранённые договоры с пагинацией
    async with async_session() as session:
        contracts, page, total_pages = await _get_contracts_page(session, page=0)

    await state.set_state(NewRequestStates.contract_number)
    await state.update_data(contract_page=0)

    if contracts:
        await message.answer(
            "Выберите номер договора из списка или введите вручную.\n"
            "Если договора нет — отправьте «-».",
            reply_markup=_contract_keyboard(contracts, page=page, total_pages=total_pages),
        )
    else:
        await message.answer("Номер договора (если нет — отправьте «-»).")


@router.callback_query(StateFilter(NewRequestStates.contract_number), F.data == "spec:contract:noop")
async def handle_contract_noop(callback: CallbackQuery):
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.contract_number), F.data.startswith("spec:contract:p:"))
async def handle_contract_page(callback: CallbackQuery, state: FSMContext):
    """Переключение страницы списка договоров."""
    try:
        page = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return

    async with async_session() as session:
        contracts, cur_page, total_pages = await _get_contracts_page(session, page=page)

    await state.update_data(contract_page=cur_page)

    if contracts:
        await callback.message.edit_reply_markup(
            reply_markup=_contract_keyboard(contracts, page=cur_page, total_pages=total_pages),
        )
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.contract_number), F.data.startswith("spec:contract:"))
async def handle_contract_choice(callback: CallbackQuery, state: FSMContext):
    _, _, contract_id_str = callback.data.split(":")
    if contract_id_str == "manual":
        await callback.message.edit_reply_markup()
        await callback.message.answer("Введите номер договора (если нет — отправьте «-»).")
        await callback.answer()
        return

    try:
        contract_id = int(contract_id_str)
    except ValueError:
        await callback.answer("Некорректный договор. Введите номер вручную.", show_alert=True)
        return

    async with async_session() as session:
        contract = await session.get(Contract, contract_id)

    if not contract:
        await callback.answer("Договор не найден. Введите номер вручную.", show_alert=True)
        return

    await state.update_data(contract_number=contract.number)
    await callback.message.edit_text(f"Договор: {contract.number}")

    async with async_session() as session:
        defect_types, page, total_pages = await _get_defect_types_page(session, page=0)

    await state.set_state(NewRequestStates.defect_type)
    await state.update_data(defect_page=0)
    if defect_types:
        await callback.message.answer(
            "Выберите тип дефекта из списка или введите свой текстом.",
            reply_markup=_defect_type_keyboard(defect_types, page=page, total_pages=total_pages),
        )
    else:
        await callback.message.answer("Тип дефекта (например, «Трещины в стене»).")
    await callback.answer()


@router.message(StateFilter(NewRequestStates.contract_number))
async def handle_contract(message: Message, state: FSMContext):
    contract = (message.text or "").strip()
    contract_number = None if contract == "-" else contract or None
    
    # Сохраняем вручную введённый договор в справочник
    if contract_number:
        async with async_session() as session:
            try:
                await RequestService._get_or_create_contract(session, contract_number, None)
                await session.commit()
            except Exception:
                await session.rollback()
            # В любом случае продолжаем — договор попадёт в заявку при создании
    
    await state.update_data(contract_number=contract_number)

    async with async_session() as session:
        defect_types, page, total_pages = await _get_defect_types_page(session, page=0)

    await state.set_state(NewRequestStates.defect_type)
    await state.update_data(defect_page=0)
    if defect_types:
        await message.answer(
            "Выберите тип дефекта из списка или введите свой текстом.",
            reply_markup=_defect_type_keyboard(defect_types, page=page, total_pages=total_pages),
        )
    else:
        await message.answer("Тип дефекта (например, «Трещины в стене»).")


@router.callback_query(StateFilter(NewRequestStates.defect_type), F.data == "spec:defect:noop")
async def handle_defect_type_noop(callback: CallbackQuery):
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.defect_type), F.data.startswith("spec:defect:p:"))
async def handle_defect_type_page(callback: CallbackQuery, state: FSMContext):
    """Переключение страницы списка типов дефектов."""
    try:
        page = int(callback.data.split(":")[3])
    except (IndexError, ValueError):
        await callback.answer()
        return

    async with async_session() as session:
        defect_types, cur_page, total_pages = await _get_defect_types_page(session, page=page)

    await state.update_data(defect_page=cur_page)

    if defect_types:
        await callback.message.edit_reply_markup(
            reply_markup=_defect_type_keyboard(defect_types, page=cur_page, total_pages=total_pages),
        )
    await callback.answer()
    return


@router.callback_query(StateFilter(NewRequestStates.defect_type), F.data.startswith("spec:defect:"))
async def handle_defect_type_choice(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    type_id = parts[2] if len(parts) >= 3 else ""
    if type_id == "manual":
        await callback.answer("Введите тип дефекта сообщением.")
        return
    if type_id == "noop" or type_id == "p":
        return  # уже обработано выше

    try:
        defect_type_id = int(type_id)
    except ValueError:
        await callback.answer()
        return

    async with async_session() as session:
        defect = await session.scalar(select(DefectType).where(DefectType.id == defect_type_id))

    if not defect:
        await callback.answer("Тип не найден. Введите вручную.", show_alert=True)
        return

    await state.update_data(defect_type=defect.name)
    await state.set_state(NewRequestStates.inspection_datetime)
    await callback.message.edit_text(f"Тип дефекта: {defect.name}")
    await _prompt_inspection_calendar(callback.message)
    await callback.answer()


@router.message(StateFilter(NewRequestStates.defect_type))
async def handle_defect_type(message: Message, state: FSMContext):
    defect = message.text.strip()
    if defect == "-":
        await state.update_data(defect_type=None)
        await state.set_state(NewRequestStates.inspection_datetime)
        await _prompt_inspection_calendar(message)
        return

    if not defect:
        await message.answer("Введите тип дефекта текстом или выберите из списка.")
        return

    # Сохраняем введённый вручную тип дефекта в справочник, чтобы он появлялся в списке в следующий раз
    async with async_session() as session:
        try:
            await RequestService._get_or_create_defect_type(session, defect)
            await session.commit()
        except Exception:
            await session.rollback()
        # В любом случае продолжаем — тип попадёт в заявку при создании

    await state.update_data(defect_type=defect)
    await state.set_state(NewRequestStates.inspection_datetime)
    await message.answer(f"Тип дефекта «{defect}» сохранён в справочник и будет в списке при следующих заявках.")
    await _prompt_inspection_calendar(message)


@router.message(StateFilter(NewRequestStates.inspection_datetime))
async def handle_inspection_datetime(message: Message, state: FSMContext):
    text = message.text.strip()
    if text == "-":
        await state.update_data(inspection_datetime=None, inspection_date=None)
        await state.set_state(NewRequestStates.inspection_location)
        await _prompt_inspection_location(message)
        return

    await message.answer(
        "Дата выбирается через календарь. Нажмите на нужный день или отправьте «-», если дата неизвестна."
    )


@router.callback_query(
    StateFilter(NewRequestStates.inspection_datetime),
    F.data.startswith(f"cal:{SPEC_CALENDAR_PREFIX}:"),
)
async def specialist_calendar_callback(callback: CallbackQuery, state: FSMContext):
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar(SPEC_CALENDAR_PREFIX, year=new_year, month=new_month)
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        selected = date(payload.year, payload.month, payload.day)
        await state.update_data(
            inspection_date=selected.isoformat(),
            inspection_datetime=None,
        )
        await state.set_state(NewRequestStates.inspection_time)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(
            f"Дата осмотра: {selected.strftime('%d.%m.%Y')}.\n"
            "Введите время в формате ЧЧ:ММ или отправьте «-», если время пока неизвестно."
        )
        await callback.answer(f"Выбрано {selected.strftime('%d.%m.%Y')}")
        return

    await callback.answer()


@router.message(StateFilter(NewRequestStates.inspection_location))
async def handle_inspection_location(message: Message, state: FSMContext):
    location = message.text.strip()
    await state.update_data(inspection_location=None if location == "-" else location)

    async with async_session() as session:
        data = await state.get_data()
        specialist_id = data.get("specialist_id")
        
        # Получаем текущего пользователя для проверки "(я)"
        current_user = await session.scalar(
            select(User).where(User.telegram_id == message.from_user.id)
        )
        current_user_id = current_user.id if current_user else None
        
        # Получаем инженеров
        engineers_query = select(User).where(User.role == UserRole.ENGINEER)
        
        # Получаем суперадминов (менеджеры с is_super_admin = True)
        superadmins_query = (
            select(User)
            .join(Leader, User.id == Leader.user_id)
            .where(User.role == UserRole.MANAGER, Leader.is_super_admin == True)
        )
        
        # Объединяем запросы
        engineers_result = await session.execute(engineers_query)
        engineers = list(engineers_result.scalars().all())
        
        superadmins_result = await session.execute(superadmins_query)
        superadmins = list(superadmins_result.scalars().all())
        
        # Получаем самого специалиста, если он не инженер и не суперадмин
        specialist = None
        if specialist_id:
            specialist = await session.get(User, specialist_id)
            if specialist:
                # Проверяем, не является ли он уже в списке
                engineer_ids = {eng.id for eng in engineers}
                superadmin_ids = {sa.id for sa in superadmins}
                if specialist.id not in engineer_ids and specialist.id not in superadmin_ids:
                    # Добавляем специалиста в список
                    engineers.append(specialist)
                else:
                    specialist = None  # Уже в списке, не добавляем отдельно

    # Объединяем всех кандидатов
    all_candidates = engineers + superadmins
    if specialist and specialist not in all_candidates:
        all_candidates.append(specialist)
    
    if not all_candidates:
        await message.answer("Нет доступных инженеров. Обратитесь к руководителю.")
        await state.clear()
        return

    # Сортируем по имени
    all_candidates.sort(key=lambda u: u.full_name)
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{user.full_name}{' (я)' if current_user_id and user.id == current_user_id else ''}",
                    callback_data=f"assign_engineer:{user.id}",
                )
            ]
            for user in all_candidates
        ]
    )
    await state.set_state(NewRequestStates.engineer)
    await message.answer("Выберите ответственного инженера для заявки:", reply_markup=kb)


@router.callback_query(StateFilter(NewRequestStates.engineer), F.data.startswith("assign_engineer:"))
async def handle_engineer_callback(callback: CallbackQuery, state: FSMContext):
    try:
        engineer_id = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer("Ошибка при выборе инженера. Попробуйте снова.", show_alert=True)
        return
    
    # Проверяем, что выбранный пользователь существует и может быть инженером
    async with async_session() as session:
        engineer_user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.id == engineer_id)
        )
        if not engineer_user:
            await callback.answer("Выбранный пользователь не найден.", show_alert=True)
            return
        
        # Проверяем, что пользователь может быть инженером
        can_be_engineer = (
            engineer_user.role == UserRole.ENGINEER
            or engineer_user.role == UserRole.SPECIALIST
            or (engineer_user.role == UserRole.MANAGER 
                and engineer_user.leader_profile 
                and engineer_user.leader_profile.is_super_admin)
        )
        if not can_be_engineer:
            await callback.answer("Выбранный пользователь не может быть назначен инженером.", show_alert=True)
            return
    
    await state.update_data(engineer_id=engineer_id)
    await state.set_state(NewRequestStates.letter)
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass
    await callback.message.answer(
        "Прикрепите файл обращения (письмо) в формате PDF/документа или отправьте «-», если письма нет.\n"
        "Для отмены напишите «Отмена».",
    )
    await callback.answer()


@router.message(StateFilter(NewRequestStates.letter), F.document)
async def handle_letter_document(message: Message, state: FSMContext):
    document = message.document
    await state.update_data(
        letter_file_id=document.file_id,
        letter_file_name=document.file_name,
    )
    await _send_summary(message, state)


@router.message(StateFilter(NewRequestStates.letter))
async def handle_letter_choice(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Создание заявки отменено.")
        return
    if text in {"-", "нет", "без письма"}:
        await state.update_data(letter_file_id=None, letter_file_name=None)
        await _send_summary(message, state)
        return

    await message.answer("Прикрепите файл обращения (например, PDF) или отправьте «-», если письма нет.")


@router.callback_query(F.data == "spec:confirm_request", StateFilter(NewRequestStates.confirmation))
async def confirm_request(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    async with async_session() as session:
        specialist = await session.scalar(select(User).where(User.id == data["specialist_id"]))
        if not specialist:
            await callback.message.answer("Не удалось идентифицировать специалиста. Попробуйте снова.")
            await state.clear()
            await callback.answer()
            return

        engineer_user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.id == data["engineer_id"])
        )
        if not engineer_user:
            await callback.message.answer("Выбранный инженер не найден. Попробуйте снова.")
            await state.clear()
            await callback.answer()
            return

        # Убеждаемся, что у выбранного инженера есть профиль Engineer, если он не является инженером по роли
        # Это нужно для специалистов и супер-админов, которые могут быть назначены как инженеры
        from app.infrastructure.db.models.roles.engineer import Engineer
        
        # Проверяем, есть ли профиль Engineer
        if engineer_user.role != UserRole.ENGINEER:
            engineer_profile = await session.scalar(
                select(Engineer).where(Engineer.user_id == engineer_user.id)
            )
            if not engineer_profile:
                # Создаем профиль Engineer для специалиста или супер-админа
                engineer_profile = Engineer(user_id=engineer_user.id)
                session.add(engineer_profile)
                await session.flush()

        try:
            # Срок устранения указывает только инженер, не специалист
            create_data = RequestCreateData(
                title=data["title"],
                description=data["description"],
                object_name=data["object_name"],
                address=data["address"],
                apartment=data.get("apartment"),
                contact_person=data["contact_person"],
                contact_phone=data["contact_phone"],
                contract_number=data.get("contract_number"),
                defect_type_name=data.get("defect_type"),
                inspection_datetime=data.get("inspection_datetime"),
                inspection_location=data.get("inspection_location"),
                specialist_id=data["specialist_id"],
                engineer_id=data["engineer_id"],
                due_at=None,
            )
            request = await RequestService.create_request(session, create_data)

            letter_file_id = data.get("letter_file_id")
            if letter_file_id:
                session.add(
                    Act(
                        request_id=request.id,
                        type=ActType.LETTER,
                        file_id=letter_file_id,
                        file_name=data.get("letter_file_name"),
                        uploaded_by_id=data["specialist_id"],
                    )
                )

            await session.commit()

            request_label = format_request_label(request)
            request_title = request.title
            due_at = request.due_at
        except Exception as e:
            await session.rollback()
            safe_msg = html.escape(str(e))
            await callback.message.answer(
                f"❌ Ошибка при создании заявки: {safe_msg}\n"
                "Попробуйте создать заявку заново или обратитесь к администратору."
            )
            await state.clear()
            await callback.answer()
            return

    await callback.message.answer(
        f"✅ Заявка {request_label} создана и назначена инженеру.\n"
        "Следите за статусом в разделе «📄 Мои заявки»."
    )
    await state.clear()
    await callback.answer("Заявка создана")

    engineer_telegram = getattr(engineer_user, "telegram_id", None) if engineer_user else None
    if engineer_telegram:
        due_text = format_moscow(due_at) or "не задан"
        notification = (
            f"Новая заявка {request_label}.\n"
            f"Название: {request_title}\n"
            f"Объект: {data['object_name']}\n"
            f"Адрес: {data['address']}\n"
            f"Срок устранения: {due_text}"
        )
        if data.get("letter_file_id"):
            notification += "\nПисьмо: приложено."
        try:
            await callback.message.bot.send_message(chat_id=int(engineer_telegram), text=notification)
        except Exception:
            pass


@router.callback_query(F.data == "spec:cancel_request", StateFilter(NewRequestStates.confirmation))
async def cancel_request(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Создание заявки отменено.")
    await callback.answer()


# --- вспомогательные функции ---


async def _send_summary(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    summary = _build_request_summary(data)
    await state.set_state(NewRequestStates.confirmation)
    
    # Создаем кнопки для подтверждения
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data="spec:confirm_request")
    builder.button(text="❌ Отменить", callback_data="spec:cancel_request")
    builder.adjust(1)
    
    await message.answer(summary, reply_markup=builder.as_markup())


def _build_request_summary(data: dict) -> str:
    inspection_dt = data.get("inspection_datetime")
    inspection_text = format_moscow(inspection_dt) or "не указан"

    due_at_raw = data.get("due_at")
    due_at = (
        datetime.fromisoformat(due_at_raw) if isinstance(due_at_raw, str) else due_at_raw
    )
    due_text = format_moscow(due_at, "%d.%m.%Y") if due_at else "—"

    letter_text = "приложено" if data.get("letter_file_id") else "нет"

    apartment_text = data.get('apartment') or '—'
    return (
        "Проверьте данные:\n"
        f"🔹 Заголовок: {data['title']}\n"
        f"🔹 Объект: {data['object_name']}\n"
        f"🔹 Адрес: {data['address']}\n"
        f"🔹 Квартира: {apartment_text}\n"
        f"🔹 Контакт: {data['contact_person']} / {data['contact_phone']}\n"
        f"🔹 Договор: {data.get('contract_number') or '—'}\n"
        f"🔹 Тип дефекта: {data.get('defect_type') or '—'}\n"
        f"🔹 Осмотр: {inspection_text}\n"
        f"🔹 Место осмотра: {data.get('inspection_location') or 'адрес объекта'}\n"
        f"🔹 Срок устранения: {due_text} (установит инженер)\n"
        f"🔹 Письмо: {letter_text}\n\n"
        "Нажмите кнопку ниже для подтверждения или отмены создания заявки."
    )

def _specialist_filter_conditions(filter_payload: dict[str, Any] | None) -> list:
    """Строит условия фильтрации для заявок специалиста."""
    logger.info(f"[SPECIALIST FILTER] Building conditions for filter_payload: {filter_payload}")
    if not filter_payload:
        logger.info("[SPECIALIST FILTER] No filter_payload, returning empty conditions")
        return []
    
    # Поддержка старого формата фильтра для обратной совместимости
    if "mode" in filter_payload:
        logger.info("[SPECIALIST FILTER] Using legacy filter format")
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
        logger.info(f"[SPECIALIST FILTER] Legacy conditions: {conditions}")
        return conditions
    
    # Новый формат фильтра
    logger.info("[SPECIALIST FILTER] Using new filter format")
    conditions = build_filter_conditions(filter_payload)
    logger.info(f"[SPECIALIST FILTER] Final conditions: {conditions}")
    return conditions


def _specialist_filter_label(filter_payload: dict[str, Any] | None) -> str:
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


def _build_advanced_filter_menu_keyboard(current_filter: dict[str, Any] | None = None, filter_scope: str | None = None) -> InlineKeyboardMarkup:
    """Строит главное меню расширенного фильтра согласно дизайну."""
    builder = InlineKeyboardBuilder()
    
    # Для суперадминов добавляем кнопку переключения области фильтрации в начале
    if filter_scope is not None:
        scope_text = "🌐 Все заявки" if filter_scope == "all" else "📋 Только мои заявки"
        scope_callback = "spec:flt:scope:mine" if filter_scope == "all" else "spec:flt:scope:all"
        builder.button(text=scope_text, callback_data=scope_callback)
    
    # Первая строка: По адресу, по контакту, По ЖК
    address_text = "🏠 По адресу"
    if current_filter and current_filter.get("address"):
        address_text += " ✓"
    builder.button(text=address_text, callback_data="spec:flt:address")
    
    contact_text = "👤 По контакту"
    if current_filter and current_filter.get("contact_person"):
        contact_text += " ✓"
    builder.button(text=contact_text, callback_data="spec:flt:contact")
    
    object_text = "🏢 По ЖК"
    if current_filter and current_filter.get("object_id"):
        object_name = current_filter.get("object_name", "")
        if object_name:
            object_text += f" ✓"
        else:
            object_text += " ✓"
    builder.button(text=object_text, callback_data="spec:flt:object")
    
    # Вторая строка: По инженеру, Период времени, По статусу
    engineer_text = "🔧 По инженеру"
    if current_filter and current_filter.get("engineer_id"):
        engineer_text += " ✓"
    builder.button(text=engineer_text, callback_data="spec:flt:engineer")
    
    period_text = "📅 Период времени"
    if current_filter and (current_filter.get("date_start") or current_filter.get("date_end")):
        period_text += " ✓"
    builder.button(text=period_text, callback_data="spec:flt:date")
    
    status_text = "📊 По статусу"
    if current_filter and current_filter.get("statuses"):
        status_count = len(current_filter["statuses"])
        status_text += f" ({status_count})"
    builder.button(text=status_text, callback_data="spec:flt:status")
    
    # Третья строка: По мастеру, Номер заявки, По договору
    master_text = "👷 По мастеру"
    if current_filter and current_filter.get("master_id"):
        master_text += " ✓"
    builder.button(text=master_text, callback_data="spec:flt:master")
    
    number_text = "🔢 Номер заявки"
    if current_filter and current_filter.get("request_number"):
        number_text += " ✓"
    builder.button(text=number_text, callback_data="spec:flt:number")
    
    contract_text = "📄 По договору"
    if current_filter and current_filter.get("contract_id"):
        contract_text += " ✓"
    builder.button(text=contract_text, callback_data="spec:flt:contract")
    
    # Четвертая строка: По дефектам
    defect_text = "⚠️ По дефектам"
    if current_filter and current_filter.get("defect_type_id"):
        defect_text += " ✓"
    builder.button(text=defect_text, callback_data="spec:flt:defect")
    
    # Кнопки управления
    builder.button(text="✅ Применить", callback_data="spec:flt:apply")
    builder.button(text="♻️ Сбросить", callback_data="spec:flt:clear")
    builder.button(text="✖️ Отмена", callback_data="spec:flt:cancel")

    # Все кнопки фильтра показываем столбиком, чтобы текст не обрезался
    builder.adjust(1)
    
    # Располагаем кнопки по 3 в ряд (как в дизайне)
    builder.adjust(3, 3, 3, 1, 1, 1)
    return builder.as_markup()


def _build_status_selection_keyboard(selected_statuses: list[str] | None = None) -> InlineKeyboardMarkup:
    """Строит клавиатуру для выбора статусов."""
    builder = InlineKeyboardBuilder()
    
    # Статусы из ТЗ
    status_options = [
        ("Новая", "new"),
        ("Принята в работу", "assigned"),
        ("Приступили к выполнению", "in_progress"),
        ("Выполнена", "completed"),
        ("Ожидает подписания", "ready_for_sign"),
        ("Отмена", "cancelled"),
    ]
    
    selected_set = set(selected_statuses or [])
    
    for display_name, status_key in status_options:
        prefix = "✅ " if display_name in selected_set else "☐ "
        builder.button(
            text=f"{prefix}{display_name}",
            callback_data=f"spec:flt:status_toggle:{status_key}"
        )
    
    builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
    builder.adjust(1)
    return builder.as_markup()


def _build_object_selection_keyboard(objects: list[Object], selected_object_id: int | None = None) -> InlineKeyboardMarkup:
    """Строит клавиатуру для выбора объекта."""
    builder = InlineKeyboardBuilder()
    
    for obj in objects:
        prefix = "✅ " if selected_object_id and obj.id == selected_object_id else ""
        builder.button(
            text=f"{prefix}{obj.name}",
            callback_data=f"spec:flt:object_select:{obj.id}"
        )
    
    if selected_object_id:
        builder.button(text="❌ Убрать объект", callback_data="spec:flt:object_remove")
    
    builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
    builder.adjust(1)
    return builder.as_markup()


def _build_date_mode_keyboard() -> InlineKeyboardMarkup:
    """Строит клавиатуру для выбора режима фильтрации по дате."""
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 По дате создания", callback_data="spec:flt:date_mode:created")
    builder.button(text="📋 По плановой дате", callback_data="spec:flt:date_mode:planned")
    builder.button(text="✅ По дате выполнения", callback_data="spec:flt:date_mode:completed")
    builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
    builder.adjust(1)
    return builder.as_markup()


def _specialist_filter_menu_keyboard() -> InlineKeyboardMarkup:
    """Старое меню фильтра (для обратной совместимости)."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🏠 По адресу", callback_data="spec:flt:mode:address")
    builder.button(text="📅 По дате", callback_data="spec:flt:mode:date")
    builder.button(text="🗓 Сегодня", callback_data="spec:flt:quick:today")
    builder.button(text="7 дней", callback_data="spec:flt:quick:7d")
    builder.button(text="30 дней", callback_data="spec:flt:quick:30d")
    builder.button(text="Этот месяц", callback_data="spec:flt:quick:this_month")
    builder.button(text="Прошлый месяц", callback_data="spec:flt:quick:prev_month")
    builder.button(text="♻️ Сбросить фильтр", callback_data="spec:flt:clear")
    builder.button(text="✖️ Отмена", callback_data="spec:flt:cancel")
    builder.adjust(2)
    return builder.as_markup()


def _specialist_filter_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✖️ Отмена", callback_data="spec:flt:cancel")
    builder.adjust(1)
    return builder.as_markup()


async def _fetch_specialist_requests_page(
    session,
    specialist_id: int,
    page: int,
    filter_payload: dict[str, Any] | None = None,
    is_super_admin: bool = False,
    filter_scope: str | None = None,  # "mine" или "all" для суперадминов
) -> tuple[list[Request], int, int, int]:
    logger.info(f"[FETCH REQUESTS] Fetching page {page} for specialist_id {specialist_id}, is_super_admin: {is_super_admin}, filter_scope: {filter_scope}")
    logger.info(f"[FETCH REQUESTS] filter_payload: {filter_payload}")
    
    # Определяем, нужно ли ограничивать по specialist_id или engineer_id
    # Для суперадмина: если filter_scope == "all", показываем все заявки; если "mine" - только заявки, где он инженер
    # Для обычного специалиста: всегда только свои заявки (specialist_id)
    base_conditions = []
    if is_super_admin:
        if filter_scope == "all":
            logger.info(f"[FETCH REQUESTS] Super admin mode - showing ALL requests (no filter)")
        else:
            # Для суперадмина "Мои заявки" = заявки, где он назначен инженером
            base_conditions.append(Request.engineer_id == specialist_id)
            logger.info(f"[FETCH REQUESTS] Super admin mode - showing OWN requests as engineer (engineer_id: {specialist_id})")
    else:
        base_conditions.append(Request.specialist_id == specialist_id)
        logger.info(f"[FETCH REQUESTS] Regular specialist - adding specialist_id filter: {specialist_id}")
    logger.info(f"[FETCH REQUESTS] base_conditions: {base_conditions}")
    
    conditions = _specialist_filter_conditions(filter_payload)
    logger.info(f"[FETCH REQUESTS] filter conditions: {conditions}")
    
    all_conditions = base_conditions + conditions
    logger.info(f"[FETCH REQUESTS] all_conditions count: {len(all_conditions)}")
    logger.info(f"[FETCH REQUESTS] all_conditions: {all_conditions}")
    
    # Выполняем запрос на подсчет общего количества
    count_query = select(func.count()).select_from(Request).where(*all_conditions)
    logger.info(f"[FETCH REQUESTS] Executing count query")
    total = await session.scalar(count_query)
    total = int(total or 0)
    logger.info(f"[FETCH REQUESTS] Total requests found: {total}")
    
    total_pages = total_pages_for(total, REQUESTS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    logger.info(f"[FETCH REQUESTS] Total pages: {total_pages}, clamped page: {page}")
    
    # Выполняем запрос на получение заявок
    select_query = (
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.engineer),
            selectinload(Request.master),
            selectinload(Request.work_items),
        )
        .where(*all_conditions)
        .order_by(Request.created_at.desc())
        .limit(REQUESTS_PAGE_SIZE)
        .offset(page * REQUESTS_PAGE_SIZE)
    )
    logger.info(f"[FETCH REQUESTS] Executing select query with limit {REQUESTS_PAGE_SIZE}, offset {page * REQUESTS_PAGE_SIZE}")
    
    result = await session.execute(select_query)
    requests = list(result.scalars().all())
    logger.info(f"[FETCH REQUESTS] Retrieved {len(requests)} requests")
    
    if len(requests) > 0:
        logger.info(f"[FETCH REQUESTS] First request ID: {requests[0].id}, number: {requests[0].number}")
    
    return requests, page, total_pages, total


async def _show_specialist_requests_list(
    message: Message,
    session,
    specialist_id: int,
    page: int,
    *,
    context: str = "list",
    filter_payload: dict[str, Any] | None = None,
    edit: bool = False,
    is_super_admin: bool = False,
    filter_scope: str | None = None,
) -> None:
    requests, page, total_pages, total = await _fetch_specialist_requests_page(
        session,
        specialist_id,
        page,
        filter_payload=filter_payload,
        is_super_admin=is_super_admin,
        filter_scope=filter_scope,
    )

    if not requests:
        text = (
            "Заявок по заданному фильтру не найдено."
            if context == "filter"
            else "У вас пока нет заявок. Создайте первую через «➕ Создать заявку»."
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
        status = STATUS_TITLES.get(req.status, req.status.value)
        if context == "filter":
            detail_cb = f"spec:detail:{req.id}:f:{page}"
        else:
            detail_cb = f"spec:detail:{req.id}:{page}"
        label = format_request_label(req)
        list_lines.append(f"{idx}. {html.escape(label)} · {status}")
        builder.button(
            text=f"{idx}. {label} · {status}",
            callback_data=detail_cb,
        )
        # Под кнопкой заявки — корзинка удаления (безвозвратно из БД)
        if req.status != RequestStatus.CLOSED:
            builder.button(text="🗑", callback_data=f"spec:delete:{req.id}:{ctx_key}:{page}")
    builder.adjust(1)  # заявка — строка, под ней корзинка

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(
                InlineKeyboardButton(
                    text="⬅️",
                    callback_data=f"spec:{'filter' if context == 'filter' else 'list'}:{page - 1}",
                )
            )
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="spec:noop"))
        if page < total_pages - 1:
            nav.append(
                InlineKeyboardButton(
                    text="➡️",
                    callback_data=f"spec:{'filter' if context == 'filter' else 'list'}:{page + 1}",
                )
            )
        builder.row(*nav)

    if context == "filter":
        label = _specialist_filter_label(filter_payload)
        header = "Результаты фильтрации. Выберите заявку:"
        if label:
            header = f"{header}\n\n<b>Фильтр:</b>\n{html.escape(label)}"
    else:
        header = "Выберите заявку, чтобы посмотреть подробности и актуальный статус."
    requests_list = "\n".join(list_lines)
    footer = f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    text = f"{header}\n\n{requests_list}{footer}"

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())

async def _load_specialist_requests(session, specialist_id: int) -> list[Request]:
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


def _format_specialist_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    engineer = request.engineer.full_name if request.engineer else "—"
    master = request.master.full_name if request.master else "—"
    due_text = format_moscow(request.due_at) or "не задан"
    inspection_text = format_moscow(request.inspection_scheduled_at) or "не назначен"
    inspection_done = format_moscow(request.inspection_completed_at) or "нет"
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
        f"Инженер: {engineer}",
        f"Мастер: {master}",
        f"Осмотр: {inspection_text}",
        f"Осмотр завершён: {inspection_done}",
        f"Срок устранения: {due_text}",
        f"Адрес: {request.address}",
        f"Контакт: {request.contact_person} · {request.contact_phone}",
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
    if request.inspection_location:
        lines.append(f"Место осмотра: {request.inspection_location}")

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
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        lines.append("")
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        act_count = len(request.acts) - letter_count
        if act_count:
            lines.append(f"📝 Акты: {act_count}")
        if letter_count:
            letter_text = "приложено" if letter_count == 1 else f"приложено ({letter_count})"
            lines.append(f"✉️ Письма/файлы: {letter_text}")
            lines.append("   (нажмите на кнопку ниже, чтобы открыть файл)")
    if request.photos:
        lines.append(f"📷 Фотоотчётов: {len(request.photos)}")
    if request.feedback:
        fb = request.feedback[-1]
        lines.append(
            f"⭐️ Отзыв: качество {fb.rating_quality or '—'}, сроки {fb.rating_time or '—'}, культура {fb.rating_culture or '—'}"
        )
        if fb.comment:
            lines.append(f"«{fb.comment}»")

    lines.append("")
    lines.append("Поддерживайте актуальные статусы и бюджеты, чтобы команда видела прогресс.")
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
    """Форматирует часы для аналитики (часы и минуты)."""
    return format_hours_minutes(value)


def _build_specialist_analytics(requests: list[Request]) -> str:
    from collections import Counter

    now = now_moscow()
    status_counter = Counter(req.status for req in requests)
    total = len(requests)
    active = sum(1 for req in requests if req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED})
    overdue = sum(
        1
        for req in requests
        if req.due_at and req.due_at < now and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
    )
    closed = status_counter.get(RequestStatus.CLOSED, 0)

    planned_budget = float(sum(req.planned_budget or 0 for req in requests))
    actual_budget = float(sum(req.actual_budget or 0 for req in requests))
    planned_hours = float(sum(req.planned_hours or 0 for req in requests))
    actual_hours = float(sum(req.actual_hours or 0 for req in requests))

    durations = []
    for req in requests:
        if req.work_started_at and req.work_completed_at:
            durations.append((req.work_completed_at - req.work_started_at).total_seconds() / 3600)
    avg_duration = sum(durations) / len(durations) if durations else 0

    lines = [
        "📊 <b>Аналитика по вашим заявкам</b>",
        f"Всего заявок: {total}",
        f"Активные: {active}",
        f"Закрытые: {closed}",
        f"Просроченные: {overdue}",
        "",
        f"Плановый бюджет суммарно: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет суммарно: {_format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {_format_currency(actual_budget - planned_budget)} ₽",
        f"Плановые часы суммарно: {format_hours_minutes(planned_hours)}",
        f"Фактические часы суммарно: {format_hours_minutes(actual_hours)}",
        f"Средняя длительность закрытой заявки: {format_hours_minutes(avg_duration)}",
    ]

    if status_counter:
        lines.append("")
        lines.append("Статусы:")
        for status, count in status_counter.most_common():
            lines.append(f"• {STATUS_TITLES.get(status, status.value)} — {count}")

    upcoming = [
        req
        for req in requests
        if req.due_at and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED} and 0 <= (req.due_at - now).total_seconds() <= 72 * 3600
    ]
    if upcoming:
        lines.append("")
        lines.append("⚠️ Срок закрытия в ближайшие 72 часа:")
        for req in upcoming:
            due_text = format_moscow(req.due_at) or "не задан"
            lines.append(f"• {req.number} — до {due_text}")

    return "\n".join(lines)
@router.message(StateFilter(NewRequestStates.inspection_time))
async def handle_inspection_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == "-":
        await state.update_data(inspection_datetime=None, inspection_date=None)
        await state.set_state(NewRequestStates.inspection_location)
        await _prompt_inspection_location(message)
        return

    try:
        time_value = datetime.strptime(text, "%H:%M").time()
    except ValueError:
        await message.answer("Не удалось распознать время. Используйте формат ЧЧ:ММ.")
        return

    data = await state.get_data()
    date_text = data.get("inspection_date")
    if not date_text:
        await message.answer("Сначала выберите дату через календарь.")
        await state.set_state(NewRequestStates.inspection_datetime)
        await _prompt_inspection_calendar(message)
        return

    selected_date = date.fromisoformat(date_text)
    inspection_dt = combine_moscow(selected_date, time_value)
    await state.update_data(inspection_datetime=inspection_dt, inspection_date=None)
    await state.set_state(NewRequestStates.inspection_location)
    await _prompt_inspection_location(message)
