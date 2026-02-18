"""Модуль фильтрации заявок специалиста."""
from __future__ import annotations

import logging
from typing import Any

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Contract, DefectType, Object, Request, User, UserRole
from app.infrastructure.db.session import async_session
from app.utils.advanced_filters import DateFilterMode, format_filter_label
from app.utils.request_filters import quick_date_range
from app.handlers.specialist.utils import get_specialist, is_super_admin
from app.handlers.specialist.filters.keyboards import (
    build_advanced_filter_menu_keyboard,
    build_status_selection_keyboard,
    build_object_selection_keyboard,
    build_date_mode_keyboard,
)
from app.handlers.specialist.filters.utils import (
    specialist_filter_conditions,
    specialist_filter_label,
    clean_filter_payload,
)

logger = logging.getLogger(__name__)

router = Router()


class SpecialistFilterStates(StatesGroup):
    """Состояния для настройки фильтра заявок."""
    scope_selection = State()
    main_menu = State()
    status_selection = State()
    object_selection = State()
    date_mode_selection = State()
    date_input = State()
    address_input = State()
    contact_input = State()
    engineer_selection = State()
    master_selection = State()
    number_input = State()
    contract_selection = State()
    defect_selection = State()


@router.message(F.text == "🔍 Фильтр заявок")
async def specialist_filter_start(message: Message, state: FSMContext):
    """Открывает новое расширенное меню фильтра."""
    async with async_session() as session:
        specialist = await get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

    data = await state.get_data()
    current_filter = data.get("spec_filter")
    
    is_super = is_super_admin(specialist)
    
    if is_super:
        filter_scope = data.get("filter_scope")
        if not filter_scope:
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
        reply_markup=build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("spec:flt:scope:"))
async def specialist_filter_scope_select(callback: CallbackQuery, state: FSMContext):
    """Выбор области фильтрации для суперадмина."""
    scope = callback.data.split(":")[3]
    
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
        reply_markup=build_advanced_filter_menu_keyboard(current_filter, filter_scope=scope),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("spec:flt:quick:"))
async def specialist_filter_quick(callback: CallbackQuery, state: FSMContext):
    """Быстрый выбор периода."""
    code = callback.data.split(":")[3]
    quick = quick_date_range(code)
    if not quick:
        await callback.answer("Неизвестный период.", show_alert=True)
        return
    start, end, label = quick
    filter_payload = {
        "date_mode": DateFilterMode.CREATED,
        "date_start": start.isoformat(),
        "date_end": end.isoformat(),
    }
    await state.update_data(spec_filter=filter_payload)
    await state.set_state(None)

    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        # Отложенный импорт для избежания циклической зависимости
        from app.handlers.specialist.list import show_specialist_requests_list
        await show_specialist_requests_list(
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
    """Очистка фильтра."""
    await state.update_data(spec_filter=None)
    await state.set_state(None)
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        is_super = is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        # Отложенный импорт для избежания циклической зависимости
        from app.handlers.specialist.list import show_specialist_requests_list
        await show_specialist_requests_list(
            callback.message,
            session,
            specialist.id,
            page=0,
            context="list",
            filter_payload=None,
            edit=True,
            is_super_admin=is_super,
            filter_scope=filter_scope,
        )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:cancel")
async def specialist_filter_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена настройки фильтра."""
    await state.set_state(None)
    await callback.answer("Фильтр отменён.")


@router.callback_query(F.data == "spec:flt:back")
async def specialist_filter_back(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню фильтра."""
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
    
    data = await state.get_data()
    current_filter = data.get("spec_filter")
    is_super = is_super_admin(specialist)
    filter_scope = data.get("filter_scope") if is_super else None
    
    await state.set_state(SpecialistFilterStates.main_menu)
    
    filter_info = ""
    if current_filter:
        filter_info = f"\n\n<b>Текущие настройки:</b>\n{format_filter_label(current_filter)}"
    
    scope_text = "по всем заявкам" if (is_super and filter_scope == "all") else "по вашим заявкам"
    await callback.message.edit_text(
        f"🔍 <b>Фильтр заявок</b>\n\n"
        f"Фильтрация {scope_text}.\n"
        f"Выберите параметры фильтрации:{filter_info}",
        reply_markup=build_advanced_filter_menu_keyboard(current_filter, filter_scope=filter_scope),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "spec:flt:apply")
async def specialist_filter_apply(callback: CallbackQuery, state: FSMContext):
    """Применение фильтра."""
    logger.info("[FILTER APPLY] Starting filter apply")
    data = await state.get_data()
    filter_payload = data.get("spec_filter")
    
    if not filter_payload:
        await callback.answer("Выберите хотя бы один параметр фильтрации.", show_alert=True)
        return
    
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
    
    if not has_filter:
        await callback.answer("Выберите хотя бы один параметр фильтрации.", show_alert=True)
        return
    
    cleaned_filter = clean_filter_payload(filter_payload)
    
    if not cleaned_filter:
        await callback.answer("Выберите хотя бы один валидный параметр фильтрации.", show_alert=True)
        return
    
    await state.update_data(spec_filter=cleaned_filter)
    await state.set_state(None)
    
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        is_super = is_super_admin(specialist)
        data = await state.get_data()
        filter_scope = data.get("filter_scope") if is_super else None
        
        # Отложенный импорт для избежания циклической зависимости
        from app.handlers.specialist.list import show_specialist_requests_list
        try:
            await show_specialist_requests_list(
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
            await callback.answer("Фильтр применён.")
        except Exception as e:
            logger.error(f"[FILTER APPLY] Error applying filter: {e}", exc_info=True)
            await callback.answer(f"Ошибка при применении фильтра: {str(e)}", show_alert=True)


# Остальные обработчики фильтров (status, object, date, address, contact, engineer, master, number, contract, defect)
# остаются в legacy файле и будут постепенно перенесены в этот модуль
# Они автоматически подключаются через legacy_router в главном __init__.py
