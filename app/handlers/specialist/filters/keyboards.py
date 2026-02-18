"""Клавиатуры для фильтрации заявок."""
from __future__ import annotations

from typing import Any

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models import Object


def build_advanced_filter_menu_keyboard(
    current_filter: dict[str, Any] | None = None,
    filter_scope: str | None = None
) -> InlineKeyboardMarkup:
    """Строит главное меню расширенного фильтра."""
    builder = InlineKeyboardBuilder()
    
    # Для суперадминов добавляем кнопку переключения области фильтрации
    if filter_scope is not None:
        scope_text = "🌐 Все заявки" if filter_scope == "all" else "📋 Только мои заявки"
        scope_callback = "spec:flt:scope:mine" if filter_scope == "all" else "spec:flt:scope:all"
        builder.button(text=scope_text, callback_data=scope_callback)
        builder.adjust(1)
    
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
    
    # Располагаем кнопки по 3 в ряд
    builder.adjust(3, 3, 3, 1, 1, 1)
    return builder.as_markup()


def build_status_selection_keyboard(selected_statuses: list[str] | None = None) -> InlineKeyboardMarkup:
    """Строит клавиатуру для выбора статусов."""
    builder = InlineKeyboardBuilder()
    
    status_options = [
        ("Новая", "new"),
        ("Принята в работу", "assigned"),
        ("Приступили к выполнению", "in_progress"),
        ("Выполнена", "completed"),
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


def build_object_selection_keyboard(objects: list[Object], selected_object_id: int | None = None) -> InlineKeyboardMarkup:
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


def build_date_mode_keyboard() -> InlineKeyboardMarkup:
    """Строит клавиатуру для выбора режима фильтрации по дате."""
    builder = InlineKeyboardBuilder()
    builder.button(text="📅 По дате создания", callback_data="spec:flt:date_mode:created")
    builder.button(text="📋 По плановой дате", callback_data="spec:flt:date_mode:planned")
    builder.button(text="✅ По дате выполнения", callback_data="spec:flt:date_mode:completed")
    builder.button(text="⬅️ Назад", callback_data="spec:flt:back")
    builder.adjust(1)
    return builder.as_markup()


def build_filter_cancel_keyboard() -> InlineKeyboardMarkup:
    """Строит клавиатуру отмены фильтра."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✖️ Отмена", callback_data="spec:flt:cancel")
    builder.adjust(1)
    return builder.as_markup()
