"""Клавиатуры для деталей заявки инженера."""
from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models import Request, RequestStatus


def build_detail_keyboard(
    request_id: int,
    request: Request | None = None,
    *,
    list_context: str = "list",
    list_page: int = 0,
) -> InlineKeyboardMarkup:
    """Строит клавиатуру для деталей заявки."""
    builder = InlineKeyboardBuilder()
    # После осмотра: гарантия / не гарантия (не гарантия → отмена заявки)
    if request and request.status == RequestStatus.INSPECTED and request.inspection_completed_at:
        builder.button(text="✅ Гарантия", callback_data=f"eng:warranty_yes:{request_id}")
        builder.button(text="❌ Не гарантия", callback_data=f"eng:warranty_no:{request_id}")
    builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request_id}")
    if request and not request.inspection_completed_at:
        builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request_id}")
    builder.button(text="⏱ Плановые часы", callback_data=f"eng:set_planned_hours:{request_id}")
    builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request_id}")
    builder.button(text="⏱ Срок устранения", callback_data=f"eng:set_term:{request_id}")
    builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request_id}")
    builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request_id}")
    if request and request.photos:
        builder.button(text="📷 Просмотреть фото", callback_data=f"eng:photos:{request_id}")
    if request and request.status != RequestStatus.CLOSED:
        builder.button(text="🗑 Удалить", callback_data=f"eng:delete:{request_id}:detail")
    back_cb = f"eng:list:{list_page}" if list_context == "list" else f"eng:filter:{list_page}"
    builder.button(text="⬅️ Назад к списку", callback_data=back_cb)
    builder.adjust(1)
    return builder.as_markup()
