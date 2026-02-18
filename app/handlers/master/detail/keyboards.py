"""Клавиатуры для деталей заявки мастера."""
from __future__ import annotations

from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models import Request, RequestStatus


def build_detail_keyboard(
    request_id: int,
    request: Request | None = None,
    *,
    list_page: int = 0,
) -> InlineKeyboardBuilder:
    """Создает клавиатуру для деталей заявки мастера."""
    builder = InlineKeyboardBuilder()
    builder.button(text="📷 Посмотреть дефекты", callback_data=f"master:view_defects:{request_id}")
    
    # Проверяем, начата ли работа
    if request and request.status == RequestStatus.IN_PROGRESS:
        # Проверяем наличие активной сессии
        has_active_session = False
        if request.work_sessions:
            has_active_session = any(
                ws.finished_at is None for ws in request.work_sessions
            )
        
        if has_active_session:
            builder.button(text="✅ Работа начата", callback_data=f"master:work_started:{request_id}")
        else:
            builder.button(text="▶️ Начать работу", callback_data=f"master:start:{request_id}")
    else:
        builder.button(text="▶️ Начать работу", callback_data=f"master:start:{request_id}")
    
    builder.button(text="🗓 План выхода", callback_data=f"master:schedule:{request_id}")
    builder.button(text="⏹ Завершить работу", callback_data=f"master:finish:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"master:update_fact:{request_id}")
    builder.button(text="📦 Редактировать материалы", callback_data=f"master:edit_materials:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data=f"master:list:{list_page}")
    builder.adjust(1)
    return builder.as_markup()
