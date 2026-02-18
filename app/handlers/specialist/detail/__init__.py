"""Модуль просмотра деталей заявки специалистом."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Act, ActType, Photo, Request, RequestStatus
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.utils.request_formatters import STATUS_TITLES, format_hours_minutes, format_request_label
from app.utils.timezone import format_moscow
from app.handlers.specialist.utils import get_specialist
from app.handlers.specialist.detail.formatters import (
    format_specialist_request_detail,
    calculate_cost_breakdown,
    format_currency,
)

router = Router()


@router.callback_query(F.data.startswith("spec:detail:"))
async def specialist_request_detail(callback: CallbackQuery, state: FSMContext):
    """Обработчик просмотра деталей заявки."""
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
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await session.scalar(
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
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        if not request:
            await callback.message.edit_text("Заявка не найдена или была удалена.")
            await callback.answer()
            return
        
        # Проверяем, является ли специалист инженером на этой заявке
        from app.handlers.engineer import _get_engineer
        engineer = await _get_engineer(session, callback.from_user.id)
        is_engineer = engineer and request.engineer_id == engineer.id

    detail_text = format_specialist_request_detail(request)
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
        reason_text = reasons[0][:35] + "..." if reasons and len(reasons[0]) > 35 else (reasons[0] if reasons else "не выполнены условия")
        builder.button(
            text=f"⚠️ {reason_text}",
            callback_data=f"spec:close_info:{request.id}",
        )
    
    # Кнопка удаления заявки
    ctx_key = "filter" if context == "filter" else "list"
    if request.status != RequestStatus.CLOSED:
        builder.button(text="🗑 Удалить", callback_data=f"spec:delete:{request.id}:detail")

    back_callback = f"spec:list:{page}" if context == "list" else f"spec:filter:{page}"
    refresh_callback = (
        f"spec:detail:{request.id}:f:{page}" if context == "filter" else f"spec:detail:{request.id}:{page}"
    )
    builder.button(text="⬅️ Назад к списку", callback_data=back_callback)
    builder.button(text="🔄 Обновить", callback_data=refresh_callback)
    
    # Сохраняем контекст фильтра в state для восстановления при возврате
    if context == "filter":
        data = await state.get_data()
        filter_payload = data.get("spec_filter")
        if not filter_payload:
            await state.update_data(spec_filter={})
    
    await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("spec:photos:"))
async def specialist_view_photos(callback: CallbackQuery):
    """Просмотр всех фото заявки для специалиста."""
    request_id = int(callback.data.split(":")[2])
    
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(selectinload(Request.photos))
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
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


@router.callback_query(F.data.startswith("spec:file:"))
async def specialist_open_file(callback: CallbackQuery):
    """Отправляет прикреплённый файл пользователю."""
    _, _, act_id_str = callback.data.split(":")
    act_id = int(act_id_str)
    
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        act = await session.scalar(
            select(Act)
            .join(Request)
            .where(
                Act.id == act_id,
                Act.type == ActType.LETTER,
                Request.specialist_id == specialist.id,
            )
        )
        
        if not act:
            await callback.answer("Файл не найден.", show_alert=True)
            return
        
        if not act.file_id:
            await callback.answer("Файл недоступен.", show_alert=True)
            return
        
        try:
            await callback.message.bot.send_document(
                chat_id=callback.message.chat.id,
                document=act.file_id,
                caption=f"📎 {act.file_name or 'Файл'}",
            )
            await callback.answer()
        except Exception as e:
            await callback.answer(f"Ошибка отправки файла: {str(e)}", show_alert=True)


@router.callback_query(F.data.startswith("spec:delete:"))
async def specialist_delete_prompt(callback: CallbackQuery):
    """Показывает подтверждение безвозвратного удаления заявки из БД."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    from_detail = len(parts) >= 4 and parts[3] == "detail"
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
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await session.scalar(
            select(Request).where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
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
    """Безвозвратное удаление заявки из БД."""
    from sqlalchemy import delete
    
    parts = callback.data.split(":")
    request_id = int(parts[2])
    return_to_list = len(parts) >= 5
    ctx_key = parts[3] if return_to_list else "list"
    page = int(parts[4]) if return_to_list else 0
    
    data = await state.get_data()
    filter_payload = data.get("spec_filter") if ctx_key == "filter" else None

    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await session.scalar(
            select(Request).where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        if request.status == RequestStatus.CLOSED:
            await callback.answer("Заявка уже закрыта.", show_alert=True)
            return
        
        label = format_request_label(request)
        
        # Удаляем заявку
        await session.execute(delete(Request).where(Request.id == request_id))
        await session.commit()
        
        await callback.message.edit_text(f"✅ Заявка {label} удалена.")
        await callback.answer("Заявка удалена")
        
        # Возвращаемся к списку, если нужно
        if return_to_list:
            from app.handlers.specialist.list import show_specialist_requests_list
            from app.handlers.specialist.utils import is_super_admin
            
            is_super = is_super_admin(specialist)
            filter_scope = data.get("filter_scope") if is_super else None
            
            # Используем новую сессию для возврата к списку
            async with async_session() as new_session:
                await show_specialist_requests_list(
                    callback.message,
                    new_session,
                    specialist.id,
                    page=page,
                    context=ctx_key,
                    filter_payload=filter_payload,
                    edit=True,
                    is_super_admin=is_super,
                    filter_scope=filter_scope,
                )
