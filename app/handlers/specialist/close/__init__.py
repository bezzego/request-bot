"""Модуль закрытия заявок специалистом."""
from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestStatus
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.utils.request_formatters import format_request_label
from app.handlers.specialist.utils import get_specialist

router = Router()


class CloseRequestStates(StatesGroup):
    """Состояния для закрытия заявки."""
    confirmation = State()
    comment = State()


@router.callback_query(F.data.startswith("spec:close_info:"))
async def specialist_close_info(callback: CallbackQuery):
    """Показывает информацию о том, почему заявку нельзя закрыть."""
    
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
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
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
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
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
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
        specialist = await get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            await state.clear()
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
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
