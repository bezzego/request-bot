"""Модуль просмотра деталей заявки инженером."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, InputMediaPhoto, Message
from sqlalchemy import delete, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Photo, PhotoType, Request, RequestStatus
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.utils.request_formatters import format_request_label
from app.handlers.engineer.utils import get_engineer
from app.handlers.engineer.list import (
    fetch_engineer_requests_page,
    show_engineer_requests_list,
)
from app.handlers.engineer.detail.formatters import format_engineer_request_detail
from app.handlers.engineer.detail.keyboards import build_detail_keyboard

router = Router()

# Максимум фото одного типа за раз
MAX_PHOTOS_PER_TYPE = 100

# Экспортируем функции для использования в других модулях
__all__ = [
    "load_request",
    "show_request_detail",
    "send_all_photos",
    "send_photos_by_type",
    "format_engineer_request_detail",
    "build_detail_keyboard",
    "refresh_request_detail",
]


async def load_request(session, engineer_id: int, request_id: int) -> Request | None:
    """Загружает заявку с полными связями для инженера."""
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.work_sessions),
            selectinload(Request.master),
            selectinload(Request.engineer),
            selectinload(Request.specialist),
            selectinload(Request.photos),
            selectinload(Request.acts),
        )
        .where(Request.id == request_id, Request.engineer_id == engineer_id)
    )


async def show_request_detail(
    message: Message,
    request: Request,
    *,
    edit: bool = False,
    list_context: str = "list",
    list_page: int = 0,
) -> None:
    """Показывает детали заявки."""
    text = format_engineer_request_detail(request)
    keyboard = build_detail_keyboard(request.id, request, list_context=list_context, list_page=list_page)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


async def send_all_photos(message: Message, photos: list[Photo]) -> None:
    """Отправка всех фото заявки, разделённых по типам (BEFORE, PROCESS, AFTER)."""
    if not photos:
        return
    
    # Разделяем фото по типам
    before_photos = [p for p in photos if p.type == PhotoType.BEFORE]
    process_photos = [p for p in photos if p.type == PhotoType.PROCESS]
    after_photos = [p for p in photos if p.type == PhotoType.AFTER]
    
    # Отправляем фото по типам
    if before_photos:
        await message.answer("📷 <b>Фото дефектов (до работ)</b>", parse_mode="HTML")
        await send_photos_by_type(message, before_photos)
    
    if process_photos:
        await message.answer("📷 <b>Фото в процессе работ</b>", parse_mode="HTML")
        await send_photos_by_type(message, process_photos)
    
    if after_photos:
        await message.answer("📷 <b>Фото после работ</b>", parse_mode="HTML")
        await send_photos_by_type(message, after_photos)


async def send_photos_by_type(message: Message, photos: list[Photo]) -> None:
    """Отправка фото одного типа пачками по 10 (media_group)."""
    if not photos:
        return
    total = len(photos)
    to_send = photos[:MAX_PHOTOS_PER_TYPE]
    if total > MAX_PHOTOS_PER_TYPE:
        await message.answer(f"Показано {MAX_PHOTOS_PER_TYPE} из {total} (остальные сохранены в заявке).")

    # Пачки по 10 (лимит media_group в Telegram)
    chunk_size = 10
    i = 0
    while i < len(to_send):
        chunk = to_send[i : i + chunk_size]
        i += chunk_size
        media_list: list[InputMediaPhoto] = [
            InputMediaPhoto(media=p.file_id, caption=p.caption or None) for p in chunk
        ]
        try:
            if len(media_list) == 1:
                await message.answer_photo(media_list[0].media, caption=media_list[0].caption)
            else:
                await message.answer_media_group(media_list)
        except TelegramBadRequest as e:
            if "Video" in str(e) or "video" in str(e):
                # В пачке есть видео — отправляем по одному
                for p in chunk:
                    try:
                        await message.answer_photo(p.file_id, caption=p.caption or None)
                    except TelegramBadRequest:
                        try:
                            await message.answer_video(p.file_id, caption=p.caption or None)
                        except Exception:
                            pass
                    except Exception:
                        pass
            else:
                for p in chunk:
                    try:
                        await message.answer_photo(p.file_id, caption=p.caption or None)
                    except Exception:
                        try:
                            await message.answer_video(p.file_id, caption=p.caption or None)
                        except Exception:
                            pass
        except Exception:
            for p in chunk:
                try:
                    await message.answer_photo(p.file_id, caption=p.caption or None)
                except Exception:
                    try:
                        await message.answer_video(p.file_id, caption=p.caption or None)
                    except Exception:
                        pass


@router.callback_query(F.data.startswith("eng:detail:"))
async def engineer_request_detail(callback: CallbackQuery, state: FSMContext):
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    # Сохраняем ID последней просмотренной заявки в FSM
    await state.update_data(request_id=request.id)

    await show_request_detail(callback.message, request, edit=True, list_context=context, list_page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("eng:back"))
async def engineer_back_to_list(callback: CallbackQuery):
    """Возврат к списку заявок."""
    parts = callback.data.split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
            page = 0

    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:delete:"))
async def engineer_delete_prompt(callback: CallbackQuery):
    """Показывает подтверждение безвозвратного удаления заявки из БД."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    from_detail = len(parts) >= 4 and parts[3] == "detail"
    if from_detail:
        cancel_cb = f"eng:detail:{request_id}"
        confirm_cb = f"eng:delete_confirm:{request_id}"
        ctx_key, page = "list", 0
    else:
        ctx_key = parts[3] if len(parts) >= 4 else "list"
        page = int(parts[4]) if len(parts) >= 5 else 0
        cancel_cb = f"eng:{ctx_key}:{page}"
        confirm_cb = f"eng:delete_confirm:{request_id}:{ctx_key}:{page}"

    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await load_request(session, engineer.id, request_id)
    if not request:
        await callback.answer("Заявка не найдена.", show_alert=True)
        return
    if request.status == RequestStatus.CLOSED:
        await callback.answer("Заявка уже закрыта.", show_alert=True)
        return
    label = format_request_label(request)
    from aiogram.utils.keyboard import InlineKeyboardBuilder
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


@router.callback_query(F.data.startswith("eng:delete_confirm:"))
async def engineer_delete_confirm(callback: CallbackQuery, state: FSMContext):
    """Безвозвратное удаление заявки из БД."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    return_to_list = len(parts) >= 5
    ctx_key = parts[3] if return_to_list else "list"
    page = int(parts[4]) if return_to_list else 0

    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await load_request(session, engineer.id, request_id)
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
            filter_payload = (await state.get_data()).get("eng_filter") if context == "filter" else None
            _, _, total_pages, _ = await fetch_engineer_requests_page(session, engineer.id, 0, filter_payload=filter_payload)
            safe_page = min(page, max(0, total_pages - 1)) if total_pages else 0
            await show_engineer_requests_list(
                callback.message,
                session,
                engineer.id,
                page=safe_page,
                context=context,
                filter_payload=filter_payload,
                edit=True,
            )
            await callback.answer("Заявка удалена из базы")
            return

    await callback.message.edit_text("✅ Заявка удалена из базы.")
    await callback.answer("Заявка удалена")


@router.callback_query(F.data.startswith("eng:photos:"))
async def engineer_view_photos(callback: CallbackQuery):
    """Просмотр всех фото заявки для инженера."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        # Загружаем все фото заявки
        photos = (
            await session.execute(
                select(Photo)
                .where(Photo.request_id == request.id)
                .order_by(Photo.created_at.asc())
            )
        ).scalars().all()

    if not photos:
        await callback.answer("Фото не найдены.", show_alert=True)
        return

    await send_all_photos(callback.message, photos)
    await callback.answer()


async def refresh_request_detail(bot, chat_id: int, engineer_telegram_id: int, request_id: int) -> None:
    """Обновляет детали заявки через бота (для внешних вызовов)."""
    async with async_session() as session:
        engineer = await get_engineer(session, engineer_telegram_id)
        if not engineer:
            return
        request = await load_request(session, engineer.id, request_id)

    if not request:
        return

    if not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=format_engineer_request_detail(request),
            reply_markup=build_detail_keyboard(request.id, request),
        )
    except Exception:
        pass
