from __future__ import annotations

from datetime import datetime, timezone
from typing import Sequence

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    ActType,
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
    WorkItem,
)
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.services.work_catalog import get_work_catalog
from app.handlers.common.work_fact_view import (
    build_category_keyboard,
    build_quantity_keyboard,
    decode_quantity,
    format_category_message,
    format_quantity_message,
)

router = Router()
import logging

logger = logging.getLogger(__name__)


class EngineerStates(StatesGroup):
    schedule_datetime = State()
    # Состояния для завершения осмотра
    inspection_waiting_photos = State()  # Ожидание отправки фото
    inspection_waiting_comment = State()  # Ожидание комментария
    inspection_final_confirm = State()  # Финальное подтверждение завершения осмотра


STATUS_TITLES = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначен мастер",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


@router.message(F.text == "📋 Мои заявки")
async def engineer_requests(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("У вас пока нет назначенных заявок. Ожидайте распределения.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы управлять этапами и бюджетом.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("eng:detail:"))
async def engineer_request_detail(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    # Save the last viewed request id into FSM so subsequent photos (even without
    # captions) can be associated correctly when the user is working with this card.
    await state.update_data(request_id=request.id)

    await _show_request_detail(callback.message, request, edit=True)
    await callback.answer()


@router.callback_query(F.data == "eng:back")
async def engineer_back_to_list(callback: CallbackQuery):
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await callback.message.edit_text("Активных заявок нет. Ожидайте распределения.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"eng:detail:{req.id}",
        )
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите заявку, чтобы управлять этапами и бюджетом.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:schedule:"))
async def engineer_schedule(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(EngineerStates.schedule_datetime)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Введите дату и время осмотра в формате «ДД.ММ.ГГГГ ЧЧ:ММ».\n"
        "Можно добавить место осмотра после точки с запятой: 25.10.2025 10:00; Склад №3."
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.schedule_datetime))
async def engineer_schedule_datetime(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    data = await state.get_data()
    request_id = data.get("request_id")

    parts = [part.strip() for part in message.text.split(";")]
    datetime_part = parts[0]
    location_part = parts[1] if len(parts) > 1 else None
    try:
        inspection_dt = datetime.strptime(datetime_part, "%d.%m.%Y %H:%M")
    except ValueError:
        await message.answer("Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ ЧЧ:ММ.")
        return

    inspection_dt = inspection_dt.replace(tzinfo=timezone.utc)

    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.assign_engineer(
            session,
            request,
            engineer_id=engineer.id,
            inspection_datetime=inspection_dt,
            inspection_location=location_part or request.inspection_location,
        )
        await session.commit()

    await message.answer(
        f"Осмотр по заявке {request.number} назначен на {inspection_dt.strftime('%d.%m.%Y %H:%M')}."
    )
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:inspect:"))
async def engineer_inspection(callback: CallbackQuery, state: FSMContext):
    """Начало процесса завершения осмотра."""
    request_id = int(callback.data.split(":")[2])
    
    # Сохраняем request_id и очищаем временные данные
    await state.set_state(EngineerStates.inspection_waiting_photos)
    await state.update_data(
        request_id=request_id,
        photos=[],
        photo_file_ids=[],
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="📷 Отправить фото",
        callback_data=f"eng:inspection:start_photos:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    
    await callback.message.answer(
        "Для завершения осмотра отправьте фото дефектов.\n"
        "Нажмите кнопку «📷 Отправить фото», чтобы начать загрузку.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:start_photos:")
)
async def engineer_inspection_start_photos(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки фото."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    await state.set_state(EngineerStates.inspection_waiting_photos)
    await callback.message.edit_text(
        "📷 Жду ваши фотографии.\n"
        "Отправьте все необходимые фото дефектов. После отправки всех фото нажмите «✅ Подтвердить фото».",
        reply_markup=_waiting_photos_keyboard(request_id),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:confirm_photos:")
)
async def engineer_inspection_confirm_photos(callback: CallbackQuery, state: FSMContext):
    """Подтверждение отправленных фото."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    photos = data.get("photos", [])
    if not photos:
        await callback.answer("Сначала отправьте хотя бы одно фото.", show_alert=True)
        return

    # Сохраняем фото в БД
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await state.clear()
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        # Сохраняем все фото
        for photo_data in photos:
            new_photo = Photo(
                request_id=request.id,
                type=PhotoType.BEFORE,
                file_id=photo_data["file_id"],
                caption=photo_data.get("caption"),
            )
            session.add(new_photo)
        
        await session.commit()
        logger.info(
            "Saved %s photos for request_id=%s user=%s",
            len(photos),
            request.id,
            callback.from_user.id,
        )
    
    # Переходим к вводу комментария
    await state.set_state(EngineerStates.inspection_waiting_comment)
    await callback.message.edit_text(
        "✅ Фото сохранены.\n\n"
        "Напишите комментарий к осмотру (или отправьте «-», если комментарий не требуется).",
    )
    await callback.answer()


@router.message(StateFilter(EngineerStates.inspection_waiting_comment))
async def engineer_inspection_comment(message: Message, state: FSMContext):
    """Обработка комментария к осмотру."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    if not text:
        await message.answer("Введите комментарий или «-», либо отправьте «Отмена».")
        return
    
    comment = None if text == "-" else text
    data = await state.get_data()
    request_id = data.get("request_id")
    
    await state.update_data(comment=comment)
    await state.set_state(EngineerStates.inspection_final_confirm)
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Завершить осмотр",
        callback_data=f"eng:inspection:final_confirm:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    
    await message.answer(
        "Комментарий сохранён.\n\n"
        "Нажмите «✅ Завершить осмотр», чтобы завершить процесс.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(
    StateFilter(EngineerStates.inspection_final_confirm),
    F.data.startswith("eng:inspection:final_confirm:")
)
async def engineer_inspection_final_confirm(callback: CallbackQuery, state: FSMContext):
    """Финальное завершение осмотра."""
    request_id = int(callback.data.split(":")[3])

    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await state.clear()
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        comment = data.get("comment")
        await RequestService.record_inspection(
            session,
            request,
            engineer_id=engineer.id,
            notes=comment,
            completed_at=datetime.now(timezone.utc),
        )
        await session.commit()

    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.answer("Осмотр завершён.")
    await callback.message.answer(f"✅ Осмотр по заявке {request.number} отмечен как выполненный.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.callback_query(F.data == "eng:inspection:cancel")
async def engineer_inspection_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена процесса завершения осмотра."""
    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await callback.answer("Действие отменено.")
    await callback.message.answer("Действие отменено.")


@router.callback_query(F.data.startswith("eng:add_plan:"))
async def engineer_add_plan(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

    catalog = get_work_catalog()
    text = f"{header}\n\n{format_category_message(None)}"
    markup = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="ep",
        request_id=request_id,
    )
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:ep:"))
async def engineer_work_catalog_plan(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "ep":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    catalog = get_work_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="ep",
                request_id=request_id,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            new_quantity = current_quantity or 1.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.add_plan_from_catalog(
                session,
                request,
                catalog_item=catalog_item,
                planned_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="ep",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"План обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.callback_query(F.data.startswith("eng:update_fact:"))
async def engineer_update_fact(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

    catalog = get_work_catalog()
    text = f"{header}\n\n{format_category_message(None)}"
    markup = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="e",
        request_id=request_id,
    )
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:e:"))
async def engineer_work_catalog(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "e":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    catalog = get_work_catalog()

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

        if action in {"browse", "back"}:
            target = rest[0] if rest else "root"
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            text = f"{header}\n\n{format_category_message(category)}"
            markup = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="e",
                request_id=request_id,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            new_quantity = current_quantity or 0.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.update_actual_from_catalog(
                session,
                request,
                catalog_item=catalog_item,
                actual_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="e",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Факт обновлён: {new_quantity:.2f}")

            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.callback_query(F.data.startswith("eng:assign_master:"))
async def engineer_assign_master(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        masters = (
            (
                await session.execute(
                    select(User).where(User.role == UserRole.MASTER).order_by(User.full_name)
                )
            )
            .scalars()
            .all()
        )

    if not masters:
        await callback.answer("Активных мастеров нет. Обратитесь к руководителю.", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    for master in masters:
        builder.button(
            text=f"{master.full_name}",
            callback_data=f"eng:pick_master:{request_id}:{master.id}",
        )
    builder.button(text="⬅️ Назад", callback_data=f"eng:detail:{request_id}")
    builder.adjust(1)

    await callback.message.edit_text("Выберите мастера для заявки:", reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("eng:pick_master:"))
async def engineer_pick_master(callback: CallbackQuery):
    _, _, request_id_str, master_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    master_id = int(master_id_str)

    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        master = await session.scalar(select(User).where(User.id == master_id, User.role == UserRole.MASTER))
        if not master:
            await callback.answer("Мастер не найден.", show_alert=True)
            return

        await RequestService.assign_master(
            session,
            request,
            master_id=master.id,
            assigned_by=engineer.id,
        )
        await session.commit()

    try:
        await callback.bot.send_message(
            chat_id=master.telegram_id,
            text=(
                f"Вам назначена заявка {request.number}.\n"
                f"Объект: {request.object.name if request.object else request.address}."
            ),
        )
    except Exception:
        # Игнорируем ошибки отправки уведомления
        pass

    await callback.answer("Мастер назначен.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:ready:"))
async def engineer_ready_for_sign(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await _get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        await RequestService.mark_ready_for_sign(session, request, user_id=engineer.id)
        await session.commit()

    await callback.answer("Статус обновлён.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.message(F.text == "📊 Аналитика")
async def engineer_analytics(message: Message):
    async with async_session() as session:
        engineer = await _get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Эта функция доступна только инженерам.")
            return

        requests = await _load_engineer_requests(session, engineer.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Ожидайте назначенных заявок.")
        return

    summary = _build_engineer_analytics(requests)
    await message.answer(summary)


@router.message(StateFilter(EngineerStates.inspection_waiting_photos), F.photo)
async def engineer_inspection_photo(message: Message, state: FSMContext):
    """Обработка фото во время завершения осмотра."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем фото
    photo = message.photo[-1]
    caption = (message.caption or "").strip() or None
    
    # Добавляем фото в список
    photos = data.get("photos", [])
    photos.append({
        "file_id": photo.file_id,
        "caption": caption,
    })
    
    await state.update_data(photos=photos)
    
    photo_count = len(photos)
    await message.answer(
        f"✅ Фото {photo_count} получено.\n"
        f"Отправлено фото: {photo_count}\n\n"
        "Отправьте ещё фото или нажмите «✅ Подтвердить фото».",
        reply_markup=_waiting_photos_keyboard(request_id),
    )


@router.message(StateFilter(EngineerStates.inspection_waiting_photos), F.document)
async def engineer_inspection_document(message: Message, state: FSMContext):
    """Обработка документов-изображений во время завершения осмотра."""
    doc = message.document
    if not doc or not (doc.mime_type or "").startswith("image/"):
        return

    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем документ как фото
    caption = (message.caption or "").strip() or None
    
    # Добавляем фото в список
    photos = data.get("photos", [])
    photos.append({
        "file_id": doc.file_id,
        "caption": caption,
    })
    
    await state.update_data(photos=photos)
    
    photo_count = len(photos)
    await message.answer(
        f"✅ Фото {photo_count} получено (документ).\n"
        f"Отправлено фото: {photo_count}\n\n"
        "Отправьте ещё фото или нажмите «✅ Подтвердить фото».",
        reply_markup=_waiting_photos_keyboard(request_id),
    )


# --- служебные функции ---


def _waiting_photos_keyboard(request_id: int):
    """Клавиатура во время ожидания фото."""
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✅ Подтвердить фото",
        callback_data=f"eng:inspection:confirm_photos:{request_id}",
    )
    builder.button(
        text="🔄 Отправить заново",
        callback_data=f"eng:inspection:restart_photos:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(
    StateFilter(EngineerStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:restart_photos:")
)
async def engineer_inspection_restart_photos(callback: CallbackQuery, state: FSMContext):
    """Начать загрузку фото заново."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return
    
    await state.update_data(photos=[], photo_file_ids=[])
    await callback.message.edit_text(
        "📷 Жду ваши фотографии.\n"
        "Отправьте все необходимые фото дефектов. После отправки всех фото нажмите «✅ Подтвердить фото».",
        reply_markup=_waiting_photos_keyboard(request_id),
    )
    await callback.answer("Начните отправку фото заново.")




async def _get_engineer(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.ENGINEER)
    )




async def _load_engineer_requests(session, engineer_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.master),
                )
                .where(Request.engineer_id == engineer_id)
                .order_by(Request.created_at.desc())
                .limit(20)
            )
        )
        .scalars()
        .all()
    )


async def _load_request(session, engineer_id: int, request_id: int) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.master),
            selectinload(Request.photos),
            selectinload(Request.acts),
        )
        .where(Request.id == request_id, Request.engineer_id == engineer_id)
    )


async def _refresh_request_detail(bot, chat_id: int, engineer_telegram_id: int, request_id: int) -> None:
    async with async_session() as session:
        engineer = await _get_engineer(session, engineer_telegram_id)
        if not engineer:
            return
        request = await _load_request(session, engineer.id, request_id)

    if not request:
        return

    if not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=_format_request_detail(request),
            reply_markup=_detail_keyboard(request.id),
        )
    except Exception:
        pass


async def _show_request_detail(message: Message, request: Request, *, edit: bool = False) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(request_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request_id}")
    builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request_id}")
    builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request_id}")
    builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request_id}")
    builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data="eng:back")
    builder.adjust(1)
    return builder.as_markup()


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    master = request.master.full_name if request.master else "не назначен"
    object_name = request.object.name if request.object else request.address
    due_text = request.due_at.strftime("%d.%m.%Y %H:%M") if request.due_at else "не задан"
    inspection = (
        request.inspection_scheduled_at.strftime("%d.%m.%Y %H:%M")
        if request.inspection_scheduled_at
        else "не назначен"
    )
    work_end = (
        request.work_completed_at.strftime("%d.%m.%Y %H:%M")
        if request.work_completed_at
        else "—"
    )

    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)

    lines = [
        f"📄 <b>{request.number}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Объект: {object_name}",
        f"Мастер: {master}",
        f"Осмотр: {inspection}",
        f"Работы завершены: {work_end}",
        f"Срок устранения: {due_text}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if request.contract:
        lines.append(f"Договор: {request.contract.number}")
    if request.defect_type:
        lines.append(f"Тип дефекта: {request.defect_type.name}")

    if request.work_items:
        lines.append("")
        lines.append("📦 <b>Позиции бюджета</b>")
        for item in request.work_items:
            lines.append(
                f"• {item.name} — план {_format_currency(item.planned_cost)} ₽ / "
                f"факт {_format_currency(item.actual_cost)} ₽"
            )
            if item.actual_hours is not None:
                lines.append(
                    f"  Часы: {_format_hours(item.planned_hours)} → {_format_hours(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        if letter_count:
            lines.append("")
            lines.append("✉️ Письмо специалиста: приложено")

    return "\n".join(lines)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"


def _build_engineer_analytics(requests: Sequence[Request]) -> str:
    from collections import Counter

    now = datetime.now(timezone.utc)
    counter = Counter(req.status for req in requests)
    total = len(requests)
    scheduled = counter.get(RequestStatus.INSPECTION_SCHEDULED, 0)
    in_progress = counter.get(RequestStatus.IN_PROGRESS, 0) + counter.get(RequestStatus.ASSIGNED, 0)
    completed = counter.get(RequestStatus.COMPLETED, 0) + counter.get(RequestStatus.READY_FOR_SIGN, 0)
    closed = counter.get(RequestStatus.CLOSED, 0)
    overdue = sum(
        1
        for req in requests
        if req.due_at and req.due_at < now and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
    )

    planned_budget = float(sum(req.planned_budget or 0 for req in requests))
    actual_budget = float(sum(req.actual_budget or 0 for req in requests))
    planned_hours = float(sum(req.planned_hours or 0 for req in requests))
    actual_hours = float(sum(req.actual_hours or 0 for req in requests))

    upcoming = [
        req
        for req in requests
        if req.due_at
        and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
        and 0 <= (req.due_at - now).total_seconds() <= 72 * 3600
    ]

    lines = [
        "📊 <b>Аналитика по вашим заявкам</b>",
        f"Всего: {total}",
        f"Назначен осмотр: {scheduled}",
        f"В работе: {in_progress}",
        f"Завершены: {completed}",
        f"Закрыты: {closed}",
        f"Просрочено: {overdue}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {_format_currency(actual_budget - planned_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if upcoming:
        lines.append("")
        lines.append("⚠️ Срок устранения в ближайшие 72 часа:")
        for req in upcoming:
            lines.append(f"• {req.number} — до {req.due_at.strftime('%d.%m.%Y %H:%M')}")

    return "\n".join(lines)


# --- служебные функции для каталога ---


async def _update_catalog_message(message: Message, text: str, markup) -> None:
    try:
        await message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            await message.edit_reply_markup(reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)


async def _get_work_item(session, request_id: int, name: str) -> WorkItem | None:
    return await session.scalar(
        select(WorkItem).where(
            WorkItem.request_id == request_id,
            func.lower(WorkItem.name) == name.lower(),
        )
    )


def _catalog_header(request: Request) -> str:
    return f"Заявка {request.number} · {request.title}"
