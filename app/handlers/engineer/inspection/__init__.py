"""Модуль осмотра заявок инженером."""
from __future__ import annotations

import logging
from datetime import date, datetime

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models import Photo, PhotoType
from app.infrastructure.db.session import async_session
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month
from app.services.request_service import RequestService
from app.utils.request_formatters import format_request_label
from app.utils.timezone import combine_moscow, format_moscow, now_moscow
from app.handlers.engineer.utils import get_engineer
from app.handlers.engineer.detail import load_request, show_request_detail

router = Router()
ENGINEER_CALENDAR_PREFIX = "eng_schedule"
logger = logging.getLogger(__name__)


class EngineerInspectionStates(StatesGroup):
    """Состояния для осмотра заявок инженером."""
    schedule_date = State()
    schedule_time = State()
    inspection_waiting_photos = State()  # Ожидание отправки фото
    inspection_waiting_comment = State()  # Ожидание комментария
    inspection_final_confirm = State()  # Финальное подтверждение завершения осмотра


def _waiting_photos_keyboard(request_id: int, photo_count: int = 0, video_count: int = 0):
    """Клавиатура во время ожидания фото."""
    builder = InlineKeyboardBuilder()
    total = photo_count + video_count
    if total > 0:
        builder.button(
            text=f"✅ Подтвердить ({total})",
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


async def _prompt_schedule_calendar(message: Message):
    """Показать календарь для выбора даты осмотра."""
    await message.answer(
        "Когда назначить комиссионный осмотр?\n"
        "Выберите дату через календарь или отправьте «-» (или «-; новое место»), если дата пока не определена.\n"
        "Для отмены напишите «Отмена».",
        reply_markup=build_calendar(ENGINEER_CALENDAR_PREFIX),
    )


async def _complete_engineer_schedule(
    message: Message,
    state: FSMContext,
    *,
    inspection_dt: datetime | None,
    location: str | None,
) -> None:
    """Завершить назначение осмотра."""
    data = await state.get_data()
    request_id = data.get("request_id")
    if not request_id:
        await message.answer("Не удалось определить заявку. Начните процесс заново.")
        await state.clear()
        return

    async with async_session() as session:
        engineer = await get_engineer(session, message.from_user.id)
        if not engineer:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.assign_engineer(
            session,
            request,
            engineer_id=engineer.id,
            inspection_datetime=inspection_dt,
            inspection_location=location or request.inspection_location,
        )
        await session.commit()
        request_label = format_request_label(request)

    if inspection_dt:
        inspection_text = format_moscow(inspection_dt) or "—"
        main_line = f"Осмотр по заявке {request_label} назначен на {inspection_text}."
    else:
        main_line = f"Информация об осмотре заявки {request_label} обновлена."
    if location:
        main_line += f"\nМесто осмотра: {location}"

    await message.answer(main_line)
    await state.clear()
    from app.handlers.engineer.detail import refresh_request_detail
    if message.bot:
        await refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("eng:schedule:"))
async def engineer_schedule(callback: CallbackQuery, state: FSMContext):
    """Начало процесса назначения осмотра."""
    request_id = int(callback.data.split(":")[2])
    
    # Проверяем доступ к заявке
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        
        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена или больше не закреплена за вами.", show_alert=True)
            return
    
    await state.set_state(EngineerInspectionStates.schedule_date)
    await state.update_data(request_id=request_id)
    await _prompt_schedule_calendar(callback.message)
    await callback.answer()


@router.message(StateFilter(EngineerInspectionStates.schedule_date))
async def engineer_schedule_date_text(message: Message, state: FSMContext):
    """Обработка текстового ввода даты осмотра."""
    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    if text.startswith("-"):
        location = None
        if ";" in text:
            _, location_part = text.split(";", 1)
            location = location_part.strip() or None
        await _complete_engineer_schedule(
            message,
            state,
            inspection_dt=None,
            location=location,
        )
        return

    await message.answer(
        "Дата выбирается через календарь. Нажмите на нужный день или отправьте «-», если дата пока неизвестна."
    )


@router.callback_query(
    StateFilter(EngineerInspectionStates.schedule_date),
    F.data.startswith(f"cal:{ENGINEER_CALENDAR_PREFIX}:"),
)
async def engineer_schedule_calendar(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора даты через календарь."""
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar(ENGINEER_CALENDAR_PREFIX, year=new_year, month=new_month)
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        selected = date(payload.year, payload.month, payload.day)
        await state.update_data(schedule_date=selected.isoformat())
        await state.set_state(EngineerInspectionStates.schedule_time)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(
            f"Дата осмотра: {selected.strftime('%d.%m.%Y')}.\n"
            "Введите время в формате ЧЧ:ММ или «-», если время пока не определено.\n"
            "Можно добавить место после точки с запятой: 10:00; Склад №3."
        )
        await callback.answer(f"Выбрано {selected.strftime('%d.%m.%Y')}")
        return

    await callback.answer()


@router.message(StateFilter(EngineerInspectionStates.schedule_time))
async def engineer_schedule_time(message: Message, state: FSMContext):
    """Обработка ввода времени осмотра."""
    text = (message.text or "").strip()
    lowered = text.lower()
    if lowered == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in text.split(";")]
    time_part = parts[0] if parts else ""
    location_part = parts[1] if len(parts) > 1 else None

    if time_part == "-":
        await _complete_engineer_schedule(
            message,
            state,
            inspection_dt=None,
            location=location_part,
        )
        return

    try:
        time_value = datetime.strptime(time_part, "%H:%M").time()
    except ValueError:
        await message.answer("Не удалось распознать время. Используйте формат ЧЧ:ММ.")
        return

    data = await state.get_data()
    date_str = data.get("schedule_date")
    if not date_str:
        await message.answer("Сначала выберите дату через календарь.")
        await state.set_state(EngineerInspectionStates.schedule_date)
        await _prompt_schedule_calendar(message)
        return

    selected_date = date.fromisoformat(date_str)
    inspection_dt = combine_moscow(selected_date, time_value)
    await _complete_engineer_schedule(
        message,
        state,
        inspection_dt=inspection_dt,
        location=location_part,
    )


@router.callback_query(F.data.startswith("eng:inspect:"))
async def engineer_inspection(callback: CallbackQuery, state: FSMContext):
    """Начало процесса завершения осмотра."""
    request_id = int(callback.data.split(":")[2])
    
    # Проверяем доступ к заявке
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return
        
        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена или больше не закреплена за вами.", show_alert=True)
            return
    
    # Сохраняем request_id и очищаем временные данные
    await state.set_state(EngineerInspectionStates.inspection_waiting_photos)
    await state.update_data(
        request_id=request_id,
        photos=[],
        videos=[],
        photo_file_ids=[],
        status_message_id=None,
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="📷 Отправить фото/видео",
        callback_data=f"eng:inspection:start_photos:{request_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data="eng:inspection:cancel",
    )
    builder.adjust(1)
    
    await callback.message.answer(
        "Для завершения осмотра отправьте фото или видео дефектов.\n"
        "Нажмите кнопку «📷 Отправить фото/видео», чтобы начать загрузку.\n"
        "Можно отправить несколько фото/видео подряд, затем подтвердить все сразу.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerInspectionStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:start_photos:"),
)
async def engineer_inspection_start_photos(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки фото."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return

    await state.set_state(EngineerInspectionStates.inspection_waiting_photos)
    status_msg = await callback.message.edit_text(
        "📷 Жду ваши фотографии и видео.\n"
        "Отправьте все необходимые фото/видео дефектов подряд.\n"
        "После отправки всех файлов нажмите «✅ Подтвердить».",
        reply_markup=_waiting_photos_keyboard(request_id, photo_count=0, video_count=0),
    )
    await state.update_data(status_message_id=status_msg.message_id)
    await callback.answer()


@router.callback_query(
    StateFilter(EngineerInspectionStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:confirm_photos:"),
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
    videos = data.get("videos", [])
    total_files = len(photos) + len(videos)
    
    if total_files == 0:
        await callback.answer("Сначала отправьте хотя бы одно фото или видео.", show_alert=True)
        return

    # Сохраняем фото и видео в БД
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
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
        
        # Сохраняем все видео (как фото с типом BEFORE)
        for video_data in videos:
            new_photo = Photo(
                request_id=request.id,
                type=PhotoType.BEFORE,
                file_id=video_data["file_id"],
                caption=video_data.get("caption"),
            )
            session.add(new_photo)
        
        await session.commit()
        logger.info(
            "Saved %s photos and %s videos for request_id=%s user=%s",
            len(photos),
            len(videos),
            request.id,
            callback.from_user.id,
        )
    
    # Переходим к вводу комментария
    await state.set_state(EngineerInspectionStates.inspection_waiting_comment)
    files_text = []
    if len(photos) > 0:
        files_text.append(f"{len(photos)} фото")
    if len(videos) > 0:
        files_text.append(f"{len(videos)} видео")
    files_summary = " и ".join(files_text) if files_text else "файлы"
    
    await callback.message.edit_text(
        f"✅ Сохранено: {files_summary}.\n\n"
        "Напишите комментарий к осмотру (или отправьте «-», если комментарий не требуется).",
    )
    await callback.answer()


@router.message(StateFilter(EngineerInspectionStates.inspection_waiting_comment))
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
    await state.set_state(EngineerInspectionStates.inspection_final_confirm)
    
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
    StateFilter(EngineerInspectionStates.inspection_final_confirm),
    F.data.startswith("eng:inspection:final_confirm:"),
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await state.clear()
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
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
            completed_at=now_moscow(),
        )
        await session.commit()

    await state.clear()
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.answer("Осмотр завершён.")
    await callback.message.answer(f"✅ Осмотр по заявке {format_request_label(request)} отмечен как выполненный.")
    from app.handlers.engineer.detail import refresh_request_detail
    if callback.bot:
        await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


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


@router.message(StateFilter(EngineerInspectionStates.inspection_waiting_photos), F.photo)
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
        "is_video": False,
    })
    
    videos = data.get("videos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(photos=photos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


@router.message(StateFilter(EngineerInspectionStates.inspection_waiting_photos), F.video)
async def engineer_inspection_video(message: Message, state: FSMContext):
    """Обработка видео во время завершения осмотра."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Получаем видео
    video = message.video
    caption = (message.caption or "").strip() or None
    
    # Добавляем видео в список
    videos = data.get("videos", [])
    videos.append({
        "file_id": video.file_id,
        "caption": caption,
        "is_video": True,
    })
    
    photos = data.get("photos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(videos=videos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


@router.message(StateFilter(EngineerInspectionStates.inspection_waiting_photos), F.document)
async def engineer_inspection_document(message: Message, state: FSMContext):
    """Обработка документов-изображений во время завершения осмотра."""
    doc = message.document
    mime_type = doc.mime_type or ""
    
    # Поддерживаем только изображения
    if not mime_type.startswith("image/"):
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
        "is_video": False,
    })
    
    videos = data.get("videos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    await state.update_data(photos=photos)
    
    # Обновляем статусное сообщение
    status_message_id = data.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить»."
                ),
                reply_markup=_waiting_photos_keyboard(request_id, photo_count, video_count),
            )
        except Exception:
            pass


@router.callback_query(
    StateFilter(EngineerInspectionStates.inspection_waiting_photos),
    F.data.startswith("eng:inspection:restart_photos:"),
)
async def engineer_inspection_restart_photos(callback: CallbackQuery, state: FSMContext):
    """Начать загрузку фото заново."""
    request_id = int(callback.data.split(":")[3])
    
    data = await state.get_data()
    if data.get("request_id") != request_id:
        await callback.answer("Ошибка. Начните заново.", show_alert=True)
        await state.clear()
        return
    
    await state.update_data(photos=[], videos=[], photo_file_ids=[], status_message_id=None)
    status_msg = await callback.message.edit_text(
        "🔄 Список очищен. Отправьте фото/видео заново.\n"
        "Отправьте все необходимые фото/видео подряд, затем подтвердите все сразу.",
        reply_markup=_waiting_photos_keyboard(request_id, photo_count=0, video_count=0),
    )
    await state.update_data(status_message_id=status_msg.message_id)
    await callback.answer("Начните отправку фото/видео заново.")
