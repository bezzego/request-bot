"""Модуль начала и завершения работы мастера."""
from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Photo, PhotoType, Request, WorkSession
from app.infrastructure.db.session import async_session
from app.keyboards.master_kb import finish_photo_kb, master_kb
from app.services.request_service import RequestService
from app.utils.request_formatters import format_request_label
from app.utils.timezone import now_moscow
from app.handlers.master.states import MasterStates
from app.handlers.master.utils import get_master, load_request
from app.handlers.master.detail import refresh_request_detail
from app.handlers.master.work.utils import (
    FINISH_CONTEXT_KEY,
    PHOTO_CONFIRM_TEXT,
    CANCEL_TEXT,
    load_finish_context,
    save_finish_context,
    build_finish_status,
    render_finish_summary,
    cleanup_finish_summary,
    refresh_finish_summary_from_context,
    send_finish_report,
    notify_engineer,
    format_location_url,
)

logger = logging.getLogger(__name__)

router = Router()


@router.callback_query(F.data.startswith("master:start:"))
async def master_start_work(callback: CallbackQuery, state: FSMContext):
    """Начать работу мастера - запрашиваем геопозицию."""
    request_id = int(callback.data.split(":")[2])
    
    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        # Проверяем, не начата ли уже работа
        active_session = await session.scalar(
            select(WorkSession).where(
                WorkSession.request_id == request.id,
                WorkSession.master_id == master.id,
                WorkSession.finished_at.is_(None),
            )
        )
        if active_session:
            await callback.answer("Работа уже начата.", show_alert=True)
            return

        before_photos = [photo for photo in (request.photos or []) if photo.type == PhotoType.BEFORE]
        if not before_photos:
            await callback.answer("Инженер ещё не приложил фото дефектов.", show_alert=True)
            await callback.message.answer(
                "Старт работ недоступен: инженер должен прикрепить фото дефектов. Свяжитесь с инженером."
            )
            return

    # Переводим в состояние ожидания геопозиции
    await state.set_state(MasterStates.waiting_start_location)
    await state.update_data(request_id=request_id)
    
    await callback.message.answer(
        "Для начала работы отправьте вашу геопозицию.\n"
        "Нажмите кнопку «📍 Отправить геопозицию» или отправьте геопозицию вручную.",
        reply_markup=master_kb,
    )
    await callback.answer()


@router.message(StateFilter(MasterStates.waiting_start_location), F.location)
async def master_start_work_location(message: Message, state: FSMContext):
    """Обработка геопозиции для начала работы."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    location = message.location
    latitude = location.latitude
    longitude = location.longitude
    
    async with async_session() as session:
        master = await get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await load_request(session, master.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        # Начинаем работу с геопозицией
        await RequestService.start_work(
            session,
            request,
            master_id=master.id,
            latitude=latitude,
            longitude=longitude,
            address=request.address,
        )
        await session.commit()
        request_label = format_request_label(request)
        await notify_engineer(
            message.bot,
            request,
            text=(
                f"🔨 Мастер {master.full_name} начал работу по заявке {request_label}.\n"
                f"📍 Геопозиция: {format_location_url(latitude, longitude)}"
            ),
            location=(latitude, longitude),
        )
    
    # Возвращаем основную клавиатуру
    await message.answer(
        "✅ Работа начата. Геопозиция сохранена.",
        reply_markup=master_kb,
    )
    await state.clear()
    await refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("master:finish:"))
async def master_finish_prompt(callback: CallbackQuery, state: FSMContext):
    """Запускает мастер завершения работ с проверкой требований."""
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return

    try:
        request_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return

    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        active_session = await session.scalar(
            select(WorkSession)
            .where(
                WorkSession.request_id == request.id,
                WorkSession.master_id == master.id,
                WorkSession.finished_at.is_(None),
            )
            .order_by(WorkSession.started_at.desc())
        )
        if not active_session:
            await callback.answer("Работа не была начата.", show_alert=True)
            return

    data = await state.get_data()
    finish_context = data.get(FINISH_CONTEXT_KEY) or {}
    if finish_context.get("request_id") != request_id:
        finish_context = {
            "request_id": request_id,
            "session_id": active_session.id,
            "photos_confirmed": False,
            "new_photo_count": 0,
            "fact_confirmed": False,
            "finish_latitude": None,
            "finish_longitude": None,
            "message_id": None,
            "chat_id": callback.message.chat.id,
        }
    else:
        finish_context["session_id"] = active_session.id
        finish_context.setdefault("finish_latitude", None)
        finish_context.setdefault("finish_longitude", None)
        finish_context.setdefault("new_photo_count", 0)
        finish_context.setdefault("fact_confirmed", False)
        finish_context.setdefault("photos_confirmed", False)
        finish_context["chat_id"] = callback.message.chat.id

    await state.update_data({FINISH_CONTEXT_KEY: finish_context})
    await state.set_state(MasterStates.finish_dashboard)
    await render_finish_summary(callback.bot, finish_context, state)
    await callback.answer()


@router.callback_query(F.data.startswith("master:finish_photo:"))
async def master_finish_photo_prompt(callback: CallbackQuery, state: FSMContext):
    """Запуск шага загрузки фото выполненной работы."""
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return

    try:
        request_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return

    finish_context = await load_finish_context(state)
    if not finish_context or finish_context.get("request_id") != request_id:
        await callback.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.", show_alert=True)
        return
    if finish_context.get("photos_confirmed"):
        await callback.answer("Фото уже подтверждены.", show_alert=True)
        return

    finish_context["new_photo_count"] = 0
    finish_context["photos_confirmed"] = False
    finish_context["photos"] = []
    finish_context["videos"] = []
    finish_context["status_message_id"] = None
    await save_finish_context(state, finish_context)
    await state.set_state(MasterStates.finish_photo_upload)
    status_msg = await callback.message.answer(
        "Прикрепите все необходимые фото/видео выполненной работы.\n"
        "Можно отправить несколько фото/видео подряд.\n"
        "Когда закончите, нажмите «✅ Подтвердить фото». Для отмены отправьте «Отмена».",
        reply_markup=finish_photo_kb,
    )
    finish_context["status_message_id"] = status_msg.message_id
    await save_finish_context(state, finish_context)
    await callback.answer()


@router.callback_query(F.data.startswith("master:finish_geo:"))
async def master_finish_geo_prompt(callback: CallbackQuery, state: FSMContext):
    """Запрос геопозиции завершения работы."""
    parts = callback.data.split(":")
    if len(parts) != 3:
        await callback.answer()
        return

    try:
        request_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return

    finish_context = await load_finish_context(state)
    if not finish_context or finish_context.get("request_id") != request_id:
        await callback.answer("Процесс завершения не найден.", show_alert=True)
        return

    await state.set_state(MasterStates.waiting_finish_location)
    await callback.message.answer(
        "Отправьте геопозицию завершения работ.\n"
        "Используйте кнопку «📍 Отправить геопозицию» или прикрепите координаты вручную.\n"
        "Для отмены напишите «Отмена».",
        reply_markup=master_kb,
    )
    await callback.answer()


@router.callback_query(F.data == "master:finish_cancel")
async def master_finish_cancel(callback: CallbackQuery, state: FSMContext):
    """Отменяет текущий мастер завершения."""
    finish_context = await load_finish_context(state)
    if finish_context:
        await cleanup_finish_summary(callback.bot, finish_context, "Процесс завершения отменён.")
    await state.clear()
    await callback.answer("Процесс завершения остановлен.")


@router.callback_query(F.data.startswith("master:finish_submit:"))
async def master_finish_submit(callback: CallbackQuery, state: FSMContext):
    """Финальное завершение работы после выполнения всех условий."""
    parts = callback.data.split(":")
    if len(parts) < 3:
        await callback.answer()
        return

    try:
        request_id = int(parts[2])
    except ValueError:
        await callback.answer("Некорректная заявка.", show_alert=True)
        return
    mode = parts[3] if len(parts) > 3 else "final"
    finalize = mode != "session"

    finish_context = await load_finish_context(state)
    if not finish_context or finish_context.get("request_id") != request_id:
        await callback.answer("Процесс завершения не найден. Начните заново.", show_alert=True)
        return

    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        status = await build_finish_status(session, request, finish_context)
        if not status.all_ready:
            await callback.answer("Выполните все условия перед завершением.", show_alert=True)
            await render_finish_summary(callback.bot, finish_context, state)
            return

        latitude = finish_context.get("finish_latitude")
        longitude = finish_context.get("finish_longitude")
        session_id = finish_context.get("session_id")
        await RequestService.finish_work(
            session,
            request,
            master_id=master.id,
            session_id=session_id,
            latitude=latitude,
            longitude=longitude,
            finished_at=now_moscow(),
            hours_reported=None,
            completion_notes=None,
            finalize=finalize,
        )
        await session.commit()

        await send_finish_report(callback.bot, request, master, status, finalized=finalize)

    master_text = (
        "Завершение работ зафиксировано и передано инженеру. Спасибо за оперативность."
        if finalize
        else "Смена закрыта. Инженер получил обновление, можно продолжить работы позже."
    )
    summary_text = "Работы успешно завершены." if finalize else "Смена зафиксирована."

    await callback.message.answer(master_text, reply_markup=master_kb)
    await cleanup_finish_summary(callback.bot, finish_context, summary_text)
    await state.clear()
    await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
    await callback.answer("Готово.")


@router.message(StateFilter(MasterStates.waiting_finish_location), F.location)
async def master_finish_work_location(message: Message, state: FSMContext):
    """Обработка геопозиции завершения работы в мастере завершения."""
    finish_context = await load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.")
        await state.clear()
        return

    latitude = message.location.latitude
    longitude = message.location.longitude

    async with async_session() as session:
        master = await get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа к заявке.")
            await state.clear()
            return

        request = await load_request(session, master.id, finish_context["request_id"])
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        work_session = None
        session_id = finish_context.get("session_id")
        if session_id:
            work_session = await session.get(WorkSession, session_id)
        if not work_session:
            work_session = await session.scalar(
                select(WorkSession)
                .where(
                    WorkSession.request_id == request.id,
                    WorkSession.master_id == master.id,
                    WorkSession.finished_at.is_(None),
                )
                .order_by(WorkSession.started_at.desc())
            )
        if not work_session:
            await message.answer("Активная смена не найдена. Начните процесс заново.")
            await state.clear()
            return

        work_session.finished_latitude = latitude
        work_session.finished_longitude = longitude
        await session.commit()

    finish_context["finish_latitude"] = latitude
    finish_context["finish_longitude"] = longitude
    await save_finish_context(state, finish_context)
    await state.set_state(MasterStates.finish_dashboard)
    await message.answer("Геопозиция завершения сохранена.", reply_markup=master_kb)
    await render_finish_summary(message.bot, finish_context, state)


@router.message(StateFilter(MasterStates.waiting_finish_location))
async def master_finish_location_fallback(message: Message, state: FSMContext):
    """Подсказки/отмена во время ожидания геопозиции."""
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.set_state(MasterStates.finish_dashboard)
        await message.answer("Ожидание геопозиции отменено.", reply_markup=master_kb)
        await refresh_finish_summary_from_context(message.bot, state)
    else:
        await message.answer("Отправьте геопозицию или напишите «Отмена», чтобы вернуться назад.")


@router.message(StateFilter(MasterStates.finish_photo_upload), F.photo)
async def master_finish_photo_collect(message: Message, state: FSMContext):
    """Собирает фото, отправленные во время мастера завершения."""
    finish_context = await load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.", reply_markup=master_kb)
        await state.clear()
        return

    photo = message.photo[-1]
    caption = (message.caption or "").strip() or None
    
    # Добавляем фото в список
    photos = finish_context.get("photos", [])
    photos.append({
        "file_id": photo.file_id,
        "caption": caption,
        "is_video": False,
    })
    
    videos = finish_context.get("videos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    finish_context["photos"] = photos
    finish_context["new_photo_count"] = photo_count + video_count
    await save_finish_context(state, finish_context)
    
    # Обновляем статусное сообщение
    status_message_id = finish_context.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить фото»."
                ),
                reply_markup=finish_photo_kb,
            )
        except Exception:
            pass


@router.message(StateFilter(MasterStates.finish_photo_upload), F.video)
async def master_finish_video_collect(message: Message, state: FSMContext):
    """Собирает видео, отправленные во время мастера завершения."""
    finish_context = await load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.", reply_markup=master_kb)
        await state.clear()
        return

    video = message.video
    caption = (message.caption or "").strip() or None
    
    # Добавляем видео в список
    videos = finish_context.get("videos", [])
    videos.append({
        "file_id": video.file_id,
        "caption": caption,
        "is_video": True,
    })
    
    photos = finish_context.get("photos", [])
    photo_count = len(photos)
    video_count = len(videos)
    
    finish_context["videos"] = videos
    finish_context["new_photo_count"] = photo_count + video_count
    await save_finish_context(state, finish_context)
    
    # Обновляем статусное сообщение
    status_message_id = finish_context.get("status_message_id")
    if status_message_id:
        try:
            await message.bot.edit_message_text(
                chat_id=message.chat.id,
                message_id=status_message_id,
                text=(
                    f"📷 Получено: {photo_count} фото, {video_count} видео\n"
                    "Отправьте ещё фото/видео или нажмите «✅ Подтвердить фото»."
                ),
                reply_markup=finish_photo_kb,
            )
        except Exception:
            pass


@router.message(StateFilter(MasterStates.finish_photo_upload))
async def master_finish_photo_text(message: Message, state: FSMContext):
    """Обрабатывает подтверждение/отмену шага с фото."""
    text = (message.text or "").strip()
    lower_text = text.lower()
    finish_context = await load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.", reply_markup=master_kb)
        await state.clear()
        return

    if lower_text == CANCEL_TEXT.lower():
        await state.set_state(MasterStates.finish_dashboard)
        await message.answer("Загрузка фото отменена.", reply_markup=master_kb)
        await refresh_finish_summary_from_context(message.bot, state)
        return

    if lower_text == PHOTO_CONFIRM_TEXT.lower() or "подтверд" in lower_text:
        photos = finish_context.get("photos", [])
        videos = finish_context.get("videos", [])
        total_files = len(photos) + len(videos)
        
        if total_files <= 0:
            await message.answer("Отправьте хотя бы одно фото или видео перед подтверждением.")
            return

        # Сохраняем все фото и видео в БД
        request_id = finish_context.get("request_id")
        async with async_session() as session:
            master = await get_master(session, message.from_user.id)
            if not master:
                await message.answer("Нет доступа к заявке.", reply_markup=master_kb)
                await state.clear()
                return
            
            request = await load_request(session, master.id, request_id)
            if not request:
                await message.answer("Заявка не найдена.", reply_markup=master_kb)
                await state.clear()
                return
            
            # Сохраняем все фото
            for photo_data in photos:
                new_photo = Photo(
                    request_id=request.id,
                    type=PhotoType.AFTER,
                    file_id=photo_data["file_id"],
                    caption=photo_data.get("caption"),
                )
                session.add(new_photo)
            
            # Сохраняем все видео (как фото с типом AFTER)
            for video_data in videos:
                new_photo = Photo(
                    request_id=request.id,
                    type=PhotoType.AFTER,
                    file_id=video_data["file_id"],
                    caption=video_data.get("caption"),
                )
                session.add(new_photo)
            
            await session.commit()
            logger.info(
                "Master finish: saved %s photos and %s videos for request_id=%s user=%s",
                len(photos),
                len(videos),
                request.id,
                message.from_user.id,
            )

        finish_context["photos_confirmed"] = True
        finish_context["new_photo_count"] = total_files
        await save_finish_context(state, finish_context)
        await state.set_state(MasterStates.finish_dashboard)
        
        files_text = []
        if len(photos) > 0:
            files_text.append(f"{len(photos)} фото")
        if len(videos) > 0:
            files_text.append(f"{len(videos)} видео")
        files_summary = " и ".join(files_text) if files_text else "файлы"
        
        await message.answer(
            f"✅ Сохранено: {files_summary}. Спасибо!",
            reply_markup=master_kb,
        )
        await render_finish_summary(message.bot, finish_context, state)
        return

    await message.answer(
        "Прикрепите фото или нажмите «✅ Подтвердить фото», когда закончите. Для отмены отправьте «Отмена».",
        reply_markup=finish_photo_kb,
    )
