"""Функции для отправки фото дефектов."""
from __future__ import annotations

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InputMediaPhoto, InputMediaVideo, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models import Photo, PhotoType


async def send_defect_photos_with_start_button(message: Message, photos: list[Photo], request_id: int) -> None:
    """Отправка фото дефектов с кнопкой 'Начать работу' под последним сообщением."""
    before_photos = [photo for photo in photos if photo.type == PhotoType.BEFORE]
    if not before_photos:
        return

    # Строим клавиатуру с кнопкой "Начать работу"
    builder = InlineKeyboardBuilder()
    builder.button(
        text="▶️ Начать работу",
        callback_data=f"master:start:{request_id}",
    )
    builder.adjust(1)
    start_button_markup = builder.as_markup()

    # По умолчанию все — фото; при ошибке (есть видео) переразделим в except
    photo_items: list[Photo] = list(before_photos)
    video_items: list[Photo] = []

    # Пробуем отправить все файлы как фото, при ошибке разделяем на фото и видео
    photo_chunk: list[InputMediaPhoto] = []
    total_items = len(before_photos)
    last_chunk_index = (total_items - 1) // 10
    current_chunk = 0
    
    # Сначала пробуем отправить все как фото
    try:
        for idx, photo in enumerate(before_photos):
            caption = photo.caption or ""
            if idx == 0:
                prefix = "📷 Фото дефектов (до работ)"
                caption = f"{prefix}\n{caption}".strip() if caption else prefix
            
            photo_media = InputMediaPhoto(media=photo.file_id, caption=caption if idx == 0 else photo.caption or None)
            photo_chunk.append(photo_media)
            
            is_last_item = (idx == total_items - 1)
            is_last_chunk = (current_chunk == last_chunk_index)
            
            if len(photo_chunk) == 10 or is_last_item:
                try:
                    if len(photo_chunk) == 1:
                        item = photo_chunk[0]
                        if is_last_item:
                            await message.answer_photo(
                                item.media,
                                caption=item.caption,
                                reply_markup=start_button_markup,
                            )
                        else:
                            await message.answer_photo(item.media, caption=item.caption)
                    else:
                        if is_last_item:
                            await message.answer_media_group(photo_chunk)
                            await message.answer(
                                "Просмотрите фото дефектов выше.",
                                reply_markup=start_button_markup,
                            )
                        else:
                            await message.answer_media_group(photo_chunk)
                    photo_chunk = []
                    current_chunk += 1
                except TelegramBadRequest as e:
                    if "can't use file of type Video as Photo" in str(e) or "Video" in str(e):
                        # Есть видео в группе, нужно разделить
                        raise
                    else:
                        raise
    except TelegramBadRequest:
        # Есть видео, разделяем на фото и видео
        photo_items.clear()
        video_items.clear()
        test_message_ids: list[int] = []
        
        # Определяем тип каждого файла, пробуя отправить
        for photo in before_photos:
            try:
                test_msg = await message.bot.send_photo(
                    chat_id=message.chat.id,
                    photo=photo.file_id,
                )
                test_message_ids.append(test_msg.message_id)
                photo_items.append(photo)
            except TelegramBadRequest as e:
                if "can't use file of type Video as Photo" in str(e) or "Video" in str(e):
                    video_items.append(photo)
                else:
                    # Другая ошибка, пробуем как видео
                    try:
                        test_msg = await message.bot.send_video(
                            chat_id=message.chat.id,
                            video=photo.file_id,
                        )
                        test_message_ids.append(test_msg.message_id)
                        video_items.append(photo)
                    except Exception:
                        pass
        
        # Удаляем тестовые сообщения
        for msg_id in test_message_ids:
            try:
                await message.bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=msg_id,
                )
            except Exception:
                pass
    
    # Отправляем фото группами
    photo_chunk: list[InputMediaPhoto] = []
    total_photos = len(photo_items)
    last_photo_index = (total_photos - 1) // 10 if total_photos > 0 else -1
    current_photo_chunk = 0

    for idx, photo in enumerate(photo_items):
        caption = photo.caption or ""
        if idx == 0:
            prefix = "📷 Фото дефектов (до работ)"
            caption = f"{prefix}\n{caption}".strip() if caption else prefix
        
        photo_media = InputMediaPhoto(media=photo.file_id, caption=caption if idx == 0 else photo.caption or None)
        photo_chunk.append(photo_media)
        
        is_last_photo = (idx == total_photos - 1)
        is_last_photo_chunk = (current_photo_chunk == last_photo_index)
        
        if len(photo_chunk) == 10 or is_last_photo:
            try:
                if len(photo_chunk) == 1:
                    item = photo_chunk[0]
                    if is_last_photo:
                        await message.answer_photo(
                            item.media,
                            caption=item.caption,
                            reply_markup=start_button_markup if is_last_photo and len(video_items) == 0 else None,
                        )
                    else:
                        await message.answer_photo(item.media, caption=item.caption)
                else:
                    if is_last_photo:
                        await message.answer_media_group(photo_chunk)
                        if len(video_items) == 0:
                            await message.answer(
                                "Просмотрите фото дефектов выше.",
                                reply_markup=start_button_markup,
                            )
                    else:
                        await message.answer_media_group(photo_chunk)
                photo_chunk = []
                current_photo_chunk += 1
            except Exception:
                pass
    
    # Отправляем видео группами
    video_chunk: list[InputMediaVideo] = []
    total_videos = len(video_items)
    last_video_index = (total_videos - 1) // 10 if total_videos > 0 else -1
    current_video_chunk = 0

    for idx, photo in enumerate(video_items):
        caption = photo.caption or ""
        if idx == 0 and len(photo_items) == 0:
            prefix = "📷 Видео дефектов (до работ)"
            caption = f"{prefix}\n{caption}".strip() if caption else prefix
        
        video_media = InputMediaVideo(media=photo.file_id, caption=caption if idx == 0 and len(photo_items) == 0 else photo.caption or None)
        video_chunk.append(video_media)
        
        is_last_video = (idx == total_videos - 1)
        is_last_video_chunk = (current_video_chunk == last_video_index)
        
        if len(video_chunk) == 10 or is_last_video:
            try:
                if len(video_chunk) == 1:
                    item = video_chunk[0]
                    if is_last_video:
                        await message.answer_video(
                            item.media,
                            caption=item.caption,
                            reply_markup=start_button_markup,
                        )
                    else:
                        await message.answer_video(item.media, caption=item.caption)
                else:
                    if is_last_video:
                        await message.answer_media_group(video_chunk)
                        await message.answer(
                            "Просмотрите видео дефектов выше.",
                            reply_markup=start_button_markup,
                        )
                    else:
                        await message.answer_media_group(video_chunk)
                video_chunk = []
                current_video_chunk += 1
            except Exception:
                pass
