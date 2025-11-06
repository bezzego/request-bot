from __future__ import annotations

from datetime import datetime, timezone
import logging

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message, InputMediaPhoto
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from sqlalchemy import func, or_, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
    WorkItem,
    WorkSession,
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

logger = logging.getLogger(__name__)

router = Router()


class MasterStates(StatesGroup):
    waiting_start_location = State()  # Ожидание геопозиции для начала работы
    finish_report = State()  # Ожидание комментария для завершения работы
    waiting_finish_location = State()  # Ожидание геопозиции для завершения работы


STATUS_TITLES = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначена мастеру",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


@router.message(F.text == "📥 Мои заявки")
async def master_requests(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Эта функция доступна только мастерам.")
            return

        requests = await _load_master_requests(session, master.id)

    if not requests:
        await message.answer("У вас пока нет назначенных заявок. Ожидайте задач от инженера.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"master:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("master:detail:"))
async def master_request_detail(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    await _show_request_detail(callback.message, request, edit=True)
    await callback.answer()


@router.callback_query(F.data == "master:back")
async def master_back_to_list(callback: CallbackQuery):
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        requests = await _load_master_requests(session, master.id)

    if not requests:
        await callback.message.edit_text("Нет активных заявок. Ожидайте новых задач.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"master:detail:{req.id}",
        )
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("master:view_defects:"))
async def master_view_defects(callback: CallbackQuery):
    """Показать фото дефектов для мастера."""
    request_id = int(callback.data.split(":")[2])
    
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await _load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
        before_photos = [photo for photo in (request.photos or []) if photo.type == PhotoType.BEFORE]
        if not before_photos:
            await callback.answer("Фото дефектов пока нет.", show_alert=True)
            await callback.message.answer(
                "Инженер ещё не приложил фото дефектов. Свяжитесь с инженером."
            )
            return
    
    # Отправляем фото дефектов
    await _send_defect_photos_with_start_button(callback.message, before_photos, request_id)
    await callback.answer()


@router.callback_query(F.data.startswith("master:start:"))
async def master_start_work(callback: CallbackQuery, state: FSMContext):
    """Начать работу мастера - запрашиваем геопозицию."""
    request_id = int(callback.data.split(":")[2])
    
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
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
    
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
    
    location_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геопозицию", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    
    await callback.message.answer(
        "Для начала работы отправьте вашу геопозицию.\n"
        "Нажмите кнопку «📍 Отправить геопозицию» или отправьте геопозицию вручную.",
        reply_markup=location_keyboard,
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
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        # Начинаем работу с геопозицией
        work_session = await RequestService.start_work(
            session,
            request,
            master_id=master.id,
            latitude=latitude,
            longitude=longitude,
            address=request.address,
        )
        await session.commit()
        
        # Отправляем уведомление инженеру
        if request.engineer_id:
            engineer = await session.scalar(
                select(User).where(User.id == request.engineer_id)
            )
            if engineer and engineer.telegram_id:
                location_url = f"https://www.google.com/maps?q={latitude},{longitude}"
                try:
                    await message.bot.send_message(
                        chat_id=engineer.telegram_id,
                        text=(
                            f"🔨 Мастер {master.full_name} начал работу по заявке {request.number}.\n"
                            f"📍 Геопозиция: {location_url}\n"
                            f"Адрес: {request.address}"
                        ),
                    )
                    # Отправляем геопозицию отдельным сообщением
                    await message.bot.send_location(
                        chat_id=engineer.telegram_id,
                        latitude=latitude,
                        longitude=longitude,
                    )
                except Exception as e:
                    import logging
                    logging.warning("Failed to notify engineer about work start: %s", e)
    
    # Убираем клавиатуру с геопозицией
    from aiogram.types import ReplyKeyboardRemove
    await message.answer(
        "✅ Работа начата. Геопозиция сохранена.",
        reply_markup=ReplyKeyboardRemove(),
    )
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("master:finish:"))
async def master_finish_prompt(callback: CallbackQuery, state: FSMContext):
    """Проверка требований перед завершением работы."""
    request_id = int(callback.data.split(":")[2])

    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        # Проверяем активную сессию работы
        active_session = await session.scalar(
            select(WorkSession).where(
                WorkSession.request_id == request.id,
                WorkSession.master_id == master.id,
                WorkSession.finished_at.is_(None),
            )
        )
        if not active_session:
            await callback.answer("Работа не была начата.", show_alert=True)
            return

        missing = await _get_finish_requirements(session, request.id, active_session.id)
        if missing:
            await callback.answer("Не все требования выполнены.", show_alert=True)
            await callback.message.answer(
                "Чтобы завершить работы, выполните:\n" + "\n".join(f"• {item}" for item in missing)
            )
            return

    # Все требования выполнены, запрашиваем геопозицию для завершения
    await state.set_state(MasterStates.waiting_finish_location)
    await state.update_data(request_id=request_id)
    
    from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
    
    location_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📍 Отправить геопозицию", request_location=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    
    await callback.message.answer(
        "Для завершения работы отправьте вашу геопозицию.\n"
        "Нажмите кнопку «📍 Отправить геопозицию» или отправьте геопозицию вручную.",
        reply_markup=location_keyboard,
    )
    await callback.answer()


@router.message(StateFilter(MasterStates.waiting_finish_location), F.location)
async def master_finish_work_location(message: Message, state: FSMContext):
    """Обработка геопозиции для завершения работы."""
    data = await state.get_data()
    request_id = data.get("request_id")
    
    if not request_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    location = message.location
    latitude = location.latitude
    longitude = location.longitude
    
    # Сохраняем геопозицию и переходим к комментарию
    await state.update_data(finish_latitude=latitude, finish_longitude=longitude)
    await state.set_state(MasterStates.finish_report)
    
    from aiogram.types import ReplyKeyboardRemove
    await message.answer(
        "Геопозиция сохранена.\n\n"
        "Добавьте комментарий по результату работ или отправьте «-».\n"
        "Для отмены введите «Отмена».",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(MasterStates.finish_report))
async def master_finish_work(message: Message, state: FSMContext):
    """Завершение работы с комментарием."""
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    comment_text = message.text.strip()
    if comment_text == "-":
        comment_text = None

    data = await state.get_data()
    request_id = data.get("request_id")
    finish_latitude = data.get("finish_latitude")
    finish_longitude = data.get("finish_longitude")

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        # Находим активную сессию
        active_session = await session.scalar(
            select(WorkSession).where(
                WorkSession.request_id == request.id,
                WorkSession.master_id == master.id,
                WorkSession.finished_at.is_(None),
            )
        )
        if not active_session:
            await message.answer("Активная сессия работы не найдена.")
            await state.clear()
            return

        await RequestService.finish_work(
            session,
            request,
            master_id=master.id,
            session_id=active_session.id,
            latitude=finish_latitude,
            longitude=finish_longitude,
            finished_at=datetime.now(timezone.utc),
            hours_reported=None,
            completion_notes=comment_text,
        )
        await session.commit()

    await message.answer("Завершение работ зафиксировано. Спасибо за оперативность.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("master:update_fact:"))
async def master_update_fact(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = _catalog_header(request)

    catalog = get_work_catalog()
    text = f"{header}\n\n{format_category_message(None)}"
    markup = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="m",
        request_id=request_id,
    )
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:m:"))
async def master_work_catalog(callback: CallbackQuery):
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "m":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    catalog = get_work_catalog()

    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
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
                role_key="m",
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
                role_key="m",
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
                role_key="m",
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
                author_id=master.id,
            )
            await session.commit()

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=new_quantity)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="m",
                request_id=request_id,
                new_quantity=new_quantity,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")

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


@router.message(F.text == "📸 Инструкция по фотоотчёту")
async def master_photo_instruction(message: Message):
    await message.answer(
        "Для фиксации хода работ отправляйте фото с подписью вида:\n"
        "<code>RQ-123 описание фотографии</code>\n"
        "Бот автоматически сохранит фото в карточке заявки. Перед завершением работ\n"
        "обязательно приложите фото «до/после» и акт выполненных работ."
    )


@router.message(F.photo)
async def master_photo(message: Message):
    caption = (message.caption or "").strip()
    logger.debug("Master photo handler start: user=%s caption=%r", message.from_user.id, caption)

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            logger.warning("Master photo: user %s is not a master", message.from_user.id)
            return

        request: Request | None = None
        comment: str | None = None
        number_hint: str | None = None

        # 1. Try caption RQ-... pattern
        if caption:
            parts = caption.split()
            number_hint = parts[0]
            if number_hint.upper().startswith("RQ-"):
                comment = " ".join(parts[1:]) if len(parts) > 1 else None
                request = await session.scalar(
                    select(Request).where(Request.number == number_hint, Request.master_id == master.id)
                )
                if not request and number_hint[3:].isdigit():
                    alt = number_hint[3:]
                    logger.debug("Master photo: caption lookup failed, trying alt=%s", alt)
                    request = await session.scalar(
                        select(Request).where(Request.number == alt, Request.master_id == master.id)
                    )

        # 2. Try reply-to message (if user replied to card)
        if not request and message.reply_to_message:
            replied_text = message.reply_to_message.text or ""
            logger.debug("Master photo: reply_to text=%r", replied_text)
            for token in replied_text.split():
                if token.upper().startswith("RQ-"):
                    number_hint = token
                    break
                if token.isdigit():
                    number_hint = token
                    break
            if number_hint:
                request = await session.scalar(
                    select(Request).where(Request.number == number_hint, Request.master_id == master.id)
                )
                if not request and number_hint.isdigit():
                    alt = f"RQ-{number_hint}"
                    request = await session.scalar(
                        select(Request).where(Request.number == alt, Request.master_id == master.id)
                    )

        # 3. Try active work session
        if not request:
            active_session = await session.scalar(
                select(WorkSession)
                .where(
                    WorkSession.master_id == master.id,
                    WorkSession.finished_at.is_(None),
                )
                .order_by(WorkSession.started_at.desc())
            )
            if active_session:
                request = await session.get(Request, active_session.request_id)
                logger.debug("Master photo: using active session request_id=%s", active_session.request_id)

        # 4. Fallback to most recent assigned/in-progress request
        if not request:
            request = await session.scalar(
                select(Request)
                .where(Request.master_id == master.id)
                .order_by(Request.updated_at.desc())
            )
            if request:
                logger.debug("Master photo: fallback to latest request %s", request.number)

        if not request:
            await message.answer(
                "Не удалось определить заявку. Добавьте подпись с номером вида «RQ-123 описание» "
                "или отправьте фото в ответ на карточку заявки."
            )
            logger.warning("Master photo: request not resolved for user=%s caption=%r", message.from_user.id, caption)
            return

        photo = message.photo[-1]
        new_photo = Photo(
            request_id=request.id,
            type=PhotoType.PROCESS,
            file_id=photo.file_id,
            caption=comment,
        )
        session.add(new_photo)
        await session.commit()
        logger.info(
            "Master photo saved: request_id=%s user=%s file_id=%s caption=%s",
            request.id,
            message.from_user.id,
            photo.file_id,
            comment,
        )

    label = request.number
    await message.answer(f"Фото добавлено к заявке {label}.")


@router.message(F.location)
async def master_location(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            return

        work_session = await session.scalar(
            select(WorkSession)
            .where(WorkSession.master_id == master.id, WorkSession.finished_at.is_(None))
            .order_by(WorkSession.started_at.desc())
        )

        if work_session:
            work_session.started_latitude = message.location.latitude
            work_session.started_longitude = message.location.longitude
            await session.commit()
            await message.answer("Геопозиция старта работ сохранена.")
            return

        last_session = await session.scalar(
            select(WorkSession)
            .where(
                WorkSession.master_id == master.id,
                WorkSession.finished_at.isnot(None),
                WorkSession.finished_latitude.is_(None),
            )
            .order_by(WorkSession.finished_at.desc())
        )

        if last_session:
            last_session.finished_latitude = message.location.latitude
            last_session.finished_longitude = message.location.longitude
            await session.commit()
            await message.answer("Геопозиция завершения работ сохранена.")
            return


# --- служебные функции ---


async def _get_finish_requirements(session, request_id: int, work_session_id: int) -> list[str]:
    """Проверяет требования для завершения работы.
    
    Требования:
    - Фото готовой работы (PROCESS или AFTER)
    - Заполнен факт выполненных работ
    - Геопозиция завершения работы
    """
    # Проверяем фото готовой работы
    photo_count = await session.scalar(
        select(func.count(Photo.id)).where(
            Photo.request_id == request_id,
            Photo.type.in_((PhotoType.PROCESS, PhotoType.AFTER)),
        )
    )
    
    # Проверяем факт выполненных работ
    fact_count = await session.scalar(
        select(func.count(WorkItem.id)).where(
            WorkItem.request_id == request_id,
            or_(
                func.coalesce(WorkItem.actual_quantity, 0) > 0,
                func.coalesce(WorkItem.actual_cost, 0) > 0,
            ),
        )
    )
    
    # Проверяем геопозицию завершения (она будет запрошена позже, если все остальное готово)
    # Поэтому здесь не проверяем геопозицию - она будет запрошена в master_finish_prompt
    # после проверки фото и факта

    missing: list[str] = []
    if not photo_count:
        missing.append("загрузите минимум одно фото выполненных работ")
    if not fact_count:
        missing.append("зафиксируйте факт выполненных работ через каталог")
    # Геопозиция будет запрошена отдельно, если фото и факт готовы
    
    return missing


async def _send_defect_photos(message: Message, photos: list[Photo]) -> None:
    """Отправка фото дефектов (старая версия, для совместимости)."""
    before_photos = [photo for photo in photos if photo.type == PhotoType.BEFORE]
    if not before_photos:
        return

    chunk: list[InputMediaPhoto] = []
    for idx, photo in enumerate(before_photos):
        caption = photo.caption or ""
        if not chunk:
            prefix = "Фото дефектов (до работ)."
            caption = f"{prefix}\n{caption}".strip()
        chunk.append(InputMediaPhoto(media=photo.file_id, caption=caption or None))

        if len(chunk) == 10:
            await _send_media_chunk(message, chunk)
            chunk = []

    if chunk:
        await _send_media_chunk(message, chunk)


async def _send_defect_photos_with_start_button(message: Message, photos: list[Photo], request_id: int) -> None:
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

    chunk: list[InputMediaPhoto] = []
    total_photos = len(before_photos)
    last_chunk_index = (total_photos - 1) // 10  # Индекс последнего чанка (0-based)
    current_chunk = 0

    for idx, photo in enumerate(before_photos):
        caption = photo.caption or ""
        if not chunk:
            prefix = "📷 Фото дефектов (до работ)"
            caption = f"{prefix}\n{caption}".strip() if caption else prefix
        chunk.append(InputMediaPhoto(media=photo.file_id, caption=caption or None))

        # Если набрали 10 фото или это последнее фото
        if len(chunk) == 10 or idx == total_photos - 1:
            is_last_chunk = (current_chunk == last_chunk_index)
            
            if len(chunk) == 1:
                # Если одно фото в чанке
                item = chunk[0]
                if is_last_chunk:
                    # Если это последний чанк, добавляем кнопку к фото
                    await message.answer_photo(
                        item.media,
                        caption=item.caption,
                        reply_markup=start_button_markup,
                    )
                else:
                    await message.answer_photo(item.media, caption=item.caption)
            else:
                # Если несколько фото в чанке
                if is_last_chunk:
                    # Если это последний чанк, отправляем медиа-группу, затем кнопку отдельным сообщением
                    await message.answer_media_group(chunk)
                    await message.answer(
                        "Просмотрите фото дефектов выше.",
                        reply_markup=start_button_markup,
                    )
                else:
                    await message.answer_media_group(chunk)
            
            chunk = []
            current_chunk += 1


async def _send_media_chunk(message: Message, media: list[InputMediaPhoto]) -> None:
    if len(media) == 1:
        item = media[0]
        await message.answer_photo(item.media, caption=item.caption)
    else:
        await message.answer_media_group(media)


async def _update_catalog_message(message: Message, text: str, markup) -> None:
    """Обновляет сообщение каталога работ.
    
    Обрабатывает случай, когда сообщение не изменилось (Telegram API не позволяет
    редактировать сообщение без изменений).
    """
    try:
        await message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as exc:
        error_msg = str(exc).lower()
        if "message is not modified" in error_msg:
            # Сообщение не изменилось - это нормально, просто игнорируем
            # Не пытаемся редактировать reply_markup, так как это тоже может вызвать ошибку
            pass
        else:
            # Другая ошибка - отправляем новое сообщение
            try:
                await message.answer(text, reply_markup=markup)
            except Exception:
                # Если и это не получилось, просто игнорируем
                pass


async def _get_work_item(session, request_id: int, name: str) -> WorkItem | None:
    return await session.scalar(
        select(WorkItem)
        .where(
            WorkItem.request_id == request_id,
            func.lower(WorkItem.name) == name.lower(),
        )
    )


def _catalog_header(request: Request) -> str:
    return f"Заявка {request.number} · {request.title}"


async def _get_master(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.MASTER)
    )


async def _load_master_requests(session, master_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.work_sessions),
                    selectinload(Request.photos),
                )
                .where(Request.master_id == master_id)
                .order_by(Request.created_at.desc())
                .limit(20)
            )
        )
        .scalars()
        .all()
    )


async def _load_request(session, master_id: int, request_id: int) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.work_sessions),
            selectinload(Request.photos),
        )
        .where(Request.id == request_id, Request.master_id == master_id)
    )


async def _refresh_request_detail(bot, chat_id: int, master_telegram_id: int, request_id: int) -> None:
    async with async_session() as session:
        master = await _get_master(session, master_telegram_id)
        if not master:
            return
        request = await _load_request(session, master.id, request_id)

    if not request or not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=_format_request_detail(request),
            reply_markup=_detail_keyboard(request.id, request),
        )
    except Exception:
        pass


async def _show_request_detail(message: Message, request: Request, *, edit: bool = False) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id, request)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(request_id: int, request: Request | None = None) -> InlineKeyboardBuilder:
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
    
    builder.button(text="⏹ Завершить работу", callback_data=f"master:finish:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"master:update_fact:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data="master:back")
    builder.adjust(1)
    return builder.as_markup()


@router.callback_query(F.data.startswith("master:work_started:"))
async def master_work_started_info(callback: CallbackQuery):
    """Информация о том, что работа уже начата."""
    await callback.answer("Работа уже начата. Используйте кнопку «Завершить работу» для завершения.", show_alert=True)


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    due_text = request.due_at.strftime("%d.%m.%Y %H:%M") if request.due_at else "не задан"
    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    defects_photos = sum(1 for photo in (request.photos or []) if photo.type == PhotoType.BEFORE)

    lines = [
        f"🧾 <b>{request.number}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Срок устранения: {due_text}",
        f"Адрес: {request.address}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if defects_photos:
        lines.append(f"Фото дефектов: {defects_photos} (будут показаны перед стартом работ)")
    else:
        lines.append("Фото дефектов: пока нет, запросите у инженера.")

    if request.work_items:
        lines.append("")
        lines.append("Позиции бюджета (с указанием факта):")
        for item in request.work_items:
            lines.append(
                f"• {item.name} — факт {_format_currency(item.actual_cost)} ₽ / {_format_hours(item.actual_hours)}"
            )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.work_sessions:
        lines.append("")
        lines.append("Рабочие сессии:")
        for session in sorted(request.work_sessions, key=lambda ws: ws.started_at):
            start = session.started_at.strftime("%d.%m %H:%M") if session.started_at else "—"
            finish = session.finished_at.strftime("%d.%m %H:%M") if session.finished_at else "—"
            lines.append(f"• {start} → {finish} | {_format_hours(session.hours_reported)}")
            if session.notes:
                lines.append(f"  → {session.notes}")

    lines.append("")
    lines.append("Совет: отправляйте геопозицию после нажатия «Начать работу» и перед завершением.")
    lines.append("Не забудьте приложить фотоотчёт с подписью формата `RQ-номер комментарий`.")
    return "\n".join(lines)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"
