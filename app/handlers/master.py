from __future__ import annotations

import html
import logging
from dataclasses import dataclass

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InputMediaPhoto, InputMediaVideo, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, or_, select
from sqlalchemy.orm import selectinload

from app.handlers.common.work_fact_view import (
    build_category_keyboard,
    build_quantity_keyboard,
    decode_quantity,
    format_category_message,
    format_quantity_message,
)
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
from app.keyboards.master_kb import finish_photo_kb, master_kb
from app.services.request_service import RequestService
from app.services.work_catalog import get_work_catalog
from app.utils.pagination import clamp_page, total_pages_for
from app.utils.request_formatters import format_hours_minutes, format_request_label, STATUS_TITLES
from app.utils.timezone import format_moscow, now_moscow
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month

logger = logging.getLogger(__name__)

router = Router()
REQUESTS_PAGE_SIZE = 10


class MasterStates(StatesGroup):
    waiting_start_location = State()  # Ожидание геопозиции для начала работы
    finish_dashboard = State()  # Требования к завершению
    finish_photo_upload = State()  # Сбор фото готовой работы
    waiting_finish_location = State()  # Ожидание геопозиции для завершения работы
    schedule_date = State()  # Плановые выходы мастера
    quantity_input = State()  # Ввод количества вручную


FINISH_CONTEXT_KEY = "finish_context"
PHOTO_CONFIRM_TEXT = "✅ Подтвердить фото"
CANCEL_TEXT = "Отмена"
PHOTO_TYPES_FOR_FINISH = (PhotoType.PROCESS, PhotoType.AFTER)


async def _fetch_master_requests_page(
    session,
    master_id: int,
    page: int,
) -> tuple[list[Request], int, int, int]:
    conditions = [Request.master_id == master_id]
    total = await session.scalar(select(func.count()).select_from(Request).where(*conditions))
    total = int(total or 0)
    total_pages = total_pages_for(total, REQUESTS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    requests = (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.work_sessions),
                    selectinload(Request.photos),
                    selectinload(Request.engineer),
                )
                .where(*conditions)
                .order_by(Request.created_at.desc())
                .limit(REQUESTS_PAGE_SIZE)
                .offset(page * REQUESTS_PAGE_SIZE)
            )
        )
        .scalars()
        .all()
    )
    return requests, page, total_pages, total


async def _show_master_requests_list(
    message: Message,
    session,
    master_id: int,
    page: int,
    *,
    edit: bool = False,
) -> None:
    requests, page, total_pages, total = await _fetch_master_requests_page(session, master_id, page)

    if not requests:
        text = "У вас пока нет назначенных заявок. Ожидайте задач от инженера."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    builder = InlineKeyboardBuilder()
    start_index = page * REQUESTS_PAGE_SIZE
    list_lines = []
    for idx, req in enumerate(requests, start=start_index + 1):
        label = format_request_label(req)
        status_title = STATUS_TITLES.get(req.status, req.status.value)
        list_lines.append(f"{idx}. {html.escape(label)}\n<b>{html.escape(status_title)}</b>")
        builder.button(
            text=f"{idx}. {label} · {status_title}",
            callback_data=f"master:detail:{req.id}:{page}",
        )
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"master:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="master:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"master:list:{page + 1}"))
        builder.row(*nav)

    requests_list = "\n\n".join(list_lines)
    text = (
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт."
        f"\n\n{requests_list}"
        f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    )

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@router.message(F.text == "📥 Мои заявки")
async def master_requests(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Эта функция доступна только мастерам.")
            return

        await _show_master_requests_list(message, session, master.id, page=0)


@router.callback_query(F.data.startswith("master:list:"))
async def master_requests_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_master_requests_list(
            callback.message,
            session,
            master.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "master:noop")
async def master_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("master:detail:"))
async def master_request_detail(callback: CallbackQuery):
    parts = callback.data.split(":")
    request_id = int(parts[2])
    page = 0
    if len(parts) >= 4:
        try:
            page = int(parts[3])
        except ValueError:
            page = 0
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

    await _show_request_detail(callback.message, request, edit=True, list_page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("master:back"))
async def master_back_to_list(callback: CallbackQuery):
    parts = callback.data.split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_master_requests_list(
            callback.message,
            session,
            master.id,
            page=page,
            edit=True,
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
        await _notify_engineer(
            message.bot,
            request,
            text=(
                f"🔨 Мастер {master.full_name} начал работу по заявке {request_label}.\n"
                f"📍 Геопозиция: {_format_location_url(latitude, longitude)}"
            ),
            location=(latitude, longitude),
        )
    
    # Возвращаем основную клавиатуру
    await message.answer(
        "✅ Работа начата. Геопозиция сохранена.",
        reply_markup=master_kb,
    )
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


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
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
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
    await _render_finish_summary(callback.bot, finish_context, state)
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

    finish_context = await _load_finish_context(state)
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
    await _save_finish_context(state, finish_context)
    await state.set_state(MasterStates.finish_photo_upload)
    status_msg = await callback.message.answer(
        "Прикрепите все необходимые фото/видео выполненной работы.\n"
        "Можно отправить несколько фото/видео подряд.\n"
        "Когда закончите, нажмите «✅ Подтвердить фото». Для отмены отправьте «Отмена».",
        reply_markup=finish_photo_kb,
    )
    finish_context["status_message_id"] = status_msg.message_id
    await _save_finish_context(state, finish_context)
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

    finish_context = await _load_finish_context(state)
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
    finish_context = await _load_finish_context(state)
    if finish_context:
        await _cleanup_finish_summary(callback.bot, finish_context, "Процесс завершения отменён.")
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

    finish_context = await _load_finish_context(state)
    if not finish_context or finish_context.get("request_id") != request_id:
        await callback.answer("Процесс завершения не найден. Начните заново.", show_alert=True)
        return

    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        status = await _build_finish_status(session, request, finish_context)
        if not status.all_ready:
            await callback.answer("Выполните все условия перед завершением.", show_alert=True)
            await _render_finish_summary(callback.bot, finish_context, state)
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

        await _send_finish_report(callback.bot, request, master, status, finalized=finalize)

    master_text = (
        "Завершение работ зафиксировано и передано инженеру. Спасибо за оперативность."
        if finalize
        else "Смена закрыта. Инженер получил обновление, можно продолжить работы позже."
    )
    summary_text = "Работы успешно завершены." if finalize else "Смена зафиксирована."

    await callback.message.answer(master_text, reply_markup=master_kb)
    await _cleanup_finish_summary(callback.bot, finish_context, summary_text)
    await state.clear()
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
    await callback.answer("Готово.")


@router.message(StateFilter(MasterStates.waiting_finish_location), F.location)
async def master_finish_work_location(message: Message, state: FSMContext):
    """Обработка геопозиции завершения работы в мастере завершения."""
    finish_context = await _load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.")
        await state.clear()
        return

    latitude = message.location.latitude
    longitude = message.location.longitude

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа к заявке.")
            await state.clear()
            return

        request = await _load_request(session, master.id, finish_context["request_id"])
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
    await _save_finish_context(state, finish_context)
    await state.set_state(MasterStates.finish_dashboard)
    await message.answer("Геопозиция завершения сохранена.", reply_markup=master_kb)
    await _render_finish_summary(message.bot, finish_context, state)


@router.message(StateFilter(MasterStates.waiting_finish_location))
async def master_finish_location_fallback(message: Message, state: FSMContext):
    """Подсказки/отмена во время ожидания геопозиции."""
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.set_state(MasterStates.finish_dashboard)
        await message.answer("Ожидание геопозиции отменено.", reply_markup=master_kb)
        await _refresh_finish_summary_from_context(message.bot, state)
    else:
        await message.answer("Отправьте геопозицию или напишите «Отмена», чтобы вернуться назад.")


@router.callback_query(F.data.startswith("master:update_fact:"))
async def master_update_fact(callback: CallbackQuery):
    """Старт обновления факта: сразу показываем виды работ (материалы автоподсчёт)."""
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
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="m",
        request_id=request_id,
    )
    text = f"{header}\n\n{format_category_message(None, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("master:edit_materials:"))
async def master_edit_materials(callback: CallbackQuery):
    """Открывает каталог материалов для редактирования объёмов."""
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

    from app.services.material_catalog import get_material_catalog
    catalog = get_material_catalog()
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="mm",
        request_id=request_id,
        is_material=True,
    )
    text = f"{header}\n\n{format_category_message(None, is_material=True, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("master:close_materials:"))
async def master_close_materials(callback: CallbackQuery):
    """Закрывает сообщение со списком материалов."""
    try:
        await callback.message.delete()
    except Exception:
        await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer()


@router.callback_query(F.data.startswith("material:mm:"))
async def master_material_catalog(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога материалов для обновления факта мастером."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "mm":
        await callback.answer()
        return

    try:
        request_id = int(request_id_str)
    except ValueError:
        await callback.answer("Некорректный идентификатор заявки.", show_alert=True)
        return

    from app.services.material_catalog import get_material_catalog
    catalog = get_material_catalog()

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

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="mm",
                request_id=request_id,
                is_material=True,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, is_material=True, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            new_quantity = current_quantity or 0.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="mm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            work_item = await _get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="mm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return

            new_quantity = decode_quantity(quantity_code)
            await RequestService.update_actual_from_material_catalog(
                session,
                request,
                catalog_item=catalog_item,
                actual_quantity=new_quantity,
                author_id=master.id,
            )
            await session.commit()

            # Перезагружаем заявку для получения актуальных данных
            await session.refresh(request, ["work_items"])

            finish_context = await _load_finish_context(state)
            if finish_context and finish_context.get("request_id") == request_id:
                finish_context["fact_confirmed"] = True
                await _save_finish_context(state, finish_context)

            # Рассчитываем стоимость материала для отображения
            material_cost = round(catalog_item.price * new_quantity, 2)
            
            text = (
                f"{header}\n\n"
                f"📦 <b>{catalog_item.name}</b>\n"
                f"Объём: {new_quantity:.2f} {catalog_item.unit or 'шт'}\n"
                f"Цена за единицу: {catalog_item.price:,.2f} ₽\n"
                f"<b>Стоимость: {material_cost:,.2f} ₽</b>\n\n"
                f"✅ Материал сохранён. Стоимость пересчитана автоматически."
            ).replace(",", " ")
            
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="mm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}. Стоимость: {material_cost:,.2f} ₽")

            # Обновляем меню завершения в фоне, не закрывая меню каталога
            await _refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            return

        if action == "manual":
            if len(rest) < 1:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Материал не найден в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=True,
                quantity_page=page,
            )
            await state.set_state(MasterStates.quantity_input)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            return

    await callback.answer()


@router.message(StateFilter(MasterStates.quantity_input))
async def master_quantity_input(message: Message, state: FSMContext):
    """Обработка ручного ввода количества для мастера."""
    try:
        quantity = float(message.text.strip().replace(",", "."))
        if quantity < 0:
            await message.answer("Количество не может быть отрицательным. Введите положительное число.")
            return
    except ValueError:
        await message.answer("Неверный формат. Введите число (можно с десятичной частью, например: 2.5).")
        return
    
    data = await state.get_data()
    request_id = data.get("quantity_request_id")
    item_id = data.get("quantity_item_id")
    role_key = data.get("quantity_role_key")
    is_material = data.get("quantity_is_material", True)  # По умолчанию материал для обратной совместимости
    page = data.get("quantity_page")
    
    if not request_id or not item_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    # Используем правильный каталог в зависимости от типа
    if is_material:
        from app.services.material_catalog import get_material_catalog
        catalog = get_material_catalog()
    else:
        from app.services.work_catalog import get_work_catalog
        catalog = get_work_catalog()
    
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        item_type = "материал" if is_material else "работа"
        await message.answer(f"{item_type.capitalize()} не найден в каталоге.")
        await state.clear()
        return
    
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
        
        header = _catalog_header(request)
        work_item = await _get_work_item(session, request.id, catalog_item.name)
        current_quantity = (
            float(work_item.actual_quantity)
            if work_item and work_item.actual_quantity is not None
            else None
        )
        
        text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=quantity, current_quantity=current_quantity, is_material=is_material)}"
        markup = build_quantity_keyboard(
            catalog_item=catalog_item,
            role_key=role_key,
            request_id=request_id,
            new_quantity=quantity,
            is_material=is_material,
            page=page,
        )
        await message.answer(text, reply_markup=markup)
        await state.clear()


@router.callback_query(F.data.startswith("work:m:"))
async def master_work_catalog(callback: CallbackQuery, state: FSMContext):
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

        if action in {"browse", "back", "page"}:
            target = rest[0] if rest else "root"
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            category = None if target == "root" else catalog.get_category(target)
            if target != "root" and not category:
                await callback.answer("Категория недоступна.", show_alert=True)
                return

            markup, page, total_pages = build_category_keyboard(
                catalog=catalog,
                category=category,
                role_key="m",
                request_id=request_id,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, page=page, total_pages=total_pages)}"
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "item":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
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
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "qty":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
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
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer()
            return

        if action == "manual":
            if not rest:
                await callback.answer()
                return
            item_id = rest[0]
            page = 0
            if len(rest) > 1:
                try:
                    page = int(rest[1])
                except ValueError:
                    page = 0
            catalog_item = catalog.get_item(item_id)
            if not catalog_item:
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
                quantity_page=page,
            )
            await state.set_state(MasterStates.quantity_input)
            unit = catalog_item.unit or "шт"
            await callback.message.answer(
                f"Введите количество вручную (единица измерения: {unit}).\n"
                "Можно использовать десятичные числа, например: 2.5 или 10.75"
            )
            await callback.answer()
            return

        if action == "save":
            if len(rest) < 2:
                await callback.answer()
                return
            item_id, quantity_code = rest[:2]
            page = 0
            if len(rest) > 2:
                try:
                    page = int(rest[2])
                except ValueError:
                    page = 0
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

            # Перезагружаем заявку для получения актуальных данных о материалах
            await session.refresh(request, ["work_items"])
            
            finish_context = await _load_finish_context(state)
            if finish_context and finish_context.get("request_id") == request_id:
                finish_context["fact_confirmed"] = True
                await _save_finish_context(state, finish_context)

            # Обновляем сообщение с количеством, показывая что сохранено
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
                page=page,
            )
            await _update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")

            # Показываем список автоматически рассчитанных материалов
            await _show_materials_after_work_save(
                callback.bot,
                callback.message.chat.id,
                request,
                request_id,
            )

            # Обновляем меню завершения в фоне, не закрывая меню каталога
            await _refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await _refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await _refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
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


@router.message(StateFilter(MasterStates.finish_photo_upload), F.photo)
async def master_finish_photo_collect(message: Message, state: FSMContext):
    """Собирает фото, отправленные во время мастера завершения."""
    finish_context = await _load_finish_context(state)
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
    await _save_finish_context(state, finish_context)
    
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
    finish_context = await _load_finish_context(state)
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
    await _save_finish_context(state, finish_context)
    
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
    finish_context = await _load_finish_context(state)
    if not finish_context:
        await message.answer("Процесс завершения не найден. Нажмите «Завершить работу» ещё раз.", reply_markup=master_kb)
        await state.clear()
        return

    if lower_text == CANCEL_TEXT.lower():
        await state.set_state(MasterStates.finish_dashboard)
        await message.answer("Загрузка фото отменена.", reply_markup=master_kb)
        await _refresh_finish_summary_from_context(message.bot, state)
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
            master = await _get_master(session, message.from_user.id)
            if not master:
                await message.answer("Нет доступа к заявке.", reply_markup=master_kb)
                await state.clear()
                return
            
            request = await _load_request(session, master.id, request_id)
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
        await _save_finish_context(state, finish_context)
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
        await _render_finish_summary(message.bot, finish_context, state)
        return

    await message.answer(
        "Прикрепите фото или нажмите «✅ Подтвердить фото», когда закончите. Для отмены отправьте «Отмена».",
        reply_markup=finish_photo_kb,
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
                request = await _get_request_for_master(session, master.id, number_hint)
                if not request and number_hint[3:].isdigit():
                    alt = number_hint[3:]
                    logger.debug("Master photo: caption lookup failed, trying alt=%s", alt)
                    request = await _get_request_for_master(session, master.id, alt)

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
                request = await _get_request_for_master(session, master.id, number_hint)
                if not request and number_hint.isdigit():
                    alt = f"RQ-{number_hint}"
                    request = await _get_request_for_master(session, master.id, alt)

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
                request = await _load_request(session, master.id, active_session.request_id)
                logger.debug("Master photo: using active session request_id=%s", active_session.request_id)

        # 4. Fallback to most recent assigned/in-progress request
        if not request:
            request = await session.scalar(
                select(Request)
                .options(selectinload(Request.engineer))
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

    label = format_request_label(request)
    await message.answer(f"Фото добавлено к заявке {label}.")
    await _notify_engineer(
        message.bot,
        request,
        text=f"📸 Мастер {master.full_name} добавил фото к заявке {label}.",
    )


@router.message(F.location)
async def master_location(message: Message, state: FSMContext):
    """Обработка геопозиции в обычном режиме (не в процессе начала/завершения работы)."""
    # Пропускаем обработку, если мастер находится в специальных состояниях
    # (специфичные обработчики для этих состояний обработают геопозицию)
    current_state = await state.get_state()
    if current_state:
        state_str = str(current_state)
        if (
            "waiting_start_location" in state_str
            or "waiting_finish_location" in state_str
        ):
            # Геопозиция будет обработана специфичными обработчиками для этих состояний
            return
    
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
            request = await _load_request(session, master.id, work_session.request_id)
            if request:
                label = format_request_label(request)
                await _notify_engineer(
                    message.bot,
                    request,
                    text=(
                        f"📍 Мастер {master.full_name} обновил геопозицию старта по заявке {label}: "
                        f"{_format_location_url(message.location.latitude, message.location.longitude)}"
                    ),
                    location=(message.location.latitude, message.location.longitude),
                )
            await message.answer("Геопозиция старта работ сохранена.", reply_markup=master_kb)
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
            request = await _load_request(session, master.id, last_session.request_id)
            if request:
                label = format_request_label(request)
                await _notify_engineer(
                    message.bot,
                    request,
                    text=(
                        f"📍 Мастер {master.full_name} обновил геопозицию завершения по заявке {label}: "
                        f"{_format_location_url(message.location.latitude, message.location.longitude)}"
                    ),
                    location=(message.location.latitude, message.location.longitude),
                )
            await message.answer("Геопозиция завершения работ сохранена.", reply_markup=master_kb)
            return


# --- служебные функции ---


@dataclass
class FinishStatus:
    request_id: int
    request_number: str
    request_title: str
    photos_confirmed: bool
    photos_total: int
    location_ready: bool
    fact_ready: bool
    finish_location: tuple[float | None, float | None]

    @property
    def all_ready(self) -> bool:
        return self.photos_confirmed and self.location_ready and self.fact_ready

    def missing_items(self) -> list[str]:
        items: list[str] = []
        if not self.photos_confirmed:
            items.append("отправьте фото готовой работы")
        if not self.location_ready:
            items.append("передайте геопозицию завершения")
        if not self.fact_ready:
            items.append("заполните факт выполненных работ")
        return items


async def _load_finish_context(state: FSMContext) -> dict | None:
    data = await state.get_data()
    context = data.get(FINISH_CONTEXT_KEY)
    if isinstance(context, dict):
        return context
    return None


async def _save_finish_context(state: FSMContext, context: dict | None) -> None:
    await state.update_data({FINISH_CONTEXT_KEY: context})


async def _build_finish_status(
    session,
    request: Request,
    finish_context: dict,
) -> FinishStatus:
    photo_total = int(finish_context.get("new_photo_count") or 0)
    has_fact = bool(
        await session.scalar(
            select(func.count(WorkItem.id)).where(
                WorkItem.request_id == request.id,
                or_(
                    func.coalesce(WorkItem.actual_quantity, 0) > 0,
                    func.coalesce(WorkItem.actual_cost, 0) > 0,
                ),
            )
        )
    )
    fact_ready = has_fact and bool(finish_context.get("fact_confirmed"))
    latitude = finish_context.get("finish_latitude")
    longitude = finish_context.get("finish_longitude")
    return FinishStatus(
        request_id=request.id,
        request_number=format_request_label(request),
        request_title=request.title,
        photos_confirmed=bool(finish_context.get("photos_confirmed")),
        photos_total=photo_total,
        location_ready=latitude is not None and longitude is not None,
        fact_ready=fact_ready,
        finish_location=(latitude, longitude),
    )


def _format_finish_summary(request: Request, status: FinishStatus) -> str:
    lines = [
        f"🧾 <b>{status.request_number}</b> · {request.title}",
        "",
        "Чтобы завершить работы, выполните условия:",
        _format_finish_line("Фото готовой работы", status.photos_confirmed, extra=f"{status.photos_total} шт."),
        _format_finish_line("Геопозиция завершения", status.location_ready),
        _format_finish_line("Факт выполненных работ", status.fact_ready),
    ]
    lines.append("")
    if status.all_ready:
        lines.append("Все условия выполнены — закройте смену или завершите заявку.")
    else:
        lines.append("После выполнения каждого шага кнопка исчезнет из списка.")
    return "\n".join(lines)


def _format_finish_line(label: str, ready: bool, *, extra: str | None = None) -> str:
    prefix = "✅" if ready else "▫️"
    text = f"{prefix} {label}"
    if extra:
        text = f"{text} · {extra}"
    return text


def _finish_summary_keyboard(status: FinishStatus):
    builder = InlineKeyboardBuilder()
    request_id = status.request_id
    if not status.photos_confirmed:
        builder.button(text="📷 Отправить фото", callback_data=f"master:finish_photo:{request_id}")
    if not status.location_ready:
        builder.button(text="📍 Отправить геопозицию", callback_data=f"master:finish_geo:{request_id}")
    if not status.fact_ready:
        builder.button(text="📊 Заполнить факт", callback_data=f"master:update_fact:{request_id}")
    if status.all_ready:
        builder.button(
            text="⏸ Закрыть смену",
            callback_data=f"master:finish_submit:{request_id}:session",
        )
        builder.button(
            text="🏁 Завершить полностью",
            callback_data=f"master:finish_submit:{request_id}:final",
        )
    builder.button(text="❌ Отменить", callback_data="master:finish_cancel")
    builder.adjust(1)
    return builder.as_markup()


async def _render_finish_summary(bot, finish_context: dict, state: FSMContext) -> None:
    if not bot or not finish_context:
        return

    chat_id = finish_context.get("chat_id")
    if not chat_id:
        return

    async with async_session() as session:
        request = await session.scalar(
            select(Request)
            .options(selectinload(Request.engineer))
            .where(Request.id == finish_context["request_id"])
        )
        if not request:
            await _save_finish_context(state, None)
            return
        status = await _build_finish_status(session, request, finish_context)

    text = _format_finish_summary(request, status)
    keyboard = _finish_summary_keyboard(status)
    message_id = finish_context.get("message_id")

    if message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except TelegramBadRequest as exc:
            error_text = str(exc).lower()
            if "message to delete not found" in error_text or "message can't be deleted" in error_text:
                pass
            else:
                raise
        except Exception as exc:  # pragma: no cover
            logger.warning("Failed to delete previous finish summary: %s", exc)

    try:
        sent = await bot.send_message(chat_id, text, reply_markup=keyboard)
        finish_context["message_id"] = sent.message_id
    except Exception as exc:  # pragma: no cover - сеть/telegram
        logger.warning("Failed to render finish summary: %s", exc)
    finally:
        finish_context["photos_confirmed"] = status.photos_confirmed
        await _save_finish_context(state, finish_context)


async def _refresh_finish_summary_from_context(
    bot,
    state: FSMContext,
    *,
    request_id: int | None = None,
) -> None:
    finish_context = await _load_finish_context(state)
    if not finish_context:
        return
    if request_id and finish_context.get("request_id") != request_id:
        return
    await _render_finish_summary(bot, finish_context, state)


async def _cleanup_finish_summary(bot, finish_context: dict | None, final_text: str) -> None:
    if not bot or not finish_context:
        return
    message_id = finish_context.get("message_id")
    chat_id = finish_context.get("chat_id")
    if not message_id or not chat_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramBadRequest:
        pass
    except Exception:
        pass
    try:
        await bot.send_message(chat_id, final_text)
    except Exception:
        pass


async def _send_finish_report(
    bot,
    request: Request,
    master: User,
    status: FinishStatus,
    *,
    finalized: bool,
) -> None:
    if not bot or not request.engineer or not request.engineer.telegram_id:
        return

    async with async_session() as session:
        photos = (
            await session.execute(
                select(Photo)
                .where(
                    Photo.request_id == request.id,
                    Photo.type.in_(PHOTO_TYPES_FOR_FINISH),
                )
                .order_by(Photo.created_at.asc())
            )
        ).scalars().all()

    verb = "завершил работы" if finalized else "завершил смену"
    label = format_request_label(request)
    caption_lines = [
        f"✅ Мастер {master.full_name} {verb} по заявке {label}.",
    ]
    if not finalized:
        caption_lines.append("Статус заявки остаётся «В работе».")
    caption_lines.append(f"📷 Фотоотчёт: {len(photos)} шт." if photos else "Фотоотчёт отсутствует.")
    if status.location_ready and status.finish_location[0] is not None and status.finish_location[1] is not None:
        lat, lon = status.finish_location
        caption_lines.append(f"📍 {_format_location_url(lat, lon)}")
    caption_text = "\n".join(caption_lines)

    try:
        if photos:
            media: list[InputMediaPhoto] = []
            for idx, photo in enumerate(photos):
                caption = caption_text if idx == 0 else None
                media.append(InputMediaPhoto(media=photo.file_id, caption=caption))
            await bot.send_media_group(request.engineer.telegram_id, media)
        else:
            await bot.send_message(request.engineer.telegram_id, caption_text)
    except Exception as exc:  # pragma: no cover - зависит от Telegram API
        logger.warning("Failed to send finish report to engineer for request %s: %s", request.number, exc)


async def _send_defect_photos(message: Message, photos: list[Photo]) -> None:
    """Отправка фото дефектов (старая версия, для совместимости)."""
    before_photos = [photo for photo in photos if photo.type == PhotoType.BEFORE]
    if not before_photos:
        return

    chunk: list[InputMediaPhoto] = []
    for _idx, photo in enumerate(before_photos):
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


async def _get_request_for_master(session, master_id: int, number: str) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(selectinload(Request.engineer))
        .where(Request.number == number, Request.master_id == master_id)
    )


def _catalog_header(request: Request) -> str:
    return f"Заявка {format_request_label(request)} · {request.title}"


async def _get_master(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.MASTER)
    )


async def _notify_engineer(
    bot,
    request: Request | None,
    text: str,
    *,
    location: tuple[float, float] | None = None,
) -> None:
    if not bot or not request or not request.engineer or not request.engineer.telegram_id:
        return
    try:
        await bot.send_message(request.engineer.telegram_id, text)
        if location:
            lat, lon = location
            await bot.send_location(request.engineer.telegram_id, latitude=lat, longitude=lon)
    except Exception as exc:
        logger.warning("Failed to notify engineer for request %s: %s", request.number, exc)


def _format_location_url(latitude: float, longitude: float) -> str:
    return f"https://www.google.com/maps?q={latitude},{longitude}"


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
                    selectinload(Request.engineer),
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
            selectinload(Request.engineer),
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


async def _show_request_detail(
    message: Message,
    request: Request,
    *,
    edit: bool = False,
    list_page: int = 0,
) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id, request, list_page=list_page)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(
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


@router.callback_query(F.data.startswith("master:work_started:"))
async def master_work_started_info(callback: CallbackQuery):
    """Информация о том, что работа уже начата."""
    await callback.answer("Работа уже начата. Используйте кнопку «Завершить работу» для завершения.", show_alert=True)


@router.callback_query(F.data.startswith("master:location_hint:"))
async def master_location_hint(callback: CallbackQuery):
    await callback.message.answer(
        "Чтобы отправить геопозицию, нажмите кнопку «📍 Отправить геопозицию» на клавиатуре ниже.",
        reply_markup=master_kb,
    )
    await callback.answer()


@router.callback_query(F.data.startswith("master:schedule:"))
async def master_schedule(callback: CallbackQuery, state: FSMContext):
    """Запуск выбора планового выхода мастера по заявке."""
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

    await state.set_state(MasterStates.schedule_date)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Выберите дату вашего выхода на объект.\n"
        "Используйте календарь ниже.",
        reply_markup=build_calendar(prefix="master_schedule"),
    )
    await callback.answer()


@router.callback_query(
    StateFilter(MasterStates.schedule_date),
    F.data.startswith("cal:master_schedule:"),
)
async def master_schedule_calendar(callback: CallbackQuery, state: FSMContext):
    """Обработка нажатий по календарю мастера."""
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar("master_schedule", year=new_year, month=new_month),
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        data = await state.get_data()
        request_id = data.get("request_id")
        if not request_id:
            await state.clear()
            await callback.answer("Не удалось определить заявку.", show_alert=True)
            return

        selected_date = f"{payload.day:02d}.{payload.month:02d}.{payload.year}"

        async with async_session() as session:
            master = await _get_master(session, callback.from_user.id)
            if not master:
                await state.clear()
                await callback.answer("Нет доступа.", show_alert=True)
                return

            request = await _load_request(session, master.id, request_id)
            if not request:
                await state.clear()
                await callback.answer("Заявка не найдена.", show_alert=True)
                return

            label = format_request_label(request)

        # Убираем календарь
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        await state.clear()

        # Сообщение мастеру
        await callback.message.answer(
            f"Плановый выход на объект по заявке {label} назначен на {selected_date}."
        )

        # Уведомляем инженера, если есть
        if request.engineer and request.engineer.telegram_id:
            try:
                await callback.message.bot.send_message(
                    chat_id=int(request.engineer.telegram_id),
                    text=(
                        f"🗓 Мастер {master.full_name} запланировал выход на объект по заявке {label} "
                        f"на {selected_date}."
                    ),
                )
            except Exception:
                pass

        await callback.answer()


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    due_text = format_moscow(request.due_at) or "не задан"
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    defects_photos = sum(1 for photo in (request.photos or []) if photo.type == PhotoType.BEFORE)
    
    # Рассчитываем разбивку стоимостей
    cost_breakdown = _calculate_cost_breakdown(request.work_items or [])

    label = format_request_label(request)
    lines = [
        f"🧾 <b>{label}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Срок устранения: {due_text}",
        f"Адрес: {request.address}",
        f"Контактное лицо: {request.contact_person or '—'}",
        f"Телефон: {request.contact_phone or '—'}",
        "",
        f"Плановая стоимость видов работ: {_format_currency(cost_breakdown['planned_work_cost'])} ₽",
        f"Плановая стоимость материалов: {_format_currency(cost_breakdown['planned_material_cost'])} ₽",
        f"Плановая общая стоимость: {_format_currency(cost_breakdown['planned_total_cost'])} ₽",
        f"Фактическая стоимость видов работ: {_format_currency(cost_breakdown['actual_work_cost'])} ₽",
        f"Фактическая стоимость материалов: {_format_currency(cost_breakdown['actual_material_cost'])} ₽",
        f"Фактическая общая стоимость: {_format_currency(cost_breakdown['actual_total_cost'])} ₽",
        f"Плановые часы: {format_hours_minutes(planned_hours)}",
        f"Фактические часы: {format_hours_minutes(actual_hours)}",
    ]

    if defects_photos:
        lines.append(f"Фото дефектов: {defects_photos} (будут показаны перед стартом работ)")
    else:
        lines.append("Фото дефектов: пока нет, запросите у инженера.")

    if request.work_items:
        lines.append("")
        lines.append("Позиции бюджета (план / факт):")
        for item in request.work_items:
            is_material = bool(
                item.planned_material_cost
                or item.actual_material_cost
                or ("материал" in (item.category or "").lower())
            )
            emoji = "📦" if is_material else "🛠"
            planned_cost = item.planned_cost
            actual_cost = item.actual_cost
            if planned_cost in (None, 0):
                planned_cost = item.planned_material_cost
            if actual_cost in (None, 0):
                actual_cost = item.actual_material_cost
            unit = item.unit or ""
            qty_part = ""
            if item.planned_quantity is not None or item.actual_quantity is not None:
                pq = item.planned_quantity if item.planned_quantity is not None else 0
                aq = item.actual_quantity if item.actual_quantity is not None else 0
                qty_part = f" | объём: {pq:.2f} → {aq:.2f} {unit}".rstrip()
            lines.append(
                f"{emoji} {item.name} — план {_format_currency(planned_cost)} ₽ / "
                f"факт {_format_currency(actual_cost)} ₽{qty_part}"
            )
            if item.actual_hours is not None:
                lines.append(
                    f"  Часы: {format_hours_minutes(item.planned_hours)} → {format_hours_minutes(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.work_sessions:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        for session in sorted(request.work_sessions, key=lambda ws: ws.started_at):
            start = format_moscow(session.started_at, "%d.%m %H:%M") or "—"
            finish = format_moscow(session.finished_at, "%d.%m %H:%M") if session.finished_at else "в работе"
            duration_h = (
                float(session.hours_reported)
                if session.hours_reported is not None
                else (float(session.hours_calculated) if session.hours_calculated is not None else None)
            )
            if duration_h is None and session.started_at and session.finished_at:
                delta = session.finished_at - session.started_at
                duration_h = delta.total_seconds() / 3600
            duration_str = format_hours_minutes(duration_h) if duration_h is not None else "—"
            lines.append(f"• {start} — {finish} · {duration_str}")
            if session.notes:
                lines.append(f"  → {session.notes}")
    elif (request.actual_hours or 0) > 0:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        lines.append(f"• Суммарно: {format_hours_minutes(float(request.actual_hours or 0))} (учёт до внедрения сессий)")

    lines.append("")
    lines.append("Совет: отправляйте геопозицию после нажатия «Начать работу» и перед завершением.")
    lines.append("Не забудьте приложить фотоотчёт с подписью формата `RQ-номер комментарий`.")
    return "\n".join(lines)


def _calculate_cost_breakdown(work_items) -> dict[str, float]:
    """Рассчитывает разбивку стоимостей по работам и материалам."""
    planned_work_cost = 0.0
    planned_material_cost = 0.0
    actual_work_cost = 0.0
    actual_material_cost = 0.0
    
    for item in work_items:
        # Плановая стоимость работ
        if item.planned_cost is not None:
            planned_work_cost += float(item.planned_cost)
        
        # Плановая стоимость материалов
        if item.planned_material_cost is not None:
            planned_material_cost += float(item.planned_material_cost)
        
        # Фактическая стоимость работ
        if item.actual_cost is not None:
            actual_work_cost += float(item.actual_cost)
        
        # Фактическая стоимость материалов
        if item.actual_material_cost is not None:
            actual_material_cost += float(item.actual_material_cost)
    
    return {
        "planned_work_cost": planned_work_cost,
        "planned_material_cost": planned_material_cost,
        "planned_total_cost": planned_work_cost + planned_material_cost,
        "actual_work_cost": actual_work_cost,
        "actual_material_cost": actual_material_cost,
        "actual_total_cost": actual_work_cost + actual_material_cost,
    }


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    return format_hours_minutes(value)


async def _show_materials_after_work_save(
    bot,
    chat_id: int,
    request: Request,
    request_id: int,
) -> None:
    """Показывает мастеру список автоматически рассчитанных материалов после сохранения работы."""
    from app.services.material_catalog import get_material_catalog
    
    # Получаем материалы, которые были автоматически рассчитаны
    # Материал определяется по наличию actual_material_cost или по категории, содержащей "материал"
    material_items = [
        item for item in (request.work_items or [])
        if (
            (item.actual_material_cost is not None and item.actual_material_cost > 0)
            or (item.actual_quantity is not None and item.actual_quantity > 0 
                and ("материал" in (item.category or "").lower() or item.planned_material_cost is not None))
        )
        and item.actual_cost is None  # Исключаем работы (у них actual_cost)
    ]
    
    if not material_items:
        # Если материалов нет, не показываем сообщение
        return
    
    material_catalog = get_material_catalog()
    header = _catalog_header(request)
    
    lines = [
        f"{header}",
        "",
        "📦 <b>Автоматически рассчитанные материалы:</b>",
        "",
    ]
    
    total_material_cost = 0.0
    for item in material_items:
        quantity = item.actual_quantity or 0.0
        # Используем actual_material_cost, если есть, иначе рассчитываем из цены каталога
        cost = item.actual_material_cost
        if cost is None or cost == 0:
            # Пытаемся найти материал в каталоге для получения цены
            catalog_item = material_catalog.find_item_by_name(item.name)
            if catalog_item and quantity > 0:
                cost = round(catalog_item.price * quantity, 2)
            else:
                cost = 0.0
        
        unit = item.unit or "шт"
        total_material_cost += cost
        price_per_unit = cost / quantity if quantity > 0 else 0.0
        lines.append(
            f"📦 <b>{item.name}</b>\n"
            f"   Объём: {quantity:.2f} {unit}\n"
            f"   Цена за единицу: {_format_currency(price_per_unit)} ₽\n"
            f"   Стоимость: {_format_currency(cost)} ₽"
        )
    
    lines.append("")
    lines.append(f"<b>Итого по материалам: {_format_currency(total_material_cost)} ₽</b>")
    lines.append("")
    lines.append("Вы можете изменить объём каждого материала, нажав кнопку ниже.")
    
    text = "\n".join(lines)
    
    builder = InlineKeyboardBuilder()
    builder.button(
        text="✏️ Редактировать материалы",
        callback_data=f"master:edit_materials:{request_id}",
    )
    builder.button(
        text="✖️ Закрыть",
        callback_data=f"master:close_materials:{request_id}",
    )
    builder.adjust(1)
    
    try:
        await bot.send_message(chat_id, text, reply_markup=builder.as_markup())
    except Exception as exc:
        logger.warning("Failed to show materials list: %s", exc)
