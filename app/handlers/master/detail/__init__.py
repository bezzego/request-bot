"""Модуль деталей заявки мастера."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from app.infrastructure.db.models import PhotoType
from app.infrastructure.db.session import async_session
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month
from app.keyboards.master_kb import master_kb
from app.handlers.master.utils import get_master, load_request
from app.handlers.master.list import show_master_requests_list
from app.handlers.master.detail.formatters import format_request_detail
from app.handlers.master.detail.keyboards import build_detail_keyboard
from app.handlers.master.detail.photos import send_defect_photos_with_start_button
from app.handlers.master.states import MasterStates

router = Router()


@router.callback_query(F.data.startswith("master:detail:"))
async def master_request_detail(callback: CallbackQuery):
    """Обработчик просмотра деталей заявки."""
    parts = callback.data.split(":")
    request_id = int(parts[2])
    page = 0
    if len(parts) >= 4:
        try:
            page = int(parts[3])
        except ValueError:
            page = 0
    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    await show_request_detail(callback.message, request, edit=True, list_page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("master:back"))
async def master_back_to_list(callback: CallbackQuery):
    """Обработчик возврата к списку заявок."""
    parts = callback.data.split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await show_master_requests_list(
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
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await load_request(session, master.id, request_id)
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
    await send_defect_photos_with_start_button(callback.message, before_photos, request_id)
    await callback.answer()


@router.callback_query(F.data.startswith("master:work_started:"))
async def master_work_started_info(callback: CallbackQuery):
    """Информация о том, что работа уже начата."""
    await callback.answer("Работа уже начата. Используйте кнопку «Завершить работу» для завершения.", show_alert=True)


@router.callback_query(F.data.startswith("master:location_hint:"))
async def master_location_hint(callback: CallbackQuery):
    """Подсказка по отправке геопозиции."""
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
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        request = await load_request(session, master.id, request_id)
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
            master = await get_master(session, callback.from_user.id)
            if not master:
                await state.clear()
                await callback.answer("Нет доступа.", show_alert=True)
                return

            request = await load_request(session, master.id, request_id)
            if not request:
                await state.clear()
                await callback.answer("Заявка не найдена.", show_alert=True)
                return

            from app.utils.request_formatters import format_request_label
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


async def show_request_detail(
    message: Message,
    request,
    *,
    edit: bool = False,
    list_page: int = 0,
) -> None:
    """Показать детали заявки."""
    text = format_request_detail(request)
    keyboard = build_detail_keyboard(request.id, request, list_page=list_page)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


async def refresh_request_detail(bot, chat_id: int, master_telegram_id: int, request_id: int) -> None:
    """Обновить детали заявки через бота."""
    async with async_session() as session:
        master = await get_master(session, master_telegram_id)
        if not master:
            return
        request = await load_request(session, master.id, request_id)

    if not request or not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=format_request_detail(request),
            reply_markup=build_detail_keyboard(request.id, request),
        )
    except Exception:
        pass
