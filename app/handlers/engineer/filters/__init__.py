"""Модуль фильтрации заявок инженером."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from app.infrastructure.db.session import async_session
from app.utils.request_filters import parse_date_range, quick_date_range
from app.handlers.engineer.utils import get_engineer, engineer_filter_menu_keyboard, engineer_filter_cancel_keyboard
from app.handlers.engineer.list import show_engineer_requests_list

router = Router()


class EngineerFilterStates(StatesGroup):
    """Состояния для фильтрации заявок инженером."""
    mode = State()
    value = State()


@router.message(F.text == "🔍 Фильтр заявок")
async def engineer_filter_start(message: Message, state: FSMContext):
    """Начало настройки фильтра заявок."""
    await state.set_state(EngineerFilterStates.mode)
    await message.answer(
        "🔍 <b>Фильтр заявок</b>\n\n"
        "Выберите способ фильтрации или быстрый период:",
        reply_markup=engineer_filter_menu_keyboard(),
        parse_mode="HTML",
    )


@router.message(StateFilter(EngineerFilterStates.mode))
async def engineer_filter_mode(message: Message, state: FSMContext):
    """Обработка выбора режима фильтрации через текстовый ввод."""
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Фильтр отменён.")
        return
    if text not in {"адрес", "дата"}:
        await message.answer("Введите «Адрес» или «Дата», либо нажмите «Отмена».")
        return
    await state.update_data(mode=text)
    await state.set_state(EngineerFilterStates.value)
    if text == "адрес":
        await message.answer(
            "Введите часть адреса (улица, дом и т.п.).",
            reply_markup=engineer_filter_cancel_keyboard(),
        )
    else:
        await message.answer(
            "Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.\n"
            "Можно одну дату (ДД.ММ.ГГГГ) — покажем заявки за этот день.",
            reply_markup=engineer_filter_cancel_keyboard(),
        )


@router.callback_query(F.data.startswith("eng:flt:mode:"))
async def engineer_filter_mode_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора режима фильтрации через callback."""
    mode = callback.data.split(":")[3]
    if mode == "address":
        await state.update_data(mode="адрес")
        await state.set_state(EngineerFilterStates.value)
        await callback.message.edit_text(
            "Введите часть адреса (улица, дом и т.п.).",
            reply_markup=engineer_filter_cancel_keyboard(),
        )
    elif mode == "date":
        await state.update_data(mode="дата")
        await state.set_state(EngineerFilterStates.value)
        await callback.message.edit_text(
            "Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.\n"
            "Можно одну дату (ДД.ММ.ГГГГ) — покажем заявки за этот день.",
            reply_markup=engineer_filter_cancel_keyboard(),
        )
    await callback.answer()


@router.callback_query(F.data.startswith("eng:flt:quick:"))
async def engineer_filter_quick(callback: CallbackQuery, state: FSMContext):
    """Быстрый выбор периода для фильтрации."""
    code = callback.data.split(":")[3]
    quick = quick_date_range(code)
    if not quick:
        await callback.answer("Неизвестный период.", show_alert=True)
        return
    start, end, label = quick
    filter_payload = {
        "mode": "дата",
        "start": start.isoformat(),
        "end": end.isoformat(),
        "value": "",
        "label": label,
    }
    await state.update_data(eng_filter=filter_payload)
    await state.set_state(None)

    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=0,
            context="filter",
            filter_payload=filter_payload,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "eng:flt:clear")
async def engineer_filter_clear(callback: CallbackQuery, state: FSMContext):
    """Очистка фильтра."""
    await state.update_data(eng_filter=None)
    await state.set_state(None)
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await show_engineer_requests_list(
            callback.message,
            session,
            engineer.id,
            page=0,
            context="list",
            edit=True,
        )
    await callback.answer("Фильтр сброшен.")


@router.callback_query(F.data == "eng:flt:cancel")
async def engineer_filter_cancel(callback: CallbackQuery, state: FSMContext):
    """Отмена настройки фильтра."""
    await state.set_state(None)
    await callback.message.edit_text("Фильтр отменён.")
    await callback.answer()


@router.message(StateFilter(EngineerFilterStates.value))
async def engineer_filter_apply(message: Message, state: FSMContext):
    """Применение фильтра с введенным значением."""
    data = await state.get_data()
    mode = data.get("mode")
    value = (message.text or "").strip()
    if value.lower() == "отмена":
        await state.clear()
        await message.answer("Фильтр отменён.")
        return

    async with async_session() as session:
        engineer = await get_engineer(session, message.from_user.id)
        if not engineer:
            await state.clear()
            await message.answer("Нет доступа.")
            return

        filter_payload: dict[str, str] = {"mode": mode or "", "value": value}
        if mode == "адрес":
            if not value:
                await message.answer("Адрес не может быть пустым. Введите часть адреса.")
                return
            filter_payload["value"] = value
        elif mode == "дата":
            start, end, error = parse_date_range(value)
            if error:
                await message.answer(error)
                return
            filter_payload["start"] = start.isoformat()
            filter_payload["end"] = end.isoformat()

        await state.update_data(eng_filter=filter_payload)
        await state.set_state(None)
        await show_engineer_requests_list(
            message,
            session,
            engineer.id,
            page=0,
            context="filter",
            filter_payload=filter_payload,
            edit=False,
        )


@router.callback_query(F.data.startswith("eng:flt:page:"))
async def engineer_filter_page(callback: CallbackQuery, state: FSMContext):
    """Пагинация для фильтрованных заявок."""
    try:
        page = int(callback.data.split(":")[3])
    except (ValueError, IndexError):
        page = 0
    data = await state.get_data()
    filter_payload = data.get("eng_filter")
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
            context="filter",
            filter_payload=filter_payload,
            edit=True,
        )
    await callback.answer()
