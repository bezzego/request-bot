"""Модуль управления бюджетом заявок инженером."""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message

from app.handlers.common.work_fact_view import (
    build_category_keyboard,
    build_quantity_keyboard,
    decode_quantity,
    format_category_message,
    format_quantity_message,
)
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService
from app.services.work_catalog import get_work_catalog
from app.handlers.engineer.utils import get_engineer
from app.handlers.engineer.detail import load_request, refresh_request_detail
from app.handlers.engineer.budget.utils import (
    get_work_item,
    catalog_header,
    update_catalog_message,
)

router = Router()


class EngineerBudgetStates(StatesGroup):
    """Состояния для управления бюджетом инженером."""
    quantity_input_plan = State()  # Ввод количества для плана
    quantity_input_fact = State()  # Ввод количества для факта


# ========== ПЛАНОВЫЕ ПОЗИЦИИ ==========


@router.callback_query(F.data.startswith("eng:add_plan:"))
async def engineer_add_plan(callback: CallbackQuery):
    """Старт добавления плана: сразу показываем виды работ (материалы автоподсчёт)."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

    catalog = get_work_catalog()
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="ep",
        request_id=request_id,
    )
    text = f"{header}\n\n{format_category_message(None, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:ep:"))
async def engineer_work_catalog_plan(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога работ для добавления в план инженером."""
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

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
                role_key="ep",
                request_id=request_id,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, page=page, total_pages=total_pages)}"
            await update_catalog_message(callback.message, text, markup)
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

            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            await RequestService.add_plan_from_catalog(
                session,
                request,
                catalog_item=catalog_item,
                planned_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
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
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
                quantity_page=page,
            )
            await state.set_state(EngineerBudgetStates.quantity_input_plan)
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
            if callback.bot:
                await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
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


@router.callback_query(F.data.startswith("material:epm:"))
async def engineer_material_catalog_plan(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога материалов для добавления в план инженером."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "epm":
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

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
                role_key="epm",
                request_id=request_id,
                is_material=True,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, is_material=True, page=page, total_pages=total_pages)}"
            await update_catalog_message(callback.message, text, markup)
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

            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            new_quantity = current_quantity or 1.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            await RequestService.add_plan_from_material_catalog(
                session,
                request,
                catalog_item=catalog_item,
                planned_quantity=new_quantity,
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.planned_quantity)
                if work_item and work_item.planned_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="epm",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
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
            await state.set_state(EngineerBudgetStates.quantity_input_plan)
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
            if callback.bot:
                await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
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


# ========== ОБНОВЛЕНИЕ ФАКТА ==========


@router.callback_query(F.data.startswith("eng:update_fact:"))
async def engineer_update_fact(callback: CallbackQuery):
    """Старт обновления факта: сразу показываем виды работ (материалы автоподсчёт)."""
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

    catalog = get_work_catalog()
    markup, page, total_pages = build_category_keyboard(
        catalog=catalog,
        category=None,
        role_key="e",
        request_id=request_id,
    )
    text = f"{header}\n\n{format_category_message(None, page=page, total_pages=total_pages)}"
    await callback.message.answer(text, reply_markup=markup)
    await callback.answer()


@router.callback_query(F.data.startswith("work:e:"))
async def engineer_work_catalog_fact(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога работ для обновления факта инженером."""
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

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
                role_key="e",
                request_id=request_id,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, page=page, total_pages=total_pages)}"
            await update_catalog_message(callback.message, text, markup)
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

            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await get_work_item(session, request.id, catalog_item.name)
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
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
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
                await callback.answer("Работа не найдена в каталоге.", show_alert=True)
                return
            
            await state.update_data(
                quantity_request_id=request_id,
                quantity_item_id=item_id,
                quantity_role_key=role_key,
                quantity_is_material=False,
                quantity_page=page,
            )
            await state.set_state(EngineerBudgetStates.quantity_input_fact)
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
            if callback.bot:
                await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
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


@router.callback_query(F.data.startswith("material:em:"))
async def engineer_material_catalog_fact(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога материалов для обновления факта инженером."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer()
        return

    _, role_key, request_id_str, action, *rest = parts
    if role_key != "em":
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
        engineer = await get_engineer(session, callback.from_user.id)
        if not engineer:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, engineer.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

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
                role_key="em",
                request_id=request_id,
                is_material=True,
                page=page,
            )
            text = f"{header}\n\n{format_category_message(category, is_material=True, page=page, total_pages=total_pages)}"
            await update_catalog_message(callback.message, text, markup)
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

            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            new_quantity = current_quantity or 0.0

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )

            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
                author_id=engineer.id,
            )
            await session.commit()

            # Обновляем сообщение с количеством, показывая что сохранено
            work_item = await get_work_item(session, request.id, catalog_item.name)
            current_quantity = (
                float(work_item.actual_quantity)
                if work_item and work_item.actual_quantity is not None
                else None
            )
            text = f"{header}\n\n{format_quantity_message(catalog_item=catalog_item, new_quantity=new_quantity, current_quantity=current_quantity, is_material=True)}"
            markup = build_quantity_keyboard(
                catalog_item=catalog_item,
                role_key="em",
                request_id=request_id,
                new_quantity=new_quantity,
                is_material=True,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")
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
            await state.set_state(EngineerBudgetStates.quantity_input_fact)
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
            if callback.bot:
                await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
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


# ========== РУЧНОЙ ВВОД КОЛИЧЕСТВА ==========


@router.message(StateFilter(EngineerBudgetStates.quantity_input_plan))
async def engineer_quantity_input_plan(message: Message, state: FSMContext):
    """Обработка ручного ввода количества для плана."""
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
    is_material = data.get("quantity_is_material", False)
    page = data.get("quantity_page")
    
    if not request_id or not item_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    from app.services.work_catalog import get_work_catalog
    from app.services.material_catalog import get_material_catalog
    
    catalog = get_material_catalog() if is_material else get_work_catalog()
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        await message.answer("Элемент каталога не найден.")
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
        
        header = catalog_header(request)
        work_item = await get_work_item(session, request.id, catalog_item.name)
        current_quantity = (
            float(work_item.planned_quantity)
            if work_item and work_item.planned_quantity is not None
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


@router.message(StateFilter(EngineerBudgetStates.quantity_input_fact))
async def engineer_quantity_input_fact(message: Message, state: FSMContext):
    """Обработка ручного ввода количества для факта."""
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
    is_material = data.get("quantity_is_material", False)
    page = data.get("quantity_page")
    
    if not request_id or not item_id:
        await message.answer("Ошибка. Начните процесс заново.")
        await state.clear()
        return
    
    from app.services.work_catalog import get_work_catalog
    from app.services.material_catalog import get_material_catalog
    
    catalog = get_material_catalog() if is_material else get_work_catalog()
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        await message.answer("Элемент каталога не найден.")
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
        
        header = catalog_header(request)
        work_item = await get_work_item(session, request.id, catalog_item.name)
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
