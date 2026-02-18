"""Модуль работы с материалами и обновления факта мастера."""
from __future__ import annotations

import logging

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy.orm import selectinload

from app.handlers.common.work_fact_view import (
    build_category_keyboard,
    build_quantity_keyboard,
    decode_quantity,
    format_category_message,
    format_quantity_message,
)
from app.infrastructure.db.session import async_session
from app.services.material_catalog import get_material_catalog
from app.services.request_service import RequestService
from app.services.work_catalog import get_work_catalog
from app.handlers.master.states import MasterStates
from app.handlers.master.utils import get_master, load_request
from app.handlers.master.detail import refresh_request_detail
from app.handlers.master.work.utils import (
    load_finish_context,
    save_finish_context,
    refresh_finish_summary_from_context,
)
from app.handlers.master.materials.utils import (
    get_work_item,
    catalog_header,
    update_catalog_message,
    format_currency,
)

logger = logging.getLogger(__name__)

router = Router()


@router.callback_query(F.data.startswith("master:update_fact:"))
async def master_update_fact(callback: CallbackQuery):
    """Старт обновления факта: сразу показываем виды работ (материалы автоподсчёт)."""
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

        header = catalog_header(request)

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
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        header = catalog_header(request)

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


@router.callback_query(F.data.startswith("work:m:"))
async def master_work_catalog(callback: CallbackQuery, state: FSMContext):
    """Обработчик каталога работ для обновления факта мастером."""
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
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
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
                role_key="m",
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
                role_key="m",
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
                role_key="m",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
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
            
            finish_context = await load_finish_context(state)
            if finish_context and finish_context.get("request_id") == request_id:
                finish_context["fact_confirmed"] = True
                await save_finish_context(state, finish_context)

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
                role_key="m",
                request_id=request_id,
                new_quantity=new_quantity,
                page=page,
            )
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}")

            # Показываем список автоматически рассчитанных материалов
            await show_materials_after_work_save(
                callback.bot,
                callback.message.chat.id,
                request,
                request_id,
            )

            # Обновляем меню завершения в фоне, не закрывая меню каталога
            await refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            return

        if action == "finish":
            # Закрываем меню и отправляем заявку
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)
            await refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            await callback.answer("Заявка отправлена.")
            return

        if action == "close":
            try:
                await callback.message.delete()
            except Exception:
                await callback.message.edit_reply_markup(reply_markup=None)
            await refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
            await callback.answer()
            return

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

    catalog = get_material_catalog()

    async with async_session() as session:
        master = await get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await load_request(session, master.id, request_id)
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
                role_key="mm",
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
                role_key="mm",
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
                role_key="mm",
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
                author_id=master.id,
            )
            await session.commit()

            # Перезагружаем заявку для получения актуальных данных
            await session.refresh(request, ["work_items"])

            finish_context = await load_finish_context(state)
            if finish_context and finish_context.get("request_id") == request_id:
                finish_context["fact_confirmed"] = True
                await save_finish_context(state, finish_context)

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
            await update_catalog_message(callback.message, text, markup)
            await callback.answer(f"Сохранено {new_quantity:.2f}. Стоимость: {material_cost:,.2f} ₽")

            # Обновляем меню завершения в фоне, не закрывая меню каталога
            await refresh_finish_summary_from_context(callback.bot, state, request_id=request_id)
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
        catalog = get_material_catalog()
    else:
        catalog = get_work_catalog()
    
    catalog_item = catalog.get_item(item_id)
    
    if not catalog_item:
        item_type = "материал" if is_material else "работа"
        await message.answer(f"{item_type.capitalize()} не найден в каталоге.")
        await state.clear()
        return
    
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


async def show_materials_after_work_save(
    bot,
    chat_id: int,
    request,
    request_id: int,
) -> None:
    """Показывает мастеру список автоматически рассчитанных материалов после сохранения работы."""
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
    header = catalog_header(request)
    
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
            f"   Цена за единицу: {format_currency(price_per_unit)} ₽\n"
            f"   Стоимость: {format_currency(cost)} ₽"
        )
    
    lines.append("")
    lines.append(f"<b>Итого по материалам: {format_currency(total_material_cost)} ₽</b>")
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
