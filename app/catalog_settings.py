from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.infrastructure.db.models.user import User, UserRole
from app.infrastructure.db.session import async_session
from app.services.work_catalog import CATALOG_FILE, get_work_catalog
from sqlalchemy import select
from sqlalchemy.orm import selectinload

router = Router()
logger = logging.getLogger(__name__)


class CatalogSettingsStates(StatesGroup):
    """Состояния для редактирования каталога."""
    main_menu = State()
    view_groups = State()
    view_works = State()
    add_group_name = State()
    add_work_name = State()
    add_work_code = State()
    add_work_unit = State()
    add_work_price = State()
    add_work_group = State()
    add_material_name = State()
    add_material_unit = State()
    add_material_qty = State()
    add_material_price = State()
    edit_work_name = State()
    edit_work_code = State()
    edit_work_unit = State()
    edit_work_price = State()
    edit_work_group = State()
    edit_material_name = State()
    edit_material_unit = State()
    edit_material_qty = State()
    edit_material_price = State()
    delete_confirm = State()


def _load_catalog_data() -> dict[str, Any]:
    """Загружает данные каталога из JSON файла."""
    if not CATALOG_FILE.exists():
        return {"works": []}
    with CATALOG_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_catalog_data(data: dict[str, Any]) -> None:
    """Сохраняет данные каталога в JSON файл."""
    with CATALOG_FILE.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    # Сбрасываем кэш каталога
    get_work_catalog.cache_clear()
    logger.info("Каталог сохранён, кэш очищен")


def _get_groups(data: dict[str, Any]) -> list[str]:
    """Получает список всех групп из каталога."""
    groups = set()
    for work in data.get("works", []):
        group = work.get("group")
        if group:
            groups.add(group)
    return sorted(groups)


def _get_works_by_group(data: dict[str, Any], group: str | None = None) -> list[dict[str, Any]]:
    """Получает список работ, отфильтрованных по группе."""
    works = data.get("works", [])
    if group is None:
        return works
    return [w for w in works if w.get("group") == group]


async def _check_access(message: Message) -> bool:
    """Проверяет, имеет ли пользователь доступ к настройкам каталога."""
    async with async_session() as session:
        user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not user:
            return False
        
        # Доступ имеют: специалисты, инженеры и супер-админы
        if user.role == UserRole.SPECIALIST or user.role == UserRole.ENGINEER:
            return True
        
        if user.role == UserRole.MANAGER and user.leader_profile and user.leader_profile.is_super_admin:
            return True
        
        return False


@router.message(F.text == "⚙️ Настройки")
async def catalog_settings_start(message: Message, state: FSMContext):
    """Начало работы с настройками каталога."""
    if not await _check_access(message):
        await message.answer("⚠️ У вас нет доступа к настройкам каталога.")
        return
    
    await state.set_state(CatalogSettingsStates.main_menu)
    await _show_main_menu(message)


async def _show_main_menu(message: Message):
    """Показывает главное меню настроек каталога."""
    data = _load_catalog_data()
    groups = _get_groups(data)
    works_count = len(data.get("works", []))
    
    text = (
        "⚙️ <b>Настройки каталога работ и материалов</b>\n\n"
        f"📊 Статистика:\n"
        f"• Групп: {len(groups)}\n"
        f"• Видов работ: {works_count}\n\n"
        "Выберите действие:"
    )
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📁 Просмотр групп", callback_data="cat:view_groups")],
            [InlineKeyboardButton(text="➕ Добавить группу", callback_data="cat:add_group")],
            [InlineKeyboardButton(text="➕ Добавить работу", callback_data="cat:add_work")],
            [InlineKeyboardButton(text="🔍 Найти работу", callback_data="cat:search_work")],
            [InlineKeyboardButton(text="❌ Закрыть", callback_data="cat:close")],
        ]
    )
    
    await message.answer(text, reply_markup=kb)


@router.callback_query(F.data == "cat:close")
async def catalog_close(callback: CallbackQuery, state: FSMContext):
    """Закрывает настройки каталога."""
    await state.clear()
    await callback.message.delete()
    await callback.answer("Настройки закрыты")


@router.callback_query(F.data == "cat:view_groups")
async def catalog_view_groups(callback: CallbackQuery, state: FSMContext):
    """Показывает список групп."""
    data = _load_catalog_data()
    groups = _get_groups(data)
    
    if not groups:
        await callback.answer("Группы не найдены", show_alert=True)
        return
    
    text = "📁 <b>Группы работ:</b>\n\n"
    kb_builder = InlineKeyboardBuilder()
    
    for group in groups:
        works_in_group = len(_get_works_by_group(data, group))
        text += f"• <b>{group}</b> ({works_in_group} работ)\n"
        kb_builder.button(text=f"📂 {group}", callback_data=f"cat:group:{group}")
    
    kb_builder.button(text="➕ Добавить группу", callback_data="cat:add_group")
    kb_builder.button(text="⬅️ Назад", callback_data="cat:main_menu")
    kb_builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=kb_builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("cat:group:"))
async def catalog_view_group_works(callback: CallbackQuery, state: FSMContext):
    """Показывает работы в выбранной группе."""
    group = callback.data.split(":", 2)[2]
    data = _load_catalog_data()
    works = _get_works_by_group(data, group)
    
    if not works:
        await callback.answer("В этой группе нет работ", show_alert=True)
        return
    
    text = f"📂 <b>Группа: {group}</b>\n\n<b>Работы:</b>\n\n"
    kb_builder = InlineKeyboardBuilder()
    
    for idx, work in enumerate(works, 1):
        name = work.get("name", "Без названия")
        code = work.get("code", "")
        unit = work.get("unit", "")
        price = work.get("price_per_unit", 0)
        materials_count = len(work.get("materials", []))
        
        text += f"{idx}. <b>{name}</b>\n"
        text += f"   Код: {code}\n"
        text += f"   Ед.: {unit} | Цена: {price:.2f} ₽\n"
        text += f"   Материалов: {materials_count}\n\n"
        
        kb_builder.button(
            text=f"✏️ {name[:30]}",
            callback_data=f"cat:edit_work:{idx-1}:{group}"
        )
    
    kb_builder.button(text="➕ Добавить работу в группу", callback_data=f"cat:add_work_to_group:{group}")
    kb_builder.button(text="⬅️ Назад к группам", callback_data="cat:view_groups")
    kb_builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=kb_builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "cat:main_menu")
async def catalog_main_menu(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню."""
    await state.set_state(CatalogSettingsStates.main_menu)
    await _show_main_menu(callback.message)
    await callback.answer()


@router.callback_query(F.data == "cat:add_group")
async def catalog_add_group_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления новой группы."""
    await state.set_state(CatalogSettingsStates.add_group_name)
    await callback.message.edit_text(
        "➕ <b>Добавление новой группы</b>\n\n"
        "Введите название группы (например: \"Стены\", \"Пол\", \"Сантехника\"):\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.add_group_name))
async def catalog_add_group_name(message: Message, state: FSMContext):
    """Обработка названия новой группы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление группы отменено.")
        return
    
    if not text:
        await message.answer("Название группы не может быть пустым. Попробуйте снова.")
        return
    
    data = _load_catalog_data()
    groups = _get_groups(data)
    
    if text in groups:
        await message.answer(f"Группа «{text}» уже существует. Выберите другое название.")
        return
    
    # Группа создаётся автоматически при добавлении работы, поэтому просто возвращаемся в меню
    await state.update_data(new_group=text)
    await state.set_state(CatalogSettingsStates.main_menu)
    await message.answer(
        f"✅ Группа «{text}» будет создана при добавлении первой работы в неё.\n"
        "Теперь добавьте работу и выберите эту группу."
    )
    await _show_main_menu(message)


@router.callback_query(F.data == "cat:add_work")
async def catalog_add_work_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления новой работы."""
    data = _load_catalog_data()
    groups = _get_groups(data)
    
    if not groups:
        await callback.answer(
            "Сначала создайте группу! Нажмите «➕ Добавить группу».",
            show_alert=True
        )
        return
    
    await state.set_state(CatalogSettingsStates.add_work_name)
    await state.update_data(new_work={})
    await callback.message.edit_text(
        "➕ <b>Добавление новой работы</b>\n\n"
        "Шаг 1/5: Введите название работы:\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.add_work_name))
async def catalog_add_work_name(message: Message, state: FSMContext):
    """Обработка названия новой работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление работы отменено.")
        return
    
    if not text:
        await message.answer("Название работы не может быть пустым. Попробуйте снова.")
        return
    
    data = await state.get_data()
    work = data.get("new_work", {})
    work["name"] = text
    await state.update_data(new_work=work)
    await state.set_state(CatalogSettingsStates.add_work_code)
    await message.answer(
        f"✅ Название: {text}\n\n"
        "Шаг 2/5: Введите код работы (латинскими буквами, например: \"wall_plaster\"):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_work_code))
async def catalog_add_work_code(message: Message, state: FSMContext):
    """Обработка кода новой работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление работы отменено.")
        return
    
    if not text:
        await message.answer("Код работы не может быть пустым. Попробуйте снова.")
        return
    
    # Проверяем уникальность кода
    data = _load_catalog_data()
    existing_codes = {w.get("code") for w in data.get("works", []) if w.get("code")}
    if text in existing_codes:
        await message.answer(f"Код «{text}» уже используется. Введите другой код.")
        return
    
    work_data = await state.get_data()
    work = work_data.get("new_work", {})
    work["code"] = text
    await state.update_data(new_work=work)
    await state.set_state(CatalogSettingsStates.add_work_unit)
    await message.answer(
        f"✅ Код: {text}\n\n"
        "Шаг 3/5: Введите единицу измерения (например: \"м.кв.\", \"шт.\", \"м.п.\"):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_work_unit))
async def catalog_add_work_unit(message: Message, state: FSMContext):
    """Обработка единицы измерения новой работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление работы отменено.")
        return
    
    if not text:
        await message.answer("Единица измерения не может быть пустой. Попробуйте снова.")
        return
    
    work_data = await state.get_data()
    work = work_data.get("new_work", {})
    work["unit"] = text
    await state.update_data(new_work=work)
    await state.set_state(CatalogSettingsStates.add_work_price)
    await message.answer(
        f"✅ Единица: {text}\n\n"
        "Шаг 4/5: Введите цену за единицу (только число, например: 500.50):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_work_price))
async def catalog_add_work_price(message: Message, state: FSMContext):
    """Обработка цены новой работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await state.clear()
        await message.answer("Добавление работы отменено.")
        return
    
    try:
        price = float(text.replace(",", "."))
        if price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError:
        await message.answer("Введите корректное число для цены (например: 500.50).")
        return
    
    work_data = await state.get_data()
    work = work_data.get("new_work", {})
    work["price_per_unit"] = price
    await state.update_data(new_work=work)
    
    # Показываем выбор группы
    data = _load_catalog_data()
    groups = _get_groups(data)
    
    if not groups:
        # Если групп нет, создаём группу "Прочее"
        work["group"] = "Прочее"
        await _save_new_work(message, state, work)
        return
    
    kb_builder = InlineKeyboardBuilder()
    for group in groups:
        kb_builder.button(text=f"📂 {group}", callback_data=f"cat:select_group:{group}")
    kb_builder.button(text="➕ Создать новую группу", callback_data="cat:create_group_for_work")
    kb_builder.button(text="❌ Отмена", callback_data="cat:cancel_add_work")
    kb_builder.adjust(1)
    
    await state.set_state(CatalogSettingsStates.add_work_group)
    await message.answer(
        f"✅ Цена: {price:.2f} ₽\n\n"
        "Шаг 5/5: Выберите группу для работы:",
        reply_markup=kb_builder.as_markup()
    )


@router.callback_query(F.data.startswith("cat:select_group:"), StateFilter(CatalogSettingsStates.add_work_group))
async def catalog_select_group(callback: CallbackQuery, state: FSMContext):
    """Выбор группы для новой работы."""
    group = callback.data.split(":", 2)[2]
    work_data = await state.get_data()
    work = work_data.get("new_work", {})
    work["group"] = group
    await _save_new_work(callback.message, state, work)
    await callback.answer()


@router.callback_query(F.data == "cat:create_group_for_work", StateFilter(CatalogSettingsStates.add_work_group))
async def catalog_create_group_for_work(callback: CallbackQuery, state: FSMContext):
    """Создание новой группы для работы."""
    await state.set_state(CatalogSettingsStates.add_group_name)
    await callback.message.edit_text(
        "➕ <b>Создание новой группы</b>\n\n"
        "Введите название группы:\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.callback_query(F.data == "cat:cancel_add_work")
async def catalog_cancel_add_work(callback: CallbackQuery, state: FSMContext):
    """Отмена добавления работы."""
    await state.clear()
    await callback.message.edit_text("Добавление работы отменено.")
    await callback.answer()


async def _save_new_work(message: Message, state: FSMContext, work: dict[str, Any]):
    """Сохраняет новую работу в каталог."""
    data = _load_catalog_data()
    if "works" not in data:
        data["works"] = []
    
    # Убеждаемся, что есть поле materials
    if "materials" not in work:
        work["materials"] = []
    
    data["works"].append(work)
    _save_catalog_data(data)
    
    await state.clear()
    await message.answer(
        f"✅ <b>Работа добавлена!</b>\n\n"
        f"Название: {work.get('name')}\n"
        f"Код: {work.get('code')}\n"
        f"Группа: {work.get('group', 'Прочее')}\n"
        f"Единица: {work.get('unit')}\n"
        f"Цена: {work.get('price_per_unit', 0):.2f} ₽\n\n"
        "Теперь вы можете добавить материалы к этой работе."
    )
    await _show_main_menu(message)


@router.callback_query(F.data.startswith("cat:edit_work:"))
async def catalog_edit_work_start(callback: CallbackQuery, state: FSMContext):
    """Начало редактирования работы."""
    parts = callback.data.split(":")
    if len(parts) < 4:
        await callback.answer("Ошибка в данных", show_alert=True)
        return
    
    work_idx = int(parts[2])
    group = parts[3]
    
    data = _load_catalog_data()
    works = _get_works_by_group(data, group)
    
    if work_idx >= len(works):
        await callback.answer("Работа не найдена", show_alert=True)
        return
    
    work = works[work_idx]
    await state.update_data(editing_work_idx=work_idx, editing_work_group=group, editing_work=work.copy())
    
    name = work.get("name", "Без названия")
    code = work.get("code", "")
    unit = work.get("unit", "")
    price = work.get("price_per_unit", 0)
    materials = work.get("materials", [])
    name = work.get("name", "Без названия")
    code = work.get("code", "")
    unit = work.get("unit", "")
    price = work.get("price_per_unit", 0)
    materials = work.get("materials", [])
    
    text = (
        f"✏️ <b>Редактирование работы</b>\n\n"
        f"<b>{name}</b>\n"
        f"Код: {code}\n"
        f"Группа: {group}\n"
        f"Единица: {unit}\n"
        f"Цена: {price:.2f} ₽\n"
        f"Материалов: {len(materials)}\n\n"
        "Выберите, что хотите изменить:"
    )
    
    kb_builder = InlineKeyboardBuilder()
    kb_builder.button(text="📝 Название", callback_data="cat:edit_work_field:name")
    kb_builder.button(text="🔤 Код", callback_data="cat:edit_work_field:code")
    kb_builder.button(text="📏 Единица измерения", callback_data="cat:edit_work_field:unit")
    kb_builder.button(text="💰 Цена", callback_data="cat:edit_work_field:price")
    kb_builder.button(text="📂 Группа", callback_data="cat:edit_work_field:group")
    kb_builder.button(text="➕ Добавить материал", callback_data="cat:add_material")
    kb_builder.button(text="📦 Материалы", callback_data="cat:view_materials")
    kb_builder.button(text="🗑 Удалить работу", callback_data="cat:delete_work")
    kb_builder.button(text="⬅️ Назад", callback_data=f"cat:group:{group}")
    kb_builder.adjust(2, 2, 1, 1, 1, 1)
    
    await callback.message.edit_text(text, reply_markup=kb_builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("cat:edit_work_field:"))
async def catalog_edit_work_field(callback: CallbackQuery, state: FSMContext):
    """Начало редактирования конкретного поля работы."""
    field = callback.data.split(":")[2]
    work_data = await state.get_data()
    work = work_data.get("editing_work", {})
    
    field_prompts = {
        "name": "Введите новое название работы:",
        "code": "Введите новый код работы (латинскими буквами):",
        "unit": "Введите новую единицу измерения:",
        "price": "Введите новую цену за единицу (число):",
        "group": "Выберите новую группу:",
    }
    
    if field not in field_prompts:
        await callback.answer("Неизвестное поле", show_alert=True)
        return
    
    if field == "group":
        # Показываем список групп для выбора
        data = _load_catalog_data()
        groups = _get_groups(data)
        kb_builder = InlineKeyboardBuilder()
        for grp in groups:
            kb_builder.button(text=f"📂 {grp}", callback_data=f"cat:set_group:{grp}")
        kb_builder.button(text="⬅️ Назад", callback_data=f"cat:edit_work:{work_data.get('editing_work_idx')}:{work_data.get('editing_work_group')}")
        kb_builder.adjust(1)
        
        await state.set_state(CatalogSettingsStates.edit_work_group)
        await callback.message.edit_text(
            f"✏️ <b>Изменение группы</b>\n\n"
            f"Текущая группа: {work.get('group', 'Прочее')}\n\n"
            "Выберите новую группу:",
            reply_markup=kb_builder.as_markup()
        )
        await callback.answer()
        return
    
    state_map = {
        "name": CatalogSettingsStates.edit_work_name,
        "code": CatalogSettingsStates.edit_work_code,
        "unit": CatalogSettingsStates.edit_work_unit,
        "price": CatalogSettingsStates.edit_work_price,
    }
    
    await state.set_state(state_map[field])
    await state.update_data(editing_field=field)
    await callback.message.edit_text(
        f"✏️ <b>Редактирование: {field}</b>\n\n"
        f"Текущее значение: {work.get(field, 'не задано')}\n\n"
        f"{field_prompts[field]}\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.edit_work_name))
async def catalog_edit_work_name_input(message: Message, state: FSMContext):
    """Обработка нового названия работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Название не может быть пустым.")
        return
    
    await _update_work_field(message, state, "name", text)


@router.message(StateFilter(CatalogSettingsStates.edit_work_code))
async def catalog_edit_work_code_input(message: Message, state: FSMContext):
    """Обработка нового кода работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Код не может быть пустым.")
        return
    
    # Проверяем уникальность кода
    work_data = await state.get_data()
    current_work = work_data.get("editing_work", {})
    current_code = current_work.get("code")
    
    if text != current_code:
        data = _load_catalog_data()
        existing_codes = {w.get("code") for w in data.get("works", []) if w.get("code")}
        if text in existing_codes:
            await message.answer(f"Код «{text}» уже используется. Введите другой.")
            return
    
    await _update_work_field(message, state, "code", text)


@router.message(StateFilter(CatalogSettingsStates.edit_work_unit))
async def catalog_edit_work_unit_input(message: Message, state: FSMContext):
    """Обработка новой единицы измерения."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Единица измерения не может быть пустой.")
        return
    
    await _update_work_field(message, state, "unit", text)


@router.message(StateFilter(CatalogSettingsStates.edit_work_price))
async def catalog_edit_work_price_input(message: Message, state: FSMContext):
    """Обработка новой цены работы."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    try:
        price = float(text.replace(",", "."))
        if price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError:
        await message.answer("Введите корректное число для цены (например: 500.50).")
        return
    
    await _update_work_field(message, state, "price_per_unit", price)


@router.callback_query(F.data.startswith("cat:set_group:"), StateFilter(CatalogSettingsStates.edit_work_group))
async def catalog_set_group(callback: CallbackQuery, state: FSMContext):
    """Установка новой группы для работы."""
    new_group = callback.data.split(":", 2)[2]
    await _update_work_field(callback.message, state, "group", new_group)
    await callback.answer()


async def _update_work_field(message: Message, state: FSMContext, field: str, value: Any):
    """Обновляет поле работы и сохраняет изменения."""
    work_data = await state.get_data()
    work = work_data.get("editing_work", {})
    work[field] = value
    
    # Находим и обновляем работу в каталоге
    data = _load_catalog_data()
    old_group = work_data.get("editing_work_group")
    work_idx = work_data.get("editing_work_idx")
    
    works = _get_works_by_group(data, old_group)
    if work_idx < len(works):
        old_work = works[work_idx]
        # Находим индекс в общем списке
        all_works = data.get("works", [])
        for idx, w in enumerate(all_works):
            if w.get("code") == old_work.get("code"):
                # Обновляем работу
                all_works[idx].update(work)
                # Если изменилась группа, обновляем поле group
                if field == "group":
                    all_works[idx]["group"] = value
                break
        
        _save_catalog_data(data)
        await state.update_data(editing_work=work, editing_work_group=work.get("group", old_group))
        await message.answer(f"✅ Поле «{field}» обновлено!")
        await _return_to_edit_menu(message, state)
    else:
        await message.answer("❌ Ошибка: работа не найдена в каталоге.")


async def _return_to_edit_menu(message: Message, state: FSMContext):
    """Возвращает к меню редактирования работы."""
    work_data = await state.get_data()
    work_idx = work_data.get("editing_work_idx")
    group = work_data.get("editing_work_group")
    
    # Перезагружаем данные работы
    data = _load_catalog_data()
    works = _get_works_by_group(data, group)
    if work_idx < len(works):
        work = works[work_idx]
        await state.update_data(editing_work=work.copy())
        
        # Создаём callback для показа меню редактирования
        from aiogram.types import CallbackQuery as CB
        class FakeCallback:
            def __init__(self, msg, data):
                self.message = msg
                self.data = data
            async def answer(self):
                pass
        
        fake_cb = FakeCallback(message, f"cat:edit_work:{work_idx}:{group}")
        await catalog_edit_work_start(fake_cb, state)


@router.callback_query(F.data == "cat:view_materials")
async def catalog_view_materials(callback: CallbackQuery, state: FSMContext):
    """Показывает список материалов работы."""
    work_data = await state.get_data()
    work = work_data.get("editing_work", {})
    materials = work.get("materials", [])
    
    if not materials:
        await callback.answer("У этой работы пока нет материалов", show_alert=True)
        return
    
    text = f"📦 <b>Материалы работы: {work.get('name', 'Без названия')}</b>\n\n"
    kb_builder = InlineKeyboardBuilder()
    
    for idx, material in enumerate(materials, 1):
        name = material.get("name", "Без названия")
        unit = material.get("unit", "")
        qty = material.get("qty_per_work_unit", 0)
        price = material.get("price_per_unit", 0)
        
        text += f"{idx}. <b>{name}</b>\n"
        text += f"   Ед.: {unit} | Кол-во: {qty} | Цена: {price:.2f} ₽\n\n"
        
        kb_builder.button(
            text=f"✏️ {name[:25]}",
            callback_data=f"cat:edit_material:{idx-1}"
        )
    
    kb_builder.button(text="➕ Добавить материал", callback_data="cat:add_material")
    kb_builder.button(text="⬅️ Назад", callback_data=f"cat:edit_work:{work_data.get('editing_work_idx')}:{work_data.get('editing_work_group')}")
    kb_builder.adjust(1)
    
    await callback.message.edit_text(text, reply_markup=kb_builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "cat:add_material")
async def catalog_add_material_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления материала к работе."""
    await state.set_state(CatalogSettingsStates.add_material_name)
    await state.update_data(new_material={})
    await callback.message.edit_text(
        "➕ <b>Добавление материала</b>\n\n"
        "Шаг 1/4: Введите название материала:\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.add_material_name))
async def catalog_add_material_name(message: Message, state: FSMContext):
    """Обработка названия материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Название материала не может быть пустым.")
        return
    
    material_data = await state.get_data()
    material = material_data.get("new_material", {})
    material["name"] = text
    await state.update_data(new_material=material)
    await state.set_state(CatalogSettingsStates.add_material_unit)
    await message.answer(
        f"✅ Название: {text}\n\n"
        "Шаг 2/4: Введите единицу измерения (например: \"шт.\", \"м.п.\", \"кг\"):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_material_unit))
async def catalog_add_material_unit(message: Message, state: FSMContext):
    """Обработка единицы измерения материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Единица измерения не может быть пустой.")
        return
    
    material_data = await state.get_data()
    material = material_data.get("new_material", {})
    material["unit"] = text
    await state.update_data(new_material=material)
    await state.set_state(CatalogSettingsStates.add_material_qty)
    await message.answer(
        f"✅ Единица: {text}\n\n"
        "Шаг 3/4: Введите количество материала на единицу работы (число, например: 2.5):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_material_qty))
async def catalog_add_material_qty(message: Message, state: FSMContext):
    """Обработка количества материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    try:
        qty = float(text.replace(",", "."))
        if qty < 0:
            raise ValueError("Количество не может быть отрицательным")
    except ValueError:
        await message.answer("Введите корректное число (например: 2.5).")
        return
    
    material_data = await state.get_data()
    material = material_data.get("new_material", {})
    material["qty_per_work_unit"] = qty
    await state.update_data(new_material=material)
    await state.set_state(CatalogSettingsStates.add_material_price)
    await message.answer(
        f"✅ Количество: {qty}\n\n"
        "Шаг 4/4: Введите цену за единицу материала (число, например: 150.75):\n\n"
        "Для отмены отправьте «Отмена»."
    )


@router.message(StateFilter(CatalogSettingsStates.add_material_price))
async def catalog_add_material_price(message: Message, state: FSMContext):
    """Обработка цены материала и сохранение."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_edit_menu(message, state)
        return
    
    try:
        price = float(text.replace(",", "."))
        if price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError:
        await message.answer("Введите корректное число для цены (например: 150.75).")
        return
    
    material_data = await state.get_data()
    material = material_data.get("new_material", {})
    material["price_per_unit"] = price
    
    # Сохраняем материал в работу
    work_data = await state.get_data()
    work = work_data.get("editing_work", {})
    if "materials" not in work:
        work["materials"] = []
    work["materials"].append(material)
    
    # Обновляем работу в каталоге
    data = _load_catalog_data()
    work_idx = work_data.get("editing_work_idx")
    group = work_data.get("editing_work_group")
    works = _get_works_by_group(data, group)
    
    if work_idx < len(works):
        old_work = works[work_idx]
        all_works = data.get("works", [])
        for idx, w in enumerate(all_works):
            if w.get("code") == old_work.get("code"):
                all_works[idx] = work
                break
        
        _save_catalog_data(data)
        await state.update_data(editing_work=work)
        await message.answer(
            f"✅ <b>Материал добавлен!</b>\n\n"
            f"Название: {material.get('name')}\n"
            f"Единица: {material.get('unit')}\n"
            f"Количество: {material.get('qty_per_work_unit')}\n"
            f"Цена: {material.get('price_per_unit', 0):.2f} ₽"
        )
        await _return_to_edit_menu(message, state)
    else:
        await message.answer("❌ Ошибка: работа не найдена.")


@router.callback_query(F.data.startswith("cat:edit_material:"))
async def catalog_edit_material_start(callback: CallbackQuery, state: FSMContext):
    """Начало редактирования материала."""
    material_idx = int(callback.data.split(":")[2])
    work_data = await state.get_data()
    work = work_data.get("editing_work", {})
    materials = work.get("materials", [])
    
    if material_idx >= len(materials):
        await callback.answer("Материал не найден", show_alert=True)
        return
    
    material = materials[material_idx]
    await state.update_data(editing_material_idx=material_idx, editing_material=material.copy())
    
    name = material.get("name", "Без названия")
    unit = material.get("unit", "")
    qty = material.get("qty_per_work_unit", 0)
    price = material.get("price_per_unit", 0)
    
    text = (
        f"✏️ <b>Редактирование материала</b>\n\n"
        f"<b>{name}</b>\n"
        f"Единица: {unit}\n"
        f"Количество: {qty}\n"
        f"Цена: {price:.2f} ₽\n\n"
        "Выберите, что хотите изменить:"
    )
    
    kb_builder = InlineKeyboardBuilder()
    kb_builder.button(text="📝 Название", callback_data="cat:edit_mat_field:name")
    kb_builder.button(text="📏 Единица", callback_data="cat:edit_mat_field:unit")
    kb_builder.button(text="🔢 Количество", callback_data="cat:edit_mat_field:qty")
    kb_builder.button(text="💰 Цена", callback_data="cat:edit_mat_field:price")
    kb_builder.button(text="🗑 Удалить", callback_data="cat:delete_material")
    kb_builder.button(text="⬅️ Назад", callback_data="cat:view_materials")
    kb_builder.adjust(2, 2, 1, 1)
    
    await callback.message.edit_text(text, reply_markup=kb_builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("cat:edit_mat_field:"))
async def catalog_edit_material_field(callback: CallbackQuery, state: FSMContext):
    """Начало редактирования поля материала."""
    field = callback.data.split(":")[2]
    material_data = await state.get_data()
    material = material_data.get("editing_material", {})
    
    field_prompts = {
        "name": "Введите новое название материала:",
        "unit": "Введите новую единицу измерения:",
        "qty": "Введите новое количество (число):",
        "price": "Введите новую цену (число):",
    }
    
    if field not in field_prompts:
        await callback.answer("Неизвестное поле", show_alert=True)
        return
    
    state_map = {
        "name": CatalogSettingsStates.edit_material_name,
        "unit": CatalogSettingsStates.edit_material_unit,
        "qty": CatalogSettingsStates.edit_material_qty,
        "price": CatalogSettingsStates.edit_material_price,
    }
    
    await state.set_state(state_map[field])
    await state.update_data(editing_mat_field=field)
    await callback.message.edit_text(
        f"✏️ <b>Редактирование: {field}</b>\n\n"
        f"Текущее значение: {material.get(field if field != 'qty' else 'qty_per_work_unit', 'не задано')}\n\n"
        f"{field_prompts[field]}\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.edit_material_name))
async def catalog_edit_material_name_input(message: Message, state: FSMContext):
    """Обработка нового названия материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_material_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Название не может быть пустым.")
        return
    
    await _update_material_field(message, state, "name", text)


@router.message(StateFilter(CatalogSettingsStates.edit_material_unit))
async def catalog_edit_material_unit_input(message: Message, state: FSMContext):
    """Обработка новой единицы измерения материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_material_edit_menu(message, state)
        return
    
    if not text:
        await message.answer("Единица измерения не может быть пустой.")
        return
    
    await _update_material_field(message, state, "unit", text)


@router.message(StateFilter(CatalogSettingsStates.edit_material_qty))
async def catalog_edit_material_qty_input(message: Message, state: FSMContext):
    """Обработка нового количества материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_material_edit_menu(message, state)
        return
    
    try:
        qty = float(text.replace(",", "."))
        if qty < 0:
            raise ValueError("Количество не может быть отрицательным")
    except ValueError:
        await message.answer("Введите корректное число.")
        return
    
    await _update_material_field(message, state, "qty_per_work_unit", qty)


@router.message(StateFilter(CatalogSettingsStates.edit_material_price))
async def catalog_edit_material_price_input(message: Message, state: FSMContext):
    """Обработка новой цены материала."""
    text = (message.text or "").strip()
    
    if text.lower() == "отмена":
        await _return_to_material_edit_menu(message, state)
        return
    
    try:
        price = float(text.replace(",", "."))
        if price < 0:
            raise ValueError("Цена не может быть отрицательной")
    except ValueError:
        await message.answer("Введите корректное число для цены.")
        return
    
    await _update_material_field(message, state, "price_per_unit", price)


async def _update_material_field(message: Message, state: FSMContext, field: str, value: Any):
    """Обновляет поле материала и сохраняет изменения."""
    material_data = await state.get_data()
    material = material_data.get("editing_material", {})
    material[field] = value
    
    # Обновляем материал в работе
    work = material_data.get("editing_work", {})
    material_idx = material_data.get("editing_material_idx")
    materials = work.get("materials", [])
    
    if material_idx < len(materials):
        materials[material_idx] = material
        
        # Сохраняем в каталог
        data = _load_catalog_data()
        work_idx = material_data.get("editing_work_idx")
        group = material_data.get("editing_work_group")
        works = _get_works_by_group(data, group)
        
        if work_idx < len(works):
            old_work = works[work_idx]
            all_works = data.get("works", [])
            for idx, w in enumerate(all_works):
                if w.get("code") == old_work.get("code"):
                    all_works[idx] = work
                    break
            
            _save_catalog_data(data)
            await state.update_data(editing_work=work, editing_material=material)
            await message.answer(f"✅ Поле «{field}» обновлено!")
            await _return_to_material_edit_menu(message, state)
        else:
            await message.answer("❌ Ошибка: работа не найдена.")
    else:
        await message.answer("❌ Ошибка: материал не найден.")


async def _return_to_material_edit_menu(message: Message, state: FSMContext):
    """Возвращает к меню редактирования материала."""
    material_data = await state.get_data()
    material_idx = material_data.get("editing_material_idx")
    
    # Создаём fake callback для показа меню редактирования материала
    from aiogram.types import CallbackQuery as CB
    class FakeCallback:
        def __init__(self, msg, data):
            self.message = msg
            self.data = data
        async def answer(self):
            pass
    
    fake_cb = FakeCallback(message, f"cat:edit_material:{material_idx}")
    await catalog_edit_material_start(fake_cb, state)


@router.callback_query(F.data == "cat:delete_material")
async def catalog_delete_material_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждение удаления материала."""
    await state.set_state(CatalogSettingsStates.delete_confirm)
    await state.update_data(deleting_type="material")
    await callback.message.edit_text(
        "⚠️ <b>Подтверждение удаления</b>\n\n"
        "Вы уверены, что хотите удалить этот материал?\n\n"
        "Это действие нельзя отменить.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да, удалить", callback_data="cat:delete_confirm_yes"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cat:delete_confirm_no"),
                ]
            ]
        )
    )
    await callback.answer()


@router.callback_query(F.data == "cat:delete_work")
async def catalog_delete_work_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждение удаления работы."""
    await state.set_state(CatalogSettingsStates.delete_confirm)
    await state.update_data(deleting_type="work")
    await callback.message.edit_text(
        "⚠️ <b>Подтверждение удаления</b>\n\n"
        "Вы уверены, что хотите удалить эту работу?\n\n"
        "Все материалы этой работы также будут удалены.\n"
        "Это действие нельзя отменить.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✅ Да, удалить", callback_data="cat:delete_confirm_yes"),
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cat:delete_confirm_no"),
                ]
            ]
        )
    )
    await callback.answer()


@router.callback_query(F.data == "cat:delete_confirm_yes", StateFilter(CatalogSettingsStates.delete_confirm))
async def catalog_delete_confirm_yes(callback: CallbackQuery, state: FSMContext):
    """Выполнение удаления."""
    data = await state.get_data()
    deleting_type = data.get("deleting_type")
    
    if deleting_type == "material":
        # Удаление материала
        work = data.get("editing_work", {})
        material_idx = data.get("editing_material_idx")
        materials = work.get("materials", [])
        
        if material_idx < len(materials):
            removed_material = materials.pop(material_idx)
            
            # Сохраняем изменения
            catalog_data = _load_catalog_data()
            work_idx = data.get("editing_work_idx")
            group = data.get("editing_work_group")
            works = _get_works_by_group(catalog_data, group)
            
            if work_idx < len(works):
                old_work = works[work_idx]
                all_works = catalog_data.get("works", [])
                for idx, w in enumerate(all_works):
                    if w.get("code") == old_work.get("code"):
                        all_works[idx] = work
                        break
                
                _save_catalog_data(catalog_data)
                await state.update_data(editing_work=work)
                await callback.message.edit_text(
                    f"✅ Материал «{removed_material.get('name', 'Без названия')}» удалён."
                )
                await _return_to_edit_menu(callback.message, state)
            else:
                await callback.answer("Ошибка: работа не найдена", show_alert=True)
        else:
            await callback.answer("Ошибка: материал не найден", show_alert=True)
    
    elif deleting_type == "work":
        # Удаление работы
        work = data.get("editing_work", {})
        work_code = work.get("code")
        group = data.get("editing_work_group")
        
        catalog_data = _load_catalog_data()
        all_works = catalog_data.get("works", [])
        all_works = [w for w in all_works if w.get("code") != work_code]
        catalog_data["works"] = all_works
        
        _save_catalog_data(catalog_data)
        await state.clear()
        await callback.message.edit_text(
            f"✅ Работа «{work.get('name', 'Без названия')}» удалена."
        )
        # Возвращаемся к списку работ в группе
        fake_cb_data = f"cat:group:{group}"
        from aiogram.types import CallbackQuery as CB
        class FakeCallback:
            def __init__(self, msg, data):
                self.message = msg
                self.data = data
            async def answer(self):
                pass
        fake_cb = FakeCallback(callback.message, fake_cb_data)
        await catalog_view_group_works(fake_cb, state)
    
    await callback.answer()


@router.callback_query(F.data == "cat:delete_confirm_no", StateFilter(CatalogSettingsStates.delete_confirm))
async def catalog_delete_confirm_no(callback: CallbackQuery, state: FSMContext):
    """Отмена удаления."""
    data = await state.get_data()
    deleting_type = data.get("deleting_type")
    
    if deleting_type == "material":
        await _return_to_material_edit_menu(callback.message, state)
    elif deleting_type == "work":
        work_idx = data.get("editing_work_idx")
        group = data.get("editing_work_group")
        fake_cb_data = f"cat:edit_work:{work_idx}:{group}"
        from aiogram.types import CallbackQuery as CB
        class FakeCallback:
            def __init__(self, msg, data):
                self.message = msg
                self.data = data
            async def answer(self):
                pass
        fake_cb = FakeCallback(callback.message, fake_cb_data)
        await catalog_edit_work_start(fake_cb, state)
    
    await callback.answer()


@router.callback_query(F.data.startswith("cat:add_work_to_group:"))
async def catalog_add_work_to_group(callback: CallbackQuery, state: FSMContext):
    """Добавление работы в конкретную группу."""
    group = callback.data.split(":", 2)[2]
    await state.set_state(CatalogSettingsStates.add_work_name)
    await state.update_data(new_work={"group": group})
    await callback.message.edit_text(
        f"➕ <b>Добавление работы в группу: {group}</b>\n\n"
        "Шаг 1/5: Введите название работы:\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await callback.answer()


@router.callback_query(F.data == "cat:search_work")
async def catalog_search_work_start(callback: CallbackQuery, state: FSMContext):
    """Начало поиска работы."""
    await callback.message.edit_text(
        "🔍 <b>Поиск работы</b>\n\n"
        "Введите название работы или её код для поиска:\n\n"
        "Для отмены отправьте «Отмена»."
    )
    await state.set_state(CatalogSettingsStates.main_menu)
    await callback.answer()


@router.message(StateFilter(CatalogSettingsStates.main_menu))
async def catalog_search_work_input(message: Message, state: FSMContext):
    """Обработка поискового запроса."""
    query = (message.text or "").strip().lower()
    
    if query == "отмена":
        await state.clear()
        await message.answer("Поиск отменён.")
        return
    
    if not query:
        await message.answer("Введите поисковый запрос.")
        return
    
    data = _load_catalog_data()
    works = data.get("works", [])
    
    # Поиск по названию или коду
    results = []
    for work in works:
        name = (work.get("name", "") or "").lower()
        code = (work.get("code", "") or "").lower()
        if query in name or query in code:
            results.append(work)
    
    if not results:
        await message.answer(
            f"❌ По запросу «{query}» ничего не найдено.\n\n"
            "Попробуйте другой запрос или вернитесь в меню."
        )
        return
    
    # Показываем результаты
    text = f"🔍 <b>Результаты поиска:</b> (найдено: {len(results)})\n\n"
    kb_builder = InlineKeyboardBuilder()
    
    for idx, work in enumerate(results[:10]):  # Ограничиваем 10 результатами
        name = work.get("name", "Без названия")
        code = work.get("code", "")
        group = work.get("group", "Прочее")
        
        text += f"{idx+1}. <b>{name}</b>\n"
        text += f"   Код: {code} | Группа: {group}\n\n"
        
        # Находим индекс работы в группе для редактирования
        group_works = _get_works_by_group(data, group)
        work_idx_in_group = -1
        for i, w in enumerate(group_works):
            if w.get("code") == code:
                work_idx_in_group = i
                break
        
        if work_idx_in_group >= 0:
            kb_builder.button(
                text=f"✏️ {name[:30]}",
                callback_data=f"cat:edit_work:{work_idx_in_group}:{group}"
            )
    
    if len(results) > 10:
        text += f"\n... и ещё {len(results) - 10} результатов"
    
    kb_builder.button(text="⬅️ Назад в меню", callback_data="cat:main_menu")
    kb_builder.adjust(1)
    
    await message.answer(text, reply_markup=kb_builder.as_markup())