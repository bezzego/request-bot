from __future__ import annotations

from typing import Iterable, Protocol

from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup

from app.services.work_catalog import WorkCatalog, WorkCatalogCategory, WorkCatalogItem
from app.services.material_catalog import MaterialCatalog, MaterialCatalogCategory, MaterialCatalogItem


class CatalogCategory(Protocol):
    """Протокол для категорий каталога."""
    id: str
    name: str
    parent_id: str | None
    children_ids: tuple[str, ...]
    item_ids: tuple[str, ...]
    path: tuple[str, ...]


class CatalogItem(Protocol):
    """Протокол для элементов каталога."""
    id: str
    category_id: str
    name: str
    unit: str | None
    price: float
    formula: str | None
    path: tuple[str, ...]


class Catalog(Protocol):
    """Протокол для каталога."""
    def get_root_categories(self) -> Sequence[CatalogCategory]: ...
    def get_category(self, category_id: str) -> CatalogCategory | None: ...
    def iter_child_categories(self, category_id: str | None) -> Iterable[CatalogCategory]: ...
    def iter_items(self, category_id: str) -> Iterable[CatalogItem]: ...
    def get_item(self, item_id: str) -> CatalogItem | None: ...

QUANTITY_SCALE = 100  # две цифры после запятой


def encode_quantity(value: float) -> str:
    return str(int(round(value * QUANTITY_SCALE)))


def decode_quantity(value: str) -> float:
    return int(value) / QUANTITY_SCALE


def format_category_message(
    category: WorkCatalogCategory | MaterialCatalogCategory | None,
    is_material: bool = False,
) -> str:
    if not category:
        catalog_type = "материалов" if is_material else "работ"
        item_type = "материал" if is_material else "работу"
        return (
            f"📦 <b>Каталог {catalog_type}</b>\n"
            f"Выберите раздел, затем конкретный {item_type}.\n"
            "После выбора объём будет пересчитан автоматически."
        )

    breadcrumb = " / ".join(category.path)
    item_type = "материал" if is_material else "работу"
    return (
        f"📂 <b>{breadcrumb}</b>\n"
        f"Выберите {item_type} или откройте вложенный раздел."
    )


def build_category_keyboard(
    *,
    catalog: WorkCatalog | MaterialCatalog,
    category: WorkCatalogCategory | MaterialCatalogCategory | None,
    role_key: str,
    request_id: int,
    is_material: bool = False,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    category_id = category.id if category else None
    subcategories = (
        catalog.get_root_categories() if category is None else catalog.iter_child_categories(category_id)
    )
    
    prefix = "material" if is_material else "work"
    item_emoji = "📦" if is_material else "🛠"
    
    for sub in subcategories:
        builder.button(
            text=f"📂 {sub.name}",
            callback_data=f"{prefix}:{role_key}:{request_id}:browse:{sub.id}",
        )

    if category is not None:
        for item in catalog.iter_items(category.id):
            builder.button(
                text=f"{item_emoji} {item.name}",
                callback_data=f"{prefix}:{role_key}:{request_id}:item:{item.id}",
            )
    else:
        # на корне показываем элементы верхнего уровня
        for root in catalog.get_root_categories():
            for item in catalog.iter_items(root.id):
                builder.button(
                    text=f"{item_emoji} {item.name}",
                    callback_data=f"{prefix}:{role_key}:{request_id}:item:{item.id}",
                )

    if category is None:
        builder.button(
            text="✖️ Закрыть",
            callback_data=f"{prefix}:{role_key}:{request_id}:close:root",
        )
    else:
        parent_id = category.parent_id or "root"
        builder.button(
            text="⬅️ Назад",
            callback_data=f"{prefix}:{role_key}:{request_id}:back:{parent_id}",
        )
        builder.button(
            text="✖️ Закрыть",
            callback_data=f"{prefix}:{role_key}:{request_id}:close:{category.id}",
        )

    builder.adjust(1)
    return builder.as_markup()


def format_quantity_message(
    *,
    catalog_item: WorkCatalogItem | MaterialCatalogItem,
    new_quantity: float,
    current_quantity: float | None,
    is_material: bool = False,
) -> str:
    price = catalog_item.price
    new_cost = price * new_quantity
    unit = catalog_item.unit or ""
    unit_suffix = f" {unit}".rstrip()
    if current_quantity is not None:
        current_display = f"{current_quantity:.2f}{unit_suffix}"
    else:
        current_display = "—"
    new_display = f"{new_quantity:.2f}{unit_suffix}"
    item_emoji = "📦" if is_material else "🛠"
    item_type = "материал" if is_material else "работа"
    return (
        f"{item_emoji} <b>{catalog_item.name}</b>\n"
        f"Тип: {item_type}\n"
        f"Ед. измерения: {unit or '—'}\n"
        f"Цена за единицу: {price:,.2f} ₽\n"
        f"Текущий факт: {current_display}\n"
        f"Новый факт: {new_display}\n"
        f"Расчётная стоимость: {new_cost:,.2f} ₽\n\n"
        "Используйте кнопки для корректировки или нажмите «✍️ Ввести вручную» для точного значения. «Сохранить» перезапишет факт."
    ).replace(",", " ")


def build_quantity_keyboard(
    *,
    catalog_item: WorkCatalogItem | MaterialCatalogItem,
    role_key: str,
    request_id: int,
    new_quantity: float,
    is_material: bool = False,
) -> InlineKeyboardMarkup:
    deltas = [-5.0, -1.0, -0.5, -0.1, 0.1, 0.5, 1.0, 5.0]

    def apply_delta(delta: float) -> float:
        value = new_quantity + delta
        return round(max(0.0, value), 2)

    builder = InlineKeyboardBuilder()
    for delta in deltas[:4]:
        builder.button(
            text=f"{delta:+}",
            callback_data=_quantity_callback(role_key, request_id, catalog_item.id, apply_delta(delta), is_material),
        )
    for delta in deltas[4:]:
        builder.button(
            text=f"{delta:+}",
            callback_data=_quantity_callback(role_key, request_id, catalog_item.id, apply_delta(delta), is_material),
        )
    builder.button(
        text="0",
        callback_data=_quantity_callback(role_key, request_id, catalog_item.id, 0.0, is_material),
    )
    prefix = "material" if is_material else "work"
    builder.button(
        text="✍️ Ввести вручную",
        callback_data=f"{prefix}:{role_key}:{request_id}:manual:{catalog_item.id}",
    )
    builder.button(
        text="✅ Сохранить",
        callback_data=f"{prefix}:{role_key}:{request_id}:save:{catalog_item.id}:{encode_quantity(new_quantity)}",
    )
    builder.button(
        text="⬅️ Назад",
        callback_data=f"{prefix}:{role_key}:{request_id}:back:{catalog_item.category_id}",
    )
    builder.button(
        text="✖️ Закрыть",
        callback_data=f"{prefix}:{role_key}:{request_id}:close:{catalog_item.category_id}",
    )
    builder.adjust(4, 4, 1, 1, 1, 1)
    return builder.as_markup()


def _quantity_callback(role_key: str, request_id: int, item_id: str, quantity: float, is_material: bool = False) -> str:
    prefix = "material" if is_material else "work"
    return f"{prefix}:{role_key}:{request_id}:qty:{item_id}:{encode_quantity(quantity)}"
