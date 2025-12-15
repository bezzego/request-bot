from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence
import json


# Используем объединённый файл с работами и материалами
CATALOG_FILE = Path(__file__).resolve().parents[1] / "config" / "mat.json"


@dataclass(slots=True, frozen=True)
class MaterialCatalogItem:
    id: str
    category_id: str
    name: str
    unit: str | None
    price: float
    formula: str | None
    path: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class MaterialCatalogCategory:
    id: str
    name: str
    parent_id: str | None
    children_ids: tuple[str, ...]
    item_ids: tuple[str, ...]
    path: tuple[str, ...]


class MaterialCatalog:
    """Предопределённые материалы для план/факт бюджета заявки."""

    def __init__(
        self,
        *,
        categories: dict[str, MaterialCatalogCategory],
        items: dict[str, MaterialCatalogItem],
        root_ids: Sequence[str],
    ) -> None:
        self._categories = categories
        self._items = items
        self._root_ids = tuple(root_ids)

    def get_root_categories(self) -> Sequence[MaterialCatalogCategory]:
        return tuple(self._categories[c_id] for c_id in self._root_ids)

    def get_category(self, category_id: str) -> MaterialCatalogCategory | None:
        return self._categories.get(category_id)

    def iter_child_categories(self, category_id: str | None) -> Iterable[MaterialCatalogCategory]:
        if category_id is None:
            return self.get_root_categories()
        category = self._categories.get(category_id)
        if not category:
            return ()
        return tuple(self._categories[c_id] for c_id in category.children_ids)

    def iter_items(self, category_id: str) -> Iterable[MaterialCatalogItem]:
        category = self._categories.get(category_id)
        if not category:
            return ()
        return tuple(self._items[item_id] for item_id in category.item_ids)

    def get_item(self, item_id: str) -> MaterialCatalogItem | None:
        return self._items.get(item_id)

    def find_item_by_name(self, name: str) -> MaterialCatalogItem | None:
        lowered = name.casefold()
        for item in self._items.values():
            if item.name.casefold() == lowered:
                return item
        return None


@lru_cache
def get_material_catalog() -> MaterialCatalog:
    """Возвращает кэшированный каталог материалов из JSON."""
    raw_data = _load_catalog_json()
    works = _extract_works(raw_data)

    categories: dict[str, MaterialCatalogCategory] = {}
    items: dict[str, MaterialCatalogItem] = {}
    root_ids: list[str] = []

    counters = {"category": 0, "item": 0}
    category_ids_by_name: dict[str, str] = {}
    material_ids_by_name: dict[str, str] = {}

    for work in works:
        group_name = str(work.get("group") or work.get("группа") or "Прочее")
        if group_name not in category_ids_by_name:
            counters["category"] += 1
            category_id = f"mc{counters['category']}"
            category = MaterialCatalogCategory(
                id=category_id,
                name=group_name,
                parent_id=None,
                children_ids=(),
                item_ids=(),
                path=(group_name,),
            )
            categories[category_id] = category
            category_ids_by_name[group_name] = category_id
            root_ids.append(category_id)

        category_id = category_ids_by_name[group_name]
        materials = work.get("materials") or work.get("материалы") or []
        for material in materials:
            item_name = str(material.get("name") or material.get("название") or "")
            # Не дублируем одинаковые материалы
            if item_name in material_ids_by_name:
                continue

            counters["item"] += 1
            item_id = f"m{counters['item']}"
            unit = material.get("unit") or material.get("единица")
            price = float(material.get("price_per_unit") or material.get("цена") or material.get("price") or 0)
            formula = material.get("formula") or material.get("формула")
            item = MaterialCatalogItem(
                id=item_id,
                category_id=category_id,
                name=item_name,
                unit=str(unit) if unit else None,
                price=price,
                formula=str(formula) if formula else None,
                path=(group_name, item_name),
            )
            items[item_id] = item
            material_ids_by_name[item_name] = item_id

            cat = categories[category_id]
            categories[category_id] = MaterialCatalogCategory(
                id=cat.id,
                name=cat.name,
                parent_id=cat.parent_id,
                children_ids=cat.children_ids,
                item_ids=cat.item_ids + (item_id,),
                path=cat.path,
            )

    return MaterialCatalog(categories=categories, items=items, root_ids=root_ids)


def _load_catalog_json() -> Sequence[dict]:
    if not CATALOG_FILE.exists():
        raise FileNotFoundError(f"Каталог материалов не найден: {CATALOG_FILE}")

    with CATALOG_FILE.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    return data


def _extract_works(data: Sequence[dict] | dict) -> Sequence[dict]:
    """
    Поддержка нового формата: корневой объект с ключом "works".
    Оставляем совместимость со старым списком.
    """
    if isinstance(data, dict):
        works = data.get("works")
        if isinstance(works, list):
            return works
        raise ValueError("Файл каталога должен содержать список в ключе 'works'.")
    if isinstance(data, list):
        return data
    raise ValueError("Файл каталога материалов должен содержать список категорий или ключ 'works'.")

