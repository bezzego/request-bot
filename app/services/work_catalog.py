from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence
import json


CATALOG_FILE = Path(__file__).resolve().parents[1] / "config" / "work_catalog.json"


@dataclass(slots=True, frozen=True)
class WorkCatalogItem:
    id: str
    category_id: str
    name: str
    unit: str | None
    price: float
    formula: str | None
    path: tuple[str, ...]


@dataclass(slots=True, frozen=True)
class WorkCatalogCategory:
    id: str
    name: str
    parent_id: str | None
    children_ids: tuple[str, ...]
    item_ids: tuple[str, ...]
    path: tuple[str, ...]


class WorkCatalog:
    """Предопределённые работы и категории для план/факт бюджета заявки."""

    def __init__(
        self,
        *,
        categories: dict[str, WorkCatalogCategory],
        items: dict[str, WorkCatalogItem],
        root_ids: Sequence[str],
    ) -> None:
        self._categories = categories
        self._items = items
        self._root_ids = tuple(root_ids)

    def get_root_categories(self) -> Sequence[WorkCatalogCategory]:
        return tuple(self._categories[c_id] for c_id in self._root_ids)

    def get_category(self, category_id: str) -> WorkCatalogCategory | None:
        return self._categories.get(category_id)

    def iter_child_categories(self, category_id: str | None) -> Iterable[WorkCatalogCategory]:
        if category_id is None:
            return self.get_root_categories()
        category = self._categories.get(category_id)
        if not category:
            return ()
        return tuple(self._categories[c_id] for c_id in category.children_ids)

    def iter_items(self, category_id: str) -> Iterable[WorkCatalogItem]:
        category = self._categories.get(category_id)
        if not category:
            return ()
        return tuple(self._items[item_id] for item_id in category.item_ids)

    def get_item(self, item_id: str) -> WorkCatalogItem | None:
        return self._items.get(item_id)

    def find_item_by_name(self, name: str) -> WorkCatalogItem | None:
        lowered = name.casefold()
        for item in self._items.values():
            if item.name.casefold() == lowered:
                return item
        return None


@lru_cache
def get_work_catalog() -> WorkCatalog:
    """Возвращает кэшированный каталог работ из JSON."""
    raw_data = _load_catalog_json()
    categories: dict[str, WorkCatalogCategory] = {}
    items: dict[str, WorkCatalogItem] = {}
    root_ids: list[str] = []

    counters = {"category": 0, "item": 0}

    def add_category(node: dict, parent_id: str | None, parent_path: tuple[str, ...]) -> str:
        counters["category"] += 1
        category_id = f"c{counters['category']}"
        name = str(node.get("название") or node.get("name") or "")
        path = parent_path + (name,)
        category = WorkCatalogCategory(
            id=category_id,
            name=name,
            parent_id=parent_id,
            children_ids=(),
            item_ids=(),
            path=path,
        )
        categories[category_id] = category
        if parent_id:
            parent = categories[parent_id]
            categories[parent_id] = WorkCatalogCategory(
                id=parent.id,
                name=parent.name,
                parent_id=parent.parent_id,
                children_ids=parent.children_ids + (category_id,),
                item_ids=parent.item_ids,
                path=parent.path,
            )
        else:
            root_ids.append(category_id)

        works = node.get("работы") or node.get("works") or []
        item_ids: list[str] = []
        for work in works:
            counters["item"] += 1
            item_id = f"w{counters['item']}"
            item_name = str(work.get("название") or work.get("name") or "")
            unit = work.get("единица") or work.get("unit")
            price = float(work.get("цена") or work.get("price") or 0)
            formula = work.get("формула") or work.get("formula")
            item = WorkCatalogItem(
                id=item_id,
                category_id=category_id,
                name=item_name,
                unit=str(unit) if unit else None,
                price=price,
                formula=str(formula) if formula else None,
                path=path + (item_name,),
            )
            items[item_id] = item
            item_ids.append(item_id)

        if item_ids:
            cat = categories[category_id]
            categories[category_id] = WorkCatalogCategory(
                id=cat.id,
                name=cat.name,
                parent_id=cat.parent_id,
                children_ids=cat.children_ids,
                item_ids=cat.item_ids + tuple(item_ids),
                path=cat.path,
            )

        subcategories = node.get("подкатегории") or node.get("subcategories") or []
        for child in subcategories:
            add_category(child, category_id, path)

        return category_id

    for node in raw_data:
        add_category(node, parent_id=None, parent_path=())

    return WorkCatalog(categories=categories, items=items, root_ids=root_ids)


def _load_catalog_json() -> Sequence[dict]:
    if not CATALOG_FILE.exists():
        raise FileNotFoundError(f"Каталог работ не найден: {CATALOG_FILE}")

    with CATALOG_FILE.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    if not isinstance(data, list):
        raise ValueError("Файл каталога работ должен содержать список категорий.")
    return data
