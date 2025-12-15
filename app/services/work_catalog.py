from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Iterable, Sequence
import json


# Используем объединённый файл с работами и материалами
CATALOG_FILE = Path(__file__).resolve().parents[1] / "config" / "mat.json"


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


@dataclass(slots=True, frozen=True)
class WorkMaterialSpec:
    """Материал, привязанный к работе (для автоподсчёта по количеству работ)."""

    name: str
    unit: str | None
    qty_per_work_unit: float
    price_per_unit: float


class WorkCatalog:
    """Предопределённые работы и категории для план/факт бюджета заявки."""

    def __init__(
        self,
        *,
        categories: dict[str, WorkCatalogCategory],
        items: dict[str, WorkCatalogItem],
        root_ids: Sequence[str],
        materials_by_work: dict[str, tuple[WorkMaterialSpec, ...]],
    ) -> None:
        self._categories = categories
        self._items = items
        self._root_ids = tuple(root_ids)
        # ключ: work name (lower), значение: материалы этой работы
        self._materials_by_work = materials_by_work

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

    def get_materials_for_work(self, work_name: str) -> tuple[WorkMaterialSpec, ...]:
        """Возвращает материалы, связанные с работой (по имени, без учёта регистра)."""
        return self._materials_by_work.get(work_name.casefold(), ())

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
    works = _extract_works(raw_data)

    categories: dict[str, WorkCatalogCategory] = {}
    items: dict[str, WorkCatalogItem] = {}
    root_ids: list[str] = []
    materials_by_work: dict[str, tuple[WorkMaterialSpec, ...]] = {}

    counters = {"category": 0, "item": 0}
    category_ids_by_name: dict[str, str] = {}

    for work in works:
        group_name = str(work.get("group") or work.get("группа") or "Прочее")
        if group_name not in category_ids_by_name:
            counters["category"] += 1
            category_id = f"c{counters['category']}"
            category = WorkCatalogCategory(
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
        counters["item"] += 1
        item_id = f"w{counters['item']}"
        item_name = str(work.get("name") or work.get("название") or "")
        unit = work.get("unit") or work.get("единица")
        price = float(work.get("price_per_unit") or work.get("цена") or work.get("price") or 0)
        formula = work.get("formula") or work.get("формула")
        item = WorkCatalogItem(
            id=item_id,
            category_id=category_id,
            name=item_name,
            unit=str(unit) if unit else None,
            price=price,
            formula=str(formula) if formula else None,
            path=(group_name, item_name),
        )
        items[item_id] = item

        cat = categories[category_id]
        categories[category_id] = WorkCatalogCategory(
            id=cat.id,
            name=cat.name,
            parent_id=cat.parent_id,
            children_ids=cat.children_ids,
            item_ids=cat.item_ids + (item_id,),
            path=cat.path,
        )

        # сохраняем материалы, привязанные к работе
        materials_raw = work.get("materials") or work.get("материалы") or ()
        specs: list[WorkMaterialSpec] = []
        for material in materials_raw:
            m_name = str(material.get("name") or material.get("название") or "")
            m_unit = material.get("unit") or material.get("единица")
            qty_per_work_unit = float(material.get("qty_per_work_unit") or material.get("количество") or material.get("qty") or 0)
            price_per_unit = float(material.get("price_per_unit") or material.get("цена") or material.get("price") or 0)
            specs.append(
                WorkMaterialSpec(
                    name=m_name,
                    unit=str(m_unit) if m_unit else None,
                    qty_per_work_unit=qty_per_work_unit,
                    price_per_unit=price_per_unit,
                )
            )
        materials_by_work[item_name.casefold()] = tuple(specs)

    return WorkCatalog(
        categories=categories,
        items=items,
        root_ids=root_ids,
        materials_by_work=materials_by_work,
    )


def _load_catalog_json() -> Sequence[dict]:
    if not CATALOG_FILE.exists():
        raise FileNotFoundError(f"Каталог работ не найден: {CATALOG_FILE}")

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
        raise ValueError("Файл каталога работ должен содержать список в ключе 'works'.")
    if isinstance(data, list):
        return data
    raise ValueError("Файл каталога работ должен содержать список категорий или ключ 'works'.")
