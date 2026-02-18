"""Модуль обработчиков инженера."""
from aiogram import Router

from .list import router as list_router
from .create import router as create_router
from .detail import router as detail_router
from .filters import router as filters_router
from .inspection import router as inspection_router
from .master_assignment import router as master_assignment_router
from .budget import router as budget_router

router = Router()

# Регистрируем роутеры из подмодулей
router.include_router(list_router)
router.include_router(create_router)
router.include_router(detail_router)
router.include_router(filters_router)
router.include_router(inspection_router)
router.include_router(master_assignment_router)
router.include_router(budget_router)

# Временно импортируем остальные обработчики из legacy файла
# TODO: Постепенно перенести все обработчики из engineer.py в соответствующие модули
# Используем importlib для загрузки модуля напрямую из файла, чтобы избежать циклической зависимости
import importlib.util
import sys
from pathlib import Path

# Загружаем модуль напрямую из файла engineer.py
engineer_file = Path(__file__).parent.parent / "engineer.py"
if engineer_file.exists():
    spec = importlib.util.spec_from_file_location("engineer_legacy", engineer_file)
    if spec and spec.loader:
        engineer_legacy = importlib.util.module_from_spec(spec)
        sys.modules["engineer_legacy"] = engineer_legacy
        spec.loader.exec_module(engineer_legacy)
        router.include_router(engineer_legacy.router)
