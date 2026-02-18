"""Модуль обработчиков специалиста."""
from aiogram import Router

from .analytics import router as analytics_router
from .close import router as close_router
from .list import router as list_router
from .detail import router as detail_router
from .filters import router as filters_router
from .create import router as create_router

router = Router()

# Регистрируем роутеры из подмодулей
router.include_router(analytics_router)
router.include_router(close_router)
router.include_router(list_router)
router.include_router(detail_router)
router.include_router(filters_router)
router.include_router(create_router)

# Временно импортируем остальные обработчики из legacy файла
# TODO: Постепенно перенести все обработчики из legacy в соответствующие модули
from app.handlers.specialist_legacy import router as legacy_router

router.include_router(legacy_router)
