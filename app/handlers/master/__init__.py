"""Модуль обработчиков мастера."""
from aiogram import Router

from .list import router as list_router
from .detail import router as detail_router
from .work import router as work_router
from .materials import router as materials_router
from .photos import router as photos_router

router = Router()

# Регистрируем роутеры из подмодулей
router.include_router(list_router)
router.include_router(detail_router)
router.include_router(work_router)
router.include_router(materials_router)
router.include_router(photos_router)
