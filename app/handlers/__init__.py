from aiogram import Dispatcher

from .start import router as start_router
from .admin import router as admin_router
from .specialist import router as specialist_router
from .engineer import router as engineer_router
from .master import router as master_router
from .manager import router as manager_router
from .client import router as client_router
from .catalog_settings import router as catalog_settings_router


ROUTERS = [
    start_router,
    admin_router,
    specialist_router,
    engineer_router,
    master_router,
    manager_router,
    client_router,
    catalog_settings_router,
]


def register_routers(dispatcher: Dispatcher) -> None:
    for router in ROUTERS:
        dispatcher.include_router(router)
