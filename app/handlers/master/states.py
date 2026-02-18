"""Состояния FSM для мастера."""
from __future__ import annotations

from aiogram.fsm.state import State, StatesGroup


class MasterStates(StatesGroup):
    """Состояния FSM для мастера."""
    waiting_start_location = State()  # Ожидание геопозиции для начала работы
    finish_dashboard = State()  # Требования к завершению
    finish_photo_upload = State()  # Сбор фото готовой работы
    waiting_finish_location = State()  # Ожидание геопозиции для завершения работы
    schedule_date = State()  # Плановые выходы мастера
    quantity_input = State()  # Ввод количества вручную
