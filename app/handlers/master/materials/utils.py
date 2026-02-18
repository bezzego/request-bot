"""Вспомогательные функции для модуля материалов мастера."""
from __future__ import annotations

from aiogram.exceptions import TelegramBadRequest
from aiogram.types import Message
from sqlalchemy import func, select

from app.infrastructure.db.models import WorkItem
from app.utils.request_formatters import format_request_label


async def get_work_item(session, request_id: int, name: str) -> WorkItem | None:
    """Получить позицию работ по названию."""
    return await session.scalar(
        select(WorkItem).where(
            WorkItem.request_id == request_id,
            func.lower(WorkItem.name) == name.lower(),
        )
    )


def catalog_header(request) -> str:
    """Сформировать заголовок для каталога."""
    return f"Заявка {format_request_label(request)} · {request.title}"


async def update_catalog_message(message: Message, text: str, markup) -> None:
    """Обновить сообщение каталога."""
    try:
        await message.edit_text(text, reply_markup=markup)
    except TelegramBadRequest as exc:
        if "message is not modified" in str(exc).lower():
            await message.edit_reply_markup(reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)


def format_currency(value: float | None) -> str:
    """Форматировать валюту."""
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")
