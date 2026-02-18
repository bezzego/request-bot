"""Вспомогательные функции для модуля работы мастера."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import func, or_, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Photo, PhotoType, Request, WorkItem, WorkSession
from app.infrastructure.db.session import async_session
from app.utils.request_formatters import format_request_label

if TYPE_CHECKING:
    from app.infrastructure.db.models import User


FINISH_CONTEXT_KEY = "finish_context"
PHOTO_CONFIRM_TEXT = "✅ Подтвердить фото"
CANCEL_TEXT = "Отмена"
PHOTO_TYPES_FOR_FINISH = (PhotoType.PROCESS, PhotoType.AFTER)


@dataclass
class FinishStatus:
    """Статус завершения работы."""
    request_id: int
    request_number: str
    request_title: str
    photos_confirmed: bool
    photos_total: int
    location_ready: bool
    fact_ready: bool
    finish_location: tuple[float | None, float | None]

    @property
    def all_ready(self) -> bool:
        """Все условия выполнены."""
        return self.photos_confirmed and self.location_ready and self.fact_ready

    def missing_items(self) -> list[str]:
        """Список недостающих элементов."""
        items: list[str] = []
        if not self.photos_confirmed:
            items.append("отправьте фото готовой работы")
        if not self.location_ready:
            items.append("передайте геопозицию завершения")
        if not self.fact_ready:
            items.append("заполните факт выполненных работ")
        return items


async def load_finish_context(state) -> dict | None:
    """Загрузить контекст завершения из состояния."""
    data = await state.get_data()
    context = data.get(FINISH_CONTEXT_KEY)
    if isinstance(context, dict):
        return context
    return None


async def save_finish_context(state, context: dict | None) -> None:
    """Сохранить контекст завершения в состояние."""
    await state.update_data({FINISH_CONTEXT_KEY: context})


async def build_finish_status(
    session,
    request: Request,
    finish_context: dict,
) -> FinishStatus:
    """Построить статус завершения работы."""
    photo_total = int(finish_context.get("new_photo_count") or 0)
    has_fact = bool(
        await session.scalar(
            select(func.count(WorkItem.id)).where(
                WorkItem.request_id == request.id,
                or_(
                    func.coalesce(WorkItem.actual_quantity, 0) > 0,
                    func.coalesce(WorkItem.actual_cost, 0) > 0,
                ),
            )
        )
    )
    fact_ready = has_fact and bool(finish_context.get("fact_confirmed"))
    latitude = finish_context.get("finish_latitude")
    longitude = finish_context.get("finish_longitude")
    return FinishStatus(
        request_id=request.id,
        request_number=format_request_label(request),
        request_title=request.title,
        photos_confirmed=bool(finish_context.get("photos_confirmed")),
        photos_total=photo_total,
        location_ready=latitude is not None and longitude is not None,
        fact_ready=fact_ready,
        finish_location=(latitude, longitude),
    )


def format_finish_summary(request: Request, status: FinishStatus) -> str:
    """Форматировать сводку завершения работы."""
    lines = [
        f"🧾 <b>{status.request_number}</b> · {request.title}",
        "",
        "Чтобы завершить работы, выполните условия:",
        format_finish_line("Фото готовой работы", status.photos_confirmed, extra=f"{status.photos_total} шт."),
        format_finish_line("Геопозиция завершения", status.location_ready),
        format_finish_line("Факт выполненных работ", status.fact_ready),
    ]
    lines.append("")
    if status.all_ready:
        lines.append("Все условия выполнены — закройте смену или завершите заявку.")
    else:
        lines.append("После выполнения каждого шага кнопка исчезнет из списка.")
    return "\n".join(lines)


def format_finish_line(label: str, ready: bool, *, extra: str | None = None) -> str:
    """Форматировать строку условия завершения."""
    prefix = "✅" if ready else "▫️"
    text = f"{prefix} {label}"
    if extra:
        text = f"{text} · {extra}"
    return text


def format_location_url(latitude: float, longitude: float) -> str:
    """Форматировать URL геопозиции."""
    return f"https://www.google.com/maps?q={latitude},{longitude}"


async def notify_engineer(
    bot,
    request: Request | None,
    text: str,
    *,
    location: tuple[float, float] | None = None,
) -> None:
    """Уведомить инженера о событии."""
    if not bot or not request or not request.engineer or not request.engineer.telegram_id:
        return
    try:
        await bot.send_message(request.engineer.telegram_id, text)
        if location:
            lat, lon = location
            await bot.send_location(request.engineer.telegram_id, latitude=lat, longitude=lon)
    except Exception as exc:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning("Failed to notify engineer for request %s: %s", request.number, exc)


def build_finish_summary_keyboard(status: FinishStatus):
    """Создать клавиатуру для сводки завершения работы."""
    from aiogram.utils.keyboard import InlineKeyboardBuilder
    
    builder = InlineKeyboardBuilder()
    request_id = status.request_id
    if not status.photos_confirmed:
        builder.button(text="📷 Отправить фото", callback_data=f"master:finish_photo:{request_id}")
    if not status.location_ready:
        builder.button(text="📍 Отправить геопозицию", callback_data=f"master:finish_geo:{request_id}")
    if not status.fact_ready:
        builder.button(text="📊 Заполнить факт", callback_data=f"master:update_fact:{request_id}")
    if status.all_ready:
        builder.button(
            text="⏸ Закрыть смену",
            callback_data=f"master:finish_submit:{request_id}:session",
        )
        builder.button(
            text="🏁 Завершить полностью",
            callback_data=f"master:finish_submit:{request_id}:final",
        )
    builder.button(text="❌ Отменить", callback_data="master:finish_cancel")
    builder.adjust(1)
    return builder.as_markup()


async def send_finish_report(
    bot,
    request: Request,
    master: User,
    status: FinishStatus,
    *,
    finalized: bool,
) -> None:
    """Отправить отчет о завершении работы инженеру."""
    if not bot or not request.engineer or not request.engineer.telegram_id:
        return

    from aiogram.types import InputMediaPhoto
    
    async with async_session() as session:
        photos = (
            await session.execute(
                select(Photo)
                .where(
                    Photo.request_id == request.id,
                    Photo.type.in_(PHOTO_TYPES_FOR_FINISH),
                )
                .order_by(Photo.created_at.asc())
            )
        ).scalars().all()

    verb = "завершил работы" if finalized else "завершил смену"
    label = format_request_label(request)
    caption_lines = [
        f"✅ Мастер {master.full_name} {verb} по заявке {label}.",
    ]
    if not finalized:
        caption_lines.append("Статус заявки остаётся «В работе».")
    caption_lines.append(f"📷 Фотоотчёт: {len(photos)} шт." if photos else "Фотоотчёт отсутствует.")
    if status.location_ready and status.finish_location[0] is not None and status.finish_location[1] is not None:
        lat, lon = status.finish_location
        caption_lines.append(f"📍 {format_location_url(lat, lon)}")
    caption_text = "\n".join(caption_lines)

    try:
        if photos:
            media: list[InputMediaPhoto] = []
            for idx, photo in enumerate(photos):
                caption = caption_text if idx == 0 else None
                media.append(InputMediaPhoto(media=photo.file_id, caption=caption))
            await bot.send_media_group(request.engineer.telegram_id, media)
        else:
            await bot.send_message(request.engineer.telegram_id, caption_text)
    except Exception as exc:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning("Failed to send finish report to engineer for request %s: %s", request.number, exc)


async def render_finish_summary(bot, finish_context: dict, state) -> None:
    """Отрендерить сводку завершения работы."""
    if not bot or not finish_context:
        return

    chat_id = finish_context.get("chat_id")
    if not chat_id:
        return

    from aiogram.exceptions import TelegramBadRequest
    import logging
    logger = logging.getLogger(__name__)

    async with async_session() as session:
        request = await session.scalar(
            select(Request)
            .options(selectinload(Request.engineer))
            .where(Request.id == finish_context["request_id"])
        )
        if not request:
            await save_finish_context(state, None)
            return
        status = await build_finish_status(session, request, finish_context)

    text = format_finish_summary(request, status)
    keyboard = build_finish_summary_keyboard(status)
    message_id = finish_context.get("message_id")

    if message_id:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except TelegramBadRequest as exc:
            error_text = str(exc).lower()
            if "message to delete not found" in error_text or "message can't be deleted" in error_text:
                pass
            else:
                raise
        except Exception as exc:
            logger.warning("Failed to delete previous finish summary: %s", exc)

    try:
        sent = await bot.send_message(chat_id, text, reply_markup=keyboard)
        finish_context["message_id"] = sent.message_id
    except Exception as exc:
        logger.warning("Failed to render finish summary: %s", exc)
    finally:
        finish_context["photos_confirmed"] = status.photos_confirmed
        await save_finish_context(state, finish_context)


async def cleanup_finish_summary(bot, finish_context: dict | None, final_text: str) -> None:
    """Очистить сводку завершения работы."""
    if not bot or not finish_context:
        return
    message_id = finish_context.get("message_id")
    chat_id = finish_context.get("chat_id")
    if not message_id or not chat_id:
        return
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass
    try:
        await bot.send_message(chat_id, final_text)
    except Exception:
        pass


async def refresh_finish_summary_from_context(
    bot,
    state,
    *,
    request_id: int | None = None,
) -> None:
    """Обновить сводку завершения из контекста."""
    finish_context = await load_finish_context(state)
    if not finish_context:
        return
    if request_id and finish_context.get("request_id") != request_id:
        return
    await render_finish_summary(bot, finish_context, state)
