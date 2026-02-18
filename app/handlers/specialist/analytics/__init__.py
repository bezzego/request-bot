"""Модуль аналитики для специалиста."""
from collections import Counter

from aiogram import F, Router
from aiogram.types import Message

from app.infrastructure.db.models import Request, RequestStatus
from app.infrastructure.db.session import async_session
from app.utils.request_formatters import STATUS_TITLES, format_hours_minutes
from app.utils.timezone import now_moscow, format_moscow
from app.handlers.specialist.utils import get_specialist, load_specialist_requests

router = Router()


def format_currency(value: float | None) -> str:
    """Форматирует валюту."""
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def build_specialist_analytics(requests: list[Request]) -> str:
    """Строит текст аналитики по заявкам специалиста."""
    now = now_moscow()
    status_counter = Counter(req.status for req in requests)
    total = len(requests)
    active = sum(1 for req in requests if req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED})
    overdue = sum(
        1
        for req in requests
        if req.due_at and req.due_at < now and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED}
    )
    closed = status_counter.get(RequestStatus.CLOSED, 0)

    planned_budget = float(sum(req.planned_budget or 0 for req in requests))
    actual_budget = float(sum(req.actual_budget or 0 for req in requests))
    planned_hours = float(sum(req.planned_hours or 0 for req in requests))
    actual_hours = float(sum(req.actual_hours or 0 for req in requests))

    durations = []
    for req in requests:
        if req.work_started_at and req.work_completed_at:
            durations.append((req.work_completed_at - req.work_started_at).total_seconds() / 3600)
    avg_duration = sum(durations) / len(durations) if durations else 0

    lines = [
        "📊 <b>Аналитика по вашим заявкам</b>",
        f"Всего заявок: {total}",
        f"Активные: {active}",
        f"Закрытые: {closed}",
        f"Просроченные: {overdue}",
        "",
        f"Плановый бюджет суммарно: {format_currency(planned_budget)} ₽",
        f"Фактический бюджет суммарно: {format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {format_currency(actual_budget - planned_budget)} ₽",
        f"Плановые часы суммарно: {format_hours_minutes(planned_hours)}",
        f"Фактические часы суммарно: {format_hours_minutes(actual_hours)}",
        f"Средняя длительность закрытой заявки: {format_hours_minutes(avg_duration)}",
    ]

    if status_counter:
        lines.append("")
        lines.append("Статусы:")
        for status, count in status_counter.most_common():
            lines.append(f"• {STATUS_TITLES.get(status, status.value)} — {count}")

    upcoming = [
        req
        for req in requests
        if req.due_at and req.status not in {RequestStatus.CLOSED, RequestStatus.CANCELLED} and 0 <= (req.due_at - now).total_seconds() <= 72 * 3600
    ]
    if upcoming:
        lines.append("")
        lines.append("⚠️ Срок закрытия в ближайшие 72 часа:")
        for req in upcoming:
            due_text = format_moscow(req.due_at) or "не задан"
            lines.append(f"• {req.number} — до {due_text}")

    return "\n".join(lines)


@router.message(F.text == "📊 Аналитика")
async def specialist_analytics(message: Message):
    """Обработчик команды аналитики."""
    async with async_session() as session:
        specialist = await get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        requests = await load_specialist_requests(session, specialist.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Создайте заявку, чтобы начать работу.")
        return

    summary_text = build_specialist_analytics(requests)
    await message.answer(summary_text)
