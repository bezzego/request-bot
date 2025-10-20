from __future__ import annotations

from datetime import datetime, timedelta

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import FSInputFile, Message
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.handlers.admin import admin_kb
from app.infrastructure.db.models import Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.services.export import ExportService
from app.services.reporting import ReportingService


router = Router()


async def _get_manager(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.MANAGER)
    )


@router.message(F.text == "👥 Управление пользователями")
async def handle_admin_menu(message: Message):
    async with async_session() as session:
        manager = await _get_manager(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только руководителям.")
            return
    await message.answer("Выберите действие:", reply_markup=admin_kb)


@router.message(F.text == "📊 Отчёты и статистика")
async def manager_reports(message: Message):
    now = datetime.now()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    async with async_session() as session:
        manager = await _get_manager(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только руководителям.")
            return

        summary = await ReportingService.period_summary(session, start=start, end=now)
        rating = await ReportingService.engineer_rating(session, start=start, end=now)
        feedback = await ReportingService.feedback_summary(session, start=start, end=now)

    lines = [
        "📊 <b>Отчёт по текущему месяцу</b>",
        f"Заявок создано: {summary.total_created}",
        f"Заявок закрыто: {summary.total_closed}",
        f"Активных: {summary.total_active}",
        f"Плановый бюджет: {summary.planned_budget:,.2f} ₽",
        f"Фактический бюджет: {summary.actual_budget:,.2f} ₽",
        f"Отклонение бюджета: {summary.budget_delta:,.2f} ₽",
        f"Плановые часы: {summary.planned_hours:,.1f}",
        f"Фактические часы: {summary.actual_hours:,.1f}",
        f"Среднее время на заявку: {summary.avg_hours_per_request:,.1f} ч",
        f"Закрыто в срок: {summary.closed_in_time} ( {summary.on_time_percent:.1f}% )",
        f"Просрочено: {summary.closed_overdue}",
        f"Среднее время выполнения: {summary.average_completion_time_hours:,.1f} ч",
        f"Общие затраты (750 ₽/ч): {summary.total_costs:,.2f} ₽",
        f"Индекс эффективности: {summary.efficiency_percent:.1f}%",
        f"Средние оценки клиентов: качество {feedback['quality']:.1f}, сроки {feedback['time']:.1f}, культура {feedback['culture']:.1f}",
    ]

    if rating:
        lines.append("\n🏆 <b>Рейтинг инженеров</b>")
        for position, engineer in enumerate(rating, start=1):
            lines.append(
                f"{position}. {engineer.full_name} — {engineer.closed_requests} заявок, "
                f"эффективность {engineer.efficiency_percent:.1f}%"
            )
    else:
        lines.append("\nНет закрытых заявок за период для расчёта рейтинга.")

    lines.append("\nИспользуйте команду /export_requests для выгрузки CSV.")
    await message.answer("\n".join(lines))


@router.message(F.text == "📋 Все заявки")
async def show_recent_requests(message: Message):
    async with async_session() as session:
        manager = await _get_manager(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только руководителям.")
            return

        stmt = (
            select(Request)
            .options(selectinload(Request.engineer))
            .order_by(Request.created_at.desc())
            .limit(10)
        )
        requests = (await session.execute(stmt)).scalars().all()

        if not requests:
            await message.answer("Заявки ещё не созданы.")
            return

        lines = ["📋 <b>Последние 10 заявок:</b>"]
        for req in requests:
            lines.append(
                f"#{req.number} — {req.title}\n"
                f"Статус: {req.status.value} | Инженер: {req.engineer.full_name if req.engineer else '—'}"
            )

    await message.answer("\n\n".join(lines))


@router.message(Command("export_requests"))
async def export_requests(message: Message):
    now = datetime.now()
    start = now - timedelta(days=30)

    async with async_session() as session:
        manager = await _get_manager(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только руководителям.")
            return

        path = await ExportService.export_requests(session, start=start, end=now)

    await message.answer_document(FSInputFile(path), caption="Выгрузка заявок за последние 30 дней")
