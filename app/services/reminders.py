from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime

from aiogram import Bot
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestReminder, RequestStatus
from app.infrastructure.db.session import async_session
from app.utils.timezone import format_moscow, now_moscow


class ReminderService:
    """Загрузка и отметка напоминаний."""

    @staticmethod
    async def get_due_reminders(session: AsyncSession, now: datetime) -> list[RequestReminder]:
        stmt = (
            select(RequestReminder)
            .options(
                selectinload(RequestReminder.request)
                .selectinload(Request.specialist),
                selectinload(RequestReminder.request)
                .selectinload(Request.engineer),
                selectinload(RequestReminder.request)
                .selectinload(Request.master),
                selectinload(RequestReminder.request)
                .selectinload(Request.object),
            )
            .where(
                RequestReminder.is_sent.is_(False),
                RequestReminder.scheduled_at <= now,
            )
            .order_by(RequestReminder.scheduled_at)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    def build_message(reminder: RequestReminder) -> str:
        request = reminder.request
        status_title = STATUS_TITLES.get(request.status, request.status.value)
        if reminder.reminder_type.name == "INSPECTION":
            inspection_time = format_moscow(reminder.scheduled_at) or "не задано"
            return (
                f"🔔 Напоминание об осмотре по заявке {request.number}\n"
                f"Объект: {request.object.name if request.object else request.title}\n"
                f"Время: {inspection_time}\n"
                f"Адрес: {request.address}"
            )
        if reminder.reminder_type.name == "DOCUMENT_SIGN":
            return (
                f"📝 Требуется подписать акт по заявке {request.number}.\n"
                f"Текущий статус: {status_title}. Подтвердите документы и уведомите заказчика."
            )
        if reminder.reminder_type.name == "DEADLINE":
            deadline_time = format_moscow(reminder.scheduled_at) or "не указано"
            return (
                f"⏰ Срок выполнения по заявке {request.number} истекает "
                f"{deadline_time}. Проверьте готовность и обновите отчёт."
            )
        if reminder.reminder_type.name == "OVERDUE":
            return (
                f"⚠️ Заявка {request.number} просрочена! Текущий статус: {status_title}.\n"
                f"Свяжитесь с мастером {request.master.full_name if request.master else '—'} и обновите план."
            )
        if reminder.reminder_type.name == "REPORT":
            return (
                f"📊 Контроль заявки {request.number}.\n"
                f"Статус: {status_title}. Обновите фактические данные и отправьте отчёт, если требуется."
            )
        return f"Напоминание по заявке {request.number}."

    @staticmethod
    async def mark_sent(session: AsyncSession, reminder_id: int, payload: str | None = None) -> None:
        await session.execute(
            update(RequestReminder)
            .where(RequestReminder.id == reminder_id)
            .values(
                is_sent=True,
                sent_at=now_moscow(),
                payload=payload,
            )
        )


class ReminderScheduler:
    """Простой фоновый планировщик напоминаний."""

    def __init__(self, bot: Bot, interval_seconds: int = 120):
        self.bot = bot
        self.interval_seconds = interval_seconds
        self._task: asyncio.Task | None = None
        self._running = False

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._running = True
        self._task = asyncio.create_task(self._run(), name="reminder_scheduler")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while self._running:
            try:
                async with async_session() as session:
                    now = now_moscow()
                    reminders = await ReminderService.get_due_reminders(session, now)
                    for reminder in reminders:
                        message = ReminderService.build_message(reminder)
                        recipients = [
                            int(r.strip())
                            for r in (reminder.recipients or "").split(",")
                            if r.strip()
                        ]
                        for telegram_id in recipients:
                            try:
                                await self.bot.send_message(chat_id=telegram_id, text=message)
                            except Exception as exc:  # noqa: BLE001
                                await self.bot.send_message(
                                    chat_id=telegram_id,
                                    text=f"⚠️ Ошибка отправки напоминания: {exc}",
                                )
                        await ReminderService.mark_sent(session, reminder.id, payload=message)
                    await session.commit()
            except Exception:
                # Игнорируем ошибки и повторяем цикл через паузу
                pass

            await asyncio.sleep(self.interval_seconds)


STATUS_TITLES = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначен мастер",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}
