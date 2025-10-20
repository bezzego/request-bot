from __future__ import annotations

import asyncio
import contextlib
from datetime import datetime, timezone

from aiogram import Bot
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestReminder
from app.infrastructure.db.session import async_session


class ReminderService:
    """Загрузка и отметка напоминаний."""

    @staticmethod
    async def get_due_reminders(session: AsyncSession, now: datetime) -> list[RequestReminder]:
        stmt = (
            select(RequestReminder)
            .options(selectinload(RequestReminder.request).selectinload(Request.specialist))
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
        if reminder.reminder_type.name == "INSPECTION":
            return (
                f"🔔 Напоминание об осмотре по заявке {request.number}\n"
                f"Объект: {request.object.name if request.object else request.title}\n"
                f"Время: {reminder.scheduled_at:%d.%m.%Y %H:%M}\n"
                f"Адрес: {request.address}"
            )
        if reminder.reminder_type.name == "DOCUMENT_SIGN":
            return (
                f"📝 Требуется подписать акт по заявке {request.number}.\n"
                f"Ответственный инженер: {request.engineer.full_name if request.engineer else '—'}."
            )
        if reminder.reminder_type.name == "DEADLINE":
            return (
                f"⏰ Срок выполнения по заявке {request.number} истекает "
                f"{reminder.scheduled_at:%d.%m.%Y %H:%M}."
            )
        if reminder.reminder_type.name == "OVERDUE":
            return (
                f"⚠️ Заявка {request.number} просрочена. "
                f"Свяжитесь с мастером {request.master.full_name if request.master else '—'}."
            )
        return f"Напоминание по заявке {request.number}."

    @staticmethod
    async def mark_sent(session: AsyncSession, reminder_id: int, payload: str | None = None) -> None:
        await session.execute(
            update(RequestReminder)
            .where(RequestReminder.id == reminder_id)
            .values(
                is_sent=True,
                sent_at=datetime.now(timezone.utc),
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
                    now = datetime.now(timezone.utc)
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
