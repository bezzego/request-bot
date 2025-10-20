from __future__ import annotations

import asyncio

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from app.config.settings import settings
from app.handlers import register_routers
from app.services.reminders import ReminderScheduler


async def main() -> None:
    bot = Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dispatcher = Dispatcher()
    register_routers(dispatcher)

    reminder_scheduler = ReminderScheduler(bot)
    await reminder_scheduler.start()

    try:
        await dispatcher.start_polling(bot)
    finally:
        await reminder_scheduler.stop()


if __name__ == "__main__":
    asyncio.run(main())
