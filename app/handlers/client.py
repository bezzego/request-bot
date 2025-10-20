from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message
from sqlalchemy import select

from app.infrastructure.db.models import Feedback, Request, User, UserRole
from app.infrastructure.db.session import async_session

router = Router()



@router.message(F.text == "⭐️ Оставить отзыв")
async def client_feedback_help(message: Message):
    await message.answer("Чтобы оставить отзыв, используйте команду:\n"
                        "/feedback <номер> <качество 1-5> <сроки 1-5> <культура 1-5> [комментарий]")


@router.message(F.text == "📋 Мои заявки")
async def client_requests_placeholder(message: Message):
    await message.answer("Статус вашей заявки вы можете уточнить у инженера или специалиста.\n"
                        "В будущих версиях бот покажет информацию автоматически.")
async def _get_client(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.CLIENT)
    )


@router.message(Command("feedback"))
async def submit_feedback(message: Message):
    parts = message.text.split(maxsplit=5)
    if len(parts) < 5:
        await message.answer(
            "Использование: /feedback <номер> <качество 1-5> <сроки 1-5> <культура 1-5> [комментарий]"
        )
        return

    _, number, quality, timing, culture, *comment = parts
    try:
        quality = int(quality)
        timing = int(timing)
        culture = int(culture)
    except ValueError:
        await message.answer("Оценки должны быть целыми числами от 1 до 5.")
        return

    if not all(1 <= value <= 5 for value in (quality, timing, culture)):
        await message.answer("Оценки должны быть в диапазоне 1-5.")
        return

    comment_text = comment[0] if comment else None

    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Отправить отзыв могут только заказчики.")
            return

        request = await session.scalar(select(Request).where(Request.number == number))
        if not request:
            await message.answer("Заявка не найдена.")
            return

        feedback = await session.scalar(select(Feedback).where(Feedback.request_id == request.id))
        if not feedback:
            feedback = Feedback(request_id=request.id)
            session.add(feedback)

        feedback.rating_quality = quality
        feedback.rating_time = timing
        feedback.rating_culture = culture
        feedback.comment = comment_text

        await session.commit()

    await message.answer("Спасибо! Отзыв сохранён.")
