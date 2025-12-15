from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Feedback, Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.utils.request_formatters import format_request_label
from app.utils.timezone import format_moscow

router = Router()


class FeedbackStates(StatesGroup):
    waiting_quality = State()
    waiting_time = State()
    waiting_culture = State()
    waiting_comment = State()


STATUS_TITLES = {
    RequestStatus.NEW: "В обработке",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначен мастер",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "На согласовании",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


@router.message(F.text == "📋 Мои заявки")
async def client_requests(message: Message):
    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Доступно только заказчикам.")
            return

        requests = await _load_client_requests(session, client.id)

    if not requests:
        await message.answer("Для вас пока нет заявок. Свяжитесь со специалистом.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status = STATUS_TITLES.get(req.status, req.status.value)
        builder.button(
            text=f"{format_request_label(req)} · {status}",
            callback_data=f"client:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы посмотреть статус и сроки.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("client:detail:"))
async def client_request_detail(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        client = await _get_client(session, callback.from_user.id)
        if not client:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, client.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена.")
        await callback.answer()
        return

    await _show_request_detail(callback.message, request, edit=True)
    await callback.answer()


@router.callback_query(F.data == "client:back")
async def client_back(callback: CallbackQuery):
    async with async_session() as session:
        client = await _get_client(session, callback.from_user.id)
        if not client:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        requests = await _load_client_requests(session, client.id)

    if not requests:
        await callback.message.edit_text("Для вас пока нет заявок.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{format_request_label(req)} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"client:detail:{req.id}",
        )
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите заявку, чтобы посмотреть статус.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.message(F.text == "⭐️ Оставить отзыв")
async def client_feedback_list(message: Message):
    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Доступно только заказчикам.")
            return

        requests = await _load_client_requests(session, client.id)

    eligible = [
        req
        for req in requests
        if req.status in {RequestStatus.COMPLETED, RequestStatus.READY_FOR_SIGN, RequestStatus.CLOSED}
    ]

    if not eligible:
        await message.answer("Нет заявок, доступных для оценки.")
        return

    builder = InlineKeyboardBuilder()
    for req in eligible:
        builder.button(
            text=f"{format_request_label(req)} · {req.title}",
            callback_data=f"client:feedback:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы оставить отзыв о качестве работ.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("client:feedback:"))
async def client_feedback_start(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(FeedbackStates.waiting_quality)
    await state.update_data(request_id=request_id, ratings={})

    await callback.message.answer(
        "Оцените качество работ (1 — плохо, 5 — отлично):",
        reply_markup=_rating_keyboard("quality"),
    )
    await callback.answer()


@router.callback_query(StateFilter(FeedbackStates.waiting_quality), F.data.startswith("client:rate:quality:"))
async def client_feedback_quality(callback: CallbackQuery, state: FSMContext):
    value = int(callback.data.split(":")[3])
    data = await state.get_data()
    ratings = data.get("ratings", {})
    ratings["quality"] = value
    await state.update_data(ratings=ratings)

    await callback.message.edit_text(f"Оценка качества: {value}/5")
    await callback.message.answer(
        "Оцените соблюдение сроков:",
        reply_markup=_rating_keyboard("time"),
    )
    await state.set_state(FeedbackStates.waiting_time)
    await callback.answer()


@router.callback_query(StateFilter(FeedbackStates.waiting_time), F.data.startswith("client:rate:time:"))
async def client_feedback_time(callback: CallbackQuery, state: FSMContext):
    value = int(callback.data.split(":")[3])
    data = await state.get_data()
    ratings = data.get("ratings", {})
    ratings["time"] = value
    await state.update_data(ratings=ratings)

    await callback.message.edit_text(f"Оценка соблюдения сроков: {value}/5")
    await callback.message.answer(
        "Оцените культуру производства работ:",
        reply_markup=_rating_keyboard("culture"),
    )
    await state.set_state(FeedbackStates.waiting_culture)
    await callback.answer()


@router.callback_query(StateFilter(FeedbackStates.waiting_culture), F.data.startswith("client:rate:culture:"))
async def client_feedback_culture(callback: CallbackQuery, state: FSMContext):
    value = int(callback.data.split(":")[3])
    data = await state.get_data()
    ratings = data.get("ratings", {})
    ratings["culture"] = value
    await state.update_data(ratings=ratings)

    await callback.message.edit_text(f"Оценка культуры производства: {value}/5")
    await callback.message.answer("Добавьте комментарий (или отправьте «-», чтобы пропустить).")
    await state.set_state(FeedbackStates.waiting_comment)
    await callback.answer()


@router.message(StateFilter(FeedbackStates.waiting_comment))
async def client_feedback_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    request_id = data.get("request_id")
    ratings = data.get("ratings", {})
    comment = None if message.text.strip() == "-" else message.text.strip()

    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, client.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        feedback = await session.scalar(select(Feedback).where(Feedback.request_id == request.id))
        if not feedback:
            feedback = Feedback(request_id=request.id)
            session.add(feedback)

        feedback.rating_quality = ratings.get("quality")
        feedback.rating_time = ratings.get("time")
        feedback.rating_culture = ratings.get("culture")
        feedback.comment = comment
        await session.commit()

    await message.answer("Спасибо! Отзыв сохранён и будет учтён в KPI команды.")
    await state.clear()


@router.message(F.text == "💬 Поддержка")
async def client_support(message: Message):
    await message.answer(
        "По вопросам качества работ и сроков обращайтесь:\n"
        "• Инженер сопровождения — через чат бота\n"
        "• Горячая линия: +7 (800) 500-00-00\n"
        "• Email: support@example.com"
    )


# --- служебные функции ---


async def _get_client(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.CLIENT)
    )


async def _load_client_requests(session, client_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.engineer),
                    selectinload(Request.master),
                )
                .where(Request.customer_id == client_id)
                .order_by(Request.created_at.desc())
                .limit(20)
            )
        )
        .scalars()
        .all()
    )


async def _load_request(session, client_id: int, request_id: int) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.engineer),
            selectinload(Request.master),
            selectinload(Request.work_items),
            selectinload(Request.feedback),
        )
        .where(Request.id == request_id, Request.customer_id == client_id)
    )


def _format_request_detail(request: Request) -> str:
    status = STATUS_TITLES.get(request.status, request.status.value)
    due = format_moscow(request.due_at) or "не задан"
    engineer = request.engineer.full_name if request.engineer else "—"
    master = request.master.full_name if request.master else "—"
    label = format_request_label(request)

    lines = [
        f"📄 <b>{label}</b>",
        f"Название: {request.title}",
        f"Статус: {status}",
        f"Срок устранения: {due}",
        f"Инженер: {engineer}",
        f"Мастер: {master}",
        "",
        f"Фактический бюджет: {_format_currency(request.actual_budget)} ₽",
    ]

    if request.work_items:
        lines.append("")
        lines.append("Основные работы:")
        for item in request.work_items[:5]:
            lines.append(
                f"• {item.name} — факт {_format_currency(item.actual_cost)} ₽"
            )

    if request.feedback:
        fb = request.feedback[-1]
        lines.append("")
        lines.append(
            f"Ваша оценка: качество {fb.rating_quality or '—'}, сроки {fb.rating_time or '—'}, культура {fb.rating_culture or '—'}"
        )

    lines.append("")
    lines.append("Чтобы оставить отзыв, используйте кнопку «⭐️ Оставить отзыв».")
    return "\n".join(lines)


def _rating_keyboard(stage: str):
    builder = InlineKeyboardBuilder()
    for value in range(1, 6):
        builder.button(text=str(value), callback_data=f"client:rate:{stage}:{value}")
    builder.adjust(5)
    return builder.as_markup()


async def _show_request_detail(message: Message, request: Request, *, edit: bool = False) -> None:
    text = _format_request_detail(request)
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data="client:back")
    builder.adjust(1)
    try:
        if edit:
            await message.edit_text(text, reply_markup=builder.as_markup())
        else:
            await message.answer(text, reply_markup=builder.as_markup())
    except Exception:
        await message.answer(text, reply_markup=builder.as_markup())


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")
