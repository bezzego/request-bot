from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Feedback, Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.utils.pagination import clamp_page, total_pages_for
from app.utils.request_formatters import format_request_label, STATUS_TITLES
from app.utils.timezone import format_moscow

router = Router()


class FeedbackStates(StatesGroup):
    waiting_quality = State()
    waiting_time = State()
    waiting_culture = State()
    waiting_comment = State()


REQUESTS_PAGE_SIZE = 10


async def _fetch_client_requests_page(
    session,
    client_id: int,
    page: int,
    status_filter: set[RequestStatus] | None = None,
) -> tuple[list[Request], int, int, int]:
    conditions = [Request.customer_id == client_id]
    if status_filter:
        conditions.append(Request.status.in_(status_filter))
    total = await session.scalar(select(func.count()).select_from(Request).where(*conditions))
    total = int(total or 0)
    total_pages = total_pages_for(total, REQUESTS_PAGE_SIZE)
    page = clamp_page(page, total_pages)
    requests = (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.engineer),
                    selectinload(Request.master),
                )
                .where(*conditions)
                .order_by(Request.created_at.desc())
                .limit(REQUESTS_PAGE_SIZE)
                .offset(page * REQUESTS_PAGE_SIZE)
            )
        )
        .scalars()
        .all()
    )
    return requests, page, total_pages, total


async def _show_client_requests_list(
    message: Message,
    session,
    client_id: int,
    page: int,
    *,
    edit: bool = False,
) -> None:
    requests, page, total_pages, total = await _fetch_client_requests_page(session, client_id, page)
    if not requests:
        text = "Для вас пока нет заявок. Свяжитесь со специалистом."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    builder = InlineKeyboardBuilder()
    start_index = page * REQUESTS_PAGE_SIZE
    for idx, req in enumerate(requests, start=start_index + 1):
        status = STATUS_TITLES.get(req.status, req.status.value)
        builder.button(
            text=f"{idx}. {format_request_label(req)} · {status}",
            callback_data=f"client:detail:{req.id}:{page}",
        )
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"client:list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="client:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"client:list:{page + 1}"))
        builder.row(*nav)

    text = (
        "Выберите заявку, чтобы посмотреть статус и сроки."
        f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    )

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


async def _show_client_feedback_list(
    message: Message,
    session,
    client_id: int,
    page: int,
    *,
    edit: bool = False,
) -> None:
    eligible_statuses = {
        RequestStatus.COMPLETED,
        RequestStatus.READY_FOR_SIGN,
        RequestStatus.CLOSED,
    }
    requests, page, total_pages, total = await _fetch_client_requests_page(
        session,
        client_id,
        page,
        status_filter=eligible_statuses,
    )
    if not requests:
        text = "Нет заявок, доступных для оценки."
        if edit:
            await message.edit_text(text)
        else:
            await message.answer(text)
        return

    builder = InlineKeyboardBuilder()
    start_index = page * REQUESTS_PAGE_SIZE
    for idx, req in enumerate(requests, start=start_index + 1):
        builder.button(
            text=f"{idx}. {format_request_label(req)} · {req.title}",
            callback_data=f"client:feedback:{req.id}:{page}",
        )
    builder.adjust(1)

    if total_pages > 1:
        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"client:feedback_list:{page - 1}"))
        nav.append(InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="client:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="➡️", callback_data=f"client:feedback_list:{page + 1}"))
        builder.row(*nav)

    text = (
        "Выберите заявку, чтобы оставить отзыв о качестве работ."
        f"\n\nСтраница {page + 1}/{total_pages} · Всего: {total}"
    )

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


@router.message(F.text == "📋 Мои заявки")
async def client_requests(message: Message):
    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Доступно только заказчикам.")
            return

        await _show_client_requests_list(message, session, client.id, page=0)


@router.callback_query(F.data.startswith("client:list:"))
async def client_requests_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        client = await _get_client(session, callback.from_user.id)
        if not client:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_client_requests_list(
            callback.message,
            session,
            client.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("client:feedback_list:"))
async def client_feedback_list_page(callback: CallbackQuery):
    try:
        page = int(callback.data.split(":")[2])
    except (ValueError, IndexError):
        page = 0
    async with async_session() as session:
        client = await _get_client(session, callback.from_user.id)
        if not client:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_client_feedback_list(
            callback.message,
            session,
            client.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.callback_query(F.data == "client:noop")
async def client_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("client:detail:"))
async def client_request_detail(callback: CallbackQuery):
    parts = callback.data.split(":")
    request_id = int(parts[2])
    page = 0
    if len(parts) >= 4:
        try:
            page = int(parts[3])
        except ValueError:
            page = 0
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

    await _show_request_detail(callback.message, request, edit=True, list_page=page)
    await callback.answer()


@router.callback_query(F.data.startswith("client:back"))
async def client_back(callback: CallbackQuery):
    parts = callback.data.split(":")
    page = 0
    if len(parts) >= 3:
        try:
            page = int(parts[2])
        except ValueError:
            page = 0
    async with async_session() as session:
        client = await _get_client(session, callback.from_user.id)
        if not client:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        await _show_client_requests_list(
            callback.message,
            session,
            client.id,
            page=page,
            edit=True,
        )
    await callback.answer()


@router.message(F.text == "⭐️ Оставить отзыв")
async def client_feedback_list(message: Message):
    async with async_session() as session:
        client = await _get_client(session, message.from_user.id)
        if not client:
            await message.answer("Доступно только заказчикам.")
            return

        await _show_client_feedback_list(message, session, client.id, page=0)


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
                    selectinload(Request.object),
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
            selectinload(Request.object),
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

    # Рассчитываем разбивку стоимостей
    cost_breakdown = _calculate_cost_breakdown(request.work_items or [])
    
    lines = [
        f"📄 <b>{label}</b>",
        f"Название: {request.title}",
        f"Статус: {status}",
        f"Срок устранения: {due}",
        f"Инженер: {engineer}",
        f"Мастер: {master}",
        "",
        f"Плановая стоимость видов работ: {_format_currency(cost_breakdown['planned_work_cost'])} ₽",
        f"Плановая стоимость материалов: {_format_currency(cost_breakdown['planned_material_cost'])} ₽",
        f"Плановая общая стоимость: {_format_currency(cost_breakdown['planned_total_cost'])} ₽",
        f"Фактическая стоимость видов работ: {_format_currency(cost_breakdown['actual_work_cost'])} ₽",
        f"Фактическая стоимость материалов: {_format_currency(cost_breakdown['actual_material_cost'])} ₽",
        f"Фактическая общая стоимость: {_format_currency(cost_breakdown['actual_total_cost'])} ₽",
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


async def _show_request_detail(
    message: Message,
    request: Request,
    *,
    edit: bool = False,
    list_page: int = 0,
) -> None:
    text = _format_request_detail(request)
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад", callback_data=f"client:list:{list_page}")
    builder.adjust(1)
    try:
        if edit:
            await message.edit_text(text, reply_markup=builder.as_markup())
        else:
            await message.answer(text, reply_markup=builder.as_markup())
    except Exception:
        await message.answer(text, reply_markup=builder.as_markup())


def _calculate_cost_breakdown(work_items) -> dict[str, float]:
    """Рассчитывает разбивку стоимостей по работам и материалам."""
    planned_work_cost = 0.0
    planned_material_cost = 0.0
    actual_work_cost = 0.0
    actual_material_cost = 0.0
    
    for item in work_items:
        # Плановая стоимость работ
        if item.planned_cost is not None:
            planned_work_cost += float(item.planned_cost)
        
        # Плановая стоимость материалов
        if item.planned_material_cost is not None:
            planned_material_cost += float(item.planned_material_cost)
        
        # Фактическая стоимость работ
        if item.actual_cost is not None:
            actual_work_cost += float(item.actual_cost)
        
        # Фактическая стоимость материалов
        if item.actual_material_cost is not None:
            actual_material_cost += float(item.actual_material_cost)
    
    return {
        "planned_work_cost": planned_work_cost,
        "planned_material_cost": planned_material_cost,
        "planned_total_cost": planned_work_cost + planned_material_cost,
        "actual_work_cost": actual_work_cost,
        "actual_material_cost": actual_material_cost,
        "actual_total_cost": actual_work_cost + actual_material_cost,
    }


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")
