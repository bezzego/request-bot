from __future__ import annotations

from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Photo,
    PhotoType,
    Request,
    RequestStatus,
    User,
    UserRole,
    WorkItem,
    WorkSession,
)
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestService

router = Router()


class MasterStates(StatesGroup):
    finish_report = State()
    budget_fact = State()


STATUS_TITLES = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначена мастеру",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


@router.message(F.text == "📥 Мои заявки")
async def master_requests(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Эта функция доступна только мастерам.")
            return

        requests = await _load_master_requests(session, master.id)

    if not requests:
        await message.answer("У вас пока нет назначенных заявок. Ожидайте задач от инженера.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"master:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("master:detail:"))
async def master_request_detail(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)

    if not request:
        await callback.message.edit_text("Заявка не найдена или больше не закреплена за вами.")
        await callback.answer()
        return

    await _show_request_detail(callback.message, request, edit=True)
    await callback.answer()


@router.callback_query(F.data == "master:back")
async def master_back_to_list(callback: CallbackQuery):
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        requests = await _load_master_requests(session, master.id)

    if not requests:
        await callback.message.edit_text("Нет активных заявок. Ожидайте новых задач.")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(
            text=f"{req.number} · {STATUS_TITLES.get(req.status, req.status.value)}",
            callback_data=f"master:detail:{req.id}",
        )
    builder.adjust(1)

    await callback.message.edit_text(
        "Выберите заявку, чтобы зафиксировать работу и фотоотчёт.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("master:start:"))
async def master_start_work(callback: CallbackQuery):
    request_id = int(callback.data.split(":")[2])
    async with async_session() as session:
        master = await _get_master(session, callback.from_user.id)
        if not master:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return

        await RequestService.start_work(
            session,
            request,
            master_id=master.id,
            address=request.address,
        )
        await session.commit()

    await callback.answer("Старт работ зафиксирован.")
    await _refresh_request_detail(callback.bot, callback.message.chat.id, callback.from_user.id, request_id)


@router.callback_query(F.data.startswith("master:finish:"))
async def master_finish_prompt(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(MasterStates.finish_report)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Укажите фактическое время работ (в часах) и комментарий через «;».\n"
        "Пример: 6;Работы завершены, объект передан инженеру.\n"
        "Чтобы отменить, отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(MasterStates.finish_report))
async def master_finish_work(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in message.text.split(";")]
    if not parts:
        await message.answer("Укажите часы в формате «5» или «5.5;Комментарий».")
        return

    try:
        hours = float(parts[0].replace(",", "."))
    except ValueError:
        await message.answer("Не удалось распознать часы. Используйте число.")
        return

    comment = parts[1] if len(parts) > 1 else None
    data = await state.get_data()
    request_id = data.get("request_id")

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        await RequestService.finish_work(
            session,
            request,
            master_id=master.id,
            finished_at=datetime.now(timezone.utc),
            hours_reported=hours,
            completion_notes=comment,
        )
        await session.commit()

    await message.answer("Завершение работ зафиксировано. Не забудьте загрузить фотоотчёт.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.callback_query(F.data.startswith("master:update_fact:"))
async def master_update_fact(callback: CallbackQuery, state: FSMContext):
    request_id = int(callback.data.split(":")[2])
    await state.set_state(MasterStates.budget_fact)
    await state.update_data(request_id=request_id)
    await callback.message.answer(
        "Введите фактические данные по работе через «;»:\n"
        "Название;Факт кол-во;Факт часы;Факт стоимость;Материалы;Комментарий\n"
        "Например: Шпатлевка;45;8;12000;3500;Допработы по откосам.\n"
        "Чтобы отменить, отправьте «Отмена»."
    )
    await callback.answer()


@router.message(StateFilter(MasterStates.budget_fact))
async def master_update_fact_data(message: Message, state: FSMContext):
    if message.text.lower() == "отмена":
        await state.clear()
        await message.answer("Действие отменено.")
        return

    parts = [part.strip() for part in message.text.split(";")]
    if len(parts) < 5:
        await message.answer("Нужно минимум 5 значений через «;».")
        return

    name, actual_qty, actual_hours, actual_cost, actual_material, *comment = parts
    comment_text = comment[0] if comment else None

    def _float(value: str | None) -> float | None:
        if not value:
            return None
        return float(value.replace(",", "."))

    data = await state.get_data()
    request_id = data.get("request_id")

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            await message.answer("Нет доступа.")
            await state.clear()
            return

        request = await _load_request(session, master.id, request_id)
        if not request:
            await message.answer("Заявка не найдена.")
            await state.clear()
            return

        try:
            await RequestService.update_work_item_actual(
                session,
                request,
                name=name,
                actual_quantity=_float(actual_qty),
                actual_hours=_float(actual_hours),
                actual_cost=_float(actual_cost),
                actual_material_cost=_float(actual_material),
                notes=comment_text,
                author_id=master.id,
            )
            await session.commit()
        except ValueError as exc:
            await message.answer(str(exc))
            return

    await message.answer(f"Фактические данные по «{name}» обновлены.")
    await state.clear()
    await _refresh_request_detail(message.bot, message.chat.id, message.from_user.id, request_id)


@router.message(F.text == "📸 Инструкция по фотоотчёту")
async def master_photo_instruction(message: Message):
    await message.answer(
        "Для фиксации хода работ отправляйте фото с подписью вида:\n"
        "<code>RQ-123 описание фотографии</code>\n"
        "Бот автоматически сохранит фото в карточке заявки. Перед завершением работ\n"
        "обязательно приложите фото «до/после» и акт выполненных работ."
    )


@router.message(F.photo)
async def master_photo(message: Message):
    caption = message.caption or ""
    if "RQ-" not in caption:
        return

    parts = caption.split()
    number = parts[0]
    comment = " ".join(parts[1:]) if len(parts) > 1 else None

    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            return

        request = await session.scalar(
            select(Request)
            .where(Request.number == number, Request.master_id == master.id)
        )
        if not request:
            await message.answer("Не удалось найти заявку по указанному номеру.")
            return

        photo = message.photo[-1]
        new_photo = Photo(
            request_id=request.id,
            type=PhotoType.PROCESS,
            file_id=photo.file_id,
            caption=comment,
        )
        session.add(new_photo)
        await session.commit()

    await message.answer(f"Фото добавлено к заявке {number}.")


@router.message(F.location)
async def master_location(message: Message):
    async with async_session() as session:
        master = await _get_master(session, message.from_user.id)
        if not master:
            return

        work_session = await session.scalar(
            select(WorkSession)
            .where(WorkSession.master_id == master.id, WorkSession.finished_at.is_(None))
            .order_by(WorkSession.started_at.desc())
        )

        if work_session:
            work_session.started_latitude = message.location.latitude
            work_session.started_longitude = message.location.longitude
            await session.commit()
            await message.answer("Геопозиция старта работ сохранена.")
            return

        last_session = await session.scalar(
            select(WorkSession)
            .where(
                WorkSession.master_id == master.id,
                WorkSession.finished_at.isnot(None),
                WorkSession.finished_latitude.is_(None),
            )
            .order_by(WorkSession.finished_at.desc())
        )

        if last_session:
            last_session.finished_latitude = message.location.latitude
            last_session.finished_longitude = message.location.longitude
            await session.commit()
            await message.answer("Геопозиция завершения работ сохранена.")
            return


# --- служебные функции ---


async def _get_master(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(User.telegram_id == telegram_id, User.role == UserRole.MASTER)
    )


async def _load_master_requests(session, master_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.object),
                    selectinload(Request.contract),
                    selectinload(Request.work_items),
                    selectinload(Request.work_sessions),
                )
                .where(Request.master_id == master_id)
                .order_by(Request.created_at.desc())
                .limit(20)
            )
        )
        .scalars()
        .all()
    )


async def _load_request(session, master_id: int, request_id: int) -> Request | None:
    return await session.scalar(
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.work_items),
            selectinload(Request.work_sessions),
        )
        .where(Request.id == request_id, Request.master_id == master_id)
    )


async def _refresh_request_detail(bot, chat_id: int, master_telegram_id: int, request_id: int) -> None:
    async with async_session() as session:
        master = await _get_master(session, master_telegram_id)
        if not master:
            return
        request = await _load_request(session, master.id, request_id)

    if not request or not bot:
        return

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=_format_request_detail(request),
            reply_markup=_detail_keyboard(request.id),
        )
    except Exception:
        pass


async def _show_request_detail(message: Message, request: Request, *, edit: bool = False) -> None:
    text = _format_request_detail(request)
    keyboard = _detail_keyboard(request.id)
    try:
        if edit:
            await message.edit_text(text, reply_markup=keyboard)
        else:
            await message.answer(text, reply_markup=keyboard)
    except Exception:
        await message.answer(text, reply_markup=keyboard)


def _detail_keyboard(request_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="▶️ Начать работу", callback_data=f"master:start:{request_id}")
    builder.button(text="⏹ Завершить работу", callback_data=f"master:finish:{request_id}")
    builder.button(text="✏️ Обновить факт", callback_data=f"master:update_fact:{request_id}")
    builder.button(text="⬅️ Назад к списку", callback_data="master:back")
    builder.adjust(1)
    return builder.as_markup()


def _format_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    due_text = request.due_at.strftime("%d.%m.%Y %H:%M") if request.due_at else "не задан"
    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)

    lines = [
        f"🧾 <b>{request.number}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Срок устранения: {due_text}",
        f"Адрес: {request.address}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
    ]

    if request.work_items:
        lines.append("")
        lines.append("Позиции бюджета (с указанием факта):")
        for item in request.work_items:
            lines.append(
                f"• {item.name} — факт {_format_currency(item.actual_cost)} ₽ / {_format_hours(item.actual_hours)}"
            )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.work_sessions:
        lines.append("")
        lines.append("Рабочие сессии:")
        for session in sorted(request.work_sessions, key=lambda ws: ws.started_at):
            start = session.started_at.strftime("%d.%m %H:%M") if session.started_at else "—"
            finish = session.finished_at.strftime("%d.%m %H:%M") if session.finished_at else "—"
            lines.append(f"• {start} → {finish} | {_format_hours(session.hours_reported)}")
            if session.notes:
                lines.append(f"  → {session.notes}")

    lines.append("")
    lines.append("Совет: отправляйте геопозицию после нажатия «Начать работу» и перед завершением.")
    lines.append("Не забудьте приложить фотоотчёт с подписью формата `RQ-номер комментарий`.")
    return "\n".join(lines)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"
