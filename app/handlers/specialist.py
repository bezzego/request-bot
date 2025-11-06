from __future__ import annotations

from datetime import datetime, timezone

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Act, ActType, DefectType, Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestCreateData, RequestService


router = Router()


async def _get_specialist(session, telegram_id: int) -> User | None:
    return await session.scalar(
        select(User).where(
            User.telegram_id == telegram_id,
            User.role == UserRole.SPECIALIST,
        )
    )


async def _get_defect_types(session) -> list[DefectType]:
    return (
        (
            await session.execute(
                select(DefectType).order_by(DefectType.name.asc()).limit(12)
            )
        )
        .scalars()
        .all()
    )


def _defect_type_keyboard(defect_types: list[DefectType]):
    builder = InlineKeyboardBuilder()
    for defect in defect_types:
        builder.button(
            text=defect.name,
            callback_data=f"spec:defect:{defect.id}",
        )
    builder.button(text="✍️ Ввести вручную", callback_data="spec:defect:manual")
    builder.adjust(2)
    return builder.as_markup()


class NewRequestStates(StatesGroup):
    title = State()
    description = State()
    object_name = State()
    address = State()
    contact_person = State()
    contact_phone = State()
    contract_number = State()
    defect_type = State()
    inspection_datetime = State()
    inspection_location = State()
    engineer = State()
    remedy_term = State()
    letter = State()
    confirmation = State()


@router.message(F.text == "📄 Мои заявки")
async def specialist_requests(message: Message):
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела.")
            return

        requests = await _load_specialist_requests(session, specialist.id)

    if not requests:
        await message.answer("У вас пока нет заявок. Создайте первую через «➕ Создать заявку».")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status = req.status.value
        builder.button(
            text=f"{req.number} · {status}",
            callback_data=f"spec:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы посмотреть подробности и актуальный статус.",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data.startswith("spec:detail:"))
async def specialist_request_detail(callback: CallbackQuery):
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)

    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа к заявке.", show_alert=True)
            return

        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
                selectinload(Request.work_items),
                selectinload(Request.photos),
                selectinload(Request.acts),
                selectinload(Request.feedback),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )

    if not request:
        await callback.message.edit_text("Заявка не найдена или была удалена.")
        await callback.answer()
        return

    detail_text = _format_specialist_request_detail(request)
    builder = InlineKeyboardBuilder()
    builder.button(text="⬅️ Назад к списку", callback_data="spec:back")
    builder.button(text="🔄 Обновить", callback_data=f"spec:detail:{request.id}")
    builder.adjust(1)

    await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "spec:back")
async def specialist_back_to_list(callback: CallbackQuery):
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        requests = await _load_specialist_requests(session, specialist.id)

    if not requests:
        await callback.message.edit_text("У вас пока нет заявок. Создайте первую через «➕ Создать заявку».")
        await callback.answer()
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        builder.button(text=f"{req.number} · {req.status.value}", callback_data=f"spec:detail:{req.id}")
    builder.adjust(1)
    await callback.message.edit_text(
        "Выберите заявку, чтобы посмотреть подробности и актуальный статус.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.message(F.text == "📊 Аналитика")
async def specialist_analytics(message: Message):
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела.")
            return

        requests = await _load_specialist_requests(session, specialist.id)

    if not requests:
        await message.answer("Нет данных для аналитики. Создайте заявку, чтобы начать работу.")
        return

    summary_text = _build_specialist_analytics(requests)
    await message.answer(summary_text)


@router.message(F.text == "➕ Создать заявку")
async def start_new_request(message: Message, state: FSMContext):
    async with async_session() as session:
        user = await session.scalar(select(User).where(User.telegram_id == message.from_user.id))
        if not user or user.role != UserRole.SPECIALIST:
            await message.answer("Эта функция доступна только специалистам отдела.")
            return
        await state.set_state(NewRequestStates.title)
        await state.update_data(specialist_id=user.id)

    await message.answer("Введите короткий заголовок заявки (до 255 символов).")


@router.message(StateFilter(NewRequestStates.title))
async def handle_title(message: Message, state: FSMContext):
    title = message.text.strip()
    if not title:
        await message.answer("Заголовок не может быть пустым. Попробуйте снова.")
        return
    await state.update_data(title=title)
    await state.set_state(NewRequestStates.description)
    await message.answer("Опишите суть дефекта и требуемые работы.")


@router.message(StateFilter(NewRequestStates.description))
async def handle_description(message: Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    await state.set_state(NewRequestStates.object_name)
    await message.answer("Укажите объект (например, ЖК «Север», корпус 3).")


@router.message(StateFilter(NewRequestStates.object_name))
async def handle_object(message: Message, state: FSMContext):
    await state.update_data(object_name=message.text.strip())
    await state.set_state(NewRequestStates.address)
    await message.answer("Укажите адрес объекта.")


@router.message(StateFilter(NewRequestStates.address))
async def handle_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text.strip())
    await state.set_state(NewRequestStates.contact_person)
    await message.answer("Контактное лицо на объекте (ФИО).")


@router.message(StateFilter(NewRequestStates.contact_person))
async def handle_contact_person(message: Message, state: FSMContext):
    await state.update_data(contact_person=message.text.strip())
    await state.set_state(NewRequestStates.contact_phone)
    await message.answer("Телефон контактного лица.")


@router.message(StateFilter(NewRequestStates.contact_phone))
async def handle_contact_phone(message: Message, state: FSMContext):
    phone = message.text.strip()
    if len(phone) < 6:
        await message.answer("Похоже, номер слишком короткий. Введите номер полностью.")
        return
    await state.update_data(contact_phone=phone)
    await state.set_state(NewRequestStates.contract_number)
    await message.answer("Номер договора (если нет — отправьте «-»).")


@router.message(StateFilter(NewRequestStates.contract_number))
async def handle_contract(message: Message, state: FSMContext):
    contract = message.text.strip()
    await state.update_data(contract_number=None if contract == "-" else contract)
    await state.set_state(NewRequestStates.defect_type)

    async with async_session() as session:
        defect_types = await _get_defect_types(session)

    if defect_types:
        await message.answer(
            "Выберите тип дефекта из списка или введите свой текстом.",
            reply_markup=_defect_type_keyboard(defect_types),
        )
    else:
        await message.answer("Тип дефекта (например, «Трещины в стене»).")


@router.callback_query(StateFilter(NewRequestStates.defect_type), F.data.startswith("spec:defect:"))
async def handle_defect_type_choice(callback: CallbackQuery, state: FSMContext):
    _, _, type_id = callback.data.split(":")
    if type_id == "manual":
        await callback.answer("Введите тип дефекта сообщением.")
        return

    defect_type_id = int(type_id)
    async with async_session() as session:
        defect = await session.scalar(select(DefectType).where(DefectType.id == defect_type_id))

    if not defect:
        await callback.answer("Тип не найден. Введите вручную.", show_alert=True)
        return

    await state.update_data(defect_type=defect.name)
    await state.set_state(NewRequestStates.inspection_datetime)
    await callback.message.edit_text(f"Тип дефекта: {defect.name}")
    await callback.message.answer(
        "Когда планируется комиссионный осмотр?\n"
        "Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        "Если время ещё не известно — отправьте «-»."
    )
    await callback.answer()


@router.message(StateFilter(NewRequestStates.defect_type))
async def handle_defect_type(message: Message, state: FSMContext):
    defect = message.text.strip()
    await state.update_data(defect_type=None if defect == "-" else defect)
    await state.set_state(NewRequestStates.inspection_datetime)
    await message.answer(
        "Когда планируется комиссионный осмотр?\n"
        "Формат: <code>ДД.ММ.ГГГГ ЧЧ:ММ</code>\n"
        "Если время ещё не известно — отправьте «-»."
    )


@router.message(StateFilter(NewRequestStates.inspection_datetime))
async def handle_inspection_datetime(message: Message, state: FSMContext):
    text = message.text.strip()
    if text == "-":
        await state.update_data(inspection_datetime=None)
    else:
        try:
            inspection_dt = datetime.strptime(text, "%d.%m.%Y %H:%M")
            await state.update_data(inspection_datetime=inspection_dt)
        except ValueError:
            await message.answer("Не удалось распознать дату. Используйте формат ДД.ММ.ГГГГ ЧЧ:ММ.")
            return

    await state.set_state(NewRequestStates.inspection_location)
    await message.answer("Место осмотра (если отличается от адреса). Если совпадает — отправьте «-».")


@router.message(StateFilter(NewRequestStates.inspection_location))
async def handle_inspection_location(message: Message, state: FSMContext):
    location = message.text.strip()
    await state.update_data(inspection_location=None if location == "-" else location)

    async with async_session() as session:
        engineers = (
            await session.execute(
                select(User).where(User.role == UserRole.ENGINEER).order_by(User.full_name)
            )
        ).scalars().all()

    if not engineers:
        await message.answer("Нет доступных инженеров. Обратитесь к руководителю.")
        await state.clear()
        return

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{eng.full_name}",
                    callback_data=f"assign_engineer:{eng.id}",
                )
            ]
            for eng in engineers
        ]
    )
    await state.set_state(NewRequestStates.engineer)
    await message.answer("Выберите инженера для заявки:", reply_markup=kb)


@router.callback_query(StateFilter(NewRequestStates.engineer), F.data.startswith("assign_engineer:"))
async def handle_engineer_callback(callback: CallbackQuery, state: FSMContext):
    engineer_id = int(callback.data.split(":")[1])
    await state.update_data(engineer_id=engineer_id)
    await state.set_state(NewRequestStates.remedy_term)
    await callback.message.edit_reply_markup()
    await callback.message.answer("Выберите срок устранения замечаний: 14 или 30 дней.")
    await callback.answer()


@router.message(StateFilter(NewRequestStates.remedy_term))
async def handle_remedy_term(message: Message, state: FSMContext):
    text = message.text.strip()
    if text not in {"14", "30"}:
        await message.answer("Допустимые значения: 14 или 30.")
        return
    await state.update_data(remedy_term_days=int(text))

    await state.set_state(NewRequestStates.letter)
    await message.answer(
        "Прикрепите файл обращения (письмо) в формате PDF/документа или отправьте «-», если письма нет.\n"
        "Для отмены напишите «Отмена»."
    )


@router.message(StateFilter(NewRequestStates.letter), F.document)
async def handle_letter_document(message: Message, state: FSMContext):
    document = message.document
    await state.update_data(
        letter_file_id=document.file_id,
        letter_file_name=document.file_name,
    )
    await _send_summary(message, state)


@router.message(StateFilter(NewRequestStates.letter))
async def handle_letter_choice(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text == "отмена":
        await state.clear()
        await message.answer("Создание заявки отменено.")
        return
    if text in {"-", "нет", "без письма"}:
        await state.update_data(letter_file_id=None, letter_file_name=None)
        await _send_summary(message, state)
        return

    await message.answer("Прикрепите файл обращения (например, PDF) или отправьте «-», если письма нет.")


@router.message(StateFilter(NewRequestStates.confirmation), F.text.lower() == "подтвердить")
async def confirm_request(message: Message, state: FSMContext):
    data = await state.get_data()
    async with async_session() as session:
        specialist = await session.scalar(select(User).where(User.id == data["specialist_id"]))
        if not specialist:
            await message.answer("Не удалось идентифицировать специалиста. Попробуйте снова.")
            await state.clear()
            return

        engineer_user = await session.scalar(select(User).where(User.id == data["engineer_id"]))

        create_data = RequestCreateData(
            title=data["title"],
            description=data["description"],
            object_name=data["object_name"],
            address=data["address"],
            contact_person=data["contact_person"],
            contact_phone=data["contact_phone"],
            contract_number=data.get("contract_number"),
            defect_type_name=data.get("defect_type"),
            inspection_datetime=data.get("inspection_datetime"),
            inspection_location=data.get("inspection_location"),
            specialist_id=data["specialist_id"],
            engineer_id=data["engineer_id"],
            remedy_term_days=data.get("remedy_term_days", 14),
        )
        request = await RequestService.create_request(session, create_data)

        letter_file_id = data.get("letter_file_id")
        if letter_file_id:
            session.add(
                Act(
                    request_id=request.id,
                    type=ActType.LETTER,
                    file_id=letter_file_id,
                    file_name=data.get("letter_file_name"),
                    uploaded_by_id=data["specialist_id"],
                )
            )

        await session.commit()

        request_number = request.number
        request_title = request.title
        due_at = request.due_at

    await message.answer(
        f"✅ Заявка {request_number} создана и назначена инженеру.\n"
        "Следите за статусом в разделе «📄 Мои заявки»."
    )
    await state.clear()

    engineer_telegram = getattr(engineer_user, "telegram_id", None) if engineer_user else None
    if engineer_telegram:
        due_text = due_at.strftime("%d.%m.%Y %H:%M") if due_at else "не задан"
        notification = (
            f"Новая заявка {request_number}.\n"
            f"Название: {request_title}\n"
            f"Объект: {data['object_name']}\n"
            f"Адрес: {data['address']}\n"
            f"Срок устранения: {due_text}"
        )
        if data.get("letter_file_id"):
            notification += "\nПисьмо: приложено."
        try:
            await message.bot.send_message(chat_id=int(engineer_telegram), text=notification)
        except Exception:
            pass


@router.message(StateFilter(NewRequestStates.confirmation), F.text.lower() == "отмена")
async def cancel_request(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Создание заявки отменено.")


@router.message(StateFilter(NewRequestStates.confirmation))
async def confirmation_help(message: Message):
    await message.answer("Введите «Подтвердить» для сохранения или «Отмена» для отмены.")


# --- вспомогательные функции ---


async def _send_summary(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    summary = _build_request_summary(data)
    await state.set_state(NewRequestStates.confirmation)
    await message.answer(summary)


def _build_request_summary(data: dict) -> str:
    inspection_dt = data.get("inspection_datetime")
    if inspection_dt:
        inspection_text = inspection_dt.strftime("%d.%m.%Y %H:%M")
    else:
        inspection_text = "не указан"

    letter_text = "приложено" if data.get("letter_file_id") else "нет"

    return (
        "Проверьте данные:\n"
        f"🔹 Заголовок: {data['title']}\n"
        f"🔹 Объект: {data['object_name']}\n"
        f"🔹 Адрес: {data['address']}\n"
        f"🔹 Контакт: {data['contact_person']} / {data['contact_phone']}\n"
        f"🔹 Договор: {data.get('contract_number') or '—'}\n"
        f"🔹 Тип дефекта: {data.get('defect_type') or '—'}\n"
        f"🔹 Осмотр: {inspection_text}\n"
        f"🔹 Место осмотра: {data.get('inspection_location') or 'адрес объекта'}\n"
        f"🔹 Срок устранения: {data.get('remedy_term_days', 14)} дней\n"
        f"🔹 Письмо: {letter_text}\n\n"
        "Отправьте «Подтвердить» для создания заявки или «Отмена» для отмены."
    )

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


async def _load_specialist_requests(session, specialist_id: int) -> list[Request]:
    return (
        (
            await session.execute(
                select(Request)
                .options(
                    selectinload(Request.engineer),
                    selectinload(Request.master),
                    selectinload(Request.work_items),
                )
                .where(Request.specialist_id == specialist_id)
                .order_by(Request.created_at.desc())
                .limit(15)
            )
        )
        .scalars()
        .all()
    )


def _format_specialist_request_detail(request: Request) -> str:
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    engineer = request.engineer.full_name if request.engineer else "—"
    master = request.master.full_name if request.master else "—"
    due_text = request.due_at.strftime("%d.%m.%Y %H:%M") if request.due_at else "не задан"
    inspection_text = (
        request.inspection_scheduled_at.strftime("%d.%m.%Y %H:%M")
        if request.inspection_scheduled_at
        else "не назначен"
    )
    inspection_done = (
        request.inspection_completed_at.strftime("%d.%m.%Y %H:%M")
        if request.inspection_completed_at
        else "нет"
    )

    planned_budget = float(request.planned_budget or 0)
    actual_budget = float(request.actual_budget or 0)
    budget_delta = actual_budget - planned_budget

    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    hours_delta = actual_hours - planned_hours

    lines = [
        f"📄 <b>{request.number}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Инженер: {engineer}",
        f"Мастер: {master}",
        f"Осмотр: {inspection_text}",
        f"Осмотр завершён: {inspection_done}",
        f"Срок устранения: {due_text}",
        f"Адрес: {request.address}",
        f"Контакт: {request.contact_person} · {request.contact_phone}",
        "",
        f"Плановый бюджет: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет: {_format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {_format_currency(budget_delta)} ₽",
        f"Плановые часы: {_format_hours(planned_hours)}",
        f"Фактические часы: {_format_hours(actual_hours)}",
        f"Δ Часы: {_format_hours(hours_delta)}",
    ]

    if request.contract:
        lines.append(f"Договор: {request.contract.number}")
    if request.defect_type:
        lines.append(f"Тип дефекта: {request.defect_type.name}")
    if request.inspection_location:
        lines.append(f"Место осмотра: {request.inspection_location}")

    if request.work_items:
        lines.append("")
        lines.append("📦 <b>Позиции бюджета</b>")
        for item in request.work_items:
            lines.append(
                f"• {item.name} — план {_format_currency(item.planned_cost)} ₽ / "
                f"факт {_format_currency(item.actual_cost)} ₽"
            )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        lines.append("")
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        act_count = len(request.acts) - letter_count
        if act_count:
            lines.append(f"📝 Акты: {act_count}")
        if letter_count:
            letter_text = "приложено" if letter_count == 1 else f"приложено ({letter_count})"
            lines.append(f"✉️ Письмо: {letter_text}")
    if request.photos:
        lines.append(f"📷 Фотоотчётов: {len(request.photos)}")
    if request.feedback:
        fb = request.feedback[-1]
        lines.append(
            f"⭐️ Отзыв: качество {fb.rating_quality or '—'}, сроки {fb.rating_time or '—'}, культура {fb.rating_culture or '—'}"
        )
        if fb.comment:
            lines.append(f"«{fb.comment}»")

    lines.append("")
    lines.append("Поддерживайте актуальные статусы и бюджеты, чтобы команда видела прогресс.")
    return "\n".join(lines)


def _format_currency(value: float | None) -> str:
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"


def _build_specialist_analytics(requests: list[Request]) -> str:
    from collections import Counter

    now = datetime.now(timezone.utc)
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
        f"Плановый бюджет суммарно: {_format_currency(planned_budget)} ₽",
        f"Фактический бюджет суммарно: {_format_currency(actual_budget)} ₽",
        f"Δ Бюджет: {_format_currency(actual_budget - planned_budget)} ₽",
        f"Плановые часы суммарно: {_format_hours(planned_hours)}",
        f"Фактические часы суммарно: {_format_hours(actual_hours)}",
        f"Средняя длительность закрытой заявки: {_format_hours(avg_duration)}",
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
            lines.append(f"• {req.number} — до {req.due_at.strftime('%d.%m.%Y %H:%M')}")

    return "\n".join(lines)
