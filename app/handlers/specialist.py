from __future__ import annotations

from datetime import datetime

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request, RequestStatus, User, UserRole
from app.infrastructure.db.session import async_session
from app.services.request_service import RequestCreateData, RequestService


router = Router()


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
    confirmation = State()


@router.message(F.text == "📄 Мои заявки")
async def specialist_requests(message: Message):
    async with async_session() as session:
        user = await session.scalar(select(User).where(User.telegram_id == message.from_user.id))
        if not user or user.role != UserRole.SPECIALIST:
            await message.answer("Эта функция доступна только специалистам отдела.")
            return

        stmt = (
            select(Request)
            .options(selectinload(Request.engineer), selectinload(Request.master))
            .where(Request.specialist_id == user.id)
            .order_by(Request.created_at.desc())
            .limit(10)
        )
        requests = (await session.execute(stmt)).scalars().all()

        if not requests:
            await message.answer("У вас пока нет заявок.")
            return

        lines = ["📄 <b>Мои последние заявки:</b>"]
        for req in requests:
            status = req.status.value
            engineer = req.engineer.full_name if req.engineer else "—"
            master = req.master.full_name if req.master else "—"
            lines.append(
                f"#{req.number} — {req.title}\n"
                f"Статус: {status} | Инженер: {engineer} | Мастер: {master}"
            )

    await message.answer("\n\n".join(lines))


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
    await message.answer("Тип дефекта (например, «Трещины в стене»).")


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

    data = await state.get_data()
    summary = (
        f"Проверьте данные:\n"
        f"🔹 Заголовок: {data['title']}\n"
        f"🔹 Объект: {data['object_name']}\n"
        f"🔹 Адрес: {data['address']}\n"
        f"🔹 Контакт: {data['contact_person']} / {data['contact_phone']}\n"
        f"🔹 Договор: {data.get('contract_number') or '—'}\n"
        f"🔹 Тип дефекта: {data.get('defect_type') or '—'}\n"
        f"🔹 Осмотр: "
        f"{data.get('inspection_datetime').strftime('%d.%m.%Y %H:%M') if data.get('inspection_datetime') else 'не указан'}\n"
        f"🔹 Место осмотра: {data.get('inspection_location') or 'адрес объекта'}\n"
        f"🔹 Срок устранения: {data['remedy_term_days']} дней\n\n"
        "Отправьте «Подтвердить» для создания заявки или «Отмена» для отмены."
    )
    await state.set_state(NewRequestStates.confirmation)
    await message.answer(summary)


@router.message(StateFilter(NewRequestStates.confirmation), F.text.lower() == "подтвердить")
async def confirm_request(message: Message, state: FSMContext):
    data = await state.get_data()
    async with async_session() as session:
        specialist = await session.scalar(select(User).where(User.id == data["specialist_id"]))
        if not specialist:
            await message.answer("Не удалось идентифицировать специалиста. Попробуйте снова.")
            await state.clear()
            return

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
        await session.commit()

    await message.answer(
        f"✅ Заявка {request.number} создана и назначена инженеру.\n"
        "Следите за статусом в разделе «📄 Мои заявки»."
    )
    await state.clear()


@router.message(StateFilter(NewRequestStates.confirmation), F.text.lower() == "отмена")
async def cancel_request(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Создание заявки отменено.")


@router.message(StateFilter(NewRequestStates.confirmation))
async def confirmation_help(message: Message):
    await message.answer("Введите «Подтвердить» для сохранения или «Отмена» для отмены.")
