from __future__ import annotations

from datetime import date, datetime

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Act,
    ActType,
    DefectType,
    Leader,
    Object,
    Request,
    RequestStatus,
    User,
    UserRole,
    Contract,
)
from app.infrastructure.db.session import async_session
from app.keyboards.calendar import build_calendar, parse_calendar_callback, shift_month
from app.services.request_service import RequestCreateData, RequestService
from app.utils.request_formatters import format_request_label
from app.utils.timezone import combine_moscow, format_moscow, now_moscow

router = Router()

SPEC_CALENDAR_PREFIX = "spec_inspection"


async def _get_specialist(session, telegram_id: int) -> User | None:
    """Получает специалиста или суперадмина."""
    user = await session.scalar(
        select(User)
        .options(selectinload(User.leader_profile))
        .where(User.telegram_id == telegram_id)
    )
    if not user:
        return None
    
    # Проверяем, является ли пользователь специалистом
    if user.role == UserRole.SPECIALIST:
        return user
    
    # Проверяем, является ли пользователь суперадмином
    if user.role == UserRole.MANAGER and user.leader_profile and user.leader_profile.is_super_admin:
        return user
    
    return None


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


async def _get_saved_objects(session, limit: int = 10) -> list[Object]:
    """Получает список ранее использованных объектов (ЖК)."""
    return (
        (
            await session.execute(
                select(Object)
                .order_by(Object.created_at.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def _get_saved_addresses(session, object_name: str | None = None, limit: int = 10) -> list[str]:
    """Получает список ранее использованных адресов."""
    # Используем GROUP BY вместо DISTINCT, чтобы можно было сортировать по created_at
    if object_name:
        # Если указан объект, ищем адреса для этого объекта
        query = (
            select(Request.address, func.max(Request.created_at).label('max_created_at'))
            .join(Object, Request.object_id == Object.id)
            .where(
                Request.address.isnot(None),
                func.lower(Object.name) == object_name.lower()
            )
            .group_by(Request.address)
            .order_by(func.max(Request.created_at).desc())
            .limit(limit)
        )
    else:
        query = (
            select(Request.address, func.max(Request.created_at).label('max_created_at'))
            .where(Request.address.isnot(None))
            .group_by(Request.address)
            .order_by(func.max(Request.created_at).desc())
            .limit(limit)
        )
    
    result = await session.execute(query)
    return [row[0] for row in result.all() if row[0]]


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


async def _prompt_inspection_calendar(message: Message):
    await message.answer(
        "Когда планируется комиссионный осмотр?\n"
        "Выберите дату через календарь или отправьте «-», если дата пока не определена.",
        reply_markup=build_calendar(SPEC_CALENDAR_PREFIX),
    )


async def _get_saved_contracts(session, limit: int = 10) -> list[Contract]:
    """Возвращает последние использованные договоры."""
    return (
        (
            await session.execute(
                select(Contract).order_by(Contract.created_at.desc()).limit(limit)
            )
        )
        .scalars()
        .all()
    )


async def _prompt_inspection_location(message: Message):
    await message.answer("Место осмотра (если отличается от адреса). Если совпадает — отправьте «-».")


class NewRequestStates(StatesGroup):
    title = State()
    description = State()
    object_name = State()
    address = State()
    apartment = State()
    contact_person = State()
    contact_phone = State()
    contract_number = State()
    defect_type = State()
    inspection_datetime = State()
    inspection_time = State()
    inspection_location = State()
    engineer = State()
    letter = State()
    confirmation = State()


class CloseRequestStates(StatesGroup):
    confirmation = State()
    comment = State()


class SpecialistFilterStates(StatesGroup):
    mode = State()
    value = State()


@router.message(F.text == "📄 Мои заявки")
async def specialist_requests(message: Message):
    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        requests = await _load_specialist_requests(session, specialist.id)

    if not requests:
        await message.answer("У вас пока нет заявок. Создайте первую через «➕ Создать заявку».")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status = req.status.value
        builder.button(
            text=f"{format_request_label(req)} · {status}",
            callback_data=f"spec:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите заявку, чтобы посмотреть подробности и актуальный статус.",
        reply_markup=builder.as_markup(),
    )


@router.message(F.text == "🔍 Фильтр заявок")
async def specialist_filter_start(message: Message, state: FSMContext):
    await state.set_state(SpecialistFilterStates.mode)
    await message.answer(
        "Выберите режим фильтрации:\n"
        "• отправьте «Адрес» — для поиска по адресу\n"
        "• отправьте «Дата» — для фильтра по диапазону дат создания (формат 01.01.2025-31.01.2025)"
    )


@router.message(StateFilter(SpecialistFilterStates.mode))
async def specialist_filter_mode(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text not in {"адрес", "дата"}:
        await message.answer("Введите «Адрес» или «Дата».")
        return
    await state.update_data(mode=text)
    await state.set_state(SpecialistFilterStates.value)
    if text == "адрес":
        await message.answer("Введите часть адреса (улица, дом и т.п.).")
    else:
        await message.answer("Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.")


@router.message(StateFilter(SpecialistFilterStates.value))
async def specialist_filter_apply(message: Message, state: FSMContext):
    from datetime import datetime
    data = await state.get_data()
    mode = data.get("mode")
    value = (message.text or "").strip()

    async with async_session() as session:
        specialist = await _get_specialist(session, message.from_user.id)
        if not specialist:
            await state.clear()
            await message.answer("Нет доступа.")
            return

        query = (
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.specialist_id == specialist.id)
            .order_by(Request.created_at.desc())
        )

        if mode == "адрес":
            query = query.where(func.lower(Request.address).like(f"%{value.lower()}%"))
        elif mode == "дата":
            try:
                start_str, end_str = [p.strip() for p in value.split("-", 1)]
                start = datetime.strptime(start_str, "%d.%m.%Y")
                end = datetime.strptime(end_str, "%d.%m.%Y")
                end = end.replace(hour=23, minute=59, second=59)
            except Exception:
                await message.answer("Неверный формат. Используйте ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.")
                return
            query = query.where(Request.created_at.between(start, end))

        requests = (
            (await session.execute(query.limit(30)))
            .scalars()
            .all()
        )

    await state.clear()

    if not requests:
        await message.answer("Заявок по заданному фильтру не найдено.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status = req.status.value
        builder.button(
            text=f"{format_request_label(req)} · {status}",
            callback_data=f"spec:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Результаты фильтрации. Выберите заявку:",
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
    
    # Проверяем, является ли специалист/суперадмин инженером на этой заявке
    from app.handlers.engineer import _get_engineer
    engineer = await _get_engineer(session, callback.from_user.id)
    is_engineer = engineer and request.engineer_id == engineer.id
    
    # Если специалист/суперадмин является инженером на этой заявке, показываем кнопки инженера
    if is_engineer:
        builder.button(text="🗓 Назначить осмотр", callback_data=f"eng:schedule:{request.id}")
        builder.button(text="✅ Осмотр выполнен", callback_data=f"eng:inspect:{request.id}")
        builder.button(text="➕ Плановая позиция", callback_data=f"eng:add_plan:{request.id}")
        builder.button(text="✏️ Обновить факт", callback_data=f"eng:update_fact:{request.id}")
        builder.button(text="⏱ Срок устранения", callback_data=f"eng:set_term:{request.id}")
        builder.button(text="👷 Назначить мастера", callback_data=f"eng:assign_master:{request.id}")
        builder.button(text="📄 Готово к подписанию", callback_data=f"eng:ready:{request.id}")
    
    # Добавляем кнопки для файлов (писем)
    letter_acts = [act for act in request.acts if act.type == ActType.LETTER]
    for act in letter_acts:
        file_name = act.file_name or f"Файл {act.id}"
        # Ограничиваем длину имени файла для кнопки
        button_text = file_name[:40] + "..." if len(file_name) > 40 else file_name
        builder.button(
            text=f"📎 {button_text}",
            callback_data=f"spec:file:{act.id}",
        )
    
    # Добавляем кнопку закрытия заявки, если можно закрыть
    can_close, reasons = await RequestService.can_close_request(request)
    if request.status == RequestStatus.CLOSED:
        builder.button(
            text="✅ Заявка закрыта",
            callback_data="spec:noop",
        )
    elif can_close:
        builder.button(
            text="✅ Закрыть заявку",
            callback_data=f"spec:close:{request.id}",
        )
    else:
        # Показываем, почему нельзя закрыть (только первую причину для краткости)
        reason_text = reasons[0][:35] + "..." if reasons and len(reasons[0]) > 35 else (reasons[0] if reasons else "не выполнены условия")
        builder.button(
            text=f"⚠️ {reason_text}",
            callback_data=f"spec:close_info:{request.id}",
        )
    
    builder.button(text="⬅️ Назад к списку", callback_data="spec:back")
    builder.button(text="🔄 Обновить", callback_data=f"spec:detail:{request.id}")
    builder.adjust(1)

    await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("spec:close_info:"))
async def specialist_close_info(callback: CallbackQuery):
    """Показывает информацию о том, почему заявку нельзя закрыть."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
        can_close, reasons = await RequestService.can_close_request(request)
        if can_close:
            await callback.answer("Заявку можно закрыть.", show_alert=True)
            return
        
        reasons_text = "\n".join(f"• {reason}" for reason in reasons)
        await callback.message.answer(
            f"⚠️ <b>Заявку нельзя закрыть</b>\n\n"
            f"Причины:\n{reasons_text}\n\n"
            f"Убедитесь, что все условия выполнены, и попробуйте снова.",
        )
        await callback.answer()


@router.callback_query(F.data.startswith("spec:close:"))
async def specialist_start_close(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс закрытия заявки."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
        # Проверяем, можно ли закрыть
        can_close, reasons = await RequestService.can_close_request(request)
        if not can_close:
            reasons_text = "\n".join(f"• {reason}" for reason in reasons)
            await callback.message.answer(
                f"⚠️ <b>Заявку нельзя закрыть</b>\n\n"
                f"Причины:\n{reasons_text}",
            )
            await callback.answer()
            return
        
        if request.status == RequestStatus.CLOSED:
            await callback.answer("Заявка уже закрыта.", show_alert=True)
            return
        
        # Сохраняем данные в state
        request_label = format_request_label(request)
        await state.update_data(
            request_id=request_id,
            request_label=request_label,
        )
        await state.set_state(CloseRequestStates.comment)
        
        await callback.message.answer(
            f"📋 <b>Закрытие заявки {request_label}</b>\n\n"
            f"Заявка будет окончательно закрыта.\n\n"
            f"Введите комментарий к закрытию (или отправьте «-», чтобы пропустить):",
        )
        await callback.answer()


@router.message(StateFilter(CloseRequestStates.comment))
async def specialist_close_comment(message: Message, state: FSMContext):
    """Обрабатывает комментарий при закрытии заявки."""
    comment = message.text.strip() if message.text and message.text.strip() != "-" else None
    await state.update_data(comment=comment)
    await state.set_state(CloseRequestStates.confirmation)
    
    data = await state.get_data()
    request_label = data.get("request_label", "N/A")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить закрытие", callback_data="spec:close_confirm")
    builder.button(text="❌ Отменить", callback_data="spec:close_cancel")
    builder.adjust(1)
    
    comment_text = f"\n\nКомментарий: {comment}" if comment else "\n\nКомментарий не указан"
    await message.answer(
        f"📋 <b>Подтверждение закрытия заявки {request_label}</b>\n\n"
        f"Вы уверены, что хотите закрыть эту заявку?{comment_text}",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data == "spec:close_confirm", StateFilter(CloseRequestStates.confirmation))
async def specialist_close_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждает закрытие заявки."""
    data = await state.get_data()
    request_id = data.get("request_id")
    comment = data.get("comment")
    
    if not request_id:
        await callback.answer("Ошибка: не найден ID заявки.", show_alert=True)
        await state.clear()
        return
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            await state.clear()
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id, Request.specialist_id == specialist.id)
        )
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            await state.clear()
            return
        
        # Проверяем ещё раз перед закрытием
        can_close, reasons = await RequestService.can_close_request(request)
        if not can_close:
            reasons_text = "\n".join(f"• {reason}" for reason in reasons)
            await callback.message.answer(
                f"⚠️ <b>Не удалось закрыть заявку</b>\n\n"
                f"Причины:\n{reasons_text}",
            )
            await callback.answer()
            await state.clear()
            return
        
        try:
            await RequestService.close_request(
                session,
                request,
                user_id=specialist.id,
                comment=comment,
            )
            await session.commit()
            
            label = format_request_label(request)
            await callback.message.answer(
                f"✅ <b>Заявка {label} успешно закрыта</b>\n\n"
                f"Все работы завершены, заявка закрыта.",
            )
            await callback.answer("Заявка закрыта")
            
            # Уведомляем инженера, если он назначен
            if request.engineer and request.engineer.telegram_id:
                try:
                    await callback.message.bot.send_message(
                        chat_id=int(request.engineer.telegram_id),
                        text=f"✅ Заявка {label} закрыта специалистом.",
                    )
                except Exception:
                    pass
            
        except ValueError as e:
            await callback.message.answer(
                f"❌ <b>Ошибка при закрытии заявки</b>\n\n{str(e)}",
            )
            await callback.answer("Ошибка", show_alert=True)
        except Exception as e:
            await callback.message.answer(
                f"❌ <b>Произошла ошибка</b>\n\n{str(e)}",
            )
            await callback.answer("Ошибка", show_alert=True)
    
    await state.clear()


@router.callback_query(F.data == "spec:close_cancel")
async def specialist_close_cancel(callback: CallbackQuery, state: FSMContext):
    """Отменяет закрытие заявки."""
    await state.clear()
    await callback.message.answer("Закрытие заявки отменено.")
    await callback.answer()


@router.callback_query(F.data == "spec:noop")
async def specialist_noop(callback: CallbackQuery):
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()


@router.callback_query(F.data.startswith("spec:file:"))
async def specialist_open_file(callback: CallbackQuery):
    """Отправляет прикреплённый файл пользователю."""
    _, _, act_id_str = callback.data.split(":")
    act_id = int(act_id_str)
    
    async with async_session() as session:
        specialist = await _get_specialist(session, callback.from_user.id)
        if not specialist:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        act = await session.scalar(
            select(Act)
            .join(Request)
            .where(
                Act.id == act_id,
                Act.type == ActType.LETTER,
                Request.specialist_id == specialist.id,
            )
        )
        
        if not act:
            await callback.answer("Файл не найден.", show_alert=True)
            return
        
        try:
            # Отправляем файл пользователю
            await callback.message.bot.send_document(
                chat_id=callback.from_user.id,
                document=act.file_id,
                caption=f"📎 {act.file_name or 'Файл'}",
            )
            await callback.answer("Файл отправлен.")
        except Exception as e:
            await callback.answer(f"Ошибка при отправке файла: {str(e)}", show_alert=True)


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
        builder.button(
            text=f"{format_request_label(req)} · {req.status.value}",
            callback_data=f"spec:detail:{req.id}",
        )
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
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
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
        user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.telegram_id == message.from_user.id)
        )
        if not user:
            await message.answer("Пользователь не найден.")
            return
        
        # Проверяем, является ли пользователь специалистом или суперадмином
        is_specialist = user.role == UserRole.SPECIALIST
        is_super_admin = (
            user.role == UserRole.MANAGER 
            and user.leader_profile 
            and user.leader_profile.is_super_admin
        )
        
        if not (is_specialist or is_super_admin):
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
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
    
    # Показываем сохранённые ЖК
    async with async_session() as session:
        saved_objects = await _get_saved_objects(session, limit=10)
    
    if saved_objects:
        builder = InlineKeyboardBuilder()
        for obj in saved_objects:
            builder.button(
                text=obj.name,
                callback_data=f"spec:object:{obj.id}",
            )
        builder.button(text="✍️ Ввести вручную", callback_data="spec:object:manual")
        builder.adjust(1)
        await message.answer(
            "Выберите ЖК из списка или введите вручную:",
            reply_markup=builder.as_markup(),
        )
    else:
        await state.set_state(NewRequestStates.object_name)
        await message.answer("Укажите объект (например, ЖК «Север», корпус 3).")


@router.callback_query(StateFilter(NewRequestStates.description), F.data.startswith("spec:object"))
async def handle_object_choice(callback: CallbackQuery, state: FSMContext):
    if callback.data == "spec:object:manual":
        await state.set_state(NewRequestStates.object_name)
        await callback.message.edit_reply_markup()
        await callback.message.answer("Укажите объект (например, ЖК «Север», корпус 3).")
        await callback.answer()
        return
    
    if callback.data.startswith("spec:object:"):
        try:
            object_id = int(callback.data.split(":")[2])
            async with async_session() as session:
                obj = await session.get(Object, object_id)
                if obj:
                    object_name = obj.name
                    await state.update_data(object_name=object_name)
                    await callback.message.edit_text(f"ЖК: {object_name}")
                    
                    # Показываем сохранённые адреса для этого ЖК
                    saved_addresses = await _get_saved_addresses(session, object_name=object_name, limit=10)
                    
                    if saved_addresses:
                        await state.update_data(saved_addresses=saved_addresses)
                        await state.set_state(NewRequestStates.object_name)  # Остаёмся в этом состоянии для обработки адреса
                        builder = InlineKeyboardBuilder()
                        for idx, addr in enumerate(saved_addresses):
                            builder.button(
                                text=addr[:50],
                                callback_data=f"spec:address_idx:{idx}",
                            )
                        builder.button(text="✍️ Ввести вручную", callback_data="spec:address:manual")
                        builder.adjust(1)
                        await callback.message.answer(
                            "Выберите адрес из списка или введите вручную:",
                            reply_markup=builder.as_markup(),
                        )
                    else:
                        await state.set_state(NewRequestStates.address)
                        await callback.message.answer("Укажите адрес объекта.")
                    await callback.answer()
                    return
        except (ValueError, IndexError):
            pass
    
    await callback.answer("Ошибка выбора ЖК. Попробуйте снова.", show_alert=True)


@router.message(StateFilter(NewRequestStates.object_name))
async def handle_object(message: Message, state: FSMContext):
    object_name = message.text.strip()
    await state.update_data(object_name=object_name)
    
    # Показываем сохранённые адреса для этого ЖК
    async with async_session() as session:
        saved_addresses = await _get_saved_addresses(session, object_name=object_name, limit=10)
    
    if saved_addresses:
        # Сохраняем адреса в state для использования в callback
        await state.update_data(saved_addresses=saved_addresses)
        builder = InlineKeyboardBuilder()
        for idx, addr in enumerate(saved_addresses):
            builder.button(
                text=addr[:50],  # Ограничиваем длину текста кнопки
                callback_data=f"spec:address_idx:{idx}",
            )
        builder.button(text="✍️ Ввести вручную", callback_data="spec:address:manual")
        builder.adjust(1)
        await message.answer(
            "Выберите адрес из списка или введите вручную:",
            reply_markup=builder.as_markup(),
        )
    else:
        await state.set_state(NewRequestStates.address)
        await message.answer("Укажите адрес объекта.")


@router.callback_query(StateFilter(NewRequestStates.object_name), F.data.startswith("spec:address"))
async def handle_address_choice(callback: CallbackQuery, state: FSMContext):
    if callback.data == "spec:address:manual":
        await state.set_state(NewRequestStates.address)
        await callback.message.edit_reply_markup()
        await callback.message.answer("Укажите адрес объекта.")
        await callback.answer()
        return
    
    if callback.data.startswith("spec:address_idx:"):
        data = await state.get_data()
        saved_addresses = data.get("saved_addresses", [])
        try:
            idx = int(callback.data.split(":")[2])
            if 0 <= idx < len(saved_addresses):
                address = saved_addresses[idx]
                await state.update_data(address=address, saved_addresses=None)
                await state.set_state(NewRequestStates.apartment)
                await callback.message.edit_text(f"Адрес: {address}")
                await callback.message.answer("Укажите номер квартиры (или отправьте «-», если не применимо).")
                await callback.answer()
                return
        except (ValueError, IndexError):
            pass
    
    await callback.answer("Ошибка выбора адреса. Попробуйте снова.", show_alert=True)


@router.message(StateFilter(NewRequestStates.address))
async def handle_address(message: Message, state: FSMContext):
    await state.update_data(address=message.text.strip())
    await state.set_state(NewRequestStates.apartment)
    await message.answer("Укажите номер квартиры (или отправьте «-», если не применимо).")


@router.message(StateFilter(NewRequestStates.apartment))
async def handle_apartment(message: Message, state: FSMContext):
    apartment = message.text.strip()
    await state.update_data(apartment=None if apartment == "-" else apartment)
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

    # Показываем сохранённые договоры
    async with async_session() as session:
        contracts = await _get_saved_contracts(session, limit=10)

    if contracts:
        builder = InlineKeyboardBuilder()
        for contract in contracts:
            title = contract.number
            if contract.description:
                title = f"{contract.number} — {contract.description[:30]}"
            builder.button(
                text=title[:50],
                callback_data=f"spec:contract:{contract.id}",
            )
        builder.button(text="✍️ Ввести вручную", callback_data="spec:contract:manual")
        builder.adjust(1)
        await state.set_state(NewRequestStates.contract_number)
        await message.answer(
            "Выберите номер договора из списка или введите вручную.\n"
            "Если договора нет — отправьте «-».",
            reply_markup=builder.as_markup(),
        )
    else:
        await state.set_state(NewRequestStates.contract_number)
        await message.answer("Номер договора (если нет — отправьте «-»).")


@router.callback_query(StateFilter(NewRequestStates.contract_number), F.data.startswith("spec:contract:"))
async def handle_contract_choice(callback: CallbackQuery, state: FSMContext):
    _, _, contract_id_str = callback.data.split(":")
    if contract_id_str == "manual":
        await callback.message.edit_reply_markup()
        await callback.message.answer("Введите номер договора (если нет — отправьте «-»).")
        await callback.answer()
        return

    try:
        contract_id = int(contract_id_str)
    except ValueError:
        await callback.answer("Некорректный договор. Введите номер вручную.", show_alert=True)
        return

    async with async_session() as session:
        contract = await session.get(Contract, contract_id)

    if not contract:
        await callback.answer("Договор не найден. Введите номер вручную.", show_alert=True)
        return

    await state.update_data(contract_number=contract.number)
    await callback.message.edit_text(f"Договор: {contract.number}")

    async with async_session() as session:
        defect_types = await _get_defect_types(session)

    await state.set_state(NewRequestStates.defect_type)
    if defect_types:
        await callback.message.answer(
            "Выберите тип дефекта из списка или введите свой текстом.",
            reply_markup=_defect_type_keyboard(defect_types),
        )
    else:
        await callback.message.answer("Тип дефекта (например, «Трещины в стене»).")
    await callback.answer()


@router.message(StateFilter(NewRequestStates.contract_number))
async def handle_contract(message: Message, state: FSMContext):
    contract = (message.text or "").strip()
    await state.update_data(contract_number=None if contract == "-" else contract or None)

    async with async_session() as session:
        defect_types = await _get_defect_types(session)

    await state.set_state(NewRequestStates.defect_type)
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
    await _prompt_inspection_calendar(callback.message)
    await callback.answer()


@router.message(StateFilter(NewRequestStates.defect_type))
async def handle_defect_type(message: Message, state: FSMContext):
    defect = message.text.strip()
    await state.update_data(defect_type=None if defect == "-" else defect)
    await state.set_state(NewRequestStates.inspection_datetime)
    await _prompt_inspection_calendar(message)


@router.message(StateFilter(NewRequestStates.inspection_datetime))
async def handle_inspection_datetime(message: Message, state: FSMContext):
    text = message.text.strip()
    if text == "-":
        await state.update_data(inspection_datetime=None, inspection_date=None)
        await state.set_state(NewRequestStates.inspection_location)
        await _prompt_inspection_location(message)
        return

    await message.answer(
        "Дата выбирается через календарь. Нажмите на нужный день или отправьте «-», если дата неизвестна."
    )


@router.callback_query(
    StateFilter(NewRequestStates.inspection_datetime),
    F.data.startswith(f"cal:{SPEC_CALENDAR_PREFIX}:"),
)
async def specialist_calendar_callback(callback: CallbackQuery, state: FSMContext):
    payload = parse_calendar_callback(callback.data)
    if not payload:
        await callback.answer()
        return

    if payload.action in {"prev", "next"}:
        new_year, new_month = shift_month(payload.year, payload.month, payload.action)
        await callback.message.edit_reply_markup(
            reply_markup=build_calendar(SPEC_CALENDAR_PREFIX, year=new_year, month=new_month)
        )
        await callback.answer()
        return

    if payload.action == "day" and payload.day:
        selected = date(payload.year, payload.month, payload.day)
        await state.update_data(
            inspection_date=selected.isoformat(),
            inspection_datetime=None,
        )
        await state.set_state(NewRequestStates.inspection_time)
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.message.answer(
            f"Дата осмотра: {selected.strftime('%d.%m.%Y')}.\n"
            "Введите время в формате ЧЧ:ММ или отправьте «-», если время пока неизвестно."
        )
        await callback.answer(f"Выбрано {selected.strftime('%d.%m.%Y')}")
        return

    await callback.answer()


@router.message(StateFilter(NewRequestStates.inspection_location))
async def handle_inspection_location(message: Message, state: FSMContext):
    location = message.text.strip()
    await state.update_data(inspection_location=None if location == "-" else location)

    async with async_session() as session:
        data = await state.get_data()
        specialist_id = data.get("specialist_id")
        
        # Получаем текущего пользователя для проверки "(я)"
        current_user = await session.scalar(
            select(User).where(User.telegram_id == message.from_user.id)
        )
        current_user_id = current_user.id if current_user else None
        
        # Получаем инженеров
        engineers_query = select(User).where(User.role == UserRole.ENGINEER)
        
        # Получаем суперадминов (менеджеры с is_super_admin = True)
        superadmins_query = (
            select(User)
            .join(Leader, User.id == Leader.user_id)
            .where(User.role == UserRole.MANAGER, Leader.is_super_admin == True)
        )
        
        # Объединяем запросы
        engineers_result = await session.execute(engineers_query)
        engineers = list(engineers_result.scalars().all())
        
        superadmins_result = await session.execute(superadmins_query)
        superadmins = list(superadmins_result.scalars().all())
        
        # Получаем самого специалиста, если он не инженер и не суперадмин
        specialist = None
        if specialist_id:
            specialist = await session.get(User, specialist_id)
            if specialist:
                # Проверяем, не является ли он уже в списке
                engineer_ids = {eng.id for eng in engineers}
                superadmin_ids = {sa.id for sa in superadmins}
                if specialist.id not in engineer_ids and specialist.id not in superadmin_ids:
                    # Добавляем специалиста в список
                    engineers.append(specialist)
                else:
                    specialist = None  # Уже в списке, не добавляем отдельно

    # Объединяем всех кандидатов
    all_candidates = engineers + superadmins
    if specialist and specialist not in all_candidates:
        all_candidates.append(specialist)
    
    if not all_candidates:
        await message.answer("Нет доступных инженеров. Обратитесь к руководителю.")
        await state.clear()
        return

    # Сортируем по имени
    all_candidates.sort(key=lambda u: u.full_name)
    
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text=f"{user.full_name}{' (я)' if current_user_id and user.id == current_user_id else ''}",
                    callback_data=f"assign_engineer:{user.id}",
                )
            ]
            for user in all_candidates
        ]
    )
    await state.set_state(NewRequestStates.engineer)
    await message.answer("Выберите ответственного инженера для заявки:", reply_markup=kb)


@router.callback_query(StateFilter(NewRequestStates.engineer), F.data.startswith("assign_engineer:"))
async def handle_engineer_callback(callback: CallbackQuery, state: FSMContext):
    try:
        engineer_id = int(callback.data.split(":")[1])
    except (ValueError, IndexError):
        await callback.answer("Ошибка при выборе инженера. Попробуйте снова.", show_alert=True)
        return
    
    # Проверяем, что выбранный пользователь существует и может быть инженером
    async with async_session() as session:
        engineer_user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.id == engineer_id)
        )
        if not engineer_user:
            await callback.answer("Выбранный пользователь не найден.", show_alert=True)
            return
        
        # Проверяем, что пользователь может быть инженером
        can_be_engineer = (
            engineer_user.role == UserRole.ENGINEER
            or engineer_user.role == UserRole.SPECIALIST
            or (engineer_user.role == UserRole.MANAGER 
                and engineer_user.leader_profile 
                and engineer_user.leader_profile.is_super_admin)
        )
        if not can_be_engineer:
            await callback.answer("Выбранный пользователь не может быть назначен инженером.", show_alert=True)
            return
    
    await state.update_data(engineer_id=engineer_id, remedy_term_days=14)
    await state.set_state(NewRequestStates.letter)
    try:
        await callback.message.edit_reply_markup()
    except Exception:
        pass
    await callback.message.answer(
        "Прикрепите файл обращения (письмо) в формате PDF/документа или отправьте «-», если письма нет.\n"
        "Для отмены напишите «Отмена».",
    )
    await callback.answer()


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

        engineer_user = await session.scalar(
            select(User)
            .options(selectinload(User.leader_profile))
            .where(User.id == data["engineer_id"])
        )
        if not engineer_user:
            await message.answer("Выбранный инженер не найден. Попробуйте снова.")
            await state.clear()
            return

        # Убеждаемся, что у выбранного инженера есть профиль Engineer, если он не является инженером по роли
        # Это нужно для специалистов и супер-админов, которые могут быть назначены как инженеры
        from app.infrastructure.db.models.roles.engineer import Engineer
        
        # Проверяем, есть ли профиль Engineer
        if engineer_user.role != UserRole.ENGINEER:
            engineer_profile = await session.scalar(
                select(Engineer).where(Engineer.user_id == engineer_user.id)
            )
            if not engineer_profile:
                # Создаем профиль Engineer для специалиста или супер-админа
                engineer_profile = Engineer(user_id=engineer_user.id)
                session.add(engineer_profile)
                await session.flush()

        try:
            create_data = RequestCreateData(
                title=data["title"],
                description=data["description"],
                object_name=data["object_name"],
                address=data["address"],
                apartment=data.get("apartment"),
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

            request_label = format_request_label(request)
            request_title = request.title
            due_at = request.due_at
        except Exception as e:
            await session.rollback()
            await message.answer(
                f"❌ Ошибка при создании заявки: {str(e)}\n"
                "Попробуйте создать заявку заново или обратитесь к администратору."
            )
            await state.clear()
            return

    await message.answer(
        f"✅ Заявка {request_label} создана и назначена инженеру.\n"
        "Следите за статусом в разделе «📄 Мои заявки»."
    )
    await state.clear()

    engineer_telegram = getattr(engineer_user, "telegram_id", None) if engineer_user else None
    if engineer_telegram:
        due_text = format_moscow(due_at) or "не задан"
        notification = (
            f"Новая заявка {request_label}.\n"
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
    inspection_text = format_moscow(inspection_dt) or "не указан"

    letter_text = "приложено" if data.get("letter_file_id") else "нет"

    apartment_text = data.get('apartment') or '—'
    return (
        "Проверьте данные:\n"
        f"🔹 Заголовок: {data['title']}\n"
        f"🔹 Объект: {data['object_name']}\n"
        f"🔹 Адрес: {data['address']}\n"
        f"🔹 Квартира: {apartment_text}\n"
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
    due_text = format_moscow(request.due_at) or "не задан"
    inspection_text = format_moscow(request.inspection_scheduled_at) or "не назначен"
    inspection_done = format_moscow(request.inspection_completed_at) or "нет"
    label = format_request_label(request)

    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    hours_delta = actual_hours - planned_hours
    
    # Рассчитываем разбивку стоимостей
    cost_breakdown = _calculate_cost_breakdown(request.work_items or [])

    lines = [
        f"📄 <b>{label}</b>",
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
        f"Плановая стоимость видов работ: {_format_currency(cost_breakdown['planned_work_cost'])} ₽",
        f"Плановая стоимость материалов: {_format_currency(cost_breakdown['planned_material_cost'])} ₽",
        f"Плановая общая стоимость: {_format_currency(cost_breakdown['planned_total_cost'])} ₽",
        f"Фактическая стоимость видов работ: {_format_currency(cost_breakdown['actual_work_cost'])} ₽",
        f"Фактическая стоимость материалов: {_format_currency(cost_breakdown['actual_material_cost'])} ₽",
        f"Фактическая общая стоимость: {_format_currency(cost_breakdown['actual_total_cost'])} ₽",
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
            is_material = bool(
                item.planned_material_cost
                or item.actual_material_cost
                or ("материал" in (item.category or "").lower())
            )
            emoji = "📦" if is_material else "🛠"
            planned_cost = item.planned_cost
            actual_cost = item.actual_cost
            if planned_cost in (None, 0):
                planned_cost = item.planned_material_cost
            if actual_cost in (None, 0):
                actual_cost = item.actual_material_cost
            unit = item.unit or ""
            qty_part = ""
            if item.planned_quantity is not None or item.actual_quantity is not None:
                pq = item.planned_quantity if item.planned_quantity is not None else 0
                aq = item.actual_quantity if item.actual_quantity is not None else 0
                qty_part = f" | объём: {pq:.2f} → {aq:.2f} {unit}".rstrip()
            lines.append(
                f"{emoji} {item.name} — план {_format_currency(planned_cost)} ₽ / "
                f"факт {_format_currency(actual_cost)} ₽{qty_part}"
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
            lines.append(f"✉️ Письма/файлы: {letter_text}")
            lines.append("   (нажмите на кнопку ниже, чтобы открыть файл)")
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


def _format_hours(value: float | None) -> str:
    if value is None:
        return "0.0 ч"
    return f"{float(value):.1f} ч"


def _build_specialist_analytics(requests: list[Request]) -> str:
    from collections import Counter

    now = now_moscow()
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
            due_text = format_moscow(req.due_at) or "не задан"
            lines.append(f"• {req.number} — до {due_text}")

    return "\n".join(lines)
@router.message(StateFilter(NewRequestStates.inspection_time))
async def handle_inspection_time(message: Message, state: FSMContext):
    text = (message.text or "").strip()
    if text == "-":
        await state.update_data(inspection_datetime=None, inspection_date=None)
        await state.set_state(NewRequestStates.inspection_location)
        await _prompt_inspection_location(message)
        return

    try:
        time_value = datetime.strptime(text, "%H:%M").time()
    except ValueError:
        await message.answer("Не удалось распознать время. Используйте формат ЧЧ:ММ.")
        return

    data = await state.get_data()
    date_text = data.get("inspection_date")
    if not date_text:
        await message.answer("Сначала выберите дату через календарь.")
        await state.set_state(NewRequestStates.inspection_datetime)
        await _prompt_inspection_calendar(message)
        return

    selected_date = date.fromisoformat(date_text)
    inspection_dt = combine_moscow(selected_date, time_value)
    await state.update_data(inspection_datetime=inspection_dt, inspection_date=None)
    await state.set_state(NewRequestStates.inspection_location)
    await _prompt_inspection_location(message)
