from __future__ import annotations

from datetime import timedelta

from aiogram import F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Act,
    ActType,
    Leader,
    Request,
    RequestStatus,
    User,
    UserRole,
)
from app.infrastructure.db.session import async_session
from app.services.export import ExportService
from app.services.reporting import ReportingService
from app.services.request_service import RequestService
from app.services.user_service import UserRoleService
from app.utils.request_formatters import format_request_label
from app.utils.timezone import now_moscow

router = Router()


class ManagerCloseStates(StatesGroup):
    comment = State()
    confirmation = State()


class ManagerFilterStates(StatesGroup):
    mode = State()
    value = State()


@router.message(F.text == "👥 Управление пользователями")
async def manager_users(message: Message):
    """Показывает меню выбора фильтра пользователей."""
    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только супер-администраторам.")
            return
    
    # Создаем меню выбора фильтра
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Все пользователи", callback_data="manager:users_filter:all")
    builder.button(text="👨‍💼 Специалисты", callback_data="manager:users_filter:specialist")
    builder.button(text="🔧 Инженеры", callback_data="manager:users_filter:engineer")
    builder.button(text="👷 Мастера", callback_data="manager:users_filter:master")
    builder.button(text="👔 Менеджеры", callback_data="manager:users_filter:manager")
    builder.button(text="👤 Клиенты", callback_data="manager:users_filter:client")
    builder.button(text="🆕 Новые клиенты", callback_data="manager:users_filter:new_clients")
    builder.adjust(2)
    
    await message.answer(
        "👥 <b>Управление пользователями</b>\n\n"
        "Выберите категорию пользователей для просмотра:",
        reply_markup=builder.as_markup(),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("manager:users_filter:"))
async def manager_users_filter(callback: CallbackQuery):
    """Обработчик фильтрации пользователей по ролям."""
    if not callback.message:
        await callback.answer("Ошибка", show_alert=True)
        return
    
    # Парсим фильтр из callback_data
    try:
        filter_type = callback.data.split(":")[2]
    except (ValueError, IndexError):
        await callback.answer("Ошибка формата", show_alert=True)
        return
    
    await callback.answer()
    await _show_users_by_filter(callback.message, filter_type, edit=True)


async def _show_users_by_filter(message: Message, filter_type: str, edit: bool = False):
    """Показывает пользователей по выбранному фильтру."""
    # Получаем telegram_id из message
    telegram_id = message.from_user.id if message.from_user else None
    if not telegram_id:
        if not edit:
            await message.answer("Ошибка: не удалось определить пользователя.")
        return
    
    async with async_session() as session:
        manager = await _get_super_admin(session, telegram_id)
        if not manager:
            if not edit:
                await message.answer("Доступно только супер-администраторам.")
            return

        # Формируем запрос в зависимости от фильтра
        query = select(User)
        
        if filter_type == "all":
            query = query.order_by(User.created_at.desc())
            filter_name = "Все пользователи"
        elif filter_type == "new_clients":
            # Новые клиенты (зарегистрированные за последние 30 дней)
            thirty_days_ago = now_moscow() - timedelta(days=30)
            query = (
                query
                .where(User.role == UserRole.CLIENT)
                .where(User.created_at >= thirty_days_ago)
                .order_by(User.created_at.desc())
            )
            filter_name = "Новые клиенты (последние 30 дней)"
        else:
            # Фильтр по роли
            try:
                role = UserRole(filter_type)
                query = (
                    query
                    .where(User.role == role)
                    .order_by(User.created_at.desc())
                )
                role_names = {
                    UserRole.SPECIALIST: "Специалисты",
                    UserRole.ENGINEER: "Инженеры",
                    UserRole.MASTER: "Мастера",
                    UserRole.MANAGER: "Менеджеры",
                    UserRole.CLIENT: "Клиенты",
                }
                filter_name = role_names.get(role, filter_type)
            except ValueError:
                if not edit:
                    await message.answer("Неверный фильтр.")
                return

        # Загружаем пользователей (ограничиваем до 100 для удобства)
        users = (
            (await session.execute(query.limit(100)))
            .scalars()
            .all()
        )

    if not users:
        text = f"👥 <b>{filter_name}</b>\n\nПользователей не найдено."
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад к фильтрам", callback_data="manager:users_back")
        builder.adjust(1)
        
        if edit:
            try:
                await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
            except Exception:
                await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
        return

    # Создаем кнопки для пользователей
    builder = InlineKeyboardBuilder()
    for user in users:
        # Ограничиваем длину текста кнопки
        button_text = f"{user.full_name} · {user.role.value}"
        if len(button_text) > 60:
            button_text = button_text[:57] + "..."
        builder.button(
            text=button_text,
            callback_data=f"manager:role:{user.id}",
        )
    
    builder.adjust(1)
    
    # Добавляем кнопку "Назад к фильтрам"
    builder.button(text="⬅️ Назад к фильтрам", callback_data="manager:users_back")
    builder.adjust(1)

    text = (
        f"👥 <b>{filter_name}</b>\n\n"
        f"Найдено пользователей: {len(users)}\n\n"
        f"Выберите пользователя, чтобы изменить роль или посмотреть данные."
    )

    if edit:
        try:
            await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
        except Exception as e:
            error_msg = str(e).lower()
            if "message is not modified" in error_msg or "message to edit not found" in error_msg:
                try:
                    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
                except Exception:
                    await message.answer(text, reply_markup=builder.as_markup())
            else:
                try:
                    await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
                except Exception:
                    await message.answer(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")


@router.callback_query(F.data == "manager:users_back")
async def manager_users_back(callback: CallbackQuery):
    """Возвращает к меню выбора фильтра."""
    await callback.answer()
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            return
    
    # Создаем меню выбора фильтра
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Все пользователи", callback_data="manager:users_filter:all")
    builder.button(text="👨‍💼 Специалисты", callback_data="manager:users_filter:specialist")
    builder.button(text="🔧 Инженеры", callback_data="manager:users_filter:engineer")
    builder.button(text="👷 Мастера", callback_data="manager:users_filter:master")
    builder.button(text="👔 Менеджеры", callback_data="manager:users_filter:manager")
    builder.button(text="👤 Клиенты", callback_data="manager:users_filter:client")
    builder.button(text="🆕 Новые клиенты", callback_data="manager:users_filter:new_clients")
    builder.adjust(2)
    
    try:
        await callback.message.edit_text(
            "👥 <b>Управление пользователями</b>\n\n"
            "Выберите категорию пользователей для просмотра:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )
    except Exception:
        await callback.message.answer(
            "👥 <b>Управление пользователями</b>\n\n"
            "Выберите категорию пользователей для просмотра:",
            reply_markup=builder.as_markup(),
            parse_mode="HTML",
        )




@router.callback_query(F.data.startswith("manager:role:"))
async def manager_pick_role(callback: CallbackQuery):
    user_id = int(callback.data.split(":")[2])

    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        user = await session.scalar(select(User).where(User.id == user_id))
        if not user:
            await callback.answer("Пользователь не найден.", show_alert=True)
            return

    builder = InlineKeyboardBuilder()
    for role in UserRole:
        builder.button(
            text=role.value,
            callback_data=f"manager:set_role:{user_id}:{role.value}",
        )
    builder.button(text="Отмена", callback_data="manager:cancel_role")
    builder.button(text="⬅️ Назад к фильтрам", callback_data="manager:users_back")
    builder.adjust(2)

    await callback.message.answer(
        f"Текущая роль пользователя {user.full_name}: {user.role.value}\nВыберите новую роль:",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data == "manager:cancel_role")
async def manager_cancel_role(callback: CallbackQuery):
    await callback.answer("Изменение роли отменено.")
    await callback.message.delete()


@router.callback_query(F.data.startswith("manager:set_role:"))
async def manager_set_role(callback: CallbackQuery):
    _, _, user_id_str, role_value = callback.data.split(":")
    user_id = int(user_id_str)
    try:
        new_role = UserRole(role_value)
    except ValueError:
        await callback.answer("Некорректная роль.", show_alert=True)
        return

    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        user = await session.scalar(select(User).where(User.id == user_id))
        if not user:
            await callback.answer("Пользователь не найден.", show_alert=True)
            return

        old_role = user.role
        await UserRoleService.assign_role(session, user, new_role)
        await session.commit()

    await callback.answer("Роль обновлена.")
    await callback.message.edit_text(
        f"Роль пользователя <b>{user.full_name}</b> изменена:\n"
        f"{old_role.value} → {new_role.value}",
        parse_mode="HTML",
    )


@router.message(F.text == "📊 Отчёты и статистика")
async def manager_reports(message: Message):
    now = now_moscow()
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только супер-администраторам.")
            return

        summary = await ReportingService.period_summary(session, start=start, end=now)
        rating = await ReportingService.engineer_rating(session, start=start, end=now)
        feedback = await ReportingService.feedback_summary(session, start=start, end=now)

    lines = [
        "📊 <b>Отчёт по текущему месяцу</b>",
        f"Заявок создано: {summary.total_created}",
        f"Заявок закрыто: {summary.total_closed}",
        f"Активных: {summary.total_active}",
        f"Плановый бюджет: {summary.planned_budget:,.2f} ₽",
        f"Фактический бюджет: {summary.actual_budget:,.2f} ₽",
        f"Отклонение бюджета: {summary.budget_delta:,.2f} ₽",
        f"Плановые часы: {summary.planned_hours:,.1f}",
        f"Фактические часы: {summary.actual_hours:,.1f}",
        f"Закрыто в срок: {summary.closed_in_time} ( {summary.on_time_percent:.1f}% )",
        f"Просрочено: {summary.closed_overdue}",
        f"Среднее время выполнения: {summary.average_completion_time_hours:,.1f} ч",
        f"Общие затраты (750 ₽/ч): {summary.total_costs:,.2f} ₽",
        f"Индекс эффективности: {summary.efficiency_percent:.1f}%",
        f"Средние оценки клиентов: качество {feedback['quality']:.1f}, "
        f"сроки {feedback['time']:.1f}, культура {feedback['culture']:.1f}",
    ]

    if rating:
        lines.append("\n🏆 <b>Рейтинг инженеров</b>")
        for position, engineer in enumerate(rating, start=1):
            lines.append(
                f"{position}. {engineer.full_name} — {engineer.closed_requests} заявок, "
                f"эффективность {engineer.efficiency_percent:.1f}%"
            )
    else:
        lines.append("\nПока нет закрытых заявок для формирования рейтинга.")

    await message.answer("\n".join(lines))


@router.message(F.text == "📋 Мои заявки")
async def manager_my_requests(message: Message):
    """Обработчик для просмотра заявок суперадмина (использует функции специалиста)."""
    from app.handlers.specialist import _get_specialist, _load_specialist_requests
    
    async with async_session() as session:
        specialist_or_admin = await _get_specialist(session, message.from_user.id)
        if not specialist_or_admin:
            await message.answer("Эта функция доступна только специалистам отдела и суперадминам.")
            return

        requests = await _load_specialist_requests(session, specialist_or_admin.id)

    if not requests:
        await message.answer("У вас пока нет заявок. Создайте первую через «➕ Создать заявку».")
        return

    from aiogram.utils.keyboard import InlineKeyboardBuilder
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


@router.message(F.text == "📋 Все заявки")
async def manager_all_requests(message: Message):
    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await message.answer("Доступ ограничен.")
            return

        requests = (
            (
                await session.execute(
                    select(Request)
                    .options(
                        selectinload(Request.specialist),
                        selectinload(Request.engineer),
                        selectinload(Request.master),
                    )
                    .order_by(Request.created_at.desc())
                    .limit(30)
                )
            )
            .scalars()
            .all()
        )

    if not requests:
        await message.answer("Нет заявок в системе.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status_emoji = "✅" if req.status.value == "closed" else "🔄" if req.status.value in ["completed", "ready_for_sign"] else "📋"
        builder.button(
            text=f"{status_emoji} {format_request_label(req)} · {req.status.value}",
            callback_data=f"manager:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "📋 <b>Последние 30 заявок</b>\n\n"
        "Выберите заявку, чтобы посмотреть подробности и закрыть её.",
        reply_markup=builder.as_markup(),
    )


@router.message(F.text == "🔍 Фильтр заявок")
async def manager_filter_start(message: Message, state: FSMContext):
    await state.set_state(ManagerFilterStates.mode)
    await message.answer(
        "Выберите режим фильтрации:\n"
        "• отправьте «Адрес» — для поиска по адресу\n"
        "• отправьте «Дата» — для фильтра по диапазону дат создания (формат 01.01.2025-31.01.2025)"
    )


@router.message(StateFilter(ManagerFilterStates.mode))
async def manager_filter_mode(message: Message, state: FSMContext):
    text = (message.text or "").strip().lower()
    if text not in {"адрес", "дата"}:
        await message.answer("Введите «Адрес» или «Дата».")
        return
    await state.update_data(mode=text)
    await state.set_state(ManagerFilterStates.value)
    if text == "адрес":
        await message.answer("Введите часть адреса (улица, дом и т.п.).")
    else:
        await message.answer("Введите диапазон дат в формате ДД.ММ.ГГГГ-ДД.ММ.ГГГГ.")


@router.message(StateFilter(ManagerFilterStates.value))
async def manager_filter_apply(message: Message, state: FSMContext):
    from datetime import datetime
    data = await state.get_data()
    mode = data.get("mode")
    value = (message.text or "").strip()

    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await state.clear()
            await message.answer("Доступ ограничен.")
            return

        query = (
            select(Request)
            .options(
                selectinload(Request.specialist),
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
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
            (await session.execute(query.limit(50)))
            .scalars()
            .all()
        )

    await state.clear()

    if not requests:
        await message.answer("Заявок по заданному фильтру не найдено.")
        return

    builder = InlineKeyboardBuilder()
    for req in requests:
        status_emoji = "✅" if req.status.value == "closed" else "🔄" if req.status.value in ["completed", "ready_for_sign"] else "📋"
        builder.button(
            text=f"{status_emoji} {format_request_label(req)} · {req.status.value}",
            callback_data=f"manager:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Результаты фильтрации. Выберите заявку:",
        reply_markup=builder.as_markup(),
    )


@router.message(F.text == "📤 Экспорт Excel")
async def manager_export_prompt(message: Message):
    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только супер-администраторам.")
            return

    builder = InlineKeyboardBuilder()
    for days in (30, 90, 180):
        builder.button(text=f"За {days} дней", callback_data=f"manager:export:{days}")
    builder.button(text="Отмена", callback_data="manager:export_cancel")
    builder.adjust(1)

    await message.answer("Выберите период для выгрузки заявок:", reply_markup=builder.as_markup())


@router.callback_query(F.data == "manager:export_cancel")
async def manager_export_cancel(callback: CallbackQuery):
    await callback.answer("Выгрузка отменена.")
    await callback.message.delete()


@router.callback_query(F.data.startswith("manager:export:"))
async def manager_export(callback: CallbackQuery):
    period_days = int(callback.data.split(":")[2])
    end = now_moscow()
    start = end - timedelta(days=period_days)

    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return

        path = await ExportService.export_requests(session, start=start, end=end)

    await callback.answer("Файл сформирован.")
    await callback.message.answer_document(
        FSInputFile(path),
        caption=f"Excel-выгрузка заявок за последние {period_days} дней",
    )


@router.callback_query(F.data.startswith("manager:detail:"))
async def manager_request_detail(callback: CallbackQuery):
    """Показывает детали заявки для суперадмина с возможностью закрытия."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
                selectinload(Request.specialist),
                selectinload(Request.work_items),
                selectinload(Request.photos),
                selectinload(Request.acts),
                selectinload(Request.feedback),
            )
            .where(Request.id == request_id)
        )
        
        if not request:
            await callback.message.edit_text("Заявка не найдена.")
            await callback.answer()
            return
        
        # Используем функцию форматирования из specialist
        from app.handlers.specialist import _format_specialist_request_detail
        detail_text = _format_specialist_request_detail(request)
        
        # Проверяем, является ли суперадмин инженером на этой заявке
        from app.handlers.engineer import _get_engineer
        engineer = await _get_engineer(session, callback.from_user.id)
        is_engineer = engineer and request.engineer_id == engineer.id
        
        builder = InlineKeyboardBuilder()
        
        # Если суперадмин является инженером на этой заявке, показываем кнопки инженера
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
            button_text = file_name[:40] + "..." if len(file_name) > 40 else file_name
            builder.button(
                text=f"📎 {button_text}",
                callback_data=f"manager:file:{act.id}",
            )
        
        # Добавляем кнопку закрытия заявки, если можно закрыть
        can_close, reasons = await RequestService.can_close_request(request)
        if request.status == RequestStatus.CLOSED:
            builder.button(
                text="✅ Заявка закрыта",
                callback_data="manager:noop",
            )
        elif can_close:
            builder.button(
                text="✅ Закрыть заявку",
                callback_data=f"manager:close:{request.id}",
            )
        else:
            reason_text = reasons[0][:35] + "..." if reasons and len(reasons[0]) > 35 else (reasons[0] if reasons else "не выполнены условия")
            builder.button(
                text=f"⚠️ {reason_text}",
                callback_data=f"manager:close_info:{request.id}",
            )
        
        builder.button(text="⬅️ Назад к списку", callback_data="manager:back_to_list")
        builder.button(text="🔄 Обновить", callback_data=f"manager:detail:{request.id}")
        builder.adjust(1)
        
        try:
            await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
        except TelegramBadRequest as e:
            # Игнорируем ошибку "message is not modified" - это нормально, если данные не изменились
            if "message is not modified" not in str(e).lower():
                raise
        await callback.answer()


@router.callback_query(F.data.startswith("manager:file:"))
async def manager_open_file(callback: CallbackQuery):
    """Отправляет прикреплённый файл пользователю."""
    _, _, act_id_str = callback.data.split(":")
    act_id = int(act_id_str)
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        act = await session.scalar(
            select(Act)
            .where(Act.id == act_id, Act.type == ActType.LETTER)
        )
        
        if not act:
            await callback.answer("Файл не найден.", show_alert=True)
            return
        
        try:
            await callback.message.bot.send_document(
                chat_id=callback.from_user.id,
                document=act.file_id,
                caption=f"📎 {act.file_name or 'Файл'}",
            )
            await callback.answer("Файл отправлен.")
        except Exception as e:
            await callback.answer(f"Ошибка при отправке файла: {str(e)}", show_alert=True)


@router.callback_query(F.data.startswith("manager:close_info:"))
async def manager_close_info(callback: CallbackQuery):
    """Показывает информацию о том, почему заявку нельзя закрыть."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
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


@router.callback_query(F.data.startswith("manager:close:"))
async def manager_start_close(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс закрытия заявки."""
    _, _, request_id_str = callback.data.split(":")
    request_id = int(request_id_str)
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
        )
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            return
        
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
        
        request_label = format_request_label(request)
        await state.update_data(
            request_id=request_id,
            request_label=request_label,
        )
        await state.set_state(ManagerCloseStates.comment)
        
        await callback.message.answer(
            f"📋 <b>Закрытие заявки {request_label}</b>\n\n"
            f"Заявка будет окончательно закрыта.\n\n"
            f"Введите комментарий к закрытию (или отправьте «-», чтобы пропустить):",
        )
        await callback.answer()


@router.message(StateFilter(ManagerCloseStates.comment))
async def manager_close_comment(message: Message, state: FSMContext):
    """Обрабатывает комментарий при закрытии заявки."""
    comment = message.text.strip() if message.text and message.text.strip() != "-" else None
    await state.update_data(comment=comment)
    await state.set_state(ManagerCloseStates.confirmation)
    
    data = await state.get_data()
    request_label = data.get("request_label", "N/A")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить закрытие", callback_data="manager:close_confirm")
    builder.button(text="❌ Отменить", callback_data="manager:close_cancel")
    builder.adjust(1)
    
    comment_text = f"\n\nКомментарий: {comment}" if comment else "\n\nКомментарий не указан"
    await message.answer(
        f"📋 <b>Подтверждение закрытия заявки {request_label}</b>\n\n"
        f"Вы уверены, что хотите закрыть эту заявку?{comment_text}",
        reply_markup=builder.as_markup(),
    )


@router.callback_query(F.data == "manager:close_confirm", StateFilter(ManagerCloseStates.confirmation))
async def manager_close_confirm(callback: CallbackQuery, state: FSMContext):
    """Подтверждает закрытие заявки."""
    data = await state.get_data()
    request_id = data.get("request_id")
    comment = data.get("comment")
    
    if not request_id:
        await callback.answer("Ошибка: не найден ID заявки.", show_alert=True)
        await state.clear()
        return
    
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            await state.clear()
            return
        
        request = await session.scalar(
            select(Request)
            .options(
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.id == request_id)
        )
        
        if not request:
            await callback.answer("Заявка не найдена.", show_alert=True)
            await state.clear()
            return
        
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
                user_id=manager.id,
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
                        text=f"✅ Заявка {label} закрыта суперадмином.",
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


@router.callback_query(F.data == "manager:close_cancel")
async def manager_close_cancel(callback: CallbackQuery, state: FSMContext):
    """Отменяет закрытие заявки."""
    await state.clear()
    await callback.message.answer("Закрытие заявки отменено.")
    await callback.answer()


@router.callback_query(F.data == "manager:back_to_list")
async def manager_back_to_list(callback: CallbackQuery):
    """Возвращает к списку всех заявок."""
    async with async_session() as session:
        manager = await _get_super_admin(session, callback.from_user.id)
        if not manager:
            await callback.answer("Нет доступа.", show_alert=True)
            return
        
        requests = (
            (
                await session.execute(
                    select(Request)
                    .options(
                        selectinload(Request.specialist),
                        selectinload(Request.engineer),
                        selectinload(Request.master),
                    )
                    .order_by(Request.created_at.desc())
                    .limit(30)
                )
            )
            .scalars()
            .all()
        )
    
    if not requests:
        await callback.message.edit_text("Нет заявок в системе.")
        await callback.answer()
        return
    
    builder = InlineKeyboardBuilder()
    for req in requests:
        status_emoji = "✅" if req.status.value == "closed" else "🔄" if req.status.value in ["completed", "ready_for_sign"] else "📋"
        builder.button(
            text=f"{status_emoji} {format_request_label(req)} · {req.status.value}",
            callback_data=f"manager:detail:{req.id}",
        )
    builder.adjust(1)
    
    await callback.message.edit_text(
        "📋 <b>Последние 30 заявок</b>\n\n"
        "Выберите заявку, чтобы посмотреть подробности и закрыть её.",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data == "manager:noop")
async def manager_noop(callback: CallbackQuery):
    """Пустой обработчик для неактивных кнопок."""
    await callback.answer()


# --- служебные функции ---


async def _get_super_admin(session, telegram_id: int) -> User | None:
    stmt = (
        select(User)
        .join(Leader, Leader.user_id == User.id)
        .where(
            User.telegram_id == telegram_id,
            User.role == UserRole.MANAGER,
            Leader.is_super_admin.is_(True),
        )
    )
    return await session.scalar(stmt)
