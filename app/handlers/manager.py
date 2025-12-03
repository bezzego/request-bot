from __future__ import annotations

from datetime import timedelta

from aiogram import F, Router
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, FSInputFile, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
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
from app.utils.timezone import now_moscow

router = Router()


class ManagerCloseStates(StatesGroup):
    comment = State()
    confirmation = State()


@router.message(F.text == "👥 Управление пользователями")
async def manager_users(message: Message):
    async with async_session() as session:
        manager = await _get_super_admin(session, message.from_user.id)
        if not manager:
            await message.answer("Доступно только супер-администраторам.")
            return

        users = (
            (
                await session.execute(
                    select(User).order_by(User.created_at.desc()).limit(30)
                )
            )
            .scalars()
            .all()
        )

    if not users:
        await message.answer("Пока нет зарегистрированных пользователей.")
        return

    builder = InlineKeyboardBuilder()
    for user in users:
        builder.button(
            text=f"{user.full_name} · {user.role}",
            callback_data=f"manager:role:{user.id}",
        )
    builder.adjust(1)

    await message.answer(
        "Выберите пользователя, чтобы изменить роль или посмотреть данные.",
        reply_markup=builder.as_markup(),
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
    builder.adjust(2)

    await callback.message.answer(
        f"Текущая роль пользователя {user.full_name}: {user.role}\nВыберите новую роль:",
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
            text=f"{req.number} · {status}",
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
            text=f"{status_emoji} {req.number} · {req.status.value}",
            callback_data=f"manager:detail:{req.id}",
        )
    builder.adjust(1)

    await message.answer(
        "📋 <b>Последние 30 заявок</b>\n\n"
        "Выберите заявку, чтобы посмотреть подробности и закрыть её.",
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
        
        builder = InlineKeyboardBuilder()
        
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
        
        await callback.message.edit_text(detail_text, reply_markup=builder.as_markup())
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
        
        await state.update_data(
            request_id=request_id,
            request_number=request.number,
        )
        await state.set_state(ManagerCloseStates.comment)
        
        await callback.message.answer(
            f"📋 <b>Закрытие заявки {request.number}</b>\n\n"
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
    request_number = data.get("request_number", "N/A")
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить закрытие", callback_data="manager:close_confirm")
    builder.button(text="❌ Отменить", callback_data="manager:close_cancel")
    builder.adjust(1)
    
    comment_text = f"\n\nКомментарий: {comment}" if comment else "\n\nКомментарий не указан"
    await message.answer(
        f"📋 <b>Подтверждение закрытия заявки {request_number}</b>\n\n"
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
            
            await callback.message.answer(
                f"✅ <b>Заявка {request.number} успешно закрыта</b>\n\n"
                f"Все работы завершены, заявка закрыта.",
            )
            await callback.answer("Заявка закрыта")
            
            # Уведомляем инженера, если он назначен
            if request.engineer and request.engineer.telegram_id:
                try:
                    await callback.message.bot.send_message(
                        chat_id=int(request.engineer.telegram_id),
                        text=f"✅ Заявка {request.number} закрыта суперадмином.",
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
            text=f"{status_emoji} {req.number} · {req.status.value}",
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
