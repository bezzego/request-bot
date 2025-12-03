from __future__ import annotations

from datetime import timedelta

from aiogram import F, Router
from aiogram.types import CallbackQuery, FSInputFile, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Leader, Request, User, UserRole
from app.infrastructure.db.session import async_session
from app.services.export import ExportService
from app.services.reporting import ReportingService
from app.services.user_service import UserRoleService
from app.utils.timezone import now_moscow

router = Router()


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
                    .limit(20)
                )
            )
            .scalars()
            .all()
        )

    if not requests:
        await message.answer("Нет заявок в системе.")
        return

    lines = ["📋 <b>Последние 20 заявок</b>"]
    for req in requests:
        lines.append(
            f"#{req.number} · {req.title}\n"
            f"Статус: {req.status.value}\n"
            f"Специалист: {req.specialist.full_name if req.specialist else '—'}\n"
            f"Инженер: {req.engineer.full_name if req.engineer else '—'}\n"
            f"Мастер: {req.master.full_name if req.master else '—'}\n"
        )

    await message.answer("\n".join(lines))


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
