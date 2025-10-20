from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import (
    Contract,
    DefectType,
    Object,
    ReminderType,
    Request,
    RequestReminder,
    RequestStageHistory,
    RequestStatus,
    User,
    WorkItem,
    WorkSession,
)
from app.utils.identifiers import generate_request_number


@dataclass(slots=True)
class RequestCreateData:
    """Данные для первичного создания заявки специалистом."""

    title: str
    description: str
    object_name: str
    address: str
    contact_person: str
    contact_phone: str
    specialist_id: int
    engineer_id: int
    defect_type_name: str | None = None
    contract_number: str | None = None
    contract_description: str | None = None
    inspection_datetime: datetime | None = None
    inspection_location: str | None = None
    remedy_term_days: int = 14
    customer_id: int | None = None


@dataclass(slots=True)
class WorkItemData:
    """Позиция план-факт бюджета по заявке."""

    name: str
    category: str | None = None
    unit: str | None = None
    planned_quantity: float | None = None
    planned_hours: float | None = None
    planned_cost: float | None = None
    planned_material_cost: float | None = None
    actual_quantity: float | None = None
    actual_hours: float | None = None
    actual_cost: float | None = None
    actual_material_cost: float | None = None
    notes: str | None = None


class RequestService:
    """Бизнес-логика жизненного цикла заявок."""

    @staticmethod
    async def create_request(session: AsyncSession, data: RequestCreateData) -> Request:
        """Создаёт заявку от специалиста, назначает инженера и план осмотра."""
        request_number = await generate_request_number(session)
        object_ref = await RequestService._get_or_create_object(session, data.object_name, data.address)
        contract_ref = None
        if data.contract_number:
            contract_ref = await RequestService._get_or_create_contract(
                session,
                data.contract_number,
                data.contract_description,
            )
        defect_ref = None
        if data.defect_type_name:
            defect_ref = await RequestService._get_or_create_defect_type(session, data.defect_type_name)

        due_at = RequestService._calculate_due_date(data.inspection_datetime, data.remedy_term_days)

        request = Request(
            number=request_number,
            title=data.title,
            description=data.description,
            object=object_ref,
            contract=contract_ref,
            defect_type=defect_ref,
            address=data.address,
            contact_person=data.contact_person,
            contact_phone=data.contact_phone,
            inspection_scheduled_at=data.inspection_datetime,
            inspection_location=data.inspection_location,
            due_at=due_at,
            remedy_term_days=data.remedy_term_days,
            specialist_id=data.specialist_id,
            engineer_id=data.engineer_id,
            customer_id=data.customer_id,
            status=RequestStatus.INSPECTION_SCHEDULED
            if data.inspection_datetime
            else RequestStatus.NEW,
        )

        session.add(request)
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=data.specialist_id,
            comment="Заявка создана специалистом",
        )

        if data.inspection_datetime:
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.INSPECTION,
                scheduled_at=data.inspection_datetime,
                recipients=[data.engineer_id, data.specialist_id],
            )

        if due_at:
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.DEADLINE,
                scheduled_at=due_at,
                recipients=[data.engineer_id, data.specialist_id],
            )

        return request

    @staticmethod
    async def assign_engineer(
        session: AsyncSession,
        request: Request,
        engineer_id: int,
        inspection_datetime: datetime | None = None,
        inspection_location: str | None = None,
    ) -> Request:
        """Назначает инженера на заявку или обновляет план осмотра."""
        previous_status = request.status
        request.engineer_id = engineer_id
        if inspection_datetime:
            request.inspection_scheduled_at = inspection_datetime
            request.status = RequestStatus.INSPECTION_SCHEDULED
        if inspection_location:
            request.inspection_location = inspection_location
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=request.status,
            changed_by_id=engineer_id,
            comment="Назначен инженер и запланирован осмотр",
        )

        if request.inspection_scheduled_at:
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.INSPECTION,
                scheduled_at=request.inspection_scheduled_at,
                recipients=[engineer_id, request.specialist_id],
                replace_existing=True,
            )
        return request

    @staticmethod
    async def record_inspection(
        session: AsyncSession,
        request: Request,
        engineer_id: int,
        *,  # keyword-only
        notes: str | None = None,
        completed_at: datetime | None = None,
    ) -> Request:
        """Фиксирует факт прохождения осмотра инженером."""
        previous_status = request.status
        request.inspection_completed_at = completed_at or datetime.now(timezone.utc)
        request.inspection_notes = notes
        request.status = RequestStatus.INSPECTED
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.INSPECTED,
            changed_by_id=engineer_id,
            comment="Инженер завершил осмотр",
        )
        return request

    @staticmethod
    async def assign_master(
        session: AsyncSession,
        request: Request,
        master_id: int,
        assigned_by: int,
    ) -> Request:
        """Назначает мастера и переводит заявку в статус ASSIGNED."""
        previous_status = request.status
        request.master_id = master_id
        request.master_assigned_at = datetime.now(timezone.utc)
        if not request.due_at:
            request.due_at = datetime.now(timezone.utc) + timedelta(days=request.remedy_term_days)
        request.status = RequestStatus.ASSIGNED
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.ASSIGNED,
            changed_by_id=assigned_by,
            comment="Назначен мастер на выполнение работ",
        )

        if request.due_at:
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.DEADLINE,
                scheduled_at=request.due_at,
                recipients=[master_id, request.engineer_id or request.specialist_id],
                replace_existing=True,
            )
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.OVERDUE,
                scheduled_at=request.due_at + timedelta(days=1),
                recipients=[master_id, request.specialist_id],
                replace_existing=True,
            )
        return request

    @staticmethod
    async def start_work(
        session: AsyncSession,
        request: Request,
        master_id: int,
        *,
        latitude: float | None = None,
        longitude: float | None = None,
        address: str | None = None,
        started_at: datetime | None = None,
    ) -> WorkSession:
        """Фиксирует начало работ мастером."""
        started_at = started_at or datetime.now(timezone.utc)
        work_session = WorkSession(
            request_id=request.id,
            master_id=master_id,
            started_at=started_at,
            started_latitude=latitude,
            started_longitude=longitude,
            started_address=address,
        )
        session.add(work_session)

        previous_status = request.status
        request.work_started_at = started_at
        request.work_started_place = address
        request.status = RequestStatus.IN_PROGRESS
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.IN_PROGRESS,
            changed_by_id=master_id,
            comment="Мастер приступил к работам",
        )
        return work_session

    @staticmethod
    async def finish_work(
        session: AsyncSession,
        request: Request,
        master_id: int,
        *,
        session_id: int | None = None,
        latitude: float | None = None,
        longitude: float | None = None,
        address: str | None = None,
        finished_at: datetime | None = None,
        hours_reported: float | None = None,
        completion_notes: str | None = None,
    ) -> WorkSession:
        """Закрывает сессию работ мастера и переводит заявку в статус COMPLETED."""
        finished_at = finished_at or datetime.now(timezone.utc)
        filters = [
            WorkSession.master_id == master_id,
            WorkSession.finished_at.is_(None),
        ]
        if session_id:
            filters.append(WorkSession.id == session_id)
        else:
            filters.append(WorkSession.request_id == request.id)

        stmt = select(WorkSession).where(*filters).order_by(WorkSession.started_at.desc())
        result = await session.execute(stmt)
        work_session = result.scalars().first()
        if not work_session:
            raise ValueError("Активная смена мастера не найдена")

        work_session.finished_at = finished_at
        work_session.finished_latitude = latitude
        work_session.finished_longitude = longitude
        work_session.finished_address = address
        work_session.hours_reported = hours_reported
        if work_session.started_at and finished_at:
            delta = finished_at - work_session.started_at
            work_session.hours_calculated = round(delta.total_seconds() / 3600, 2)

        previous_status = request.status
        request.work_completed_at = finished_at
        request.completion_notes = completion_notes
        request.status = RequestStatus.COMPLETED
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.COMPLETED,
            changed_by_id=master_id,
            comment="Мастер завершил работы и направил отчёт",
        )

        await RequestService._recalculate_hours(session, request)
        return work_session

    @staticmethod
    async def mark_ready_for_sign(
        session: AsyncSession,
        request: Request,
        user_id: int,
    ) -> Request:
        """Переводит заявку в статус READY_FOR_SIGN и планирует напоминание по актам."""
        previous_status = request.status
        request.status = RequestStatus.READY_FOR_SIGN
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.READY_FOR_SIGN,
            changed_by_id=user_id,
            comment="Заявка готова к подписанию актов",
        )

        reminder_at = datetime.now(timezone.utc) + timedelta(hours=4)
        await RequestService._schedule_reminder(
            session=session,
            request=request,
            reminder_type=ReminderType.DOCUMENT_SIGN,
            scheduled_at=reminder_at,
            recipients=[request.engineer_id or request.specialist_id],
            replace_existing=True,
        )
        return request

    @staticmethod
    async def close_request(
        session: AsyncSession,
        request: Request,
        manager_id: int,
        comment: str | None = None,
    ) -> Request:
        """Окончательно закрывает заявку руководителем."""
        previous_status = request.status
        request.status = RequestStatus.CLOSED
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.CLOSED,
            changed_by_id=manager_id,
            comment=comment or "Заявка закрыта руководителем",
        )
        return request

    @staticmethod
    async def cancel_request(
        session: AsyncSession,
        request: Request,
        cancelled_by: int,
        reason: str | None = None,
    ) -> Request:
        """Отменяет заявку."""
        previous_status = request.status
        request.status = RequestStatus.CANCELLED
        request.completion_notes = reason
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.CANCELLED,
            changed_by_id=cancelled_by,
            comment=reason,
        )
        return request

    @staticmethod
    async def add_work_item(
        session: AsyncSession,
        request: Request,
        item: WorkItemData,
        author_id: int,
    ) -> WorkItem:
        """Добавляет или обновляет позицию бюджета заявки."""
        work_item = WorkItem(
            request_id=request.id,
            name=item.name,
            category=item.category,
            unit=item.unit,
            planned_quantity=item.planned_quantity,
            planned_hours=item.planned_hours,
            planned_cost=item.planned_cost,
            planned_material_cost=item.planned_material_cost,
            actual_quantity=item.actual_quantity,
            actual_hours=item.actual_hours,
            actual_cost=item.actual_cost,
            actual_material_cost=item.actual_material_cost,
            notes=item.notes,
        )
        session.add(work_item)
        await session.flush()

        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Обновлён план/факт по позиции «{item.name}»",
        )
        return work_item

    @staticmethod
    async def update_work_item_actual(
        session: AsyncSession,
        request: Request,
        *,
        name: str,
        actual_quantity: float | None = None,
        actual_hours: float | None = None,
        actual_cost: float | None = None,
        actual_material_cost: float | None = None,
        notes: str | None = None,
        author_id: int,
    ) -> WorkItem:
        stmt = select(WorkItem).where(
            WorkItem.request_id == request.id,
            func.lower(WorkItem.name) == name.lower(),
        )
        result = await session.execute(stmt)
        work_item = result.scalars().first()
        if not work_item:
            raise ValueError(f"Позиция «{name}» не найдена в заявке")

        if actual_quantity is not None:
            work_item.actual_quantity = actual_quantity
        if actual_hours is not None:
            work_item.actual_hours = actual_hours
        if actual_cost is not None:
            work_item.actual_cost = actual_cost
        if actual_material_cost is not None:
            work_item.actual_material_cost = actual_material_cost
        if notes is not None:
            work_item.notes = notes

        await session.flush()
        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Обновлены фактические данные по позиции «{name}»",
        )
        return work_item

    @staticmethod
    async def _register_stage(
        session: AsyncSession,
        *,
        request: Request,
        to_status: RequestStatus,
        changed_by_id: int | None,
        from_status: RequestStatus | None = None,
        comment: str | None = None,
    ) -> None:
        stage = RequestStageHistory(
            request_id=request.id,
            from_status=from_status,
            to_status=to_status,
            changed_by_id=changed_by_id,
            comment=comment,
        )
        session.add(stage)
        await session.flush()

    @staticmethod
    async def _schedule_reminder(
        session: AsyncSession,
        *,
        request: Request,
        reminder_type: ReminderType,
        scheduled_at: datetime,
        recipients: Iterable[int | None],
        replace_existing: bool = False,
    ) -> RequestReminder:
        """Создаёт запись напоминания. При replace_existing удаляет прошлые."""
        recipients_list = [r for r in recipients if r]
        if not recipients_list:
            raise ValueError("Не удалось определить получателей напоминания")

        if replace_existing:
            await session.execute(
                select(RequestReminder)
                .where(
                    RequestReminder.request_id == request.id,
                    RequestReminder.reminder_type == reminder_type,
                    RequestReminder.is_sent.is_(False),
                )
                .execution_options(synchronize_session="fetch")
            )

        reminder = RequestReminder(
            request_id=request.id,
            reminder_type=reminder_type,
            scheduled_at=scheduled_at,
            recipients=",".join(str(r) for r in recipients_list),
        )
        session.add(reminder)
        await session.flush()
        return reminder

    @staticmethod
    async def _recalculate_budget(session: AsyncSession, request: Request) -> None:
        stmt = (
            select(
                func.coalesce(func.sum(WorkItem.planned_cost), 0),
                func.coalesce(func.sum(WorkItem.actual_cost), 0),
                func.coalesce(func.sum(WorkItem.planned_hours), 0),
                func.coalesce(func.sum(WorkItem.actual_hours), 0),
            )
            .where(WorkItem.request_id == request.id)
        )
        result = await session.execute(stmt)
        planned_cost, actual_cost, planned_hours, actual_hours = result.one()
        request.planned_budget = float(planned_cost)
        request.actual_budget = float(actual_cost)
        request.planned_hours = float(planned_hours)
        request.actual_hours = float(actual_hours)
        await session.flush()

    @staticmethod
    async def _recalculate_hours(session: AsyncSession, request: Request) -> None:
        stmt = (
            select(
                func.coalesce(func.sum(WorkSession.hours_reported), 0),
                func.coalesce(func.sum(WorkSession.hours_calculated), 0),
            ).where(WorkSession.request_id == request.id)
        )
        result = await session.execute(stmt)
        reported, calculated = result.one()
        request.actual_hours = float(reported or calculated)
        await session.flush()

    @staticmethod
    async def _get_or_create_object(session: AsyncSession, name: str, address: str | None) -> Object:
        stmt = select(Object).where(func.lower(Object.name) == name.lower())
        result = await session.execute(stmt)
        obj = result.scalars().first()
        if obj:
            return obj
        obj = Object(name=name, address=address)
        session.add(obj)
        await session.flush()
        return obj

    @staticmethod
    async def _get_or_create_contract(
        session: AsyncSession,
        number: str,
        description: str | None,
    ) -> Contract:
        stmt = select(Contract).where(func.lower(Contract.number) == number.lower())
        result = await session.execute(stmt)
        contract = result.scalars().first()
        if contract:
            return contract
        contract = Contract(number=number, description=description)
        session.add(contract)
        await session.flush()
        return contract

    @staticmethod
    async def _get_or_create_defect_type(session: AsyncSession, name: str) -> DefectType:
        stmt = select(DefectType).where(func.lower(DefectType.name) == name.lower())
        result = await session.execute(stmt)
        defect = result.scalars().first()
        if defect:
            return defect
        defect = DefectType(name=name)
        session.add(defect)
        await session.flush()
        return defect

    @staticmethod
    def _calculate_due_date(start: datetime | None, remedy_term_days: int) -> datetime | None:
        if not start:
            return None
        return start + timedelta(days=remedy_term_days)


async def load_request(session: AsyncSession, request_number: str) -> Request | None:
    """Загружает заявку вместе со связанными данными."""
    stmt = (
        select(Request)
        .options(
            selectinload(Request.object),
            selectinload(Request.contract),
            selectinload(Request.defect_type),
            selectinload(Request.specialist),
            selectinload(Request.engineer),
            selectinload(Request.master),
            selectinload(Request.work_items),
            selectinload(Request.work_sessions),
        )
        .where(Request.number == request_number)
    )
    result = await session.execute(stmt)
    return result.scalars().first()
