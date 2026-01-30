from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
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
    UserRole,
    WorkItem,
    WorkSession,
)
from app.services.work_catalog import WorkCatalogItem, WorkMaterialSpec, get_work_catalog
from app.services.material_catalog import MaterialCatalogItem
from app.utils.identifiers import generate_request_number
from app.utils.request_formatters import get_request_status_title
from app.utils.timezone import now_moscow, to_moscow


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
    apartment: str | None = None
    defect_type_name: str | None = None
    contract_number: str | None = None
    contract_description: str | None = None
    inspection_datetime: datetime | None = None
    inspection_location: str | None = None
    remedy_term_days: int = 14
    due_at: datetime | None = None
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

        inspection_dt = to_moscow(data.inspection_datetime)
        if data.due_at is not None:
            due_at = to_moscow(data.due_at)
            baseline = to_moscow(inspection_dt) or now_moscow()
            remedy_term_days = max(0, (due_at - baseline).days) if due_at else data.remedy_term_days
        else:
            # Срок устранения не задан при создании (его указывает инженер)
            due_at = None
            remedy_term_days = data.remedy_term_days

        request = Request(
            number=request_number,
            title=data.title,
            description=data.description,
            object=object_ref,
            contract=contract_ref,
            defect_type=defect_ref,
            address=data.address,
            apartment=data.apartment,
            contact_person=data.contact_person,
            contact_phone=data.contact_phone,
            inspection_scheduled_at=inspection_dt,
            inspection_location=data.inspection_location,
            due_at=due_at,
            remedy_term_days=remedy_term_days,
            specialist_id=data.specialist_id,
            engineer_id=data.engineer_id,
            customer_id=data.customer_id,
            status=RequestStatus.INSPECTION_SCHEDULED if inspection_dt else RequestStatus.NEW,
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

        if inspection_dt:
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.INSPECTION,
                scheduled_at=inspection_dt,
                recipients=[data.engineer_id, data.specialist_id],
            )
        else:
            follow_up = now_moscow() + timedelta(hours=4)
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.REPORT,
                scheduled_at=follow_up,
                recipients=[data.engineer_id, data.specialist_id],
            )

        if due_at:
            manager_ids = await RequestService._get_manager_ids(session)
            recipients = [data.engineer_id, data.specialist_id, *(manager_ids or [])]
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.DEADLINE,
                scheduled_at=due_at,
                recipients=recipients,
            )
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.OVERDUE,
                scheduled_at=due_at + timedelta(days=1),
                recipients=recipients,
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
        inspection_datetime = to_moscow(inspection_datetime)
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
            follow_up = request.inspection_scheduled_at + timedelta(hours=2)
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.REPORT,
                scheduled_at=follow_up,
                recipients=[engineer_id, request.specialist_id],
                replace_existing=False,
            )
        return request

    @staticmethod
    async def set_remedy_term(
        session: AsyncSession,
        request: Request,
        remedy_term_days: int,
    ) -> Request:
        """Меняет срок устранения и пересчитывает дедлайн."""
        request.remedy_term_days = remedy_term_days
        if request.inspection_scheduled_at:
            request.due_at = RequestService._calculate_due_date(
                request.inspection_scheduled_at,
                remedy_term_days,
            )
        else:
            base = request.due_at or now_moscow()
            request.due_at = base + timedelta(days=remedy_term_days)
        await session.flush()
        return request

    @staticmethod
    async def set_due_date(
        session: AsyncSession,
        request: Request,
        due_at: datetime,
    ) -> Request:
        """Устанавливает срок устранения (дату дедлайна)."""
        due_at = to_moscow(due_at)
        request.due_at = due_at
        baseline = to_moscow(request.inspection_scheduled_at) or now_moscow()
        request.remedy_term_days = max(0, (due_at - baseline).days)
        await session.flush()
        return request

    @staticmethod
    async def set_engineer_planned_hours(
        session: AsyncSession,
        request: Request,
        hours: float,
    ) -> Request:
        """Устанавливает плановые часы, введённые инженером вручную (суммируются с часами из позиций)."""
        request.engineer_planned_hours = max(0.0, float(hours))
        await RequestService._recalculate_budget(session, request)
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
        completed_dt = to_moscow(completed_at) or now_moscow()
        request.inspection_completed_at = completed_dt
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
        follow_up = now_moscow() + timedelta(hours=4)
        await RequestService._schedule_reminder(
            session=session,
            request=request,
            reminder_type=ReminderType.REPORT,
            scheduled_at=follow_up,
            recipients=[request.engineer_id, request.specialist_id],
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
        request.master_assigned_at = now_moscow()
        if not request.due_at:
            request.due_at = now_moscow() + timedelta(days=request.remedy_term_days)
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
            manager_ids = await RequestService._get_manager_ids(session)
            recipients = [master_id, request.engineer_id, request.specialist_id, *(manager_ids or [])]
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.DEADLINE,
                scheduled_at=request.due_at,
                recipients=recipients,
                replace_existing=True,
            )
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.OVERDUE,
                scheduled_at=request.due_at + timedelta(days=1),
                recipients=recipients,
                replace_existing=True,
            )
        follow_up = now_moscow() + timedelta(hours=12)
        await RequestService._schedule_reminder(
            session=session,
            request=request,
            reminder_type=ReminderType.REPORT,
            scheduled_at=follow_up,
            recipients=[master_id, request.engineer_id],
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
        started_at = to_moscow(started_at) or now_moscow()
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
        follow_up = started_at + timedelta(hours=24)
        await RequestService._schedule_reminder(
            session=session,
            request=request,
            reminder_type=ReminderType.REPORT,
            scheduled_at=follow_up,
            recipients=[master_id, request.engineer_id],
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
        finalize: bool = True,
    ) -> WorkSession:
        """Закрывает смену мастера. При finalize=True переводит заявку в COMPLETED."""
        finished_at = to_moscow(finished_at) or now_moscow()
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

        await session.flush()
        # Явно передаём часы только что закрытой сессии: после flush() SELECT может ещё не видеть строку
        current_hours = (
            work_session.hours_reported
            if work_session.hours_reported is not None
            else work_session.hours_calculated
        ) or 0
        await RequestService._recalculate_hours(
            session, request,
            excluding_session_id=work_session.id,
            add_session_hours=float(current_hours),
        )

        previous_status = request.status
        if finalize:
            request.work_completed_at = finished_at
            request.completion_notes = completion_notes
            request.status = RequestStatus.COMPLETED
        await session.flush()

        if finalize:
            await RequestService._register_stage(
                session=session,
                request=request,
                from_status=previous_status,
                to_status=RequestStatus.COMPLETED,
                changed_by_id=master_id,
                comment="Мастер завершил работы и направил отчёт",
            )

            sign_at = finished_at + timedelta(hours=2)
            manager_ids = await RequestService._get_manager_ids(session)
            recipients = [request.engineer_id, request.specialist_id, *(manager_ids or [])]
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.DOCUMENT_SIGN,
                scheduled_at=sign_at,
                recipients=recipients,
                replace_existing=True,
            )
            await RequestService._schedule_reminder(
                session=session,
                request=request,
                reminder_type=ReminderType.REPORT,
                scheduled_at=finished_at + timedelta(hours=6),
                recipients=recipients,
            )
        else:
            await RequestService._register_stage(
                session=session,
                request=request,
                from_status=previous_status,
                to_status=request.status,
                changed_by_id=master_id,
                comment="Мастер завершил смену (работы продолжаются)",
            )
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

        reminder_at = now_moscow() + timedelta(hours=4)
        manager_ids = await RequestService._get_manager_ids(session)
        recipients = [request.engineer_id, request.specialist_id, *(manager_ids or [])]
        await RequestService._schedule_reminder(
            session=session,
            request=request,
            reminder_type=ReminderType.DOCUMENT_SIGN,
            scheduled_at=reminder_at,
            recipients=recipients,
            replace_existing=True,
        )
        return request

    @staticmethod
    async def can_close_request(request: Request) -> tuple[bool, list[str]]:
        """
        Проверяет, можно ли закрыть заявку.
        Возвращает (можно_ли_закрыть, список_причин_если_нельзя).
        """
        reasons = []
        
        # Проверяем статус - заявка должна быть завершена или готова к подписанию
        if request.status not in {RequestStatus.COMPLETED, RequestStatus.READY_FOR_SIGN}:
            reasons.append(
                f"Заявка должна быть в статусе «Работы завершены» или «Ожидает подписания», "
                f"текущий статус: {get_request_status_title(request.status)}"
            )
        
        # Проверяем, что работы завершены
        if not request.work_completed_at:
            reasons.append("Работы должны быть завершены мастером")
        
        # Проверяем, что мастер назначен и завершил работы
        if not request.master_id:
            reasons.append("Мастер не назначен")
        
        # Проверяем, что инженер провёл осмотр
        if not request.inspection_completed_at:
            reasons.append("Осмотр должен быть завершён инженером")
        
        return len(reasons) == 0, reasons

    @staticmethod
    async def close_request(
        session: AsyncSession,
        request: Request,
        user_id: int,
        comment: str | None = None,
    ) -> Request:
        """Окончательно закрывает заявку специалистом или суперадмином."""
        can_close, reasons = await RequestService.can_close_request(request)
        if not can_close:
            raise ValueError(f"Заявку нельзя закрыть: {', '.join(reasons)}")
        
        previous_status = request.status
        request.status = RequestStatus.CLOSED
        await session.flush()

        await RequestService._register_stage(
            session=session,
            request=request,
            from_status=previous_status,
            to_status=RequestStatus.CLOSED,
            changed_by_id=user_id,
            comment=comment or "Заявка закрыта специалистом",
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
    async def delete_request(session: AsyncSession, request: Request) -> None:
        """Полностью удаляет заявку из БД (вместе со связанными записями по cascade)."""
        await session.delete(request)
        await session.flush()

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
    async def update_actual_from_catalog(
        session: AsyncSession,
        request: Request,
        *,
        catalog_item: WorkCatalogItem,
        actual_quantity: float,
        author_id: int,
    ) -> WorkItem:
        """Обновляет факт работы и автоматически материалы по нормам."""
        norm_target = _normalize_work_item_name(catalog_item.name)
        stmt = (
            select(WorkItem)
            .where(WorkItem.request_id == request.id)
            .order_by(WorkItem.id.asc())
        )
        result = await session.execute(stmt)
        candidates = [item for item in result.scalars().all() if _normalize_work_item_name(item.name) == norm_target]

        work_item: WorkItem | None = None
        duplicates: list[WorkItem] = []
        for candidate in candidates:
            if work_item is None:
                work_item = candidate
                continue
            if _has_plan_data(candidate) and not _has_plan_data(work_item):
                duplicates.append(work_item)
                work_item = candidate
            else:
                duplicates.append(candidate)

        category_label = " / ".join(catalog_item.path[:-1]) or None

        if not work_item:
            work_item = WorkItem(
                request_id=request.id,
                name=catalog_item.name,
                category=category_label,
                unit=catalog_item.unit,
                planned_quantity=None,
                planned_hours=None,
                planned_cost=None,
                planned_material_cost=None,
            )
            session.add(work_item)
        else:
            for duplicate in duplicates:
                if not work_item.notes and duplicate.notes:
                    work_item.notes = duplicate.notes
                await session.delete(duplicate)

        work_item.category = work_item.category or category_label
        work_item.unit = catalog_item.unit or work_item.unit
        work_item.actual_quantity = float(actual_quantity)
        work_item.actual_cost = float(round(catalog_item.price * actual_quantity, 2))

        # Автодобавление фактических материалов
        await RequestService._upsert_materials_from_work(
            session=session,
            request=request,
            work_name=catalog_item.name,
            category_label=category_label,
            quantity=actual_quantity,
            is_plan=False,
        )

        await session.flush()
        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Обновлены факты по каталогу «{catalog_item.name}»",
        )
        return work_item

    @staticmethod
    async def add_plan_from_catalog(
        session: AsyncSession,
        request: Request,
        *,
        catalog_item: WorkCatalogItem,
        planned_quantity: float,
        author_id: int,
    ) -> WorkItem:
        """Добавляет/обновляет план работы и автоматически материалы по нормам."""
        stmt = select(WorkItem).where(
            WorkItem.request_id == request.id,
            func.lower(WorkItem.name) == catalog_item.name.lower(),
        )
        result = await session.execute(stmt)
        work_item = result.scalars().first()

        category_label = " / ".join(catalog_item.path[:-1]) or None
        planned_cost = float(round(catalog_item.price * planned_quantity, 2))

        if not work_item:
            work_item = WorkItem(
                request_id=request.id,
                name=catalog_item.name,
                category=category_label,
                unit=catalog_item.unit,
                planned_quantity=float(planned_quantity),
                planned_cost=planned_cost,
            )
            session.add(work_item)
        else:
            work_item.category = work_item.category or category_label
            work_item.unit = catalog_item.unit or work_item.unit
            work_item.planned_quantity = float(planned_quantity)
            work_item.planned_cost = planned_cost

        # Автодобавление плановых материалов
        await RequestService._upsert_materials_from_work(
            session=session,
            request=request,
            work_name=catalog_item.name,
            category_label=category_label,
            quantity=planned_quantity,
            is_plan=True,
        )

        await session.flush()
        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Обновлён план по каталогу «{catalog_item.name}»",
        )
        return work_item

    @staticmethod
    async def add_plan_from_material_catalog(
        session: AsyncSession,
        request: Request,
        *,
        catalog_item: MaterialCatalogItem,
        planned_quantity: float,
        author_id: int,
    ) -> WorkItem:
        """Добавляет или обновляет плановую позицию материала из каталога."""
        stmt = select(WorkItem).where(
            WorkItem.request_id == request.id,
            func.lower(WorkItem.name) == catalog_item.name.lower(),
        )
        result = await session.execute(stmt)
        work_item = result.scalars().first()

        category_label = " / ".join(catalog_item.path[:-1]) or None
        planned_material_cost = float(round(catalog_item.price * planned_quantity, 2))

        if not work_item:
            work_item = WorkItem(
                request_id=request.id,
                name=catalog_item.name,
                category=category_label,
                unit=catalog_item.unit,
                planned_quantity=float(planned_quantity),
                planned_material_cost=planned_material_cost,
            )
            session.add(work_item)
        else:
            work_item.category = work_item.category or category_label
            work_item.unit = catalog_item.unit or work_item.unit
            work_item.planned_quantity = float(planned_quantity)
            work_item.planned_material_cost = planned_material_cost

        await session.flush()
        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Добавлен план материала по каталогу «{catalog_item.name}»",
        )
        return work_item

    @staticmethod
    async def update_actual_from_material_catalog(
        session: AsyncSession,
        request: Request,
        *,
        catalog_item: MaterialCatalogItem,
        actual_quantity: float,
        author_id: int,
    ) -> WorkItem:
        """Обновляет фактические данные по материалу из каталога."""
        norm_target = _normalize_work_item_name(catalog_item.name)
        stmt = (
            select(WorkItem)
            .where(WorkItem.request_id == request.id)
            .order_by(WorkItem.id.asc())
        )
        result = await session.execute(stmt)
        candidates = [item for item in result.scalars().all() if _normalize_work_item_name(item.name) == norm_target]

        work_item: WorkItem | None = None
        duplicates: list[WorkItem] = []
        for candidate in candidates:
            if work_item is None:
                work_item = candidate
                continue
            if _has_plan_data(candidate) and not _has_plan_data(work_item):
                duplicates.append(work_item)
                work_item = candidate
            else:
                duplicates.append(candidate)

        category_label = " / ".join(catalog_item.path[:-1]) or None

        if not work_item:
            work_item = WorkItem(
                request_id=request.id,
                name=catalog_item.name,
                category=category_label,
                unit=catalog_item.unit,
                planned_quantity=None,
                planned_hours=None,
                planned_cost=None,
                planned_material_cost=None,
            )
            session.add(work_item)
        else:
            for duplicate in duplicates:
                if not work_item.notes and duplicate.notes:
                    work_item.notes = duplicate.notes
                await session.delete(duplicate)

        work_item.category = work_item.category or category_label
        work_item.unit = catalog_item.unit or work_item.unit
        work_item.actual_quantity = float(actual_quantity)
        work_item.actual_material_cost = float(round(catalog_item.price * actual_quantity, 2))

        await session.flush()
        await RequestService._recalculate_budget(session, request)

        await RequestService._register_stage(
            session=session,
            request=request,
            to_status=request.status,
            changed_by_id=author_id,
            comment=f"Обновлены факты по материалу из каталога «{catalog_item.name}»",
        )
        return work_item

    # --- internal helpers ---

    @staticmethod
    async def _upsert_materials_from_work(
        session: AsyncSession,
        request: Request,
        *,
        work_name: str,
        category_label: str | None,
        quantity: float,
        is_plan: bool,
    ) -> None:
        """
        Автоматически добавляет/обновляет материалы по нормам на единицу работы.
        quantity — план или факт в зависимости от is_plan.
        """
        catalog = get_work_catalog()
        materials: tuple[WorkMaterialSpec, ...] = catalog.get_materials_for_work(work_name)
        if not materials:
            return

        for material in materials:
            total_qty = round(material.qty_per_work_unit * quantity, 4)
            total_cost = round(material.price_per_unit * total_qty, 2)
            item_category = (category_label or "Материалы") + " • материалы"

            stmt = select(WorkItem).where(
                WorkItem.request_id == request.id,
                func.lower(WorkItem.name) == material.name.lower(),
            )
            result = await session.execute(stmt)
            work_item = result.scalars().first()

            if not work_item:
                work_item = WorkItem(
                    request_id=request.id,
                    name=material.name,
                    category=item_category,
                    unit=material.unit,
                )
                session.add(work_item)

            if is_plan:
                work_item.planned_quantity = total_qty
                work_item.planned_material_cost = total_cost
                work_item.unit = material.unit or work_item.unit
                work_item.category = work_item.category or item_category
            else:
                work_item.actual_quantity = total_qty
                work_item.actual_material_cost = total_cost
                work_item.unit = material.unit or work_item.unit
                work_item.category = work_item.category or item_category

        await session.flush()

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
        recipient_ids = [int(r) for r in recipients if r]
        if not recipient_ids:
            raise ValueError("Не удалось определить получателей напоминания")

        if replace_existing:
            await session.execute(
                delete(RequestReminder).where(
                    RequestReminder.request_id == request.id,
                    RequestReminder.reminder_type == reminder_type,
                    RequestReminder.is_sent.is_(False),
                )
            )

        rows = await session.execute(
            select(User.id, User.telegram_id).where(User.id.in_(recipient_ids))
        )
        id_to_telegram = {row.id: row.telegram_id for row in rows if row.telegram_id}

        telegram_ids: set[int] = set()
        for recipient in recipient_ids:
            telegram_id = id_to_telegram.get(recipient)
            if telegram_id:
                telegram_ids.add(int(telegram_id))
            else:
                telegram_ids.add(recipient)

        reminder = RequestReminder(
            request_id=request.id,
            reminder_type=reminder_type,
            scheduled_at=scheduled_at,
            recipients=",".join(str(r) for r in telegram_ids),
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
        planned_cost, actual_cost, work_item_planned_hours, work_item_actual_hours = result.one()
        request.planned_budget = float(planned_cost)
        request.actual_budget = float(actual_cost)
        engineer_hours = float(request.engineer_planned_hours or 0)
        request.planned_hours = engineer_hours + float(work_item_planned_hours)
        await RequestService._recalculate_actual_hours(session, request, work_item_actual_hours=float(work_item_actual_hours))
        await session.flush()

    @staticmethod
    async def _recalculate_hours(
        session: AsyncSession,
        request: Request,
        *,
        excluding_session_id: int | None = None,
        add_session_hours: float = 0,
    ) -> None:
        """Пересчитывает request.actual_hours по сессиям мастера (и позициям бюджета)."""
        await RequestService._recalculate_actual_hours(
            session,
            request,
            excluding_session_id=excluding_session_id,
            add_session_hours=add_session_hours,
        )
        await session.flush()

    @staticmethod
    async def _recalculate_actual_hours(
        session: AsyncSession,
        request: Request,
        *,
        work_item_actual_hours: float | None = None,
        excluding_session_id: int | None = None,
        add_session_hours: float = 0,
    ) -> None:
        """Суммирует фактические часы: позиции бюджета + сессии мастера (hours_reported или hours_calculated)."""
        if work_item_actual_hours is None:
            stmt = select(func.coalesce(func.sum(WorkItem.actual_hours), 0)).where(
                WorkItem.request_id == request.id
            )
            result = await session.execute(stmt)
            work_item_actual_hours = float(result.scalar() or 0)
        # По каждой завершённой сессии берём hours_reported или hours_calculated, суммируем
        filters = [
            WorkSession.request_id == request.id,
            WorkSession.finished_at.isnot(None),
        ]
        if excluding_session_id is not None:
            filters.append(WorkSession.id != excluding_session_id)
        stmt = select(
            func.coalesce(
                func.sum(
                    func.coalesce(WorkSession.hours_reported, WorkSession.hours_calculated, 0)
                ),
                0,
            )
        ).where(*filters)
        result = await session.execute(stmt)
        session_hours = float(result.scalar() or 0) + add_session_hours
        request.actual_hours = work_item_actual_hours + session_hours

    @staticmethod
    async def _get_or_create_object(session: AsyncSession, name: str, address: str | None) -> Object:
        stmt = select(Object).where(Object.name == name)
        result = await session.execute(stmt)
        obj = result.scalars().first()
        if obj:
            return obj
        await session.execute(
            insert(Object)
            .values(name=name, address=address, created_at=now_moscow())
            .on_conflict_do_nothing(index_elements=["name"])
        )
        await session.flush()
        result = await session.execute(select(Object).where(Object.name == name))
        return result.scalars().one()

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
        normalized = name.strip()
        if not normalized:
            raise ValueError("Название типа дефекта не задано")
        stmt = select(DefectType).where(func.lower(DefectType.name) == normalized.lower())
        result = await session.execute(stmt)
        defect = result.scalars().first()
        if defect:
            return defect
        insert_stmt = (
            insert(DefectType)
            .values(name=normalized)
            .on_conflict_do_update(index_elements=[DefectType.name], set_={"name": normalized})
            .returning(DefectType.id)
        )
        inserted = await session.execute(insert_stmt)
        row = inserted.first()
        if row and getattr(row, "id", None):
            defect = await session.get(DefectType, row.id)
            if defect:
                return defect
        result = await session.execute(
            select(DefectType).where(func.lower(DefectType.name) == normalized.lower())
        )
        defect = result.scalars().first()
        if defect:
            return defect
        raise RuntimeError("Не удалось сохранить тип дефекта.")

    @staticmethod
    def _calculate_due_date(start: datetime | None, remedy_term_days: int) -> datetime | None:
        baseline = to_moscow(start) or now_moscow()
        return baseline + timedelta(days=remedy_term_days)

    @staticmethod
    async def _get_manager_ids(session: AsyncSession) -> list[int]:
        rows = await session.execute(
            select(User.id).where(User.role == UserRole.MANAGER)
        )
        return list(rows.scalars().all())


def _normalize_work_item_name(value: str) -> str:
    return " ".join(value.strip().lower().split())


def _has_plan_data(item: WorkItem) -> bool:
    return any(
        field not in (None, 0)
        for field in (
            item.planned_quantity,
            item.planned_hours,
            item.planned_cost,
            item.planned_material_cost,
        )
    )


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
