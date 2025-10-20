from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Iterable, Sequence

from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import Feedback, Request, RequestStatus, User, UserRole


@dataclass(slots=True)
class PeriodSummary:
    total_created: int
    total_closed: int
    total_active: int
    planned_budget: float
    actual_budget: float
    budget_delta: float
    planned_hours: float
    actual_hours: float
    avg_hours_per_request: float
    closed_in_time: int
    closed_overdue: int
    on_time_percent: float
    average_completion_time_hours: float
    hourly_rate: float
    total_costs: float
    efficiency_percent: float


@dataclass(slots=True)
class EngineerMetrics:
    engineer_id: int
    full_name: str
    closed_requests: int
    on_time_percent: float
    budget_delta_percent: float
    avg_completion_hours: float
    efficiency_percent: float


class ReportingService:
    """Расчёт KPI и агрегированной статистики по заявкам."""

    HOURLY_RATE = 750.0

    @staticmethod
    async def period_summary(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> PeriodSummary:
        base_stmt = select(
            func.count(Request.id).label("total_created"),
            func.count(
                case((Request.status == RequestStatus.CLOSED, 1))
            ).label("total_closed"),
            func.count(
                case(
                    (Request.status.in_([RequestStatus.CLOSED, RequestStatus.CANCELLED]), None),
                    else_=1,
                )
            ).label("total_active"),
            func.coalesce(func.sum(Request.planned_budget), 0).label("planned_budget"),
            func.coalesce(func.sum(Request.actual_budget), 0).label("actual_budget"),
            func.coalesce(func.sum(Request.planned_hours), 0).label("planned_hours"),
            func.coalesce(func.sum(Request.actual_hours), 0).label("actual_hours"),
            func.count(
                case(
                    (
                        (Request.status == RequestStatus.CLOSED)
                        & (Request.work_completed_at.is_not(None))
                        & (Request.due_at.is_not(None))
                        & (Request.work_completed_at <= Request.due_at),
                        1,
                    )
                )
            ).label("closed_in_time"),
            func.count(
                case(
                    (
                        (Request.status == RequestStatus.CLOSED)
                        & (Request.work_completed_at.is_not(None))
                        & (Request.due_at.is_not(None))
                        & (Request.work_completed_at > Request.due_at),
                        1,
                    )
                )
            ).label("closed_overdue"),
            func.coalesce(
                func.avg(
                    func.extract("epoch", Request.work_completed_at - Request.work_started_at) / 3600
                ),
                0,
            ).label("avg_completion_hours"),
        ).where(Request.created_at.between(start, end))

        result = await session.execute(base_stmt)
        row = result.one()

        planned_budget = float(row.planned_budget or 0)
        actual_budget = float(row.actual_budget or 0)
        budget_delta = planned_budget - actual_budget

        planned_hours = float(row.planned_hours or 0)
        actual_hours = float(row.actual_hours or 0)

        avg_hours_per_request = (
            actual_hours / row.total_closed if row.total_closed else 0
        )

        total_closed = int(row.total_closed or 0)
        closed_in_time = int(row.closed_in_time or 0)
        closed_overdue = int(row.closed_overdue or 0)
        on_time_percent = (
            closed_in_time / total_closed * 100 if total_closed else 0
        )

        avg_completion_time = float(row.avg_completion_hours or 0)
        total_costs = actual_hours * ReportingService.HOURLY_RATE

        efficiency_numerator = budget_delta + (planned_hours - actual_hours)
        efficiency_denominator = (planned_budget + planned_hours) or 1
        efficiency_percent = efficiency_numerator / efficiency_denominator * 100

        return PeriodSummary(
            total_created=int(row.total_created or 0),
            total_closed=total_closed,
            total_active=int(row.total_active or 0),
            planned_budget=planned_budget,
            actual_budget=actual_budget,
            budget_delta=budget_delta,
            planned_hours=planned_hours,
            actual_hours=actual_hours,
            avg_hours_per_request=avg_hours_per_request,
            closed_in_time=closed_in_time,
            closed_overdue=closed_overdue,
            on_time_percent=on_time_percent,
            average_completion_time_hours=avg_completion_time,
            hourly_rate=ReportingService.HOURLY_RATE,
            total_costs=total_costs,
            efficiency_percent=efficiency_percent,
        )

    @staticmethod
    async def engineer_rating(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> list[EngineerMetrics]:
        stmt = (
            select(
                User.id,
                User.full_name,
                func.count(Request.id).label("closed_requests"),
                func.count(
                    case(
                        (
                            (Request.status == RequestStatus.CLOSED)
                            & (Request.work_completed_at.is_not(None))
                            & (Request.due_at.is_not(None))
                            & (Request.work_completed_at <= Request.due_at),
                            1,
                        )
                    )
                ).label("closed_in_time"),
                func.coalesce(func.sum(Request.planned_budget), 0).label("planned_budget"),
                func.coalesce(func.sum(Request.actual_budget), 0).label("actual_budget"),
                func.coalesce(func.avg(
                    func.extract("epoch", Request.work_completed_at - Request.work_started_at) / 3600
                ), 0).label("avg_completion_hours"),
                func.coalesce(func.sum(Request.planned_hours), 0).label("planned_hours"),
                func.coalesce(func.sum(Request.actual_hours), 0).label("actual_hours"),
            )
            .join(Request, Request.engineer_id == User.id)
            .where(
                User.role == UserRole.ENGINEER,
                Request.created_at.between(start, end),
                Request.status == RequestStatus.CLOSED,
            )
            .group_by(User.id, User.full_name)
        )

        result = await session.execute(stmt)
        metrics: list[EngineerMetrics] = []
        for row in result.all():
            closed_requests = int(row.closed_requests or 0)
            closed_in_time = int(row.closed_in_time or 0)
            on_time_percent = (
                closed_in_time / closed_requests * 100 if closed_requests else 0
            )

            planned_budget = float(row.planned_budget or 0)
            actual_budget = float(row.actual_budget or 0)
            budget_delta_percent = (
                (planned_budget - actual_budget) / planned_budget * 100
                if planned_budget
                else 0
            )

            planned_hours = float(row.planned_hours or 0)
            actual_hours = float(row.actual_hours or 0)
            efficiency_numerator = (planned_budget - actual_budget) + (planned_hours - actual_hours)
            efficiency_denominator = (planned_budget + planned_hours) or 1
            efficiency_percent = efficiency_numerator / efficiency_denominator * 100

            metrics.append(
                EngineerMetrics(
                    engineer_id=row.id,
                    full_name=row.full_name,
                    closed_requests=closed_requests,
                    on_time_percent=on_time_percent,
                    budget_delta_percent=budget_delta_percent,
                    avg_completion_hours=float(row.avg_completion_hours or 0),
                    efficiency_percent=efficiency_percent,
                )
            )
        metrics.sort(key=lambda item: item.efficiency_percent, reverse=True)
        return metrics

    @staticmethod
    async def feedback_summary(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> dict[str, float]:
        stmt = (
            select(
                func.coalesce(func.avg(Feedback.rating_quality), 0),
                func.coalesce(func.avg(Feedback.rating_time), 0),
                func.coalesce(func.avg(Feedback.rating_culture), 0),
            )
            .join(Request, Feedback.request_id == Request.id)
            .where(Feedback.created_at.between(start, end))
        )
        result = await session.execute(stmt)
        quality, time_score, culture = result.one()
        return {
            "quality": float(quality or 0),
            "time": float(time_score or 0),
            "culture": float(culture or 0),
        }
