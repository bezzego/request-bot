import enum
from datetime import datetime

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow


class RequestStatus(enum.StrEnum):
    """Статусы заявок."""

    NEW = "new"  # новая, только создана
    INSPECTION_SCHEDULED = "inspection_scheduled"  # назначен осмотр
    INSPECTED = "inspected"  # инженер провёл осмотр
    ASSIGNED = "assigned"  # согласован мастер
    IN_PROGRESS = "in_progress"  # работы запущены мастером
    COMPLETED = "completed"  # работы завершены мастером
    READY_FOR_SIGN = "ready_for_sign"  # ожидает подписания актов
    CLOSED = "closed"  # проверена и закрыта
    CANCELLED = "cancelled"  # отменена


class Request(Base):
    """Основная таблица заявок."""

    __tablename__ = "requests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    number: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[RequestStatus] = mapped_column(
        Enum(RequestStatus),
        default=RequestStatus.NEW,
        nullable=False,
        index=True,
    )

    object_id: Mapped[int | None] = mapped_column(ForeignKey("objects.id"), nullable=True)
    contract_id: Mapped[int | None] = mapped_column(ForeignKey("contracts.id"), nullable=True)
    defect_type_id: Mapped[int | None] = mapped_column(ForeignKey("defect_types.id"), nullable=True)
    customer_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)

    address: Mapped[str] = mapped_column(String(512), nullable=False)
    apartment: Mapped[str | None] = mapped_column(String(50), nullable=True)
    contact_person: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_phone: Mapped[str] = mapped_column(String(50), nullable=False)

    inspection_scheduled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    inspection_location: Mapped[str | None] = mapped_column(String(255))
    inspection_notes: Mapped[str | None] = mapped_column(Text)
    inspection_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    master_assigned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    work_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    work_started_place: Mapped[str | None] = mapped_column(String(255))
    work_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completion_notes: Mapped[str | None] = mapped_column(Text)

    due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)
    remedy_term_days: Mapped[int] = mapped_column(default=14, nullable=False)

    planned_budget: Mapped[float | None] = mapped_column(Numeric(12, 2))
    actual_budget: Mapped[float | None] = mapped_column(Numeric(12, 2))
    planned_hours: Mapped[float | None] = mapped_column(Numeric(10, 2))
    actual_hours: Mapped[float | None] = mapped_column(Numeric(10, 2))

    sheet_row_number: Mapped[int | None] = mapped_column(nullable=True)
    sheet_url: Mapped[str | None] = mapped_column(String(255))

    specialist_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False, index=True)
    engineer_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    master_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
        index=True,
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=now_moscow,
    )

    specialist: Mapped["User"] = relationship(
        back_populates="created_request",
        foreign_keys=[specialist_id],
    )
    engineer: Mapped["User"] = relationship(
        back_populates="engineer_request",
        foreign_keys=[engineer_id],
    )
    master: Mapped["User"] = relationship(
        back_populates="master_request",
        foreign_keys=[master_id],
    )
    customer: Mapped["User"] = relationship(
        foreign_keys=[customer_id],
        lazy="joined",
    )
    object: Mapped["Object"] = relationship(lazy="joined")
    contract: Mapped["Contract"] = relationship(lazy="joined")
    defect_type: Mapped["DefectType"] = relationship(lazy="joined")

    work_items: Mapped[list["WorkItem"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )
    photos: Mapped[list["Photo"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )
    acts: Mapped[list["Act"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )
    feedback: Mapped[list["Feedback"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )
    reminders: Mapped[list["RequestReminder"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )
    stage_history: Mapped[list["RequestStageHistory"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
        order_by="RequestStageHistory.changed_at",
    )
    work_sessions: Mapped[list["WorkSession"]] = relationship(
        back_populates="request",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<Request id={self.id} number={self.number!r} "
            f"status={self.status} specialist_id={self.specialist_id}>"
        )


# Составные индексы для оптимизации частых запросов
Index("ix_requests_specialist_created", Request.specialist_id, Request.created_at)
Index("ix_requests_engineer_status", Request.engineer_id, Request.status)
Index("ix_requests_master_status", Request.master_id, Request.status)
Index("ix_requests_status_created", Request.status, Request.created_at)
Index("ix_requests_due_at_status", Request.due_at, Request.status)
