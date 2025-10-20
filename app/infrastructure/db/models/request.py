import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base


class RequestStatus(enum.StrEnum):
    """Статусы заявок"""

    NEW = "new"  # новая, только создана
    INSPECTED = "inspected"  # инженер провёл осмотр
    ASSIGNED = "assigned"  # назначен мастер
    IN_PROGRESS = "in_progress"  # в работе
    COMPLETED = "completed"  # работы завершены
    CLOSED = "closed"  # проверена и закрыта
    CANCELLED = "cancelled"  # отменена


class Request(Base):
    """Основная таблица заявок"""

    __tablename__ = "requests"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    number: Mapped[str] = mapped_column(String(50), unique=True, nullable=False, index=True)
    description: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[RequestStatus] = mapped_column(
        Enum(RequestStatus), default=RequestStatus.NEW, nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True, onupdate=datetime.utcnow
    )

    # --- связи с пользователями ---
    specialist_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    engineer_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    master_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)

    specialist: Mapped["User"] = relationship(
        back_populates="created_request", foreign_keys=[specialist_id]
    )
    engineer: Mapped["User"] = relationship(
        back_populates="engineer_request", foreign_keys=[engineer_id]
    )
    master: Mapped["User"] = relationship(back_populates="master_request", foreign_keys=[master_id])

    # --- связи с другими таблицами ---
    work_items: Mapped[list["WorkItem"]] = relationship(
        back_populates="request", cascade="all, delete-orphan"
    )
    photos: Mapped[list["Photo"]] = relationship(
        back_populates="request", cascade="all, delete-orphan"
    )
    acts: Mapped[list["Act"]] = relationship(back_populates="request", cascade="all, delete-orphan")
    feedback: Mapped[list["Feedback"]] = relationship(
        back_populates="request", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Request id={self.id} status={self.status} specialist_id={self.specialist_id}>"
