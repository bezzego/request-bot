import enum
from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow


class ReminderType(enum.StrEnum):
    """Типы напоминаний по заявке."""

    INSPECTION = "inspection"
    DOCUMENT_SIGN = "document_sign"
    DEADLINE = "deadline"
    OVERDUE = "overdue"
    REPORT = "report"


class RequestReminder(Base):
    """Запланированные и отправленные напоминания."""

    __tablename__ = "request_reminders"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    reminder_type: Mapped[ReminderType] = mapped_column(Enum(ReminderType), nullable=False)
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    is_sent: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    channel: Mapped[str] = mapped_column(String(50), default="telegram", nullable=False)
    recipients: Mapped[str | None] = mapped_column(String(255), nullable=True)
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=now_moscow,
    )

    request: Mapped["Request"] = relationship(back_populates="reminders")

    def __repr__(self) -> str:
        return (
            f"<RequestReminder id={self.id} request_id={self.request_id} "
            f"type={self.reminder_type} scheduled={self.scheduled_at} sent={self.is_sent}>"
        )
