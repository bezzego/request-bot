from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow


class WorkSession(Base):
    """Учет рабочего времени мастера на объекте."""

    __tablename__ = "work_sessions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    master_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    started_latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    started_longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    started_address: Mapped[str | None] = mapped_column(String(255), nullable=True)

    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    finished_longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    finished_address: Mapped[str | None] = mapped_column(String(255), nullable=True)

    hours_reported: Mapped[float | None] = mapped_column(Float, nullable=True)
    hours_calculated: Mapped[float | None] = mapped_column(Float, nullable=True)

    notes: Mapped[str | None] = mapped_column(String(255), nullable=True)

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

    request: Mapped["Request"] = relationship(back_populates="work_sessions")
    master: Mapped["User"] = relationship()

    def __repr__(self) -> str:
        return (
            f"<WorkSession id={self.id} request_id={self.request_id} "
            f"master_id={self.master_id} started_at={self.started_at}>"
        )
