from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import DateTime, Float, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base


class WorkItem(Base):
    """Работы и материалы, привязанные к заявке (план и факт)."""

    __tablename__ = "work_items"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # ссылка на заявку
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"), nullable=False
    )

    # описание позиции
    name: Mapped[str] = mapped_column(String(255), nullable=False)

    # плановые показатели
    planned_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    planned_cost: Mapped[float | None] = mapped_column(Float, nullable=True)

    # фактические показатели
    actual_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    actual_cost: Mapped[float | None] = mapped_column(Float, nullable=True)

    # дата создания и обновления
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(ZoneInfo("Europe/Moscow"))
    )
    updated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=lambda: datetime.now(ZoneInfo("Europe/Moscow")),
    )

    # связь с заявкой
    request: Mapped["Request"] = relationship(back_populates="work_items")

    def __repr__(self) -> str:
        return (
            f"<WorkItem id={self.id} name={self.name!r} "
            f"planned={self.planned_hours}h fact={self.actual_hours}h>"
        )
