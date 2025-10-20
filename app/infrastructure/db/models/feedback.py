from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import DateTime, ForeignKey, Integer, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base


class Feedback(Base):
    """Отзывы заказчиков по завершённым заявкам."""

    __tablename__ = "feedback"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"), nullable=False
    )

    # оценки по шкале 1–5
    rating_quality: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating_time: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating_culture: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # текстовый отзыв
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(ZoneInfo("Europe/Moscow"))
    )

    # связь с заявкой
    request: Mapped["Request"] = relationship(back_populates="feedback")

    def __repr__(self) -> str:
        return (
            f"<Feedback id={self.id} request_id={self.request_id} "
            f"quality={self.rating_quality} time={self.rating_time} culture={self.rating_culture}>"
        )
