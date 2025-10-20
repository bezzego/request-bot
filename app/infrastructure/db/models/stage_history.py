from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow
from .request import RequestStatus


class RequestStageHistory(Base):
    """История переходов заявки по этапам."""

    __tablename__ = "request_stage_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"),
        nullable=False,
    )
    from_status: Mapped[RequestStatus | None] = mapped_column(Enum(RequestStatus), nullable=True)
    to_status: Mapped[RequestStatus] = mapped_column(Enum(RequestStatus), nullable=False)
    changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=now_moscow,
        nullable=False,
    )
    changed_by_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True)
    comment: Mapped[str | None] = mapped_column(Text, nullable=True)

    request: Mapped["Request"] = relationship(back_populates="stage_history")
    changed_by: Mapped["User"] = relationship()

    def __repr__(self) -> str:
        return (
            f"<RequestStageHistory id={self.id} request_id={self.request_id} "
            f"{self.from_status}->{self.to_status}>"
        )
