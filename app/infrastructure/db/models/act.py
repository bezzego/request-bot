from __future__ import annotations

import enum
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, Enum, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow

if TYPE_CHECKING:
    from app.infrastructure.db.models.request import Request
    from app.infrastructure.db.models.user import User


class ActType(enum.StrEnum):
    """Типы актов"""

    INSPECTION = "inspection"  # акт осмотра
    COMPLETION = "completion"  # акт выполненных работ
    LETTER = "letter"  # сопроводительное письмо от специалиста


class Act(Base):
    """Документы, связанные с заявкой (акты осмотра и АВР)."""

    __tablename__ = "acts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[ActType] = mapped_column(Enum(ActType), nullable=False)
    file_id: Mapped[str] = mapped_column(String(255), nullable=False)  # Telegram file_id документа
    file_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    uploaded_by_id: Mapped[int | None] = mapped_column(
        ForeignKey("users.id"), nullable=True
    )  # кто загрузил акт

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    # --- связи ---
    request: Mapped[Request] = relationship(back_populates="acts")
    uploaded_by: Mapped[User] = relationship()

    def __repr__(self) -> str:
        return f"<Act id={self.id} type={self.type} request_id={self.request_id}>"
