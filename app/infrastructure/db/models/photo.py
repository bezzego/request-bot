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


class PhotoType(enum.StrEnum):
    """Тип фото, привязанного к заявке."""

    BEFORE = "before"  # до ремонта
    PROCESS = "process"  # в процессе
    AFTER = "after"  # после ремонта


class Photo(Base):
    """Фотографии, связанные с заявкой (до/в процессе/после)."""

    __tablename__ = "photos"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    request_id: Mapped[int] = mapped_column(
        ForeignKey("requests.id", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[PhotoType] = mapped_column(Enum(PhotoType), nullable=False)
    file_id: Mapped[str] = mapped_column(String(255), nullable=False)  # Telegram file_id
    caption: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    # связь с заявкой
    request: Mapped[Request] = relationship(back_populates="photos")

    def __repr__(self) -> str:
        return f"<Photo id={self.id} type={self.type} request_id={self.request_id}>"
