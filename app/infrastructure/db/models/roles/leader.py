from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow


class Leader(Base):
    """Руководитель — управляет персоналом и ролями"""

    __tablename__ = "leaders"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    division: Mapped[str | None] = mapped_column(String(255))
    is_super_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    user: Mapped["User"] = relationship("User", back_populates="leader_profile")
