import enum
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import BigInteger, DateTime, Enum, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.db.models import Base


class UserRole(enum.StrEnum):
    """Роли пользователей в системе"""

    SPECIALIST = "specialist"
    ENGINEER = "engineer"
    MASTER = "master"
    MANAGER = "manager"
    CLIENT = "client"


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True, nullable=False)
    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str | None] = mapped_column(String(255), nullable=True, default="Нет")
    role: Mapped[UserRole] = mapped_column(Enum(UserRole), nullable=False)
    phone: Mapped[str | None] = mapped_column(String(50), nullable=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(ZoneInfo("Europe/Moscow"))
    )

    # ––– связи –––
    created_request: Mapped[list["Request"]] = relationship(
        back_populates="specialist",
        foreign_keys="Request.specialist_id",
    )
    engineer_request: Mapped[list["Request"]] = relationship(
        back_populates="engineer",
        foreign_keys="Request.engineer_id",
    )
    master_request: Mapped[list["Request"]] = relationship(
        back_populates="master",
        foreign_keys="Request.master_id",
    )

    # --- профили ролей ---
    specialist_profile = relationship("Specialist", back_populates="user", uselist=False)
    engineer_profile = relationship("Engineer", back_populates="user", uselist=False)
    master_profile = relationship("Master", back_populates="user", uselist=False)
    leader_profile = relationship("Leader", back_populates="user", uselist=False)
    customer_profile = relationship("Customer", back_populates="user", uselist=False)

    def __repr__(self) -> str:
        return f"<User id={self.id} name={self.full_name!r} role={self.role}>"
