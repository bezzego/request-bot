from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.db.models import Base
from app.utils.timezone import now_moscow


class DefectType(Base):
    """Справочник типов дефектов (трещины, течь, перегрев и т.д.)"""

    __tablename__ = "defect_types"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    def __repr__(self) -> str:
        return f"<DefectType id={self.id} name={self.name!r}>"


class Object(Base):
    """Справочник объектов (здания, участки, оборудование)."""

    __tablename__ = "objects"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    address: Mapped[str | None] = mapped_column(String(512), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    def __repr__(self) -> str:
        return f"<Object id={self.id} name={self.name!r}>"


class Contract(Base):
    """Справочник договоров (для связи с заявками)."""

    __tablename__ = "contracts"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    number: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(String(512), nullable=True)
    signed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=now_moscow)

    def __repr__(self) -> str:
        return f"<Contract id={self.id} number={self.number!r}>"
