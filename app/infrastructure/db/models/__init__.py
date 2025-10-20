from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Базовый класс для всех ORM-моделей."""


from .act import Act
from .dictionaries import Contract, DefectType, Object
from .feedback import Feedback
from .photo import Photo, PhotoType
from .reminder import ReminderType, RequestReminder
from .request import Request, RequestStatus
from .roles import Customer, Engineer, Leader, Master, Specialist
from .stage_history import RequestStageHistory
from .user import User, UserRole
from .work_item import WorkItem
from .work_session import WorkSession

__all__ = [
    "Base",
    "User",
    "UserRole",
    "Specialist",
    "Engineer",
    "Master",
    "Leader",
    "Customer",
    "Request",
    "RequestStatus",
    "RequestStageHistory",
    "WorkItem",
    "WorkSession",
    "Photo",
    "PhotoType",
    "Act",
    "Feedback",
    "DefectType",
    "Object",
    "Contract",
    "RequestReminder",
    "ReminderType",
]
