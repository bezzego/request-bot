from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Базовый класс для всех ORM-моделей"""

    pass


from .act import Act
from .dictionaries import Contract, DefectType, Object
from .feedback import Feedback
from .photo import Photo
from .request import Request
from .user import User
from .work_item import WorkItem

# Когда появятся модели — импортируй их здесь,
# чтобы Alembic мог их видеть при автогенерации миграций:
# from .user import User
# from .request import Request
# from .work_item import WorkItem
# from .photo import Photo
# from .act import Act
# from .feedback import Feedback
# from .dictionaries import DefectType, Object, Contract
