from datetime import datetime
from zoneinfo import ZoneInfo


def now_moscow() -> datetime:
    """Возвращает текущее время по московскому часовому поясу."""
    return datetime.now(ZoneInfo("Europe/Moscow"))
