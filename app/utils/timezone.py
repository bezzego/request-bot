from datetime import date, datetime, time
from zoneinfo import ZoneInfo

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def now_moscow() -> datetime:
    """Возвращает текущее время по московскому часовому поясу."""
    return datetime.now(MOSCOW_TZ)


def to_moscow(dt: datetime | None) -> datetime | None:
    """Гарантирует, что объект datetime в часовом поясе Москвы."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=MOSCOW_TZ)
    return dt.astimezone(MOSCOW_TZ)


def combine_moscow(date_part: date, time_part: time) -> datetime:
    """Комбинирует дату/время в московском часовом поясе."""
    return datetime.combine(date_part, time_part).replace(tzinfo=MOSCOW_TZ)


def format_moscow(dt: datetime | None, fmt: str = "%d.%m.%Y %H:%M") -> str | None:
    """Форматирует дату/время в строку по московскому времени."""
    if dt is None:
        return None
    localized = to_moscow(dt)
    return localized.strftime(fmt)
