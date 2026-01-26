from __future__ import annotations

from datetime import datetime, timedelta

from app.utils.timezone import now_moscow


def _naive(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None)


def parse_date_range(text: str) -> tuple[datetime | None, datetime | None, str | None]:
    """Парсит диапазон дат из строки. Возвращает (start, end, error)."""
    raw = (text or "").strip()
    if not raw:
        return None, None, "Введите дату или диапазон дат."

    lowered = raw.lower()
    if lowered in {"сегодня", "today"}:
        now = _naive(now_moscow())
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return start, end, None
    if lowered in {"вчера", "yesterday"}:
        now = _naive(now_moscow()) - timedelta(days=1)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return start, end, None

    normalized = raw.replace("—", "-").replace("–", "-")
    parts = [p.strip() for p in normalized.split("-", 1)]

    try:
        if len(parts) == 1 or parts[1] == "":
            date_value = datetime.strptime(parts[0], "%d.%m.%Y")
            start = date_value.replace(hour=0, minute=0, second=0, microsecond=0)
            end = date_value.replace(hour=23, minute=59, second=59, microsecond=0)
            return start, end, None

        start = datetime.strptime(parts[0], "%d.%m.%Y")
        end = datetime.strptime(parts[1], "%d.%m.%Y")
        end = end.replace(hour=23, minute=59, second=59, microsecond=0)
        return start, end, None
    except ValueError:
        return None, None, "Неверный формат. Используйте ДД.ММ.ГГГГ или ДД.ММ.ГГГГ-ДД.ММ.ГГГГ."


def quick_date_range(code: str) -> tuple[datetime, datetime, str] | None:
    """Возвращает быстрый диапазон дат и его описание."""
    now = _naive(now_moscow())
    if code == "today":
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return start, end, "сегодня"
    if code == "7d":
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end, "последние 7 дней"
    if code == "30d":
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        start = (now - timedelta(days=29)).replace(hour=0, minute=0, second=0, microsecond=0)
        return start, end, "последние 30 дней"
    if code == "this_month":
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end = now.replace(hour=23, minute=59, second=59, microsecond=0)
        return start, end, "текущий месяц"
    if code == "prev_month":
        first_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        end_prev = first_this_month - timedelta(seconds=1)
        start_prev = end_prev.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return start_prev, end_prev, "прошлый месяц"
    return None


def format_date_range_label(start: datetime, end: datetime) -> str:
    start_label = start.strftime("%d.%m.%Y")
    end_label = end.strftime("%d.%m.%Y")
    if start_label == end_label:
        return start_label
    return f"{start_label}–{end_label}"
