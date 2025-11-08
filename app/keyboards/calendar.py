from __future__ import annotations

import calendar
from dataclasses import dataclass

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from app.utils.timezone import now_moscow

MONTH_TITLES_RU = [
    "Январь",
    "Февраль",
    "Март",
    "Апрель",
    "Май",
    "Июнь",
    "Июль",
    "Август",
    "Сентябрь",
    "Октябрь",
    "Ноябрь",
    "Декабрь",
]
WEEKDAY_TITLES_RU = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


@dataclass(slots=True)
class CalendarCallback:
    prefix: str
    action: str
    year: int
    month: int
    day: int | None = None


def build_calendar(prefix: str, *, year: int | None = None, month: int | None = None) -> InlineKeyboardMarkup:
    """Создает инлайн-календарь для выбора даты."""
    today = now_moscow().date()
    year = year or today.year
    month = month or today.month

    month_index = max(1, min(12, month))
    month_title = MONTH_TITLES_RU[month_index - 1]

    keyboard: list[list[InlineKeyboardButton]] = []
    keyboard.append(
        [
            InlineKeyboardButton(text="«", callback_data=f"cal:{prefix}:prev:{year}:{month}"),
            InlineKeyboardButton(text=f"{month_title} {year}", callback_data="cal:noop"),
            InlineKeyboardButton(text="»", callback_data=f"cal:{prefix}:next:{year}:{month}"),
        ]
    )
    keyboard.append(
        [InlineKeyboardButton(text=day, callback_data="cal:noop") for day in WEEKDAY_TITLES_RU]
    )

    cal = calendar.Calendar(firstweekday=0)  # Monday-first
    for week in cal.monthdayscalendar(year, month):
        row: list[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(text=" ", callback_data="cal:noop"))
            else:
                row.append(
                    InlineKeyboardButton(
                        text=f"{day:02d}",
                        callback_data=f"cal:{prefix}:day:{year}:{month}:{day}",
                    )
                )
        keyboard.append(row)

    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def parse_calendar_callback(data: str) -> CalendarCallback | None:
    """Парсит callback_data от календаря."""
    parts = data.split(":")
    if len(parts) < 5 or parts[0] != "cal":
        return None
    prefix = parts[1]
    action = parts[2]
    if action == "noop":
        return None
    try:
        year = int(parts[3])
        month = int(parts[4])
        day = int(parts[5]) if len(parts) > 5 else None
    except ValueError:
        return None
    return CalendarCallback(prefix=prefix, action=action, year=year, month=month, day=day)


def shift_month(year: int, month: int, direction: str) -> tuple[int, int]:
    """Смещает месяц в указанном направлении."""
    if direction == "prev":
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    elif direction == "next":
        month += 1
        if month == 13:
            month = 1
            year += 1
    return year, month
