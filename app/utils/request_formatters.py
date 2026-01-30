from app.infrastructure.db.models import Request, RequestStatus
from app.utils.timezone import format_moscow

# Единый словарь русских названий статусов для всех ролей (мастер, инженер, админ, менеджер, специалист, клиент)
STATUS_TITLES: dict[RequestStatus, str] = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Назначен мастер",
    RequestStatus.IN_PROGRESS: "В работе",
    RequestStatus.COMPLETED: "Работы завершены",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отменена",
}


def get_request_status_title(status: RequestStatus) -> str:
    """Возвращает русское название статуса заявки."""
    return STATUS_TITLES.get(status, str(status.value))


def format_hours_minutes(hours: float | None, signed: bool = False) -> str:
    """Форматирует часы в виде «X ч Y мин» или «Y мин». Если signed=True, отрицательные выводятся с минусом (для дельты)."""
    if hours is None:
        return "0 ч"
    sign = ""
    if hours < 0:
        if not signed:
            return "0 ч"
        sign = "−"
        hours = -hours
    h = int(hours)
    m = int(round((hours - h) * 60))
    if m >= 60:
        h += 1
        m = 0
    if h == 0:
        out = f"{m} мин" if m else "0 ч"
    elif m == 0:
        out = f"{h} ч"
    else:
        out = f"{h} ч {m} мин"
    return f"{sign}{out}"


def format_request_label(request: Request) -> str:
    """Подпись заявки на кнопке: дата, объект, улица, номер квартиры."""
    date_text = format_moscow(request.inspection_scheduled_at, "%d.%m") if request.inspection_scheduled_at else None
    object_text = (request.object.name if request.object else "").strip() or None
    address_text = (request.address or "").strip() or None
    apartment_text = (f"кв. {request.apartment}" if request.apartment else None)

    parts = [p for p in (date_text, object_text, address_text, apartment_text) if p]
    if parts:
        return " ".join(parts)
    return request.number

