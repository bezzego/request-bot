from app.infrastructure.db.models import Request
from app.utils.timezone import format_moscow


def format_request_label(request: Request) -> str:
    """Human-friendly label for a request based on scheduled inspection and address.

    Falls back to the raw request number if no specialist data is available.
    """
    datetime_text = format_moscow(request.inspection_scheduled_at)
    address_text = (request.inspection_location or request.address or "").strip()

    parts = [part for part in (datetime_text, address_text) if part]
    if parts:
        return ", ".join(parts)
    return request.number

