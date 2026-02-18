"""Функции форматирования деталей заявки для инженера."""
from __future__ import annotations

from app.infrastructure.db.models import ActType, PhotoType, Request
from app.utils.request_formatters import STATUS_TITLES, format_hours_minutes, format_request_label
from app.utils.timezone import format_moscow


def format_currency(value: float | None) -> str:
    """Форматирует валюту."""
    if value is None:
        return "0.00"
    return f"{float(value):,.2f}".replace(",", " ")


def calculate_cost_breakdown(work_items) -> dict[str, float]:
    """Рассчитывает разбивку стоимостей по работам и материалам."""
    planned_work_cost = 0.0
    planned_material_cost = 0.0
    actual_work_cost = 0.0
    actual_material_cost = 0.0
    
    for item in work_items:
        if item.planned_cost is not None:
            planned_work_cost += float(item.planned_cost)
        if item.planned_material_cost is not None:
            planned_material_cost += float(item.planned_material_cost)
        if item.actual_cost is not None:
            actual_work_cost += float(item.actual_cost)
        if item.actual_material_cost is not None:
            actual_material_cost += float(item.actual_material_cost)
    
    return {
        "planned_work_cost": planned_work_cost,
        "planned_material_cost": planned_material_cost,
        "planned_total_cost": planned_work_cost + planned_material_cost,
        "actual_work_cost": actual_work_cost,
        "actual_material_cost": actual_material_cost,
        "actual_total_cost": actual_work_cost + actual_material_cost,
    }


def format_engineer_request_detail(request: Request) -> str:
    """Форматирует детальную информацию о заявке для инженера."""
    status_title = STATUS_TITLES.get(request.status, request.status.value)
    master = request.master.full_name if request.master else "не назначен"
    object_name = request.object.name if request.object else request.address
    due_text = format_moscow(request.due_at) or "не задан"
    inspection = format_moscow(request.inspection_scheduled_at) or "не назначен"
    work_end = format_moscow(request.work_completed_at) or "—"
    label = format_request_label(request)

    planned_hours = float(request.planned_hours or 0)
    actual_hours = float(request.actual_hours or 0)
    hours_delta = actual_hours - planned_hours
    
    cost_breakdown = calculate_cost_breakdown(request.work_items or [])

    lines = [
        f"📄 <b>{label}</b>",
        f"Название: {request.title}",
        f"Статус: {status_title}",
        f"Объект: {object_name}",
        f"Адрес: {request.address}",
        f"Квартира: {request.apartment or '—'}",
        f"Контактное лицо: {request.contact_person}",
        f"Телефон: {request.contact_phone}",
        f"Мастер: {master}",
        f"Осмотр: {inspection}",
        f"Работы завершены: {work_end}",
        f"Срок устранения: {due_text}",
        "",
        f"Плановая стоимость видов работ: {format_currency(cost_breakdown['planned_work_cost'])} ₽",
        f"Плановая стоимость материалов: {format_currency(cost_breakdown['planned_material_cost'])} ₽",
        f"Плановая общая стоимость: {format_currency(cost_breakdown['planned_total_cost'])} ₽",
        f"Фактическая стоимость видов работ: {format_currency(cost_breakdown['actual_work_cost'])} ₽",
        f"Фактическая стоимость материалов: {format_currency(cost_breakdown['actual_material_cost'])} ₽",
        f"Фактическая общая стоимость: {format_currency(cost_breakdown['actual_total_cost'])} ₽",
        f"Плановые часы: {format_hours_minutes(planned_hours)}",
        f"Фактические часы: {format_hours_minutes(actual_hours)}",
        f"Δ Часы: {format_hours_minutes(hours_delta, signed=True)}",
    ]

    if request.work_sessions:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        for session in sorted(request.work_sessions, key=lambda ws: ws.started_at):
            start = format_moscow(session.started_at, "%d.%m %H:%M") or "—"
            finish = format_moscow(session.finished_at, "%d.%m %H:%M") if session.finished_at else "в работе"
            duration_h = (
                float(session.hours_reported)
                if session.hours_reported is not None
                else (float(session.hours_calculated) if session.hours_calculated is not None else None)
            )
            if duration_h is None and session.started_at and session.finished_at:
                delta = session.finished_at - session.started_at
                duration_h = delta.total_seconds() / 3600
            duration_str = format_hours_minutes(duration_h) if duration_h is not None else "—"
            lines.append(f"• {start} — {finish} · {duration_str}")
            if session.notes:
                lines.append(f"  → {session.notes}")
    elif (request.actual_hours or 0) > 0:
        lines.append("")
        lines.append("⏱ <b>Время работы мастера</b>")
        lines.append(f"• Суммарно: {format_hours_minutes(float(request.actual_hours or 0))} (учёт до внедрения сессий)")

    if request.contract:
        lines.append(f"Договор: {request.contract.number}")
    if request.defect_type:
        lines.append(f"Тип дефекта: {request.defect_type.name}")

    if request.work_items:
        lines.append("")
        lines.append("📦 <b>Позиции бюджета</b>")
        for item in request.work_items:
            is_material = bool(
                item.planned_material_cost
                or item.actual_material_cost
                or ("материал" in (item.category or "").lower())
            )
            emoji = "📦" if is_material else "🛠"
            planned_cost = item.planned_cost
            actual_cost = item.actual_cost
            if planned_cost in (None, 0):
                planned_cost = item.planned_material_cost
            if actual_cost in (None, 0):
                actual_cost = item.actual_material_cost
            unit = item.unit or ""
            qty_part = ""
            if item.planned_quantity is not None or item.actual_quantity is not None:
                pq = item.planned_quantity if item.planned_quantity is not None else 0
                aq = item.actual_quantity if item.actual_quantity is not None else 0
                qty_part = f" | объём: {pq:.2f} → {aq:.2f} {unit}".rstrip()
            lines.append(
                f"{emoji} {item.name} — план {format_currency(planned_cost)} ₽ / "
                f"факт {format_currency(actual_cost)} ₽{qty_part}"
            )
            if item.actual_hours is not None:
                lines.append(
                    f"  Часы: {format_hours_minutes(item.planned_hours)} → {format_hours_minutes(item.actual_hours)}"
                )
            if item.notes:
                lines.append(f"  → {item.notes}")

    if request.acts:
        letter_count = sum(1 for act in request.acts if act.type == ActType.LETTER)
        if letter_count:
            lines.append("")
            lines.append("✉️ Письмо специалиста: приложено")

    return "\n".join(lines)
