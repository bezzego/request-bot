"""Утилиты для фильтрации заявок."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import func

from app.infrastructure.db.models import Request
from app.utils.advanced_filters import build_filter_conditions, format_filter_label
from app.utils.request_filters import format_date_range_label

logger = logging.getLogger(__name__)


def specialist_filter_conditions(filter_payload: dict[str, Any] | None) -> list:
    """Строит условия фильтрации для заявок специалиста."""
    logger.info(f"[SPECIALIST FILTER] Building conditions for filter_payload: {filter_payload}")
    if not filter_payload:
        logger.info("[SPECIALIST FILTER] No filter_payload, returning empty conditions")
        return []
    
    # Поддержка старого формата фильтра для обратной совместимости
    if "mode" in filter_payload:
        logger.info("[SPECIALIST FILTER] Using legacy filter format")
        mode = (filter_payload.get("mode") or "").strip().lower()
        value = (filter_payload.get("value") or "").strip()
        conditions: list = []
        if mode == "адрес" and value:
            conditions.append(func.lower(Request.address).like(f"%{value.lower()}%"))
        elif mode == "дата":
            start = filter_payload.get("start")
            end = filter_payload.get("end")
            if start and end:
                try:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                    conditions.append(Request.created_at.between(start_dt, end_dt))
                except ValueError:
                    pass
        logger.info(f"[SPECIALIST FILTER] Legacy conditions: {conditions}")
        return conditions
    
    # Новый формат фильтра
    logger.info("[SPECIALIST FILTER] Using new filter format")
    conditions = build_filter_conditions(filter_payload)
    logger.info(f"[SPECIALIST FILTER] Final conditions: {conditions}")
    return conditions


def specialist_filter_label(filter_payload: dict[str, Any] | None) -> str:
    """Форматирует описание фильтра для отображения."""
    if not filter_payload:
        return ""
    
    # Поддержка старого формата фильтра для обратной совместимости
    if "mode" in filter_payload:
        mode = (filter_payload.get("mode") or "").strip().lower()
        if mode == "адрес":
            value = (filter_payload.get("value") or "").strip()
            return f"адрес: {value}" if value else ""
        if mode == "дата":
            start = filter_payload.get("start")
            end = filter_payload.get("end")
            if start and end:
                try:
                    start_dt = datetime.fromisoformat(start)
                    end_dt = datetime.fromisoformat(end)
                    return f"дата: {format_date_range_label(start_dt, end_dt)}"
                except ValueError:
                    return ""
        return ""
    
    # Новый формат фильтра
    return format_filter_label(filter_payload)


def clean_filter_payload(filter_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    """Очищает фильтр от пустых значений и нормализует данные."""
    logger.info(f"[FILTER CLEAN] Input filter_payload: {filter_payload}")
    if not filter_payload:
        return None
    
    cleaned = {}
    
    # Копируем только непустые значения
    for key, value in filter_payload.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, list) and len(value) == 0:
            continue
        cleaned[key] = value
    
    logger.info(f"[FILTER CLEAN] Output cleaned filter: {cleaned}")
    return cleaned if cleaned else None
