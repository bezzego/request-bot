"""Расширенный фильтр заявок с поддержкой множественных параметров."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import and_, func, or_, select
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Object, Request, RequestStatus
from app.utils.request_formatters import STATUS_TITLES, get_request_status_title
from app.utils.timezone import format_moscow, to_moscow

logger = logging.getLogger(__name__)


# Маппинг статусов из ТЗ на статусы в системе
STATUS_MAPPING: dict[str, RequestStatus] = {
    "Новая": RequestStatus.NEW,
    "Принята в работу": RequestStatus.ASSIGNED,
    "Приступили к выполнению": RequestStatus.IN_PROGRESS,
    "Выполнена": RequestStatus.COMPLETED,
    "Ожидает подписания": RequestStatus.READY_FOR_SIGN,
    "Отмена": RequestStatus.CANCELLED,
    # "Не доступ" и "Не гарантия" - это варианты отмены, используют CANCELLED
    "Не доступ": RequestStatus.CANCELLED,
    "Не гарантия": RequestStatus.CANCELLED,
}

# Обратный маппинг для отображения
STATUS_DISPLAY_MAPPING: dict[RequestStatus, str] = {
    RequestStatus.NEW: "Новая",
    RequestStatus.INSPECTION_SCHEDULED: "Назначен осмотр",
    RequestStatus.INSPECTED: "Осмотр выполнен",
    RequestStatus.ASSIGNED: "Принята в работу",
    RequestStatus.IN_PROGRESS: "Приступили к выполнению",
    RequestStatus.COMPLETED: "Выполнена",
    RequestStatus.READY_FOR_SIGN: "Ожидает подписания",
    RequestStatus.CLOSED: "Закрыта",
    RequestStatus.CANCELLED: "Отмена",
}


class DateFilterMode:
    """Режимы фильтрации по дате."""
    CREATED = "created"  # По дате создания
    PLANNED = "planned"  # По плановой дате (inspection_scheduled_at)
    COMPLETED = "completed"  # По дате выполнения (work_completed_at)


def build_filter_conditions(
    filter_data: dict[str, Any] | None,
    base_conditions: list | None = None,
) -> list:
    """Строит SQL условия для фильтрации заявок.
    
    Args:
        filter_data: Словарь с параметрами фильтра:
            - statuses: list[str] - список статусов для фильтрации
            - object_id: int | None - ID объекта
            - address: str | None - часть адреса для поиска
            - contact_person: str | None - имя контактного лица для поиска
            - engineer_id: int | None - ID инженера
            - master_id: int | None - ID мастера
            - request_number: str | None - номер заявки (частичное совпадение)
            - contract_id: int | None - ID договора
            - defect_type_id: int | None - ID типа дефекта
            - date_mode: str - режим фильтрации по дате (created/planned/completed)
            - date_start: str | None - начальная дата (ISO format)
            - date_end: str | None - конечная дата (ISO format)
        base_conditions: Базовые условия (например, specialist_id == X)
    
    Returns:
        Список SQL условий для WHERE clause
    """
    logger.info(f"[FILTER] build_filter_conditions called with filter_data: {filter_data}")
    logger.info(f"[FILTER] base_conditions: {base_conditions}")
    
    conditions = list(base_conditions) if base_conditions else []
    
    if not filter_data:
        logger.info("[FILTER] No filter_data provided, returning base conditions only")
        return conditions
    
    logger.info(f"[FILTER] Processing filter_data keys: {list(filter_data.keys())}")
    
    # Фильтр по статусам
    statuses = filter_data.get("statuses")
    logger.info(f"[FILTER] statuses from filter_data: {statuses}")
    if statuses and isinstance(statuses, list) and len(statuses) > 0:
        status_enums = []
        for status_name in statuses:
            if not status_name:  # Пропускаем пустые значения
                continue
            if isinstance(status_name, str) and status_name in STATUS_MAPPING:
                status_enums.append(STATUS_MAPPING[status_name])
            elif isinstance(status_name, (str, RequestStatus)):
                try:
                    if isinstance(status_name, str):
                        status_enums.append(RequestStatus(status_name))
                    else:
                        status_enums.append(status_name)
                except (ValueError, TypeError):
                    pass
        if status_enums:
            logger.info(f"[FILTER] Adding status filter: {status_enums}")
            conditions.append(Request.status.in_(status_enums))
        else:
            logger.warning(f"[FILTER] No valid status enums found from {statuses}")
    
    # Фильтр по объекту
    object_id = filter_data.get("object_id")
    logger.info(f"[FILTER] object_id from filter_data: {object_id} (type: {type(object_id)})")
    if object_id is not None and object_id != "":
        try:
            object_id_int = int(object_id)
            if object_id_int > 0:
                logger.info(f"[FILTER] Adding object_id filter: {object_id_int}")
                conditions.append(Request.object_id == object_id_int)
            else:
                logger.warning(f"[FILTER] object_id <= 0: {object_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[FILTER] Failed to convert object_id to int: {object_id}, error: {e}")
    
    # Фильтр по адресу
    address = filter_data.get("address")
    logger.info(f"[FILTER] address from filter_data: {address}")
    if address and str(address).strip():
        address_str = str(address).strip()
        if address_str:
            logger.info(f"[FILTER] Adding address filter: '{address_str}'")
            # Проверяем что адрес не NULL и содержит искомую строку
            conditions.append(
                and_(
                    Request.address.isnot(None),
                    func.lower(Request.address).like(f"%{address_str.lower()}%")
                )
            )
    
    # Фильтр по контактному лицу
    contact_person = filter_data.get("contact_person")
    logger.info(f"[FILTER] contact_person from filter_data: {contact_person}")
    if contact_person and str(contact_person).strip():
        contact_str = str(contact_person).strip()
        if contact_str:
            logger.info(f"[FILTER] Adding contact_person filter: '{contact_str}'")
            conditions.append(func.lower(Request.contact_person).like(f"%{contact_str.lower()}%"))
    
    # Фильтр по инженеру
    engineer_id = filter_data.get("engineer_id")
    logger.info(f"[FILTER] engineer_id from filter_data: {engineer_id} (type: {type(engineer_id)})")
    if engineer_id is not None and engineer_id != "":
        try:
            engineer_id_int = int(engineer_id)
            if engineer_id_int > 0:
                logger.info(f"[FILTER] Adding engineer_id filter: {engineer_id_int}")
                conditions.append(Request.engineer_id == engineer_id_int)
            else:
                logger.warning(f"[FILTER] engineer_id <= 0: {engineer_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[FILTER] Failed to convert engineer_id to int: {engineer_id}, error: {e}")
    
    # Фильтр по мастеру
    master_id = filter_data.get("master_id")
    logger.info(f"[FILTER] master_id from filter_data: {master_id} (type: {type(master_id)})")
    if master_id is not None and master_id != "":
        try:
            master_id_int = int(master_id)
            if master_id_int > 0:
                logger.info(f"[FILTER] Adding master_id filter: {master_id_int}")
                conditions.append(Request.master_id == master_id_int)
            else:
                logger.warning(f"[FILTER] master_id <= 0: {master_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[FILTER] Failed to convert master_id to int: {master_id}, error: {e}")
    
    # Фильтр по номеру заявки
    request_number = filter_data.get("request_number")
    logger.info(f"[FILTER] request_number from filter_data: {request_number}")
    if request_number and str(request_number).strip():
        number_str = str(request_number).strip().upper()
        if number_str:
            logger.info(f"[FILTER] Adding request_number filter: '{number_str}'")
            # Поддерживаем частичный поиск (например, "RQ-2026" найдет все заявки за 2026 год)
            conditions.append(func.upper(Request.number).like(f"%{number_str}%"))
    
    # Фильтр по договору
    contract_id = filter_data.get("contract_id")
    logger.info(f"[FILTER] contract_id from filter_data: {contract_id} (type: {type(contract_id)})")
    if contract_id is not None and contract_id != "":
        try:
            contract_id_int = int(contract_id)
            if contract_id_int > 0:
                logger.info(f"[FILTER] Adding contract_id filter: {contract_id_int}")
                conditions.append(Request.contract_id == contract_id_int)
            else:
                logger.warning(f"[FILTER] contract_id <= 0: {contract_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[FILTER] Failed to convert contract_id to int: {contract_id}, error: {e}")
    
    # Фильтр по типу дефекта
    defect_type_id = filter_data.get("defect_type_id")
    logger.info(f"[FILTER] defect_type_id from filter_data: {defect_type_id} (type: {type(defect_type_id)})")
    if defect_type_id is not None and defect_type_id != "":
        try:
            defect_type_id_int = int(defect_type_id)
            if defect_type_id_int > 0:
                logger.info(f"[FILTER] Adding defect_type_id filter: {defect_type_id_int}")
                conditions.append(Request.defect_type_id == defect_type_id_int)
            else:
                logger.warning(f"[FILTER] defect_type_id <= 0: {defect_type_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[FILTER] Failed to convert defect_type_id to int: {defect_type_id}, error: {e}")
    
    # Фильтр по дате
    date_mode = filter_data.get("date_mode", DateFilterMode.CREATED)
    date_start_str = filter_data.get("date_start")
    date_end_str = filter_data.get("date_end")
    logger.info(f"[FILTER] date_mode: {date_mode}, date_start: {date_start_str}, date_end: {date_end_str}")
    
    if date_start_str or date_end_str:
        try:
            date_start = None
            date_end = None
            
            if date_start_str and str(date_start_str).strip():
                date_start = datetime.fromisoformat(str(date_start_str).strip())
                date_start = to_moscow(date_start)
            if date_end_str and str(date_end_str).strip():
                date_end = datetime.fromisoformat(str(date_end_str).strip())
                date_end = to_moscow(date_end)
                # Устанавливаем конец дня
                if date_end:
                    date_end = date_end.replace(hour=23, minute=59, second=59, microsecond=999999)
            
            # Выбираем поле для фильтрации в зависимости от режима
            if date_mode == DateFilterMode.CREATED:
                date_field = Request.created_at
            elif date_mode == DateFilterMode.PLANNED:
                date_field = Request.inspection_scheduled_at
            elif date_mode == DateFilterMode.COMPLETED:
                date_field = Request.work_completed_at
            else:
                date_field = Request.created_at
            
            if date_start and date_end:
                logger.info(f"[FILTER] Adding date range filter: {date_start} to {date_end} on field {date_mode}")
                conditions.append(date_field.between(date_start, date_end))
            elif date_start:
                logger.info(f"[FILTER] Adding date start filter: >= {date_start} on field {date_mode}")
                conditions.append(date_field >= date_start)
            elif date_end:
                logger.info(f"[FILTER] Adding date end filter: <= {date_end} on field {date_mode}")
                conditions.append(date_field <= date_end)
        except (ValueError, TypeError, AttributeError) as e:
            logger.error(f"[FILTER] Error processing date filter: {e}", exc_info=True)
    
    logger.info(f"[FILTER] Total conditions count: {len(conditions)}")
    logger.info(f"[FILTER] Conditions: {conditions}")
    return conditions


def format_filter_label(filter_data: dict[str, Any] | None) -> str:
    """Форматирует описание активных фильтров для отображения пользователю.
    
    Returns:
        Строка с описанием фильтров или пустая строка, если фильтров нет
    """
    if not filter_data:
        return ""
    
    parts = []
    
    # Статусы
    statuses = filter_data.get("statuses")
    if statuses and isinstance(statuses, list) and len(statuses) > 0:
        status_labels = []
        for status_name in statuses:
            if status_name in STATUS_MAPPING:
                status_enum = STATUS_MAPPING[status_name]
                status_labels.append(STATUS_DISPLAY_MAPPING.get(status_enum, status_name))
            else:
                status_labels.append(status_name)
        if status_labels:
            parts.append(f"Статус: {', '.join(status_labels)}")
    
    # Объект
    object_id = filter_data.get("object_id")
    object_name = filter_data.get("object_name")  # Может быть сохранено для отображения
    if object_id:
        if object_name:
            parts.append(f"Объект: {object_name}")
        else:
            parts.append(f"Объект: ID {object_id}")
    
    # Адрес
    address = filter_data.get("address")
    if address:
        parts.append(f"Адрес: {address}")
    
    # Контактное лицо
    contact_person = filter_data.get("contact_person")
    if contact_person:
        parts.append(f"Контакт: {contact_person}")
    
    # Инженер
    engineer_id = filter_data.get("engineer_id")
    engineer_name = filter_data.get("engineer_name")
    if engineer_id:
        if engineer_name:
            parts.append(f"Инженер: {engineer_name}")
        else:
            parts.append(f"Инженер: ID {engineer_id}")
    
    # Мастер
    master_id = filter_data.get("master_id")
    master_name = filter_data.get("master_name")
    if master_id:
        if master_name:
            parts.append(f"Мастер: {master_name}")
        else:
            parts.append(f"Мастер: ID {master_id}")
    
    # Номер заявки
    request_number = filter_data.get("request_number")
    if request_number:
        parts.append(f"Номер: {request_number}")
    
    # Договор
    contract_id = filter_data.get("contract_id")
    contract_number = filter_data.get("contract_number")
    if contract_id:
        if contract_number:
            parts.append(f"Договор: {contract_number}")
        else:
            parts.append(f"Договор: ID {contract_id}")
    
    # Тип дефекта
    defect_type_id = filter_data.get("defect_type_id")
    defect_type_name = filter_data.get("defect_type_name")
    if defect_type_id:
        if defect_type_name:
            parts.append(f"Дефект: {defect_type_name}")
        else:
            parts.append(f"Дефект: ID {defect_type_id}")
    
    # Период
    date_mode = filter_data.get("date_mode", DateFilterMode.CREATED)
    date_start_str = filter_data.get("date_start")
    date_end_str = filter_data.get("date_end")
    
    if date_start_str or date_end_str:
        try:
            date_start = None
            date_end = None
            
            if date_start_str:
                date_start = datetime.fromisoformat(date_start_str)
            if date_end_str:
                date_end = datetime.fromisoformat(date_end_str)
            
            if date_start and date_end:
                start_label = format_moscow(date_start, "%d.%m.%Y")
                end_label = format_moscow(date_end, "%d.%m.%Y")
                if start_label == end_label:
                    period_label = start_label
                else:
                    period_label = f"{start_label} – {end_label}"
            elif date_start:
                period_label = f"с {format_moscow(date_start, '%d.%m.%Y')}"
            elif date_end:
                period_label = f"до {format_moscow(date_end, '%d.%m.%Y')}"
            else:
                period_label = None
            
            if period_label:
                mode_labels = {
                    DateFilterMode.CREATED: "По дате создания",
                    DateFilterMode.PLANNED: "По плановой дате",
                    DateFilterMode.COMPLETED: "По дате выполнения",
                }
                mode_label = mode_labels.get(date_mode, "По дате")
                parts.append(f"Период ({mode_label}): {period_label}")
        except (ValueError, TypeError):
            pass
    
    if not parts:
        return ""
    
    return "\n".join(parts)


async def get_available_objects(session) -> list[Object]:
    """Получает список всех объектов для выбора в фильтре."""
    result = await session.execute(
        select(Object)
        .order_by(Object.name)
    )
    return list(result.scalars().all())
