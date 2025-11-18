from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Iterable

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request
from app.utils.timezone import format_moscow


class ExportService:
    """Выгрузка данных по заявкам в Excel (.xlsx)."""

    EXPORT_DIR = Path("exports")
    FILE_TEMPLATE = "requests_{start:%Y%m%d}_{end:%Y%m%d}.xlsx"

    @staticmethod
    async def export_requests(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> Path:
        ExportService.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        filename = ExportService.FILE_TEMPLATE.format(start=start, end=end)
        file_path = ExportService.EXPORT_DIR / filename

        requests = await ExportService._load_requests(session, start=start, end=end)

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Заявки"

        headers = ExportService._headers()
        sheet.append(headers)
        ExportService._format_header(sheet[1])
        ExportService._append_requests(sheet, requests)
        ExportService._autofit_columns(sheet, len(headers))

        workbook.save(file_path)
        return file_path

    @staticmethod
    async def _load_requests(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> list[Request]:
        stmt = (
            select(Request)
            .options(
                selectinload(Request.object),
                selectinload(Request.contract),
                selectinload(Request.defect_type),
                selectinload(Request.specialist),
                selectinload(Request.engineer),
                selectinload(Request.master),
            )
            .where(Request.created_at.between(start, end))
            .order_by(Request.created_at)
        )
        result = await session.execute(stmt)
        return result.scalars().all()

    @staticmethod
    def _headers() -> list[str]:
        return [
            "Номер",
            "Заголовок",
            "Статус",
            "Объект",
            "Адрес",
            "Договор",
            "Тип дефекта",
            "Специалист",
            "Инженер",
            "Мастер",
            "Срок устранения (дата)",
            "Фактическое завершение",
            "Плановый бюджет",
            "Фактический бюджет",
            "Плановые часы",
            "Фактические часы",
        ]

    @staticmethod
    def _format_header(header_row):
        bold = Font(bold=True)
        centered = Alignment(horizontal="center", vertical="center")
        for cell in header_row:
            cell.font = bold
            cell.alignment = centered

    @staticmethod
    def _append_requests(sheet, requests: Iterable[Request]) -> None:
        for req in requests:
            sheet.append(
                [
                    req.number,
                    req.title,
                    req.status.value,
                    req.object.name if req.object else "",
                    req.address,
                    req.contract.number if req.contract else "",
                    req.defect_type.name if req.defect_type else "",
                    req.specialist.full_name if req.specialist else "",
                    req.engineer.full_name if req.engineer else "",
                    req.master.full_name if req.master else "",
                    format_moscow(req.due_at, "%d.%m.%Y") or "",
                    format_moscow(req.work_completed_at) or "",
                    float(req.planned_budget or 0),
                    float(req.actual_budget or 0),
                    float(req.planned_hours or 0),
                    float(req.actual_hours or 0),
                ]
            )

    @staticmethod
    def _autofit_columns(sheet, columns_count: int) -> None:
        min_width = 10
        max_width = 40
        for col_idx in range(1, columns_count + 1):
            column_letter = get_column_letter(col_idx)
            max_length = 0
            for cell in sheet[column_letter]:
                value = cell.value
                if value is None:
                    continue
                max_length = max(max_length, len(str(value)))
            width = min(max(max_length + 2, min_width), max_width)
            sheet.column_dimensions[column_letter].width = width
