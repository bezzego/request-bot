from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.infrastructure.db.models import Request
from app.utils.timezone import format_moscow


class ExportService:
    """Выгрузка данных по заявкам в CSV."""

    EXPORT_DIR = Path("exports")

    @staticmethod
    async def export_requests(
        session: AsyncSession,
        *,
        start: datetime,
        end: datetime,
    ) -> Path:
        ExportService.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        filename = f"requests_{start:%Y%m%d}_{end:%Y%m%d}.csv"
        file_path = ExportService.EXPORT_DIR / filename

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
        requests = result.scalars().all()

        headers = [
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

        with file_path.open("w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file, delimiter=";")
            writer.writerow(headers)
            for req in requests:
                writer.writerow(
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
                        req.planned_budget or 0,
                        req.actual_budget or 0,
                        req.planned_hours or 0,
                        req.actual_hours or 0,
                    ]
                )

        return file_path
