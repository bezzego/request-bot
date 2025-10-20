from __future__ import annotations

import logging
from typing import Optional


class ColorFormatter(logging.Formatter):
    """Добавляет цвета к уровню логирования в консоли."""

    COLORS = {
        "DEBUG": "\033[32m",   # зелёный
        "INFO": "\033[32m",    # зелёный
        "WARNING": "\033[33m", # жёлтый
        "ERROR": "\033[31m",   # красный
        "CRITICAL": "\033[31m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        color = self.COLORS.get(original_levelname)
        if color:
            record.levelname = f"{color}{original_levelname}{self.RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


def setup_logging(level: int = logging.DEBUG) -> None:
    """Инициализирует детализированные цветные логи."""
    formatter = ColorFormatter(
        fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
