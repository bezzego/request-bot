import logging

from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.utils.logging import setup_logging

setup_logging()


class Settings(BaseSettings):
    """Настройки проекта. Все поля обязательны, читаются из .env."""

    BOT_TOKEN: str = Field(..., description="Токен Telegram-бота")

    DB_HOST: str = Field(..., description="Хост PostgreSQL")
    DB_PORT: int = Field(..., description="Порт PostgreSQL")
    DB_NAME: str = Field(..., description="Имя базы данных")
    DB_USER: str = Field(..., description="Имя пользователя БД")
    DB_PASS: str = Field(..., description="Пароль пользователя БД")

    SUPER_ADMIN_IDS: list[int] = Field(default_factory=list, description="Список Telegram ID супер-админов")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        json_loads=lambda value: value,
    )

    @field_validator("SUPER_ADMIN_IDS", mode="before")
    @classmethod
    def parse_super_admin_ids(cls, value):
        if value in (None, "", []):
            return []
        if isinstance(value, str):
            return [int(part.strip()) for part in value.split(",") if part.strip()]
        if isinstance(value, (list, tuple, set)):
            return [int(v) for v in value]
        raise ValueError("SUPER_ADMIN_IDS must be a comma separated string or list of integers")

    @property
    def DATABASE_URL(self) -> str:
        """Формирует URL для SQLAlchemy async engine."""
        return (
            f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}"
            f"@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"
        )


def load_settings() -> Settings:
    """Загружает и валидирует настройки при старте приложения."""
    try:
        settings = Settings()
        logging.info(
            f"✅ Config loaded successfully: DB={settings.DB_NAME} HOST={settings.DB_HOST}"
        )
        return settings
    except ValidationError as e:
        logging.critical("❌ Ошибка конфигурации! Проверь .env файл.")
        raise e


settings = load_settings()
