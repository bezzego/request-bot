from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.config.settings import settings
from app.infrastructure.db.models import Base

# Настройки логгирования Alembic
config = context.config
fileConfig(config.config_file_name)

# URL подключения
config.set_main_option("sqlalchemy.url", settings.DATABASE_URL)

target_metadata = Base.metadata


async def run_migrations_online() -> None:
    """Асинхронный запуск миграций"""
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)


def do_run_migrations(connection):
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,  # отслеживать изменение типов
        render_as_batch=True,  # для SQLite-совместимости, можно убрать
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_offline() -> None:
    """Запуск в offline-режиме"""
    context.configure(
        url=settings.DATABASE_URL,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


import asyncio

asyncio.run(run_migrations_online())
