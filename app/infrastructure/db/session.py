from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config.settings import settings

# Единый async engine приложения
engine = create_async_engine(
    settings.DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    future=True,
)

# Фабрика асинхронных сессий
async_session_maker: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
)


@asynccontextmanager
async def async_session() -> AsyncSession:
    """Контекстный менеджер для асинхронной сессии."""
    session = async_session_maker()
    try:
        yield session
    finally:
        await session.close()
