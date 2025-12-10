from typing import Annotated, AsyncIterator

import asyncpg
import uvicorn
from fastapi import (
    APIRouter,
    FastAPI,
    Depends,
    Request,
    HTTPException,
    status,
)
from contextlib import asynccontextmanager

from config import (
    MIN_SIZE_POOL,
    MAX_SIZE_POOL,
    TIMEOUT,
    DB_CONNECT_STR
)
from src.config.logger import logger


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Контекст жизненного цикла приложения FastAPI.

    Создаёт пул подключений к PostgreSQL при старте приложения
    и закрывает пул при завершении работы.

    :param app: Экземпляр приложения FastAPI.
    :return: None
    """
    try:
        app.state.pg_pool = await asyncpg.create_pool(
            dsn=DB_CONNECT_STR,
            min_size=MIN_SIZE_POOL,
            max_size=MAX_SIZE_POOL,
            timeout=TIMEOUT,
        )
        logger.info("PostgreSQL pool created successfully")
    except (asyncpg.PostgresError, OSError, TimeoutError) as exc:
        logger.error(f"Failed to create PostgreSQL pool: {exc}")
        raise RuntimeError("Database initialization error") from exc

    try:
        yield
    finally:
        pool = getattr(app.state, "pg_pool", None)
        if pool is not None:
            try:
                await pool.close()
                logger.info("PostgreSQL pool closed")
            except Exception as exc:
                logger.error(f"Failed to close PostgreSQL pool: {exc}")


async def get_pg_connection(
        request: Request
) -> AsyncIterator[asyncpg.Connection]:
    """
    Получение соединения с PostgreSQL из пула.

    :param request: Текущий HTTP-запрос.
    :return: Подключение к БД из пула.
    """
    pool: asyncpg.Pool = request.app.state.pg_pool

    try:
        async with pool.acquire(timeout=5) as conn:
            yield conn
    except asyncpg.PostgresError as exc:
        logger.error(f"Database error during acquire(): {exc}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        )
    except TimeoutError:
        logger.error("Database acquire() timeout")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection timeout",
        )


async def get_db_version(
    conn: Annotated[asyncpg.Connection, Depends(get_pg_connection)]
):
    """
    Получение версии PostgreSQL.

    :param conn: Активное соединение с БД, полученное через зависимость.
    :return: Строка с информацией о версии PostgreSQL.
    """
    try:
        return await conn.fetchval("SELECT version()")
    except asyncpg.PostgresError as exc:
        logger.error(f"Query failed: {exc}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database query error",
        )


def register_routes(app: FastAPI) -> None:
    """
    Регистрация роутов приложения.

    :param app: Экземпляр приложения FastAPI.
    """
    router = APIRouter(prefix="/api")
    router.add_api_route(path="/db_version", endpoint=get_db_version)
    app.include_router(router)


def create_app() -> FastAPI:
    """
    Фабрика создания экземпляра FastAPI.

    :return: Настроенный экземпляр приложения FastAPI.
    """
    app = FastAPI(title="e-Comet", lifespan=lifespan)
    register_routes(app)
    return app


if __name__ == "__main__":
    uvicorn.run("main:create_app", factory=True)
