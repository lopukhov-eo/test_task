import asyncio

from src.config.logger import logger
from src.task_2.src.config import (
    GITHUB_TOKEN,
    MAX_CONCURRENT_REQUESTS,
    REPOSITORIES_PER_PAGE,
    REQUESTS_PER_SECOND,
)
from src.task_2.src.github_repos_scrapper import GithubReposScrapper
from src.task_3.src.config import (
    CLICKHOUSE_PASS,
    CLICKHOUSE_SCHEMA_PATH,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
)
from src.task_3.src.db.clickhouse_client import ClickHouseClient
from src.task_3.src.db.repository_storage import RepositoryStorage


async def main():
    """Основная функция запуска скрейпера GitHub и сохранения данных в ClickHouse."""
    logger.info("Запуск GitHub репозиторий-скрейпера...")

    try:
        # Инициализация ClickHouse и GitHub скрейпера
        async with (
            ClickHouseClient(
                url=CLICKHOUSE_URL,
                user=CLICKHOUSE_USER,
                password=CLICKHOUSE_PASS,
                schema_file_path=CLICKHOUSE_SCHEMA_PATH,
            ) as ch,
            GithubReposScrapper(
                access_token=GITHUB_TOKEN,
                max_concurrent_requests=MAX_CONCURRENT_REQUESTS,
                requests_per_second=REQUESTS_PER_SECOND,
            ) as scrapper,
        ):

            logger.info(
                "Клиенты успешно инициализированы. Начинаем загрузку репозиториев..."
            )

            repos = await scrapper.get_repositories(limit=REPOSITORIES_PER_PAGE)
            logger.info(f"Получено {len(repos)} репозиториев с GitHub.")

            storage = RepositoryStorage(ch)
            logger.info("Сохранение данных в ClickHouse...")

            await storage.save_all(repos)

            logger.info("Все данные успешно сохранены в ClickHouse.")

    except ConnectionError as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")

    except Exception as e:
        logger.exception(f"Неизвестная ошибка: {e}")


asyncio.run(main())
