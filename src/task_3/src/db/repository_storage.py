from datetime import UTC, datetime

from src.config.logger import logger
from src.task_2.src.models.repository import Repository
from src.task_3.src.db.tables import Tables


class RepositoryStorage:
    """
    Хранилище для сохранения данных репозиториев в ClickHouse.

    :param ch: Клиент ClickHouse, реализующий методы insert_batch
    :return:
    """

    def __init__(self, ch):
        """
        Инициализация хранилища данных репозиториев.

        :param ch: Экземпляр ClickHouseClient
        :return:
        """
        self.ch = ch
        logger.info("Инициализирован RepositoryStorage")

    async def save_repositories(self, repos: list[Repository]):
        """
        Сохраняет основную информацию о репозиториях в таблицу test.repositories.

        :param repos: Список моделей Repository с данными о репозиториях
        :return:
        """
        logger.info(
            f"Начато сохранение {len(repos)} репозиториев в {Tables.REPOSITORIES}..."
        )

        rows = []
        now = datetime.now(UTC)

        for r in repos:
            rows.append(
                {
                    "name": r.name,
                    "owner": r.owner,
                    "stars": r.stars,
                    "watchers": r.watchers,
                    "forks": r.forks,
                    "language": r.language,
                    "updated": now,
                }
            )

        await self.ch.insert_batch(Tables.REPOSITORIES, rows)
        logger.info(f"Сохранение репозиториев завершено. Всего строк: {len(rows)}")

    async def save_repo_positions(self, repos: list[Repository]):
        """
        Сохраняет позиции репозиториев в таблицу test.repositories_positions.

        :param repos: Список моделей Repository с полем position
        :return:
        """
        logger.info(f"Начато сохранение позиций репозиториев в {Tables.POSITIONS}...")

        today = datetime.now(UTC).date()

        rows = [
            {
                "date": today,
                "repo": r.name,
                "position": r.position,
            }
            for r in repos
        ]

        await self.ch.insert_batch(Tables.POSITIONS, rows)
        logger.info(f"Сохранение позиций завершено. Всего строк: {len(rows)}")

    async def save_authors_commits(self, repos: list[Repository]):
        """
        Сохраняет количество коммитов авторов за сегодня в таблицу test.repositories_authors_commits.

        :param repos: Список моделей Repository с данными authors_commits_num_today
        :return:
        """
        logger.info("Начато сохранение статистики коммитов авторов...")

        today = datetime.now(UTC).date()
        rows = []

        for r in repos:
            for ac in r.authors_commits_num_today:
                rows.append(
                    {
                        "date": today,
                        "repo": r.name,
                        "author": ac.author,
                        "commits_num": ac.commits_num,
                    }
                )

        if rows:
            await self.ch.insert_batch(Tables.AUTHORS_COMMITS, rows)
            logger.info(
                f"Сохранение статистики коммитов завершено. Всего строк: {len(rows)}"
            )
        else:
            logger.info("Нет данных для сохранения статистики коммитов.")

    async def save_all(self, repos: list[Repository]):
        """
        Сохраняет все данные в БД.

        :param repos: Список моделей Repository с полной информацией
        :return:
        """
        logger.info("Запуск полного сохранения данных репозиториев...")

        await self.save_repositories(repos)
        await self.save_repo_positions(repos)
        await self.save_authors_commits(repos)

        logger.info("Полное сохранение данных завершено.")
