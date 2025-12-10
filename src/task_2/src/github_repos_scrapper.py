import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from aiohttp import ClientError, ClientSession

from src.config.logger import logger
from src.task_2.src.config import GITHUB_API_BASE_URL
from src.task_2.src.models.repository import Repository, RepositoryAuthorCommitsNum
from src.task_2.src.rate_limiter import RateLimiter


class GithubReposScrapper:
    """
    Класс для получения информации о топовых GitHub-репозиториях и подсчёта числа коммитов авторов за последние 24 часа.

    :param access_token: GitHub Personal Access Token
    :param max_concurrent_requests: Максимальное количество одновременных HTTP-запросов
    :param requests_per_second: Ограничение числа запросов в секунду (RPS)
    :return:
    """

    def __init__(
        self,
        access_token: str,
        *,
        max_concurrent_requests: int | None = None,
        requests_per_second: int | None = None,
    ):
        """
        Инициализация GitHub скрейпера.

        :param access_token: GitHub Personal Access Token
        :param max_concurrent_requests: Максимум одновременных запросов (Semaphore)
        :param requests_per_second: Ограничение RPS для GitHub API
        :return:
        """
        logger.info(
            "Инициализация GithubReposScrapper",
            extra={
                "max_concurrent_requests": max_concurrent_requests,
                "requests_per_second": requests_per_second,
            },
        )

        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {access_token}",
            }
        )

        self._mcr = asyncio.Semaphore(max_concurrent_requests or 10)
        self._rate_limiter = RateLimiter(requests_per_second or 5)

    async def __aenter__(self):
        """
        Вход в контекстный менеджер.

        :return: self
        """
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Выход из контекстного менеджера. Закрывает HTTP-сессию.

        :param exc_type: Тип исключения
        :param exc: Исключение
        :param tb: Traceback
        :return:
        """
        await self.close()

    async def _safe_request(
        self,
        method: str,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        Выполняет безопасный запрос к GitHub API с обработкой ошибок.

        :param method: HTTP-метод (GET / POST)
        :param endpoint: API endpoint GitHub
        :param params: Параметры запроса
        :return: JSON-ответ GitHub API
        """
        url = f"{GITHUB_API_BASE_URL.rstrip('/')}/{endpoint.lstrip('/')}"

        async with self._mcr:
            await self._rate_limiter.acquire()

            logger.info(
                "Выполнение запроса к GitHub API",
                extra={"method": method, "url": url, "params": params},
            )

            try:
                async with self._session.request(method, url, params=params) as resp:
                    if resp.status >= 500:
                        logger.error(
                            "Серверная ошибка GitHub",
                            extra={"status": resp.status, "url": url},
                        )
                        raise RuntimeError(f"GitHub server error {resp.status}")

                    if resp.status == 403:
                        text = await resp.text()
                        logger.error(
                            "Ошибка доступа или превышение лимитов GitHub API",
                            extra={"url": url, "response": text},
                        )
                        raise RuntimeError(f"Forbidden/Ratelimit: {text}")

                    if resp.status >= 400:
                        text = await resp.text()
                        logger.error(
                            "Клиентская ошибка GitHub API",
                            extra={"status": resp.status, "url": url, "response": text},
                        )
                        raise RuntimeError(f"Client error {resp.status}: {text}")

                    data = await resp.json()
                    logger.info("Запрос успешно выполнен", extra={"url": url})
                    return data

            except ClientError as e:
                logger.error(
                    "Сетевая ошибка GitHub API",
                    extra={"url": url, "error": str(e)},
                )
                raise RuntimeError(f"Network error: {e}") from e

            except Exception as e:
                logger.error(
                    "Непредвиденная ошибка при запросе",
                    extra={"url": url, "error": str(e)},
                )
                raise

    async def _make_request(
        self,
        endpoint: str,
        method: str = "GET",
        params: dict[str, Any] | None = None,
    ) -> Any:
        """
        Обёртка над _safe_request.

        :param endpoint: API endpoint GitHub
        :param method: HTTP-метод
        :param params: Параметры запроса
        :return: JSON-ответ GitHub API
        """
        return await self._safe_request(method, endpoint, params=params)

    async def _get_top_repositories(self, limit: int = 100) -> list[dict[str, Any]]:
        """
        Получение списка топовых GitHub репозиториев по числу звёзд.

        :param limit: Количество запрашиваемых репозиториев
        :return: Список словарей с данными репозиториев
        """
        logger.info("Запрос топовых репозиториев", extra={"limit": limit})

        data = await self._make_request(
            endpoint="search/repositories",
            params={
                "q": "stars:>1",
                "sort": "stars",
                "order": "desc",
                "per_page": limit,
            },
        )
        return data["items"]

    async def _get_repository_commits(
        self, owner: str, repo: str
    ) -> list[dict[str, Any]]:
        """
        Получение списка коммитов репозитория за последние 24 часа.

        :param owner: Логин владельца репозитория
        :param repo: Имя репозитория
        :return: Список словарей коммитов
        """
        since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()

        logger.info(
            "Запрос коммитов репозитория",
            extra={"owner": owner, "repo": repo, "since": since},
        )

        return await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"since": since},
        )

    async def get_repositories(self, limit: int = 100) -> list[Repository]:
        """
        Получение списка Repository, включая подсчёт коммитов авторов за последние 24 часа.

        :param limit: Количество репозиториев
        :return: Список объектов Repository
        """
        logger.info("Начало обработки репозиториев", extra={"limit": limit})

        repos_data = await self._get_top_repositories(limit)

        async def process_repo(repo: dict[str, Any], position: int) -> Repository:
            """
            Обрабатывает один репозиторий: получает коммиты и формирует объект Repository.

            :param repo: Сырые данные репозитория из GitHub API
            :param position: Позиция в топе
            :return: Объект Repository
            """
            owner = repo["owner"]["login"]
            repo_name = repo["name"]

            logger.info(
                "Обработка репозитория",
                extra={"owner": owner, "repo": repo_name, "position": position},
            )

            try:
                commits = await self._get_repository_commits(owner, repo_name)
            except Exception as e:
                logger.warning(
                    "Ошибка при запросе коммитов",
                    extra={"owner": owner, "repo": repo_name, "error": str(e)},
                )
                commits = []

            authors: dict[str, int] = {}
            for commit in commits:
                author_info = commit.get("author")
                if not author_info:
                    continue

                author_name = author_info.get("login") or "unknown"
                authors[author_name] = authors.get(author_name, 0) + 1

            return Repository(
                name=repo_name,
                owner=owner,
                position=position,
                stars=repo["stargazers_count"],
                watchers=repo["watchers_count"],
                forks=repo["forks_count"],
                language=repo.get("language") or "unknown",
                authors_commits_num_today=[
                    RepositoryAuthorCommitsNum(author=k, commits_num=v)
                    for k, v in authors.items()
                ],
            )

        tasks = [process_repo(repo, idx + 1) for idx, repo in enumerate(repos_data)]

        results = await asyncio.gather(*tasks)
        logger.info("Обработка репозиториев завершена")
        return results

    async def close(self):
        """
        Закрытие HTTP-сессии GitHub клиента.

        :return:
        """
        logger.info("Закрытие GitHub HTTP-сессии")
        await self._session.close()
