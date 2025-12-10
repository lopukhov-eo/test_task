from dataclasses import dataclass


@dataclass(slots=True)
class RepositoryAuthorCommitsNum:
    """Данные о количестве коммитов автора за последние сутки."""

    author: str
    commits_num: int


@dataclass(slots=True)
class Repository:
    """Модель репозитория с основной статистикой и данными о коммитах авторов."""

    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]
