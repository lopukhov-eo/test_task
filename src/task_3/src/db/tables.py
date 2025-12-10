from src.task_3.src.config import CLICKHOUSE_SCHEMA_NAME


class Tables:
    """
    Класс с именами таблиц ClickHouse для работы приложения.

    :param CLICKHOUSE_SCHEMA_NAME: Имя схемы ClickHouse
    :return:
    """

    schema = CLICKHOUSE_SCHEMA_NAME

    REPOSITORIES = f"{schema}.repositories"
    AUTHORS_COMMITS = f"{schema}.repositories_authors_commits"
    POSITIONS = f"{schema}.repositories_positions"

    @classmethod
    def all(cls):
        """
        Генератор, возвращающий список всех таблиц схемы.

        Используется для проверки наличия таблиц и их инициализации.

        :return: Имена таблиц ClickHouse в текущей схеме
        """
        for key, value in cls.__dict__.items():
            if key.isupper() and isinstance(value, str):
                yield value
