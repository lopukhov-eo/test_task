from datetime import datetime
from pathlib import Path

from aiochclient import ChClient
from aiohttp import ClientSession

from src.config.logger import logger
from src.task_3.src.db.tables import Tables


class ClickHouseClient:
    """
    Клиент для подключения к ClickHouse, инициализации схемы и выполнения запросов.

    :param url: URL ClickHouse сервера
    :param user: Логин ClickHouse
    :param password: Пароль ClickHouse
    :param schema_file_path: Путь до SQL файла со схемой таблиц
    :return:
    """

    def __init__(self, url: str, user: str, password: str, schema_file_path: str):
        """
        Инициализация клиента ClickHouse.

        :param url: URL ClickHouse сервера
        :param user: Имя пользователя ClickHouse
        :param password: Пароль пользователя ClickHouse
        :param schema_file_path: Путь к SQL файлу со схемой таблиц
        :return:
        """
        self.url = url
        self.user = user
        self.password = password
        self.schema_file_path = schema_file_path

        self.client: ChClient | None = None

        logger.info("Инициализация ClickHouseClient выполнена.")

    async def __aenter__(self):
        """
        Входит в контекстный менеджер.

        - Подключается к ClickHouse
        - Проверяет наличие схемы и создаёт при необходимости

        :return: self
        """
        await self._connect()
        await self._ensure_schema()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """
        Выходит из контекстного менеджера и закрывает все сетевые ресурсы.

        :param exc_type:
        :param exc:
        :param tb:
        :return:
        """
        await self._close()

        # Дополнительная попытка закрыть низкоуровневую сессию aiochclient
        if hasattr(self.client, "_close"):
            try:
                await self.client._close()
            except Exception as e:
                logger.warning(
                    "Ошибка при закрытии клиентской сессии", extra={"error": str(e)}
                )

        logger.info("Соединение с ClickHouse закрыто.")

    async def _connect(self):
        """
        Устанавливает соединение с ClickHouse и проверяет его.

        :return:
        """
        session = ClientSession()

        try:
            self.client = ChClient(
                session, url=self.url, user=self.user, password=self.password
            )

            # Проверочный запрос
            await self.client.fetch("SELECT 1")

            logger.info("Подключение к ClickHouse установлено.")

        except Exception as e:
            logger.error(f"Ошибка подключения к ClickHouse: {e}")
            await session.close()
            raise ConnectionError("ClickHouse недоступен.")

    async def _close(self):
        """
        Закрывает все внутренние сессии aiochclient и HTTP-клиентов.

        :return:
        """
        if not self.client:
            return

        logger.info("Закрытие сессий ClickHouse клиента...")

        # собираем все возможные HTTP-сессии
        sessions = [
            getattr(self.client, "_session", None),
            getattr(self.client, "session", None),
        ]

        http_client = getattr(self.client, "_http_client", None)
        if http_client:
            sessions.append(getattr(http_client, "_session", None))

        # закрытие сессий
        for sess in sessions:
            if sess and hasattr(sess, "close") and not sess.closed:
                try:
                    await sess.close()
                except Exception as e:
                    logger.warning(
                        "Ошибка при закрытии клиентской сессии", extra={"error": str(e)}
                    )

                connector = getattr(sess, "_connector", None) or getattr(
                    sess, "connector", None
                )
                if connector and hasattr(connector, "close") and not connector.closed:
                    try:
                        await connector.close()
                    except Exception as e:
                        logger.warning(
                            "Ошибка при закрытии клиентской сессии",
                            extra={"error": str(e)},
                        )

        logger.info("Все сетевые ресурсы ClickHouse закрыты.")

    async def _ensure_schema(self):
        """
        Проверяет существование таблиц в схеме.

        Если таблицы отсутствуют — создаёт их из SQL файла.

        :return:
        """
        logger.info("Проверка существования схемы ClickHouse...")

        exists = await self.table_exists(Tables.REPOSITORIES)

        if exists:
            logger.info("Схема ClickHouse уже существует.")
            return

        logger.info("Схема отсутствует. Начинаем инициализацию...")

        sql_text = Path(self.schema_file_path).read_text(encoding="utf-8")
        statements = self._split_sql(sql_text)

        for stmt in statements:
            stmt = stmt.strip()
            if not stmt:
                continue

            try:
                await self.client.execute(stmt)
            except Exception as e:
                logger.error(f"Ошибка выполнения SQL:\n{stmt}\nПричина: {e}")
                raise

        logger.info("Схема ClickHouse успешно создана.")

    @staticmethod
    def _split_sql(sql: str) -> list[str]:
        """
        Разделяет SQL файл на отдельные запросы по символу ';'.

        :param sql: Полный текст SQL файла
        :return: Список отдельных SQL запросов
        """
        statements = []
        current = []

        for line in sql.splitlines():
            stripped = line.strip()
            if not stripped:
                continue

            current.append(line)

            if stripped.endswith(";"):
                statements.append("\n".join(current))
                current = []

        if current:
            statements.append("\n".join(current))

        return statements

    async def table_exists(self, table: str) -> bool:
        """
        Проверяет, существует ли таблица в ClickHouse.

        :param table: Полное имя таблицы (schema.table)
        :return: True — если таблица существует
        """
        try:
            result = await self.client.fetchval(f"EXISTS {table}")
            return bool(result)
        except Exception as e:
            logger.error(f"Ошибка проверки таблицы {table}: {e}")
            raise

    async def insert_batch(self, table: str, rows: list[dict]):
        """
        Выполняет batch-вставку строк в таблицу ClickHouse.

        :param table: Имя таблицы
        :param rows: Список словарей с данными для вставки
        :return:
        """
        if not rows:
            logger.info(f"Пропуск вставки: нет строк для {table}.")
            return

        logger.info(f"Вставка {len(rows)} строк в таблицу {table}...")

        columns = list(rows[0].keys())

        values_sql = ",\n".join(
            "(" + ", ".join(self._format_value(row[col]) for col in columns) + ")"
            for row in rows
        )

        sql = f"""
            INSERT INTO {table} ({", ".join(columns)}) 
            VALUES
            {values_sql}
        """  # nosec B608

        try:
            await self.client.execute(sql)
            logger.info(f"Вставка в {table} завершена успешно.")
        except Exception as e:
            logger.error(f"Ошибка batch-вставки в {table}: {e}")
            raise RuntimeError(f"Batch insert failed: {e}")

    @staticmethod
    def _format_value(value):
        """
        Преобразует Python значения в формат, корректный для ClickHouse SQL.

        :param value: Значение для преобразования
        :return: SQL-представление значения
        """
        if value is None:
            return "NULL"

        if isinstance(value, datetime):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"

        if isinstance(value, str):
            safe = value.replace("'", "''")
            return f"'{safe}'"

        if isinstance(value, bool):
            return "1" if value else "0"

        return str(value)
