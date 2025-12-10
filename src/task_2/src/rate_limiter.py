import asyncio
from src.config.logger import logger


class RateLimiter:
    """
    Ограничитель количества операций в секунду.

    :param rate_limit: Максимальное число операций (RPS)
    :return:
    """

    def __init__(self, rate_limit: int):
        self._rate_limit = rate_limit
        self._lock = asyncio.Semaphore(rate_limit)
        self._running = True

        logger.info(
            "Инициализация RateLimiter",
            extra={"rate_limit": rate_limit},
        )

        # Фоновый таск восстановления лимита
        self._reset_task = asyncio.create_task(self._reset_loop())

    async def _reset_loop(self) -> None:
        """
        Циклически восстанавливает лимит (каждую секунду).
        """
        logger.info("Запуск внутреннего цикла RateLimiter")

        try:
            while self._running:
                await asyncio.sleep(1)
                to_release = self._rate_limit - self._lock._value

                if to_release > 0:
                    for _ in range(to_release):
                        self._lock.release()

                logger.info(
                    "RateLimiter тик",
                    extra={
                        "available_slots": self._lock._value,
                        "rate_limit": self._rate_limit,
                    },
                )

        except Exception as e:
            logger.error("Ошибка в цикле RateLimiter", extra={"error": str(e)})
            raise

    async def acquire(self):
        """
        Получение одного слота лимита.

        :return:
        """
        await self._lock.acquire()
        logger.info(
            "RateLimiter acquired slot",
            extra={"remaining_slots": self._lock._value},
        )

    async def close(self) -> None:
        """
        Останавливает внутренний цикл и освобождает ресурсы.

        :return:
        """
        logger.info("Закрытие RateLimiter")
        self._running = False
        self._reset_task.cancel()

        try:
            await self._reset_task
        except asyncio.CancelledError:
            pass
