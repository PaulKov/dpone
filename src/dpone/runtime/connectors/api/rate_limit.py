"""Thread-safe rate limiting for API connectors."""

from __future__ import annotations

import threading
import time
from typing import Any


class ThreadSafeRateLimiter:
    """
    Потокобезопасный rate limiter для параллельных запросов.

    Использует token bucket алгоритм:
    - Токены добавляются с фиксированной скоростью (requests_per_second)
    - Каждый запрос потребляет 1 токен
    - При отсутствии токенов — ожидание

    Thread-safe: можно использовать из нескольких потоков одновременно.
    """

    def __init__(
        self,
        requests_per_second: float = 1.0,
        burst_limit: int = 1,
    ):
        self.requests_per_second = requests_per_second
        self.burst_limit = burst_limit

        self._tokens = float(burst_limit)
        self._last_refill_time = time.monotonic()
        self._lock = threading.Lock()

        # Статистика
        self._total_requests = 0
        self._total_wait_time = 0.0

    def acquire(self) -> float:
        """
        Получает разрешение на запрос. Блокируется если нет токенов.

        Returns:
            Время ожидания в секундах (0.0 если не ждали)
        """
        with self._lock:
            self._refill_tokens()

            wait_time = 0.0
            if self._tokens < 1.0:
                # Вычисляем сколько ждать до появления токена
                wait_time = (1.0 - self._tokens) / self.requests_per_second

            if wait_time > 0:
                # Освобождаем lock на время ожидания
                self._lock.release()
                try:
                    time.sleep(wait_time)
                finally:
                    self._lock.acquire()
                self._refill_tokens()

            self._tokens -= 1.0
            self._total_requests += 1
            self._total_wait_time += wait_time

            return wait_time

    def _refill_tokens(self) -> None:
        """Добавляет токены на основе прошедшего времени."""
        now = time.monotonic()
        elapsed = now - self._last_refill_time

        tokens_to_add = elapsed * self.requests_per_second
        self._tokens = min(self.burst_limit, self._tokens + tokens_to_add)
        self._last_refill_time = now

    @property
    def stats(self) -> dict[str, Any]:
        """Возвращает статистику rate limiter."""
        with self._lock:
            return {
                "total_requests": self._total_requests,
                "total_wait_time": round(self._total_wait_time, 2),
                "avg_wait_time": round(self._total_wait_time / max(1, self._total_requests), 3),
                "tokens_available": round(self._tokens, 2),
            }


__all__ = ["ThreadSafeRateLimiter"]
