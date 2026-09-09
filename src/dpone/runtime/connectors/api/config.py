"""Shared API connector configuration models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class APIRetryConfig:
    """Конфигурация retry логики для API запросов."""

    max_retries: int = 3
    backoff_factor: float = 0.5
    status_forcelist: tuple[int, ...] = (429, 500, 502, 503, 504)
    allowed_methods: tuple[str, ...] = ("GET", "POST", "PUT", "DELETE", "PATCH")


@dataclass(frozen=True)
class APIRateLimitConfig:
    """Конфигурация rate limiting."""

    requests_per_second: float = 10.0
    burst_limit: int = 20


@dataclass(frozen=True)
class ConcurrencyConfig:
    """
    Конфигурация параллельного выполнения API запросов.

    Attributes:
        max_workers: Количество параллельных воркеров (1 = последовательно)
        preserve_order: Сохранять порядок результатов (медленнее, но детерминировано)
        fail_fast: Прервать при первой ошибке (иначе собираем все ошибки)
        progress_interval: Логировать прогресс каждые N задач
    """

    max_workers: int = 1
    preserve_order: bool = False
    fail_fast: bool = False
    progress_interval: int = 100


@dataclass
class PaginationConfig:
    """Конфигурация пагинации API."""

    strategy: str = "page"
    page_param: str = "page"
    limit_param: str = "limit"
    offset_param: str = "offset"
    cursor_param: str = "cursor"

    default_page_size: int = 100
    max_page_size: int = 100

    # Response parsing
    data_key: str | None = None
    total_count_key: str | None = "total_count"
    next_cursor_key: str | None = "next_cursor"
    next_page_url_key: str | None = "next"


__all__ = ["APIRateLimitConfig", "APIRetryConfig", "ConcurrencyConfig", "PaginationConfig"]
