"""Abstract HTTP API connector infrastructure."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Any

from dpone.runtime.connectors.api.config import APIRateLimitConfig, APIRetryConfig, ConcurrencyConfig, PaginationConfig
from dpone.runtime.connectors.api.credentials import APICredentials
from dpone.runtime.connectors.api.parallel import ParallelTaskExecutor
from dpone.runtime.connectors.api.rate_limit import ThreadSafeRateLimiter

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    requests = None  # type: ignore
    HTTPAdapter = None  # type: ignore
    Retry = None  # type: ignore


class AbstractAPIConnector(ABC):
    """
    Абстрактный базовый класс для API коннекторов.

    Предоставляет:
    - HTTP сессию с retry логикой
    - Rate limiting
    - Абстрактные методы для имплементации в наследниках
    - Пагинированные итераторы

    Наследники должны реализовать:
    - _parse_response(): парсинг ответа API
    - Специфичные методы для каждого endpoint
    """

    def __init__(
        self,
        credentials: APICredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        concurrency_config: ConcurrencyConfig | None = None,
        timeout: int = 30,
    ):
        if requests is None:
            raise RuntimeError("requests library is not installed. Run: pip install requests")

        self.credentials = credentials
        self.retry_config = retry_config or APIRetryConfig()
        self.rate_limit_config = rate_limit_config or APIRateLimitConfig()
        self.concurrency_config = concurrency_config or ConcurrencyConfig()
        self.timeout = timeout

        self._session: requests.Session | None = None
        self._request_count: int = 0

        self._rate_limiter = ThreadSafeRateLimiter(
            requests_per_second=self.rate_limit_config.requests_per_second,
            burst_limit=self.rate_limit_config.burst_limit,
        )

        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    # -------------------------------------------------------------------------
    # Session Management
    # -------------------------------------------------------------------------

    @property
    def session(self) -> requests.Session:
        """Ленивая инициализация HTTP сессии с retry."""
        if self._session is None:
            self._session = self._create_session()
        return self._session

    def _create_session(self) -> requests.Session:
        """Создаёт HTTP сессию с настроенной retry логикой."""
        session = requests.Session()

        # Настраиваем retry
        retry_strategy = Retry(
            total=self.retry_config.max_retries,
            backoff_factor=self.retry_config.backoff_factor,
            status_forcelist=self.retry_config.status_forcelist,
            allowed_methods=self.retry_config.allowed_methods,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        # Устанавливаем headers
        session.headers.update(self.credentials.get_headers())

        return session

    def close(self) -> None:
        """Закрывает HTTP сессию."""
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # -------------------------------------------------------------------------
    # Rate Limiting (Thread-Safe)
    # -------------------------------------------------------------------------

    def _apply_rate_limit(self) -> None:
        """
        Применяет rate limiting между запросами.

        Thread-safe: использует ThreadSafeRateLimiter с token bucket.
        """
        if self.rate_limit_config.requests_per_second <= 0:
            return

        wait_time = self._rate_limiter.acquire()

        if wait_time > 0:
            self.logger.debug(f"Rate limiting: waited {wait_time:.3f}s")

        self._request_count += 1

    # -------------------------------------------------------------------------
    # Parallel Execution
    # -------------------------------------------------------------------------

    def create_parallel_executor(
        self,
        config: ConcurrencyConfig | None = None,
    ) -> ParallelTaskExecutor:
        """
        Создаёт исполнитель для параллельных запросов.

        Использует общий rate limiter для соблюдения лимитов API.

        Args:
            config: Конфигурация параллелизма (по умолчанию из коннектора)

        Returns:
            ParallelTaskExecutor с настроенным rate limiter

        Пример:
            executor = connector.create_parallel_executor(
                ConcurrencyConfig(max_workers=10)
            )
            for result, error, case_id in executor.map(fetch_func, case_ids):
                if not error:
                    yield from result
        """
        cfg = config or self.concurrency_config
        return ParallelTaskExecutor(
            rate_limiter=self._rate_limiter,
            config=cfg,
            logger=self.logger,
        )

    @property
    def rate_limiter(self) -> ThreadSafeRateLimiter:
        """Возвращает thread-safe rate limiter."""
        return self._rate_limiter

    # -------------------------------------------------------------------------
    # HTTP Methods
    # -------------------------------------------------------------------------

    def _build_url(self, endpoint: str) -> str:
        """Строит полный URL из base endpoint и path."""
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        base = self.credentials.endpoint.rstrip("/")
        path = endpoint.lstrip("/")
        return f"{base}/{path}"

    def _request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        json_data: dict[str, Any] | None = None,
        **kwargs,
    ) -> requests.Response:
        """
        Выполняет HTTP запрос с retry и rate limiting.

        Args:
            method: HTTP метод (GET, POST, etc.)
            endpoint: Путь к endpoint (без base URL)
            params: Query параметры
            json_data: JSON body для POST/PUT
            **kwargs: Дополнительные параметры для requests

        Returns:
            requests.Response объект

        Raises:
            requests.HTTPError: При ошибке HTTP
        """
        self._apply_rate_limit()

        url = self._build_url(endpoint)
        auth = self.credentials.get_auth()

        self.logger.debug(f"{method} {url} params={params}")

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=json_data,
            auth=auth,
            timeout=self.timeout,
            **kwargs,
        )

        # Логируем ошибки
        if not response.ok:
            self.logger.warning(
                f"API error: {response.status_code} {response.reason} URL: {url} Response: {response.text[:500]}"
            )

        response.raise_for_status()
        return response

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Выполняет GET запрос и возвращает JSON."""
        response = self._request("GET", endpoint, params=params, **kwargs)
        return response.json()

    def post(
        self,
        endpoint: str,
        json_data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """Выполняет POST запрос и возвращает JSON."""
        response = self._request("POST", endpoint, params=params, json_data=json_data, **kwargs)
        return response.json()

    # -------------------------------------------------------------------------
    # Pagination
    # -------------------------------------------------------------------------

    def paginate(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        pagination: PaginationConfig | None = None,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Итерирует по страницам API с автоматической пагинацией.

        Args:
            endpoint: API endpoint
            params: Базовые query параметры
            pagination: Конфигурация пагинации
            max_pages: Максимальное количество страниц (None = без лимита)

        Yields:
            Словари с данными из каждой страницы (сырой ответ API)
        """
        pagination = pagination or PaginationConfig()
        params = dict(params or {})

        page = 1
        offset = 0
        cursor = None

        while True:
            # Применяем параметры пагинации
            if pagination.strategy == "page":
                params[pagination.page_param] = page
                params[pagination.limit_param] = pagination.default_page_size
            elif pagination.strategy == "offset":
                params[pagination.offset_param] = offset
                params[pagination.limit_param] = pagination.default_page_size
            elif pagination.strategy == "cursor" and cursor:
                params[pagination.cursor_param] = cursor
                params[pagination.limit_param] = pagination.default_page_size

            # Выполняем запрос
            response = self.get(endpoint, params=params)

            yield response

            # Проверяем лимит страниц
            if max_pages and page >= max_pages:
                self.logger.info(f"Reached max_pages limit: {max_pages}")
                break

            # Проверяем есть ли следующая страница
            if not self._has_more_pages(response, pagination, page, offset):
                break

            # Переходим к следующей странице
            page += 1
            offset += pagination.default_page_size

            if pagination.strategy == "cursor":
                cursor = self._extract_cursor(response, pagination)
                if not cursor:
                    break

    def _has_more_pages(
        self,
        response: dict[str, Any],
        pagination: PaginationConfig,
        current_page: int,
        current_offset: int,
    ) -> bool:
        """
        Определяет есть ли ещё страницы.

        Базовая реализация проверяет total_count.
        Наследники могут переопределить для специфичной логики.
        """
        if pagination.total_count_key:
            total = response.get(pagination.total_count_key, 0)
            fetched = current_offset + pagination.default_page_size
            return fetched < total

        # Fallback: проверяем есть ли данные в ответе
        data = self._extract_data(response, pagination)
        return len(data) >= pagination.default_page_size

    def _extract_cursor(
        self,
        response: dict[str, Any],
        pagination: PaginationConfig,
    ) -> str | None:
        """Извлекает курсор для следующей страницы."""
        if pagination.next_cursor_key:
            return response.get(pagination.next_cursor_key)
        return None

    def _extract_data(
        self,
        response: dict[str, Any],
        pagination: PaginationConfig | None = None,
    ) -> list[dict[str, Any]]:
        """
        Извлекает данные из ответа API.

        Args:
            response: Сырой ответ API
            pagination: Конфигурация с ключом данных

        Returns:
            Список записей
        """
        if pagination and pagination.data_key:
            data = response.get(pagination.data_key, [])
        else:
            if isinstance(response, list):
                data = response
            else:
                data = response

        return data if isinstance(data, list) else [data]

    # -------------------------------------------------------------------------
    # Abstract Methods
    # -------------------------------------------------------------------------

    @abstractmethod
    def health_check(self) -> bool:
        """
        Проверяет доступность API.

        Returns:
            True если API доступен
        """

    @abstractmethod
    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        """
        Получает ресурсы указанного типа.

        Args:
            resource_type: Тип ресурса (cases, messages, users, etc.)
            filters: Фильтры для запроса
            **kwargs: Дополнительные параметры

        Yields:
            Словари с данными ресурсов
        """

    # -------------------------------------------------------------------------
    # Utility Methods
    # -------------------------------------------------------------------------

    def get_all_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        """
        Получает все ресурсы указанного типа (не итератор, а полный список).

        Удобно для небольших наборов данных.
        Для больших объёмов используйте get_resources() итератор.
        """
        return list(self.get_resources(resource_type, filters, **kwargs))

    @property
    def request_count(self) -> int:
        """Возвращает количество выполненных запросов."""
        return self._request_count

    def reset_stats(self) -> None:
        """Сбрасывает статистику запросов."""
        self._request_count = 0
        self._last_request_time = 0.0


__all__ = ["AbstractAPIConnector"]
