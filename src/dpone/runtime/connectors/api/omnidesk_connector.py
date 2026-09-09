from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APIRateLimitConfig,
    APIRetryConfig,
    ConcurrencyConfig,
    PaginationConfig,
)
from dpone.runtime.connectors.api.omnidesk_cases_mixin import OmnideskCasesMixin
from dpone.runtime.connectors.api.omnidesk_support import OmnideskCredentials, OmnideskPagination, VaultManager
from dpone.runtime.support.timezone import to_unix_timestamp


class OmnideskConnector(OmnideskCasesMixin, AbstractAPIConnector):
    """
    Коннектор для Omnidesk API.

    """

    # Omnidesk rate limit: рекомендуется не более 1 запроса в секунду
    DEFAULT_RATE_LIMIT = APIRateLimitConfig(
        requests_per_second=1.0,
        burst_limit=5,
    )

    def __init__(
        self,
        credentials: OmnideskCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        concurrency_config: ConcurrencyConfig | None = None,
        timeout: int = 30,
    ):
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            concurrency_config=concurrency_config or ConcurrencyConfig(),
            timeout=timeout,
        )
        self._pagination = OmnideskPagination()

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        parallel_workers: int | None = None,
        timeout: int = 30,
        **kwargs,
    ) -> OmnideskConnector:
        """
        Создаёт коннектор с креденшиалами из Vault.

        mount_point определяется автоматически из ENV_CODE (переменная окружения).

        Args:
            vault_path: Путь к секрету в Vault (обязательный, указывается в YAML)
            vault_manager: VaultManager (опционально)
            rate_limit_delay: Задержка между запросами (сек), default 1.0
            max_retries: Максимум повторов при ошибке, default 3
            parallel_workers: Количество параллельных воркеров (1 = последовательно)
            timeout: Таймаут запроса (сек), default 30
            **kwargs: Дополнительные параметры для коннектора
        """
        credentials = OmnideskCredentials.from_vault(
            vault_path=vault_path,
            vault_manager=vault_manager,
        )

        # Формируем конфиги из простых параметров
        retry_config = None
        if max_retries is not None:
            retry_config = APIRetryConfig(max_retries=max_retries)

        rate_limit_config = None
        if rate_limit_delay is not None:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay if rate_limit_delay > 0 else 10.0,
                burst_limit=max(5, parallel_workers or 1),  # burst >= workers
            )

        concurrency_config = None
        if parallel_workers is not None and parallel_workers > 1:
            concurrency_config = ConcurrencyConfig(
                max_workers=parallel_workers,
                preserve_order=False,
                fail_fast=False,
                progress_interval=100,
            )

        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            concurrency_config=concurrency_config,
            timeout=timeout,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Response Parsing (Omnidesk специфика)
    # -------------------------------------------------------------------------

    def _parse_omnidesk_response(
        self,
        response: dict[str, Any],
        item_key: str,
    ) -> list[dict[str, Any]]:
        """
        Парсит ответ Omnidesk API.

        Omnidesk возвращает ответ в формате:
        {
            "0": {"case": {...}},
            "1": {"case": {...}},
            ...
            "total_count": 150
        }

        """
        items = []

        # Итерируем по числовым ключам
        index = 0
        while True:
            key = str(index)
            if key not in response:
                break

            item_wrapper = response[key]
            if item_wrapper and item_key in item_wrapper:
                item = item_wrapper[item_key]
                if item:
                    items.append(item)

            index += 1

        return items

    def _normalize_record(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Нормализует запись из Omnidesk для записи в БД.

        Использует централизованный DataTypeMapper:
        - Заменяет '-' и '' на None
        - Сериализует list/dict в JSON строку
        - Конвертирует timestamp-подобные строки в datetime объекты
        """
        from dpone.runtime.support.data_type_mapper import DataTypeMapper

        return DataTypeMapper.normalize_record_for_db(record, null_markers=("-", ""))

    def _iterate_resource(
        self,
        endpoint: str,
        item_key: str,
        params: dict[str, Any] | None = None,
        max_pages: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Универсальный итератор по ресурсам Omnidesk (DRY).

        Args:
            endpoint: API endpoint (e.g., "cases.json")
            item_key: Ключ элемента в ответе (e.g., "case", "message")
            params: Query параметры
            max_pages: Лимит страниц

        Yields:
            Нормализованные записи
        """
        for response in self.paginate(endpoint, params=params, pagination=self._pagination, max_pages=max_pages):
            items = self._parse_omnidesk_response(response, item_key)
            for item in items:
                yield self._normalize_record(item)

    # -------------------------------------------------------------------------
    # Health Check
    # -------------------------------------------------------------------------

    def health_check(self) -> bool:
        """Проверяет доступность Omnidesk API."""
        try:
            self.get("cases.json", params={"limit": 1})
            return True
        except Exception as e:
            self.logger.error(f"Omnidesk health check failed: {e}")
            return False

    # -------------------------------------------------------------------------
    # Generic Resource Method (required by abstract class)
    # -------------------------------------------------------------------------

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        """
        Получает ресурсы указанного типа.

        Args:
            resource_type: Тип ресурса (cases, messages, users, staff, groups, labels)
            filters: Фильтры для запроса
            **kwargs: Дополнительные параметры

        Yields:
            Записи ресурсов
        """
        resource_map = {
            "cases": self.get_cases,
            "messages": self.get_messages,
            "users": self.get_users,
            "staff": self.get_staff,
            "groups": self.get_groups,
            "labels": self.get_labels,
        }

        if resource_type not in resource_map:
            raise ValueError(f"Unknown resource type: {resource_type}. Supported: {list(resource_map.keys())}")

        yield from resource_map[resource_type](**(filters or {}), **kwargs)

    # -------------------------------------------------------------------------
    # Cases (Обращения)
    # -------------------------------------------------------------------------

    def get_cases(
        self,
        from_time: datetime | int | None = None,
        to_time: datetime | int | None = None,
        status: str | None = None,
        staff_id: int | None = None,
        group_id: int | None = None,
        user_id: int | None = None,
        label_id: int | None = None,
        sort: str = "updated_at_asc",
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по обращениям (cases).

        Args:
            from_time: Начало периода (datetime или unix timestamp)
            to_time: Конец периода (datetime или unix timestamp)
            status: Фильтр по статусу (open, waiting, closed)
            staff_id: Фильтр по сотруднику
            group_id: Фильтр по группе
            user_id: Фильтр по пользователю
            label_id: Фильтр по метке
            sort: Сортировка (updated_at_asc, updated_at_desc, created_at_asc, created_at_desc)
            max_pages: Максимум страниц для загрузки
            **extra_params: Дополнительные параметры API

        Yields:
            Словари с данными обращений
        """
        params: dict[str, Any] = {"sort": sort}
        params.update(extra_params)

        # Конвертируем datetime в timestamp
        if from_time:
            params["from_time"] = to_unix_timestamp(from_time)
        if to_time:
            params["to_time"] = to_unix_timestamp(to_time)

        # Добавляем фильтры
        if status:
            params["status"] = status
        if staff_id:
            params["staff_id"] = staff_id
        if group_id:
            params["group_id"] = group_id
        if user_id:
            params["user_id"] = user_id
        if label_id:
            params["label_id"] = label_id

        yield from self._iterate_resource("cases.json", "case", params=params, max_pages=max_pages)

    # -------------------------------------------------------------------------
    # Messages (Сообщения)
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # Users (Пользователи)
    # -------------------------------------------------------------------------

    def get_users(
        self,
        email: str | None = None,
        phone: str | None = None,
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по пользователям.

        Args:
            email: Фильтр по email
            phone: Фильтр по телефону
            max_pages: Максимум страниц
            **extra_params: Дополнительные параметры

        Yields:
            Словари с данными пользователей
        """
        params: dict[str, Any] = {}
        params.update(extra_params)

        if email:
            params["email"] = email
        if phone:
            params["phone"] = phone

        yield from self._iterate_resource("users.json", "user", params=params, max_pages=max_pages)

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        """Получает пользователя по ID."""
        try:
            response = self.get(f"users/{user_id}.json")
            if response and "user" in response:
                return self._normalize_record(response["user"])
            return None
        except Exception as e:
            self.logger.warning(f"Failed to get user {user_id}: {e}")
            return None

    # -------------------------------------------------------------------------
    # Staff (Сотрудники)
    # -------------------------------------------------------------------------

    def get_staff(
        self,
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по сотрудникам.

        Yields:
            Словари с данными сотрудников
        """
        params: dict[str, Any] = {}
        params.update(extra_params)

        yield from self._iterate_resource("staff.json", "staff", params=params, max_pages=max_pages)

    # -------------------------------------------------------------------------
    # Groups (Группы)
    # -------------------------------------------------------------------------

    def get_groups(
        self,
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по группам.

        Yields:
            Словари с данными групп
        """
        params: dict[str, Any] = {}
        params.update(extra_params)

        yield from self._iterate_resource("groups.json", "group", params=params, max_pages=max_pages)

    # -------------------------------------------------------------------------
    # Labels (Метки)
    # -------------------------------------------------------------------------

    def get_labels(
        self,
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по меткам.

        Yields:
            Словари с данными меток
        """
        params: dict[str, Any] = {}
        params.update(extra_params)

        yield from self._iterate_resource("labels.json", "label", params=params, max_pages=max_pages)

    # -------------------------------------------------------------------------
    # Pagination Override
    # -------------------------------------------------------------------------

    def _has_more_pages(
        self,
        response: dict[str, Any],
        pagination: PaginationConfig,
        current_page: int,
        current_offset: int,
    ) -> bool:
        """
        Определяет есть ли ещё страницы (Omnidesk специфика).

        Omnidesk возвращает total_count и данные с числовыми ключами.
        Если ключ "0" отсутствует — страница пустая.
        """
        # Проверяем есть ли данные на текущей странице
        if "0" not in response:
            return False

        # Проверяем total_count если есть
        total_count = response.get("total_count")
        if total_count is not None:
            fetched = current_page * pagination.default_page_size
            return fetched < total_count

        # Fallback: проверяем заполнена ли страница полностью
        item_count = 0
        while str(item_count) in response:
            item_count += 1

        return item_count >= pagination.default_page_size
