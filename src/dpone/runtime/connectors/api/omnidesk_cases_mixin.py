"""Case and message helpers for Omnidesk connector."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from typing import Any

from dpone.runtime.support.timezone import to_unix_timestamp


class OmnideskCasesMixin:
    def get_cases_concurrent(
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
        concurrent_requests: int = 5,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Параллельный итератор по обращениям (cases).

        Использует несколько потоков для ускорения загрузки.
        Rate limiting соблюдается глобально.

        Args:
            from_time: Начало периода
            to_time: Конец периода
            concurrent_requests: Количество параллельных запросов (default: 5)
            ... остальные параметры как в get_cases()

        Yields:
            Словари с данными обращений (в порядке страниц)
        """
        params: dict[str, Any] = {"sort": sort}
        params.update(extra_params)

        if from_time:
            params["from_time"] = to_unix_timestamp(from_time)
        if to_time:
            params["to_time"] = to_unix_timestamp(to_time)

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

        # Используем параллельную пагинацию
        for response in self.paginate_concurrent(
            "cases.json",
            params=params,
            pagination=self._pagination,
            max_pages=max_pages,
            concurrent_requests=concurrent_requests,
        ):
            items = self._parse_omnidesk_response(response, "case")
            for item in items:
                yield self._normalize_record(item)

    def get_case(self, case_id: int) -> dict[str, Any] | None:
        """
        Получает одно обращение по ID.

        Args:
            case_id: ID обращения

        Returns:
            Словарь с данными обращения или None если не найдено
        """
        try:
            response = self.get(f"cases/{case_id}.json")
            if response and "case" in response:
                return self._normalize_record(response["case"])
            return None
        except Exception as e:
            self.logger.warning(f"Failed to get case {case_id}: {e}")
            return None

    def get_cases_batch(self, case_ids: list[int]) -> list[dict[str, Any]]:
        """
        Получает несколько обращений по списку ID.

        Args:
            case_ids: Список ID обращений

        Returns:
            Список словарей с данными обращений
        """
        cases = []
        for case_id in case_ids:
            case = self.get_case(case_id)
            if case:
                cases.append(case)
        return cases

    def get_messages(
        self,
        case_id: int,
        order: str = "asc",
        max_pages: int | None = None,
        **extra_params,
    ) -> Iterator[dict[str, Any]]:
        """
        Итератор по сообщениям обращения.

        Args:
            case_id: ID обращения
            order: Порядок сортировки (asc, desc)
            max_pages: Максимум страниц
            **extra_params: Дополнительные параметры

        Yields:
            Словари с данными сообщений (с добавленным case_id)
        """
        params: dict[str, Any] = {"order": order}
        params.update(extra_params)

        endpoint = f"cases/{case_id}/messages.json"

        for message in self._iterate_resource(endpoint, "message", params=params, max_pages=max_pages):
            message["case_id"] = case_id
            yield message

    def get_all_messages_for_case(self, case_id: int) -> list[dict[str, Any]]:
        """
        Получает все сообщения для обращения (не итератор).

        Args:
            case_id: ID обращения

        Returns:
            Список сообщений
        """
        return list(self.get_messages(case_id))


__all__ = ["OmnideskCasesMixin"]
