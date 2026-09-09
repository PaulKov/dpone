from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any


from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
)
from dpone.runtime.connectors.api.yandex_webmaster_resources import (
    YandexWebmasterResourceSpec,
    list_yandex_webmaster_resources,
)
from dpone.runtime.connectors.api.yandex_webmaster_routing_mixin import YandexWebmasterRoutingMixin

logger = logging.getLogger(__name__)


def _resolve_yandex_webmaster_token(config: Mapping[str, Any]) -> str:
    token = config.get("token")
    if token is not None:
        return str(token)

    oauth_token = config.get("oauth_token")
    if oauth_token is not None:
        return str(oauth_token)

    api_key = config.get("api_key")
    if api_key is not None:
        return str(api_key)

    raise KeyError("Yandex Webmaster credentials must define 'token' or 'oauth_token'")


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class YandexWebmasterCredentials(APICredentials):
    auth_type: str = field(default="bearer", init=False)
    api_key_header: str = field(default="Authorization", init=False)
    api_key_prefix: str = field(default="OAuth", init=False)

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> YandexWebmasterCredentials:
        vm = vault_manager or get_default_manager()
        secret = vm.get_secret(mount_point=get_env_code(), path=vault_path)
        return cls.from_dict(secret)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> YandexWebmasterCredentials:
        return cls(
            endpoint=str(config.get("endpoint") or "https://api.webmaster.yandex.net/v4"),
            api_key=_resolve_yandex_webmaster_token(config),
            extra_headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )


class YandexWebmasterConnector(YandexWebmasterRoutingMixin, AbstractAPIConnector):
    DEFAULT_RATE_LIMIT = APIRateLimitConfig(
        requests_per_second=2.0,
        burst_limit=5,
    )

    def __init__(
        self,
        credentials: YandexWebmasterCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        timeout: int = 60,
    ) -> None:
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            timeout=timeout,
        )

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        timeout: int = 60,
        **_: Any,
    ) -> YandexWebmasterConnector:
        credentials = YandexWebmasterCredentials.from_vault(
            vault_path=vault_path,
            vault_manager=vault_manager,
        )
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay if rate_limit_delay > 0 else 2.0,
                burst_limit=5,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[YandexWebmasterResourceSpec]:
        return list_yandex_webmaster_resources()

    def get_user_id(self) -> int:
        payload = self.get("/user")
        user_id = payload.get("user_id")
        if user_id is None:
            raise RuntimeError(f"Yandex Webmaster API: user_id not found in response: {payload}")
        return int(user_id)

    def list_hosts(self, user_id: int | None = None) -> list[dict[str, Any]]:
        resolved_user_id = user_id or self.get_user_id()
        payload = self.get(f"/user/{resolved_user_id}/hosts")
        hosts = payload.get("hosts", [])
        if not isinstance(hosts, list):
            raise RuntimeError(f"Yandex Webmaster API: invalid hosts payload: {payload}")
        return hosts

    def resolve_user_id(self, user_id: int | None = None) -> int:
        return int(user_id) if user_id is not None else self.get_user_id()

    def resolve_host_id(
        self,
        *,
        user_id: int,
        host_id: str | None = None,
        host_url: str | None = None,
    ) -> str:
        if host_id:
            return host_id
        if not host_url:
            raise ValueError("YandexWebmasterConnector: either host_id or host_url must be provided")

        normalized_host_url = host_url.rstrip("/")
        for host in self.list_hosts(user_id=user_id):
            ascii_host_url = str(host.get("ascii_host_url", "")).rstrip("/")
            unicode_host_url = str(host.get("unicode_host_url", "")).rstrip("/")
            if normalized_host_url in {ascii_host_url, unicode_host_url}:
                return str(host["host_id"])

        raise RuntimeError(f"Yandex Webmaster API: host_url='{host_url}' not found in available hosts")

    def get_indexing_history(self, *, user_id: int, host_id: str, date_from: str, date_to: str) -> dict[str, Any]:
        return self.get(
            f"/user/{user_id}/hosts/{host_id}/indexing/history",
            params={"date_from": date_from, "date_to": date_to},
        )

    def get_in_search_history(self, *, user_id: int, host_id: str, date_from: str, date_to: str) -> dict[str, Any]:
        return self.get(
            f"/user/{user_id}/hosts/{host_id}/search-urls/in-search/history",
            params={"date_from": date_from, "date_to": date_to},
        )

    def get_search_events_history(
        self,
        *,
        user_id: int,
        host_id: str,
        date_from: str,
        date_to: str,
    ) -> dict[str, Any]:
        return self.get(
            f"/user/{user_id}/hosts/{host_id}/search-urls/events/history",
            params={"date_from": date_from, "date_to": date_to},
        )

    def get_search_queries_popular(
        self,
        *,
        user_id: int,
        host_id: str,
        date_from: str,
        date_to: str,
        order_by: str = "TOTAL_SHOWS",
        limit: int = 500,
        offset: int = 0,
        query_indicators: Sequence[str] = (
            "TOTAL_SHOWS",
            "TOTAL_CLICKS",
            "AVG_SHOW_POSITION",
            "AVG_CLICK_POSITION",
        ),
    ) -> dict[str, Any]:
        return self.get(
            f"/user/{user_id}/hosts/{host_id}/search-queries/popular",
            params={
                "order_by": order_by,
                "query_indicator": list(query_indicators),
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
                "offset": offset,
            },
        )

    def iter_search_queries_popular(
        self,
        *,
        user_id: int,
        host_id: str,
        date_from: str,
        date_to: str,
        order_by: str = "TOTAL_SHOWS",
        limit: int = 500,
        max_queries: int | None = None,
        query_indicators: Sequence[str] = (
            "TOTAL_SHOWS",
            "TOTAL_CLICKS",
            "AVG_SHOW_POSITION",
            "AVG_CLICK_POSITION",
        ),
    ) -> Iterator[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("Yandex Webmaster popular-query page limit must be greater than zero")

        yielded = 0
        offset = 0
        while True:
            payload = self.get_search_queries_popular(
                user_id=user_id,
                host_id=host_id,
                date_from=date_from,
                date_to=date_to,
                order_by=order_by,
                limit=limit,
                offset=offset,
                query_indicators=query_indicators,
            )
            queries = payload.get("queries", []) or []
            if not isinstance(queries, list) or not queries:
                break

            for query in queries:
                yield query
                yielded += 1
                if max_queries is not None and yielded >= max_queries:
                    return

            if len(queries) < limit:
                break
            offset += len(queries)

    def get_search_query_history(
        self,
        *,
        user_id: int,
        host_id: str,
        query_id: str,
        date_from: str,
        date_to: str,
        device_type: str,
        query_indicators: Sequence[str] = (
            "TOTAL_SHOWS",
            "TOTAL_CLICKS",
            "AVG_SHOW_POSITION",
            "AVG_CLICK_POSITION",
        ),
    ) -> dict[str, Any]:
        return self.get(
            f"/user/{user_id}/hosts/{host_id}/search-queries/{query_id}/history",
            params={
                "query_indicator": list(query_indicators),
                "device_type_indicator": device_type,
                "date_from": date_from,
                "date_to": date_to,
            },
        )

    def post_query_analytics_list(
        self,
        *,
        user_id: int,
        host_id: str,
        region_ids: Sequence[int],
        limit: int = 500,
        offset: int = 0,
        device_type_indicator: str = "ALL",
        search_location: str = "WEB_LOCATION",
        text_indicator: str = "QUERY",
        order_by: str = "TOTAL_SHOWS",
    ) -> dict[str, Any]:
        return self.post(
            f"/user/{user_id}/hosts/{host_id}/query-analytics/list",
            json_data={
                "order_by": order_by,
                "limit": limit,
                "offset": offset,
                "device_type_indicator": device_type_indicator,
                "search_location": search_location,
                "text_indicator": text_indicator,
                "region_ids": [int(value) for value in region_ids],
            },
        )

    def iter_query_analytics_list(
        self,
        *,
        user_id: int,
        host_id: str,
        region_ids: Sequence[int],
        limit: int = 500,
        max_queries: int | None = None,
        device_type_indicator: str = "ALL",
        search_location: str = "WEB_LOCATION",
        text_indicator: str = "QUERY",
        order_by: str = "TOTAL_SHOWS",
    ) -> Iterator[dict[str, Any]]:
        if limit <= 0:
            raise ValueError("Yandex Webmaster query-analytics page limit must be greater than zero")

        yielded = 0
        offset = 0
        while True:
            payload = self.post_query_analytics_list(
                user_id=user_id,
                host_id=host_id,
                region_ids=region_ids,
                limit=limit,
                offset=offset,
                device_type_indicator=device_type_indicator,
                search_location=search_location,
                text_indicator=text_indicator,
                order_by=order_by,
            )
            items = payload.get("text_indicator_to_statistics", []) or []
            if not isinstance(items, list) or not items:
                break

            for item in items:
                yield item
                yielded += 1
                if max_queries is not None and yielded >= max_queries:
                    return

            if len(items) < limit:
                break
            offset += len(items)

    def health_check(self) -> bool:
        try:
            self.get_user_id()
            return True
        except Exception:
            logger.exception("Yandex Webmaster health_check failed")
            return False


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


__all__ = [
    "YandexWebmasterConnector",
    "YandexWebmasterCredentials",
]
