from __future__ import annotations

import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
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
    ConcurrencyConfig,
    resolve_generic_api_token,
)
from dpone.runtime.connectors.api.similarweb_resources import (
    SimilarwebResourceSpec,
    get_similarweb_resource,
    list_similarweb_resources,
)

logger = logging.getLogger(__name__)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class SimilarwebCredentials(APICredentials):
    """Credentials for SimilarWeb Search Keywords API."""

    auth_type: str = field(default="query_param", init=False)

    def __post_init__(self) -> None:
        self.endpoint = _normalize_similarweb_endpoint(self.endpoint)

    def get_headers(self) -> dict[str, str]:
        return {
            "Accept": "application/json",
            **dict(self.extra_headers or {}),
        }

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> SimilarwebCredentials:
        vm = vault_manager or get_default_manager()
        mount_point = get_env_code()
        secret = vm.get_secret(mount_point=mount_point, path=vault_path)
        return cls(
            endpoint=secret["endpoint"],
            api_key=resolve_generic_api_token(secret, context="SimilarWeb credentials"),
            extra_headers=dict(secret.get("extra_headers") or {}),
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> SimilarwebCredentials:
        return cls(
            endpoint=str(config["endpoint"]),
            api_key=resolve_generic_api_token(config, context="SimilarWeb credentials"),
            extra_headers=dict(config.get("extra_headers") or {}),
        )


def _normalize_similarweb_endpoint(endpoint: str) -> str:
    value = str(endpoint).strip().rstrip("/")
    if not value:
        raise ValueError("SimilarWeb endpoint must not be empty")
    for suffix in (
        "/v4/website-analysis/keywords",
        "/v4",
    ):
        if value.endswith(suffix):
            return value[: -len(suffix)]
    return value


class SimilarwebConnector(AbstractAPIConnector):
    """Connector for SimilarWeb Search Keywords API."""

    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=1.0, burst_limit=2)
    DEFAULT_TIMEOUT = 60

    def __init__(
        self,
        credentials: SimilarwebCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        concurrency_config: ConcurrencyConfig | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            concurrency_config=concurrency_config or ConcurrencyConfig(),
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
        timeout: int = DEFAULT_TIMEOUT,
    ) -> SimilarwebConnector:
        credentials = SimilarwebCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None and rate_limit_delay > 0:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay,
                burst_limit=2,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[SimilarwebResourceSpec]:
        return list_similarweb_resources()

    def health_check(self) -> bool:
        try:
            first_curr = date.today().replace(day=1)
            last_prev = first_curr - timedelta(days=1)
            first_prev = last_prev.replace(day=1)
            self.fetch_resource_rows(
                resource_name="keywords",
                url="example.com",
                start_date=first_prev.isoformat(),
                end_date=last_prev.isoformat(),
                limit=1,
            )
            return True
        except Exception as exc:  # pragma: no cover - defensive, external state
            self.logger.warning("SimilarWeb health check failed: %s", exc)
            return False

    def fetch_resource_rows(
        self,
        *,
        resource_name: str,
        url: str,
        start_date: Any,
        end_date: Any,
        limit: int = 2000,
        page_size: int | None = None,
        traffic_source: str = "Organic",
        web_source: str = "Total",
        branded_type: str = "All",
        country: str = "world",
        sort: str | None = None,
        asc: bool | None = None,
    ) -> list[dict[str, Any]]:
        return list(
            self.iter_resource_rows(
                resource_name=resource_name,
                url=url,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                page_size=page_size,
                traffic_source=traffic_source,
                web_source=web_source,
                branded_type=branded_type,
                country=country,
                sort=sort,
                asc=asc,
            )
        )

    def iter_resource_rows(
        self,
        *,
        resource_name: str,
        url: str,
        start_date: Any,
        end_date: Any,
        limit: int = 2000,
        page_size: int | None = None,
        traffic_source: str = "Organic",
        web_source: str = "Total",
        branded_type: str = "All",
        country: str = "world",
        sort: str | None = None,
        asc: bool | None = None,
    ) -> Iterator[dict[str, Any]]:
        spec = get_similarweb_resource(resource_name)
        if spec.name != "keywords":
            raise ValueError(f"Unsupported SimilarWeb resource '{resource_name}'")

        if not str(url).strip():
            raise ValueError("SimilarWeb resource 'keywords' requires a non-empty url")
        total_limit = int(limit)
        if total_limit <= 0:
            raise ValueError("SimilarWeb limit must be greater than zero")
        batch_size = int(page_size if page_size is not None else total_limit)
        if batch_size <= 0:
            raise ValueError("SimilarWeb page_size must be greater than zero")

        fetched = 0
        offset = 0
        while fetched < total_limit:
            batch_limit = min(batch_size, total_limit - fetched)
            page = self.get_keywords(
                url=url,
                start_date=start_date,
                end_date=end_date,
                limit=batch_limit,
                offset=offset,
                traffic_source=traffic_source,
                web_source=web_source,
                branded_type=branded_type,
                country=country,
                sort=sort,
                asc=asc,
            )
            if not page:
                break
            for row in page:
                yield row
                fetched += 1
            if len(page) < batch_limit:
                break
            offset += len(page)

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        params = dict(filters or {})
        params.update(kwargs)
        yield from self.iter_resource_rows(
            resource_name=resource_type,
            url=str(params.pop("url")),
            start_date=params.pop("start_date"),
            end_date=params.pop("end_date"),
            limit=int(params.pop("limit", 2000)),
            page_size=params.pop("page_size", None),
            traffic_source=str(params.pop("traffic_source", "Organic")),
            web_source=str(params.pop("web_source", "Total")),
            branded_type=str(params.pop("branded_type", "All")),
            country=str(params.pop("country", "world")),
            sort=params.pop("sort", None),
            asc=params.pop("asc", None),
        )

    def get_keywords(
        self,
        *,
        url: str,
        start_date: Any,
        end_date: Any,
        limit: int = 2000,
        offset: int = 0,
        traffic_source: str = "Organic",
        web_source: str = "Total",
        branded_type: str = "All",
        country: str = "world",
        sort: str | None = None,
        asc: bool | None = None,
    ) -> list[dict[str, Any]]:
        if not self.credentials.api_key:
            raise RuntimeError("SimilarWeb API key is not configured")

        month_param = self._resolve_api_month(start_date, end_date)
        params: dict[str, Any] = {
            "api_key": self.credentials.api_key,
            "URL": str(url).strip(),
            "start_date": month_param,
            "end_date": month_param,
            "traffic_source": traffic_source,
            "web_source": web_source,
            "branded_type": branded_type,
            "country": country,
            "limit": int(limit),
            "offset": int(offset),
        }
        if sort is not None:
            params["sort"] = sort
        if asc is not None:
            params["asc"] = asc

        data = self.get(get_similarweb_resource("keywords").endpoint_path, params=params)
        keywords = data.get("keywords", [])
        if not isinstance(keywords, list):
            raise RuntimeError("SimilarWeb response does not contain a list in 'keywords'")
        return [dict(item) for item in keywords if isinstance(item, Mapping)]

    @staticmethod
    def _coerce_date(value: Any) -> date:
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value).strip()
        if len(text) >= 10:
            return date.fromisoformat(text[:10])
        raise ValueError(f"Unsupported SimilarWeb date value: {value!r}")

    @classmethod
    def _resolve_api_month(cls, start_date: Any, end_date: Any) -> str:
        start = cls._coerce_date(start_date)
        end = cls._coerce_date(end_date)
        if start.year != end.year or start.month != end.month:
            raise ValueError("SimilarWeb keywords API accepts only a single month per request")
        return start.strftime("%Y-%m")


__all__ = [
    "SimilarwebConnector",
    "SimilarwebCredentials",
    "get_default_manager",
]
