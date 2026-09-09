from __future__ import annotations

import csv
import gzip
import logging
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from io import StringIO
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any
from urllib.parse import urljoin

from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
    ConcurrencyConfig,
    resolve_generic_api_token,
)
from dpone.runtime.connectors.api.fasttrack_resources import (
    FasttrackResourceSpec,
    get_fasttrack_resource,
    list_fasttrack_resources,
)

logger = logging.getLogger(__name__)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class FasttrackCredentials(APICredentials):
    """Credentials for mixed Fasttrack dashboard/flex pull APIs.

    The legacy implementation uses two different auth/header modes:
    - dashboard reports via ``bot-key``
    - flex endpoints via ``X-Token``

    To keep rollout flexible we support both dedicated fields and a generic
    ``token``/``api_key`` fallback that can populate both slots.
    """

    dashboard_endpoint: str = "https://dashboard.fstrk.io/api/partners"
    dashboard_bot_key: str | None = None
    dashboard_extra_headers: dict[str, str] = field(default_factory=dict)

    flex_endpoint: str | None = None
    flex_x_token: str | None = None
    flex_extra_headers: dict[str, str] = field(default_factory=dict)

    # keep APICredentials contract stable
    auth_type: str = field(default="api_key", init=False)
    api_key_header: str = field(default="bot-key", init=False)
    api_key_prefix: str = field(default="", init=False)

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> FasttrackCredentials:
        vm = vault_manager or get_default_manager()
        mount_point = get_env_code()
        secret = vm.get_secret(mount_point=mount_point, path=vault_path)
        return cls.from_dict(secret)

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> FasttrackCredentials:
        generic_token = resolve_generic_api_token(config, required=False, context="Fasttrack credentials")
        return cls(
            endpoint=str(
                config.get("dashboard_endpoint") or config.get("endpoint") or "https://dashboard.fstrk.io/api/partners"
            ),
            api_key=generic_token,
            dashboard_endpoint=str(
                config.get("dashboard_endpoint") or config.get("endpoint") or "https://dashboard.fstrk.io/api/partners"
            ),
            dashboard_bot_key=(
                str(config.get("dashboard_bot_key"))
                if config.get("dashboard_bot_key") is not None
                else (
                    generic_token
                    if generic_token is not None
                    else (str(config.get("bot_key")) if config.get("bot_key") is not None else None)
                )
            ),
            dashboard_extra_headers=dict(config.get("dashboard_extra_headers") or config.get("extra_headers") or {}),
            flex_endpoint=(str(config.get("flex_endpoint")) if config.get("flex_endpoint") is not None else None),
            flex_x_token=(
                str(config.get("flex_x_token"))
                if config.get("flex_x_token") is not None
                else (str(config.get("x_token")) if config.get("x_token") is not None else generic_token)
            ),
            flex_extra_headers=dict(config.get("flex_extra_headers") or {}),
        )

    def get_headers(self) -> dict[str, str]:
        return self.get_dashboard_headers()

    def get_dashboard_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        headers.update(self.dashboard_extra_headers)
        if self.dashboard_bot_key:
            headers["bot-key"] = self.dashboard_bot_key
        return headers

    def get_flex_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {"accept": "application/json"}
        headers.update(self.flex_extra_headers)
        if self.flex_x_token:
            headers["X-Token"] = self.flex_x_token
        return headers

    def validate_for_transport(self, transport: str) -> None:
        if transport == "dashboard_report":
            if not self.dashboard_endpoint or not self.dashboard_bot_key:
                raise ValueError("Fasttrack dashboard transport requires dashboard_endpoint and dashboard_bot_key")
        elif transport == "flex_ticket":
            if not self.flex_endpoint or not self.flex_x_token:
                raise ValueError("Fasttrack flex transport requires flex_endpoint and flex_x_token")
        else:  # pragma: no cover - defensive
            raise ValueError(f"Unknown Fasttrack transport '{transport}'")


class FasttrackConnector(AbstractAPIConnector):
    """Mixed-mode connector for Fasttrack pull APIs.

    FT-1 scope intentionally keeps landing side raw-ish:
    - CSV reports are parsed with vendor column names intact
    - JSON results are returned as-is (dict rows)
    - business normalisation is deferred to core layer
    """

    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=5.0, burst_limit=5)
    DEFAULT_TIMEOUT = 60

    def __init__(
        self,
        credentials: FasttrackCredentials,
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
    ) -> FasttrackConnector:
        credentials = FasttrackCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None and rate_limit_delay > 0:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay,
                burst_limit=5,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
        )

    def get_resource_specs(self) -> list[FasttrackResourceSpec]:
        return list_fasttrack_resources()

    def health_check(self) -> bool:
        try:
            if self.credentials.flex_endpoint and self.credentials.flex_x_token:
                self.fetch_resource_rows("flex_cms_ratings", page_size=1)
            else:
                self.fetch_resource_rows("cascade_transactions", limit=1)
            return True
        except Exception as exc:
            self.logger.warning("Fasttrack health_check failed: %s", exc)
            return False

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        params = dict(filters or {})
        params.update(kwargs)
        yield from self.iter_resource_rows(resource_name=resource_type, extra_params=params)

    def fetch_resource_rows(
        self,
        resource_name: str,
        **params: Any,
    ) -> list[dict[str, Any]]:
        return list(self.iter_resource_rows(resource_name=resource_name, extra_params=params))

    def iter_resource_rows(
        self,
        *,
        resource_name: str,
        extra_params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        spec = get_fasttrack_resource(resource_name)
        params = dict(spec.default_params)
        params.update(dict(extra_params or {}))
        if spec.transport == "dashboard_report":
            yield from self._iter_dashboard_rows(spec, params)
        elif spec.transport == "flex_ticket":
            yield from self._iter_flex_rows(spec, params)
        else:  # pragma: no cover - defensive
            raise ValueError(f"Unsupported Fasttrack transport '{spec.transport}'")

    def _iter_dashboard_rows(self, spec: FasttrackResourceSpec, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        self.credentials.validate_for_transport("dashboard_report")
        url = self._build_absolute_url(self.credentials.dashboard_endpoint, spec.endpoint_path)
        response = self._request_absolute(
            "GET", url, headers=self.credentials.get_dashboard_headers(), params=dict(params)
        )
        yield from self._parse_dashboard_csv_rows(self._decode_dashboard_content(response))

    def _iter_flex_rows(self, spec: FasttrackResourceSpec, params: Mapping[str, Any]) -> Iterator[dict[str, Any]]:
        self.credentials.validate_for_transport("flex_ticket")
        assert self.credentials.flex_endpoint is not None
        next_url = self._build_absolute_url(self.credentials.flex_endpoint, spec.endpoint_path)
        next_params: dict[str, Any] | None = dict(params)
        seen_urls: set[str] = set()
        headers = self.credentials.get_flex_headers()

        while next_url:
            if next_url in seen_urls:
                raise RuntimeError(f"Fasttrack flex pagination loop detected for {next_url}")
            seen_urls.add(next_url)

            response = self._request_absolute("GET", next_url, headers=headers, params=next_params)
            payload = response.json()
            results = payload.get("results") if isinstance(payload, Mapping) else None
            items = results if isinstance(results, list) else ([] if results is None else [results])
            for item in items:
                if isinstance(item, Mapping):
                    yield dict(item)

            next_link = payload.get("next") if isinstance(payload, Mapping) else None
            if not next_link:
                break
            next_url = urljoin(self.credentials.flex_endpoint.rstrip("/") + "/", str(next_link))
            next_params = None

    def _request_absolute(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
        json_data: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ):
        self._apply_rate_limit()
        response = self.session.request(
            method=method,
            url=url,
            headers=dict(headers or {}),
            params=dict(params or {}),
            json=dict(json_data or {}) if json_data is not None else None,
            timeout=self.timeout,
            **kwargs,
        )
        if not response.ok:
            self.logger.warning(
                "Fasttrack API error: %s %s URL=%s body=%s",
                response.status_code,
                response.reason,
                url,
                response.text[:500],
            )
        response.raise_for_status()
        return response

    @staticmethod
    def _build_absolute_url(base_url: str, endpoint_path: str) -> str:
        return urljoin(base_url.rstrip("/") + "/", endpoint_path.lstrip("/"))

    @staticmethod
    def _decode_dashboard_content(response: Any) -> str:
        raw = getattr(response, "content", b"")
        if isinstance(raw, bytes):
            if raw.startswith(b"\x1f\x8b"):
                raw = gzip.decompress(raw)
            for encoding in ("utf-8-sig", "utf-8"):
                try:
                    return raw.decode(encoding)
                except UnicodeDecodeError:
                    continue
        return response.text

    @staticmethod
    def _parse_dashboard_csv_rows(content: str, delimiter: str = ";") -> list[dict[str, Any]]:
        if not content.strip():
            return []
        reader = csv.DictReader(StringIO(content), delimiter=delimiter)
        rows: list[dict[str, Any]] = []
        for row in reader:
            rows.append({str(key): value for key, value in dict(row).items()})
        return rows


__all__ = [
    "FasttrackConnector",
    "FasttrackCredentials",
]
