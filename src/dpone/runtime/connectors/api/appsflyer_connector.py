from __future__ import annotations

import logging
import re
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import TYPE_CHECKING, Any, Self

if TYPE_CHECKING:
    from vault_kv_client import VaultManager
else:
    VaultManager = Any


from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.appsflyer_resources import (
    AppsflyerResourceSpec,
    get_appsflyer_resource,
    list_appsflyer_resources,
)
from dpone.runtime.connectors.api.appsflyer_windowing_mixin import (
    AppsflyerQuotaExceededError,
    AppsflyerWindowingMixin,
)
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
    ConcurrencyConfig,
    resolve_generic_api_token,
)

logger = logging.getLogger(__name__)

_DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_NORMALIZE_RE = re.compile(r"[^a-z0-9_]+")
_CANONICAL_EXPORT_PATH = "/api/raw-data/export/app"
_QUOTA_EXCEEDED_RE = re.compile(
    r"(maximum number of install reports that can be downloaded today|limit reached for daily-report)",
    re.IGNORECASE,
)


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


@dataclass
class AppsflyerCredentials(APICredentials):
    """Креденшиалы AppsFlyer Pull API."""

    auth_type: str = "bearer"
    api_key_header: str = "Authorization"
    api_key_prefix: str = "Bearer"
    extra_params: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.endpoint = _normalize_appsflyer_endpoint(self.endpoint)

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
    ) -> AppsflyerCredentials:
        return cls._from_vault_dependencies(
            vault_path, vault_manager, env_resolver=get_env_code, manager_factory=get_default_manager
        )

    @classmethod
    def _from_vault_dependencies(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None,
        *,
        env_resolver: Callable[[], str],
        manager_factory: Callable[[], VaultManager],
    ) -> Self:
        """Resolve credentials using call-local dependencies, including facade resolvers."""
        vm = vault_manager or manager_factory()
        mount_point = env_resolver()
        secret = vm.get_secret(mount_point=mount_point, path=vault_path)
        return cls(
            endpoint=secret["endpoint"],
            api_key=resolve_generic_api_token(secret, context="AppsFlyer credentials"),
            extra_headers=dict(secret.get("extra_headers") or {}),
            extra_params=dict(secret.get("extra_params") or {}),
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> AppsflyerCredentials:
        return cls(
            endpoint=str(config["endpoint"]),
            api_key=resolve_generic_api_token(config, context="AppsFlyer credentials"),
            extra_headers=dict(config.get("extra_headers") or {}),
            extra_params=dict(config.get("extra_params") or {}),
        )


def _normalize_appsflyer_endpoint(endpoint: str) -> str:
    value = str(endpoint).strip().rstrip("/")
    if not value:
        raise ValueError("AppsFlyer endpoint must not be empty")
    if value.endswith(_CANONICAL_EXPORT_PATH):
        return value
    if value.endswith("/raw-data/export/app"):
        return value
    if value.endswith("/api/raw-data/export"):
        return f"{value}/app"
    if value.endswith("/raw-data/export"):
        return f"{value}/app"
    if value.endswith("/api/raw-data"):
        return f"{value}/export/app"
    if value.endswith("/raw-data"):
        return f"{value}/export/app"
    if value.endswith("/api"):
        return f"{value}/raw-data/export/app"
    return f"{value}{_CANONICAL_EXPORT_PATH}"


class AppsflyerConnector(AppsflyerWindowingMixin, AbstractAPIConnector):
    """Коннектор к AppsFlyer Raw Data Pull API V2."""

    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=1.0, burst_limit=3)
    ROW_LIMIT_SPLIT_RATIO = 0.98
    MIN_SPLIT_SECONDS = 60
    DEFAULT_MAXIMUM_ROWS = 1_000_000
    DEFAULT_TIMEOUT = 60

    def __init__(
        self,
        credentials: AppsflyerCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        concurrency_config: ConcurrencyConfig | None = None,
        timeout: int = DEFAULT_TIMEOUT,
        *,
        default_app_id: str | None = None,
        row_limit_split_ratio: float = ROW_LIMIT_SPLIT_RATIO,
    ) -> None:
        super().__init__(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config or self.DEFAULT_RATE_LIMIT,
            concurrency_config=concurrency_config or ConcurrencyConfig(),
            timeout=timeout,
        )
        self.default_app_id = default_app_id
        self.row_limit_split_ratio = row_limit_split_ratio

    @classmethod
    def from_vault(
        cls,
        vault_path: str,
        vault_manager: VaultManager | None = None,
        *,
        rate_limit_delay: float | None = None,
        max_retries: int | None = None,
        default_app_id: str | None = None,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> AppsflyerConnector:
        credentials = cls._credentials_from_vault(vault_path, vault_manager)
        retry_config = APIRetryConfig(max_retries=max_retries) if max_retries is not None else None
        rate_limit_config = None
        if rate_limit_delay is not None and rate_limit_delay > 0:
            rate_limit_config = APIRateLimitConfig(
                requests_per_second=1.0 / rate_limit_delay,
                burst_limit=3,
            )
        return cls(
            credentials=credentials,
            retry_config=retry_config,
            rate_limit_config=rate_limit_config,
            timeout=timeout,
            default_app_id=default_app_id,
        )

    @classmethod
    def _credentials_from_vault(cls, vault_path: str, vault_manager: VaultManager | None) -> AppsflyerCredentials:
        """Construct credentials without changing the canonical credential class."""
        return AppsflyerCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)

    def health_check(self) -> bool:
        app_id = self.default_app_id
        if not app_id:
            self.logger.warning("Appsflyer health_check skipped: default_app_id not configured")
            return False
        yesterday = date.today() - timedelta(days=1)
        try:
            self.fetch_resource_rows(
                resource_name="installs_report",
                app_id=app_id,
                from_value=yesterday,
                to_value=yesterday,
                maximum_rows=1000,
            )
            return True
        except AppsflyerQuotaExceededError as exc:  # pragma: no cover - external vendor state
            self.logger.warning("Appsflyer health_check skipped by vendor quota: %s", exc)
            return False
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning("Appsflyer health_check failed: %s", exc)
            return False

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs,
    ) -> Iterator[dict[str, Any]]:
        params = dict(filters or {})
        params.update(kwargs)
        app_id = params.pop("app_id", None) or self.default_app_id
        if not app_id:
            raise ValueError("Для AppsFlyer необходимо указать app_id")
        from_value = params.pop("from", None) or params.pop("from_value", None)
        to_value = params.pop("to", None) or params.pop("to_value", None)
        if from_value is None or to_value is None:
            raise ValueError("Для AppsFlyer необходимо указать both 'from' and 'to'")
        timezone_name = params.pop("timezone", None)
        maximum_rows = int(params.pop("maximum_rows", self.DEFAULT_MAXIMUM_ROWS))
        yield from self.iter_resource_rows(
            resource_name=resource_type,
            app_id=app_id,
            from_value=from_value,
            to_value=to_value,
            timezone_name=timezone_name,
            maximum_rows=maximum_rows,
            extra_params=params,
        )

    def get_resource_specs(self) -> list[AppsflyerResourceSpec]:
        return list_appsflyer_resources()

    def fetch_resource_rows(
        self,
        *,
        resource_name: str,
        app_id: str,
        from_value: Any,
        to_value: Any,
        timezone_name: str | None = None,
        maximum_rows: int = DEFAULT_MAXIMUM_ROWS,
        extra_params: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        return list(
            self.iter_resource_rows(
                resource_name=resource_name,
                app_id=app_id,
                from_value=from_value,
                to_value=to_value,
                timezone_name=timezone_name,
                maximum_rows=maximum_rows,
                extra_params=extra_params,
            )
        )

    def iter_resource_rows(
        self,
        *,
        resource_name: str,
        app_id: str,
        from_value: Any,
        to_value: Any,
        timezone_name: str | None = None,
        maximum_rows: int = DEFAULT_MAXIMUM_ROWS,
        extra_params: Mapping[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        spec = get_appsflyer_resource(resource_name)
        start_dt = self._coerce_from_value(from_value)
        end_dt = self._coerce_to_value(to_value)
        if start_dt > end_dt:
            raise ValueError("from must be <= to")
        yield from self._iter_window_rows(
            spec=spec,
            app_id=app_id,
            start_dt=start_dt,
            end_dt=end_dt,
            timezone_name=timezone_name,
            maximum_rows=maximum_rows,
            extra_params=extra_params,
        )


__all__ = [
    "AppsflyerCredentials",
    "AppsflyerConnector",
]
