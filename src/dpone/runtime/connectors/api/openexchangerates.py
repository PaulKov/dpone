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


from dpone._compat import UTC
from dpone.config.env import get_env_code
from dpone.runtime.connectors.api.base import (
    AbstractAPIConnector,
    APICredentials,
    APIRateLimitConfig,
    APIRetryConfig,
    resolve_generic_api_token,
)
from dpone.runtime.connectors.api.openexchangerates_resources import (
    OpenExchangeRatesResourceSpec,
    get_openexchangerates_resource,
    list_openexchangerates_resources,
)

logger = logging.getLogger(__name__)
DEFAULT_SYMBOLS: tuple[str, ...] = ("ARS", "RUB", "EUR")


def get_default_manager() -> VaultManager:
    try:
        from vault_kv_client import get_default_manager as load_default_manager
    except ImportError as exc:  # pragma: no cover - depends on optional runtime extra
        raise RuntimeError("vault-kv-client is not installed") from exc
    return load_default_manager()


def resolve_openexchangerates_date_option(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    if not raw:
        return None
    if raw.lower() == "today":
        return datetime.now(UTC).date()
    if raw.lower() == "yesterday":
        return datetime.now(UTC).date() - timedelta(days=1)
    return date.fromisoformat(raw[:10])


def normalize_openexchangerates_symbols(value: Any) -> tuple[str, ...]:
    if value is None:
        return DEFAULT_SYMBOLS
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",") if item.strip()]
    elif isinstance(value, list | tuple | set):
        items = [str(item).strip() for item in value if str(item).strip()]
    else:
        items = [str(value).strip()]

    resolved: list[str] = []
    seen: set[str] = set()
    for item in items:
        normalized = item.upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            resolved.append(normalized)
    return tuple(resolved or DEFAULT_SYMBOLS)


def _normalize_endpoint(endpoint: str) -> str:
    value = str(endpoint).strip().rstrip("/")
    if not value:
        raise ValueError("OpenExchangeRates endpoint must not be empty")
    if value.endswith("/api"):
        return value
    if value.endswith("/api/"):
        return value[:-1]
    return f"{value}/api"


def _resolve_openexchangerates_app_id(config: Mapping[str, Any]) -> str:
    app_id = config.get("app_id")
    if app_id is not None and str(app_id).strip():
        return str(app_id).strip()
    token = resolve_generic_api_token(config, required=False, context="OpenExchangeRates credentials")
    if token:
        return token
    raise KeyError("OpenExchangeRates credentials must define 'app_id' or a compatibility alias ('token'/'api_key')")


@dataclass
class OpenExchangeRatesCredentials(APICredentials):
    auth_type: str = field(default="query_param", init=False)
    app_id: str | None = None
    extra_params: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.endpoint = _normalize_endpoint(self.endpoint)

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
    ) -> OpenExchangeRatesCredentials:
        vm = vault_manager or get_default_manager()
        secret = vm.get_secret(mount_point=get_env_code(), path=vault_path)
        return cls(
            endpoint=str(secret["endpoint"]),
            app_id=_resolve_openexchangerates_app_id(secret),
            extra_headers=dict(secret.get("extra_headers") or {}),
            extra_params={str(k): str(v) for k, v in dict(secret.get("extra_params") or {}).items()},
        )

    @classmethod
    def from_dict(cls, config: Mapping[str, Any]) -> OpenExchangeRatesCredentials:
        return cls(
            endpoint=str(config["endpoint"]),
            app_id=_resolve_openexchangerates_app_id(config),
            extra_headers=dict(config.get("extra_headers") or {}),
            extra_params={str(k): str(v) for k, v in dict(config.get("extra_params") or {}).items()},
        )


class OpenExchangeRatesConnector(AbstractAPIConnector):
    DEFAULT_RATE_LIMIT = APIRateLimitConfig(requests_per_second=1.0, burst_limit=2)
    DEFAULT_TIMEOUT = 60

    def __init__(
        self,
        credentials: OpenExchangeRatesCredentials,
        retry_config: APIRetryConfig | None = None,
        rate_limit_config: APIRateLimitConfig | None = None,
        timeout: int = DEFAULT_TIMEOUT,
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
        retry_delay: float = 1.0,
        timeout: int = DEFAULT_TIMEOUT,
        **_: Any,
    ) -> OpenExchangeRatesConnector:
        credentials = OpenExchangeRatesCredentials.from_vault(vault_path=vault_path, vault_manager=vault_manager)
        retry_config = None
        if max_retries is not None:
            retry_config = APIRetryConfig(max_retries=max_retries, backoff_factor=retry_delay)
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

    def get_resource_specs(self) -> list[OpenExchangeRatesResourceSpec]:
        return list_openexchangerates_resources()

    def get_resource_names(self) -> list[str]:
        return [spec.name for spec in self.get_resource_specs()]

    def _authorized_params(self, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        authorized = dict(self.credentials.extra_params or {})
        if params:
            authorized.update(params)
        authorized["app_id"] = self.credentials.app_id
        return authorized

    def get_usage(self) -> dict[str, Any]:
        return self.get("usage.json", params=self._authorized_params())

    def health_check(self) -> bool:
        try:
            payload = self.get_usage()
            return isinstance(payload, dict)
        except Exception as exc:  # pragma: no cover - defensive, depends on external state
            self.logger.warning("OpenExchangeRates health_check failed: %s", exc)
            return False

    def fetch_historical_day(
        self,
        *,
        day: date,
        symbols: Any = None,
        base_currency: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "symbols": ",".join(normalize_openexchangerates_symbols(symbols)),
            "show_alternative": "false",
            "prettyprint": "false",
        }
        if base_currency:
            params["base"] = str(base_currency).strip().upper()

        payload = self.get(
            f"historical/{day.isoformat()}.json",
            params=self._authorized_params(params),
        )
        return self._normalize_historical_payload(
            as_of_date=day,
            payload=payload,
            requested_symbols=normalize_openexchangerates_symbols(symbols),
        )

    def get_resources(
        self,
        resource_type: str,
        filters: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        spec = get_openexchangerates_resource(resource_type)
        if spec.name != "historical_rates_daily":
            raise ValueError("OpenExchangeRates connector supports only resource='historical_rates_daily'")

        params = dict(filters or {})
        params.update(kwargs)
        symbols = params.pop("symbols", None)
        base_currency = params.pop("base_currency", None)
        day = resolve_openexchangerates_date_option(params.pop("day", None))
        start_date = resolve_openexchangerates_date_option(params.pop("start_date", None))
        end_date = resolve_openexchangerates_date_option(params.pop("end_date", None))

        if day is not None and (start_date is not None or end_date is not None):
            raise ValueError("Use either 'day' or 'start_date'/'end_date' for OpenExchangeRates requests")

        if start_date is None and day is None:
            end_date = end_date or (datetime.now(UTC).date() - timedelta(days=1))
            start_date = end_date

        if day is not None:
            yield from self.fetch_historical_day(day=day, symbols=symbols, base_currency=base_currency)
            return

        assert start_date is not None
        final_end = end_date or start_date
        if start_date > final_end:
            raise ValueError(f"OpenExchangeRates date range is invalid: {start_date} > {final_end}")

        current = start_date
        while current <= final_end:
            yield from self.fetch_historical_day(day=current, symbols=symbols, base_currency=base_currency)
            current += timedelta(days=1)

    @staticmethod
    def _normalize_historical_payload(
        *,
        as_of_date: date,
        payload: Mapping[str, Any],
        requested_symbols: tuple[str, ...],
    ) -> list[dict[str, Any]]:
        timestamp = payload.get("timestamp")
        provider_timestamp = datetime.fromtimestamp(int(timestamp), tz=UTC) if timestamp not in (None, "") else None
        base_currency = str(payload.get("base") or "").strip().upper() or None
        disclaimer = str(payload.get("disclaimer") or "").strip() or None
        license_text = str(payload.get("license") or "").strip() or None
        raw_rates = payload.get("rates") or {}
        if not isinstance(raw_rates, Mapping):
            raise ValueError("OpenExchangeRates response payload must contain an object field 'rates'")

        rows: list[dict[str, Any]] = []
        for symbol in requested_symbols:
            if symbol not in raw_rates:
                continue
            rows.append(
                {
                    "as_of_date": as_of_date.isoformat(),
                    "base_currency": base_currency,
                    "symbol": symbol,
                    "rate": float(raw_rates[symbol]),
                    "provider_timestamp_utc": provider_timestamp,
                    "disclaimer": disclaimer,
                    "license": license_text,
                }
            )
        return rows


__all__ = [
    "DEFAULT_SYMBOLS",
    "OpenExchangeRatesConnector",
    "OpenExchangeRatesCredentials",
    "OpenExchangeRatesResourceSpec",
    "normalize_openexchangerates_symbols",
    "resolve_openexchangerates_date_option",
]
