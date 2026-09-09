from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import time as dt_time

import pytest
from tools.ci.vault_jwt_preflight import resolve_vault_auth_role, run_vault_jwt_preflight

from dpone._compat import UTC


@dataclass(frozen=True, slots=True)
class PostgresIntegrationSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    connect_timeout: float = 30.0
    poll_interval: float = 1.0


@dataclass(frozen=True, slots=True)
class ClickHouseIntegrationSettings:
    host: str
    port: int
    database: str
    user: str
    password: str
    secure: bool = False
    connect_timeout: float = 30.0
    poll_interval: float = 1.0


@dataclass(frozen=True, slots=True)
class MinioIntegrationSettings:
    host: str
    port: int
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False
    internal_url: str = "http://minio:9000"
    connect_timeout: float = 45.0
    poll_interval: float = 1.0

    @property
    def endpoint(self) -> str:
        return f"{self.host}:{self.port}"


TRUTHY = {"1", "true", "yes", "on"}
FALSEY = {"0", "false", "no", "off"}
YANDEX_WEBMASTER_ENV_ALIASES: dict[str, tuple[str, ...]] = {
    "SELECTORS": ("DPONE_IT_YANDEX_WEBMASTER_SELECTORS", "DPONE_IT_YWM_SELECTORS"),
    "RESOURCE": ("DPONE_IT_YANDEX_WEBMASTER_RESOURCE", "DPONE_IT_YWM_RESOURCE"),
    "HOST_ID": ("DPONE_IT_YANDEX_WEBMASTER_HOST_ID", "DPONE_IT_YWM_HOST_ID"),
    "HOST_URL": ("DPONE_IT_YANDEX_WEBMASTER_HOST_URL", "DPONE_IT_YWM_HOST_URL"),
    "USER_ID": ("DPONE_IT_YANDEX_WEBMASTER_USER_ID", "DPONE_IT_YWM_USER_ID"),
    "DAYS_BACK": ("DPONE_IT_YANDEX_WEBMASTER_DAYS_BACK", "DPONE_IT_YWM_DAYS_BACK"),
    "VAULT_PATH": ("DPONE_IT_YANDEX_WEBMASTER_VAULT_PATH", "DPONE_IT_YWM_VAULT_PATH"),
    "DEVICE_TYPES": ("DPONE_IT_YANDEX_WEBMASTER_DEVICE_TYPES", "DPONE_IT_YWM_DEVICE_TYPES"),
    "REGION_IDS": ("DPONE_IT_YANDEX_WEBMASTER_REGION_IDS", "DPONE_IT_YWM_REGION_IDS"),
    "DAY": ("DPONE_IT_YANDEX_WEBMASTER_DAY", "DPONE_IT_YWM_DAY"),
    "DATE_FROM": ("DPONE_IT_YANDEX_WEBMASTER_DATE_FROM", "DPONE_IT_YWM_DATE_FROM"),
    "DATE_TO": ("DPONE_IT_YANDEX_WEBMASTER_DATE_TO", "DPONE_IT_YWM_DATE_TO"),
    "TIMEOUT": ("DPONE_IT_YANDEX_WEBMASTER_TIMEOUT", "DPONE_IT_YWM_TIMEOUT"),
    "MAX_RETRIES": ("DPONE_IT_YANDEX_WEBMASTER_MAX_RETRIES", "DPONE_IT_YWM_MAX_RETRIES"),
    "RATE_LIMIT_DELAY": (
        "DPONE_IT_YANDEX_WEBMASTER_RATE_LIMIT_DELAY",
        "DPONE_IT_YWM_RATE_LIMIT_DELAY",
    ),
    "PAGES_IN_SEARCH_DAILY_AGG": (
        "DPONE_IT_YANDEX_WEBMASTER_PAGES_IN_SEARCH_DAILY_AGG",
        "DPONE_IT_YWM_PAGES_IN_SEARCH_DAILY_AGG",
    ),
}


def integration_enabled(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    return str(source.get("DPONE_RUN_INTEGRATION", "0")).strip().lower() in TRUTHY


def integration_extended_enabled(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    return (
        integration_enabled(source) and str(source.get("DPONE_RUN_INTEGRATION_EXTENDED", "0")).strip().lower() in TRUTHY
    )


def require_integration() -> None:
    if not integration_enabled():
        pytest.skip(
            "Integration tests are disabled. Set DPONE_RUN_INTEGRATION=1 to enable them.",
            allow_module_level=True,
        )


def require_extended_integration() -> None:
    require_integration()
    if not integration_extended_enabled():
        pytest.skip(
            "Extended integration tests are disabled. Set DPONE_RUN_INTEGRATION_EXTENDED=1 to enable them.",
            allow_module_level=True,
        )


def read_postgres_integration_settings(env: Mapping[str, str] | None = None) -> PostgresIntegrationSettings:
    source = env or os.environ
    return PostgresIntegrationSettings(
        host=source.get("DPONE_IT_PG_HOST", "127.0.0.1"),
        port=int(source.get("DPONE_IT_PG_PORT", "55432")),
        database=source.get("DPONE_IT_PG_DATABASE", "dpone_it"),
        user=source.get("DPONE_IT_PG_USER", "dpone"),
        password=source.get("DPONE_IT_PG_PASSWORD", "dpone"),
        connect_timeout=float(source.get("DPONE_IT_PG_CONNECT_TIMEOUT", "30")),
        poll_interval=float(source.get("DPONE_IT_PG_POLL_INTERVAL", "1")),
    )


def read_clickhouse_integration_settings(env: Mapping[str, str] | None = None) -> ClickHouseIntegrationSettings:
    source = env or os.environ
    return ClickHouseIntegrationSettings(
        host=source.get("DPONE_IT_CH_HOST", "127.0.0.1"),
        port=int(source.get("DPONE_IT_CH_PORT", "59000")),
        database=source.get("DPONE_IT_CH_DATABASE", "dpone_it"),
        user=source.get("DPONE_IT_CH_USER", "default"),
        password=source.get("DPONE_IT_CH_PASSWORD", "dpone"),
        secure=str(source.get("DPONE_IT_CH_SECURE", "0")).strip().lower() in TRUTHY,
        connect_timeout=float(source.get("DPONE_IT_CH_CONNECT_TIMEOUT", "60")),
        poll_interval=float(source.get("DPONE_IT_CH_POLL_INTERVAL", "1")),
    )


def read_minio_integration_settings(env: Mapping[str, str] | None = None) -> MinioIntegrationSettings:
    source = env or os.environ
    return MinioIntegrationSettings(
        host=source.get("DPONE_IT_MINIO_HOST", "127.0.0.1"),
        port=int(source.get("DPONE_IT_MINIO_PORT", "59090")),
        access_key=source.get("DPONE_IT_MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=source.get("DPONE_IT_MINIO_SECRET_KEY", "minioadmin"),
        bucket=source.get("DPONE_IT_MINIO_BUCKET", "dpone-it"),
        secure=str(source.get("DPONE_IT_MINIO_SECURE", "0")).strip().lower() in TRUTHY,
        internal_url=source.get("DPONE_IT_MINIO_INTERNAL_URL", "http://minio:9000"),
        connect_timeout=float(source.get("DPONE_IT_MINIO_CONNECT_TIMEOUT", "60")),
        poll_interval=float(source.get("DPONE_IT_MINIO_POLL_INTERVAL", "1")),
    )


@pytest.fixture(scope="session")
def postgres_settings() -> PostgresIntegrationSettings:
    return read_postgres_integration_settings()


@pytest.fixture(scope="session")
def clickhouse_settings() -> ClickHouseIntegrationSettings:
    return read_clickhouse_integration_settings()


@pytest.fixture(scope="session")
def minio_settings() -> MinioIntegrationSettings:
    return read_minio_integration_settings()


@pytest.fixture(scope="session")
def postgres_connector(postgres_settings: PostgresIntegrationSettings):
    require_integration()
    pytest.importorskip("psycopg")
    from dpone.runtime.connectors.postgres import PostgresConnector

    deadline = time.monotonic() + postgres_settings.connect_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            probe = PostgresConnector(
                host=postgres_settings.host,
                port=postgres_settings.port,
                database=postgres_settings.database,
                user=postgres_settings.user,
                password=postgres_settings.password,
                application_name="dpone-integration-probe",
            )
            probe.execute_query("SELECT 1")
            probe.close()
            break
        except Exception as exc:  # pragma: no cover - external service bootstrap
            last_error = exc
            time.sleep(postgres_settings.poll_interval)
    else:
        raise RuntimeError(f"PostgreSQL integration service did not become ready: {last_error}")

    connector = PostgresConnector(
        host=postgres_settings.host,
        port=postgres_settings.port,
        database=postgres_settings.database,
        user=postgres_settings.user,
        password=postgres_settings.password,
        application_name="dpone-integration-tests",
    )
    try:
        yield connector
    finally:
        connector.close()


@pytest.fixture(scope="session")
def clickhouse_connector(clickhouse_settings: ClickHouseIntegrationSettings):
    require_integration()
    pytest.importorskip("clickhouse_driver")
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    deadline = time.monotonic() + clickhouse_settings.connect_timeout
    last_error: Exception | None = None
    connector = None
    while time.monotonic() < deadline:
        try:
            bootstrap = ClickHouseConnector(
                host=clickhouse_settings.host,
                port=clickhouse_settings.port,
                database="default",
                user=clickhouse_settings.user,
                password=clickhouse_settings.password,
                secure=clickhouse_settings.secure,
                application_name="dpone-integration-bootstrap",
            )
            bootstrap.execute_query("SELECT 1")
            bootstrap.execute_query(f"CREATE DATABASE IF NOT EXISTS `{clickhouse_settings.database}`")
            bootstrap.close()

            connector = ClickHouseConnector(
                host=clickhouse_settings.host,
                port=clickhouse_settings.port,
                database=clickhouse_settings.database,
                user=clickhouse_settings.user,
                password=clickhouse_settings.password,
                secure=clickhouse_settings.secure,
                application_name="dpone-integration-tests",
            )
            connector.execute_query("SELECT 1")
            break
        except Exception as exc:  # pragma: no cover - external service bootstrap
            last_error = exc
            if connector is not None:
                connector.close()
                connector = None
            time.sleep(clickhouse_settings.poll_interval)
    else:
        raise RuntimeError(f"ClickHouse integration service did not become ready: {last_error}")

    try:
        yield connector
    finally:
        if connector is not None:
            connector.close()


@pytest.fixture(scope="session")
def minio_client(minio_settings: MinioIntegrationSettings):
    require_extended_integration()
    pytest.importorskip("minio")
    from minio import Minio

    client = Minio(
        minio_settings.endpoint,
        access_key=minio_settings.access_key,
        secret_key=minio_settings.secret_key,
        secure=minio_settings.secure,
    )

    deadline = time.monotonic() + minio_settings.connect_timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client.list_buckets()
            break
        except Exception as exc:  # pragma: no cover - external service bootstrap
            last_error = exc
            time.sleep(minio_settings.poll_interval)
    else:
        raise RuntimeError(f"MinIO integration service did not become ready: {last_error}")

    if not client.bucket_exists(minio_settings.bucket):
        client.make_bucket(minio_settings.bucket)

    return client


@pytest.fixture
def postgres_schema(postgres_connector) -> Iterator[str]:
    pytest.importorskip("psycopg")
    from psycopg import sql

    schema = f"it_{uuid.uuid4().hex[:10]}"
    postgres_connector.execute_query(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield schema
    finally:
        postgres_connector.execute_query(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))


@dataclass(frozen=True, slots=True)
class AppsflyerLiveIntegrationSettings:
    vault_path: str
    app_ids: tuple[str, ...]
    resource: str = "installs_report"
    timezone: str = "Europe/Moscow"
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 2
    maximum_rows: int = 10000

    @property
    def default_app_id(self) -> str:
        return self.app_ids[0]


def integration_live_enabled(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    return integration_enabled(source) and str(source.get("DPONE_RUN_INTEGRATION_LIVE", "0")).strip().lower() in TRUTHY


def require_live_integration() -> None:
    require_integration()
    if not integration_live_enabled():
        pytest.skip(
            "Live integration tests are disabled. Set DPONE_RUN_INTEGRATION_LIVE=1 to enable them.",
            allow_module_level=True,
        )


def _env_value(source: Mapping[str, str] | None, name: str) -> str:
    return str((source or os.environ).get(name, "")).strip()


def _vault_addr(env: Mapping[str, str] | None = None) -> str:
    source = env or os.environ
    return _env_value(source, "VAULT_ADDR") or _env_value(source, "VAULT_SERVER_URL")


def _vault_jwt_value(env: Mapping[str, str] | None = None) -> str:
    source = env or os.environ
    jwt = _env_value(source, "VAULT_JWT")
    if jwt:
        return jwt

    jwt_env_var_name = _env_value(source, "VAULT_JWT_ENV_VAR")
    if jwt_env_var_name:
        jwt = _env_value(source, jwt_env_var_name)
        if jwt:
            return jwt

    return _env_value(source, "VAULT_ID_TOKEN")


def _vault_jwt_requested(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    if not _vault_addr(source):
        return False

    auth_method = _env_value(source, "VAULT_AUTH_METHOD").lower()
    role_hint = resolve_vault_auth_role(source)
    jwt_env_var_name = _env_value(source, "VAULT_JWT_ENV_VAR")
    jwt_hints = (
        auth_method == "jwt"
        or bool(role_hint)
        or bool(_env_value(source, "VAULT_AUTH_PATH"))
        or bool(_env_value(source, "VAULT_ID_TOKEN"))
        or bool(_env_value(source, "VAULT_JWT"))
        or bool(jwt_env_var_name)
        or bool(_env_value(source, "VAULT_JWT_FILE"))
    )
    return jwt_hints


def vault_runtime_configured(env: Mapping[str, str] | None = None) -> bool:
    source = env or os.environ
    has_addr = bool(_vault_addr(source))
    has_token = bool(_env_value(source, "VAULT_TOKEN"))
    has_approle = bool(_env_value(source, "VAULT_ROLE_ID")) and bool(_env_value(source, "VAULT_SECRET_ID"))
    return has_addr and (has_token or has_approle or _vault_jwt_requested(source))


def ensure_vault_runtime_auth(env: MutableMapping[str, str] | None = None) -> None:
    source = env or os.environ
    if _env_value(source, "VAULT_TOKEN"):
        return
    if _env_value(source, "VAULT_ROLE_ID") and _env_value(source, "VAULT_SECRET_ID"):
        return
    if not _vault_jwt_requested(source):
        return

    if not _vault_addr(source):
        raise RuntimeError("Vault JWT auth is configured but VAULT_ADDR or VAULT_SERVER_URL is missing.")

    role = resolve_vault_auth_role(source)
    if not role:
        raise RuntimeError(
            "Vault JWT auth is configured but no role was resolved. "
            "Set VAULT_AUTH_ROLE or VAULT_AUTH_ROLE_DEV/VAULT_AUTH_ROLE_PROD."
        )

    if not _vault_jwt_value(source) and not _env_value(source, "VAULT_JWT_FILE"):
        raise RuntimeError(
            "Vault JWT auth is configured but no JWT source was found. "
            "Set VAULT_JWT, VAULT_JWT_ENV_VAR, VAULT_ID_TOKEN, or VAULT_JWT_FILE."
        )

    source["VAULT_AUTH_ROLE"] = role
    run_vault_jwt_preflight(source)


def require_vault_live_integration() -> None:
    require_live_integration()
    if not vault_runtime_configured():
        pytest.skip(
            "Vault-backed live integration tests are disabled. Set VAULT_ADDR or VAULT_SERVER_URL and one of: "
            "VAULT_TOKEN; VAULT_ROLE_ID+VAULT_SECRET_ID; or Vault JWT auth via "
            "VAULT_AUTH_METHOD=jwt plus VAULT_AUTH_ROLE or VAULT_AUTH_ROLE_DEV/VAULT_AUTH_ROLE_PROD.",
            allow_module_level=True,
        )
    ensure_vault_runtime_auth()


def read_appsflyer_live_integration_settings(env: Mapping[str, str] | None = None) -> AppsflyerLiveIntegrationSettings:
    source = env or os.environ
    raw_app_ids = str(source.get("DPONE_IT_AF_APP_IDS", "")).strip()
    app_ids = tuple(item.strip() for item in raw_app_ids.split(",") if item.strip())
    if not app_ids:
        raise ValueError("DPONE_IT_AF_APP_IDS must contain at least one AppsFlyer app id for live integration")
    return AppsflyerLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_AF_VAULT_PATH", "api/appsflyer")).strip() or "api/appsflyer",
        app_ids=app_ids,
        resource=str(source.get("DPONE_IT_AF_RESOURCE", "installs_report")).strip() or "installs_report",
        timezone=str(source.get("DPONE_IT_AF_TIMEZONE", "Europe/Moscow")).strip() or "Europe/Moscow",
        date_from=(str(source.get("DPONE_IT_AF_DATE_FROM", "")).strip() or None),
        date_to=(str(source.get("DPONE_IT_AF_DATE_TO", "")).strip() or None),
        days_back=int(source.get("DPONE_IT_AF_DAYS_BACK", "2")),
        maximum_rows=int(source.get("DPONE_IT_AF_MAXIMUM_ROWS", "10000")),
    )


@pytest.fixture(scope="session")
def appsflyer_live_settings() -> AppsflyerLiveIntegrationSettings:
    require_vault_live_integration()
    return read_appsflyer_live_integration_settings()


@dataclass(frozen=True, slots=True)
class MindboxLiveIntegrationSettings:
    vault_path: str
    resource: str = "getactions"
    since_datetime_utc: str | None = None
    till_datetime_utc: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 1
    poll_interval: int = 30
    export_timeout: int = 3000
    batch_size: int = 1000
    utc_boundary_time: str = "21:00:00"


def _default_mindbox_hour_window_utc() -> tuple[str, str]:
    yesterday_utc = datetime.now(UTC).date() - timedelta(days=1)
    since = datetime.combine(yesterday_utc, dt_time(hour=12), tzinfo=UTC)
    till = since + timedelta(hours=1)
    return since.strftime("%Y-%m-%dT%H:%M:%SZ"), till.strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass(frozen=True, slots=True)
class SimilarwebLiveIntegrationSettings:
    vault_path: str
    domains: tuple[str, ...]
    resource: str = "keywords"
    selectors: tuple[str, ...] = ("default.keywords",)
    snapshot_month: str | None = None
    limit: int = 50
    page_size: int = 50
    min_keywords_count: int = 1
    traffic_source: str = "Organic"
    web_source: str = "Total"
    branded_type: str = "All"
    country: str = "world"
    timeout: int = 60
    max_retries: int = 1
    rate_limit_delay: float = 1.0

    @property
    def default_domain(self) -> str:
        return self.domains[0]


@dataclass(frozen=True, slots=True)
class CbrLiveIntegrationSettings:
    resource: str = "xml_daily_asp"
    day: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 3
    timeout: int = 60
    retries: int = 3
    retry_delay: float = 2.0


@dataclass(frozen=True, slots=True)
class OpenExchangeRatesLiveIntegrationSettings:
    vault_path: str
    resource: str = "historical_rates_daily"
    selectors: tuple[str, ...] = ("default.historical_rates_daily",)
    symbols: tuple[str, ...] = ("ARS", "RUB", "EUR")
    day: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 1
    timeout: int = 60
    max_retries: int = 1
    retry_delay: float = 1.0
    rate_limit_delay: float = 1.0


@dataclass(frozen=True, slots=True)
class GoogleSheetsLiveIntegrationSettings:
    vault_path: str
    resource: str = "worksheet_rows"
    selectors: tuple[str, ...] = ("app.worksheet_rows",)
    spreadsheet_id: str | None = None
    spreadsheet_url: str | None = None
    worksheet_title: str | None = None
    worksheet_index: int | None = None
    range_name: str | None = None
    header_row: int = 1
    skip_rows: int = 0
    add_metadata_columns: bool = True
    timeout: int = 60
    max_retries: int = 1
    rate_limit_delay: float = 0.2


@dataclass(frozen=True, slots=True)
class GoogleAdsLiveIntegrationSettings:
    vault_path: str
    resource: str = "ads_stats"
    selectors: tuple[str, ...] = ("app.ads_stats",)
    customer_ids: tuple[str, ...] = ()
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 3
    timeout: int = 60
    max_retries: int = 1
    rate_limit_delay: float = 1.0


@dataclass(frozen=True, slots=True)
class YandexWebmasterLiveIntegrationSettings:
    vault_path: str
    resource: str = "host_metrics_daily"
    selectors: tuple[str, ...] = ("app.host_metrics_daily",)
    user_id: int | None = None
    host_id: str | None = None
    host_url: str | None = None
    device_types: tuple[str, ...] = ()
    region_ids: tuple[int, ...] = ()
    day: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    days_back: int = 3
    timeout: int = 60
    max_retries: int = 1
    rate_limit_delay: float = 0.5
    pages_in_search_daily_agg: str = "last"


def read_mindbox_live_integration_settings(env: Mapping[str, str] | None = None) -> MindboxLiveIntegrationSettings:
    source = env or os.environ
    since_datetime_utc = str(source.get("DPONE_IT_MB_SINCE_DATETIME_UTC", "")).strip() or None
    till_datetime_utc = str(source.get("DPONE_IT_MB_TILL_DATETIME_UTC", "")).strip() or None
    date_from = str(source.get("DPONE_IT_MB_DATE_FROM", "")).strip() or None
    date_to = str(source.get("DPONE_IT_MB_DATE_TO", "")).strip() or None
    days_back_raw = str(source.get("DPONE_IT_MB_DAYS_BACK", "")).strip()
    if not since_datetime_utc and not till_datetime_utc and not date_from and not date_to and not days_back_raw:
        since_datetime_utc, till_datetime_utc = _default_mindbox_hour_window_utc()
    return MindboxLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_MB_VAULT_PATH", "api/mindbox")).strip() or "api/mindbox",
        resource=str(source.get("DPONE_IT_MB_RESOURCE", "getactions")).strip() or "getactions",
        since_datetime_utc=since_datetime_utc,
        till_datetime_utc=till_datetime_utc,
        date_from=date_from,
        date_to=date_to,
        days_back=int(days_back_raw or "1"),
        poll_interval=int(source.get("DPONE_IT_MB_POLL_INTERVAL", "30")),
        export_timeout=int(source.get("DPONE_IT_MB_EXPORT_TIMEOUT", "3000")),
        batch_size=int(source.get("DPONE_IT_MB_BATCH_SIZE", "1000")),
        utc_boundary_time=str(source.get("DPONE_IT_MB_UTC_BOUNDARY_TIME", "21:00:00")).strip() or "21:00:00",
    )


def _parse_csv_env(source: Mapping[str, str], name: str) -> tuple[str, ...]:
    raw = str(source.get(name, "")).strip()
    if not raw:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _parse_int_csv_env(source: Mapping[str, str], name: str) -> tuple[int, ...]:
    raw = str(source.get(name, "")).strip()
    if not raw:
        return ()
    return tuple(int(item.strip()) for item in raw.split(",") if item.strip())


def _read_env_alias(source: Mapping[str, str], aliases: tuple[str, ...], default: str = "") -> str:
    for env_name in aliases:
        value = source.get(env_name)
        if value is not None:
            return str(value).strip()
    return default


def _parse_csv_env_alias(source: Mapping[str, str], aliases: tuple[str, ...]) -> tuple[str, ...]:
    raw = _read_env_alias(source, aliases)
    if not raw:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _parse_int_csv_env_alias(source: Mapping[str, str], aliases: tuple[str, ...]) -> tuple[int, ...]:
    raw = _read_env_alias(source, aliases)
    if not raw:
        return ()
    return tuple(int(item.strip()) for item in raw.split(",") if item.strip())


def _yandex_webmaster_resource_from_selectors(selectors: tuple[str, ...]) -> str:
    if not selectors:
        return "host_metrics_daily"
    first = selectors[0].strip()
    return first.split(".", 1)[1] if "." in first else first


def _default_yandex_webmaster_days_back(resource: str) -> int:
    if resource in {"search_queries_history_daily", "query_analytics_by_region_daily"}:
        return 14
    return 3


def read_similarweb_live_integration_settings(
    env: Mapping[str, str] | None = None,
) -> SimilarwebLiveIntegrationSettings:
    source = env or os.environ
    domains = _parse_csv_env(source, "DPONE_IT_SW_DOMAINS")
    if not domains:
        domains = ("travel.example.com",)

    limit = int(source.get("DPONE_IT_SW_LIMIT", "5"))
    page_size_raw = str(source.get("DPONE_IT_SW_PAGE_SIZE", "")).strip()

    return SimilarwebLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_SW_VAULT_PATH", "api/similarweb")).strip() or "api/similarweb",
        domains=domains,
        resource=str(source.get("DPONE_IT_SW_RESOURCE", "keywords")).strip() or "keywords",
        selectors=_parse_csv_env(source, "DPONE_IT_SW_SELECTORS") or ("default.keywords",),
        snapshot_month=(str(source.get("DPONE_IT_SW_SNAPSHOT_MONTH", "")).strip() or None),
        limit=limit,
        page_size=int(page_size_raw) if page_size_raw else limit,
        min_keywords_count=int(source.get("DPONE_IT_SW_MIN_KEYWORDS_COUNT", "1")),
        traffic_source=str(source.get("DPONE_IT_SW_TRAFFIC_SOURCE", "Organic")).strip() or "Organic",
        web_source=str(source.get("DPONE_IT_SW_WEB_SOURCE", "Total")).strip() or "Total",
        branded_type=str(source.get("DPONE_IT_SW_BRANDED_TYPE", "All")).strip() or "All",
        country=str(source.get("DPONE_IT_SW_COUNTRY", "world")).strip() or "world",
        timeout=int(source.get("DPONE_IT_SW_TIMEOUT", "60")),
        max_retries=int(source.get("DPONE_IT_SW_MAX_RETRIES", "1")),
        rate_limit_delay=float(source.get("DPONE_IT_SW_RATE_LIMIT_DELAY", "1.0")),
    )


def read_google_sheets_live_integration_settings(
    env: Mapping[str, str] | None = None,
) -> GoogleSheetsLiveIntegrationSettings:
    source = env or os.environ
    spreadsheet_id = str(source.get("DPONE_IT_GS_SPREADSHEET_ID", "")).strip() or None
    spreadsheet_url = str(source.get("DPONE_IT_GS_SPREADSHEET_URL", "")).strip() or None
    if not spreadsheet_id and not spreadsheet_url:
        raise ValueError(
            "Google Sheets live integration requires DPONE_IT_GS_SPREADSHEET_ID or DPONE_IT_GS_SPREADSHEET_URL"
        )

    worksheet_index_raw = str(source.get("DPONE_IT_GS_WORKSHEET_INDEX", "")).strip()
    return GoogleSheetsLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_GS_VAULT_PATH", "api/google_sheets")).strip() or "api/google_sheets",
        resource=str(source.get("DPONE_IT_GS_RESOURCE", "worksheet_rows")).strip() or "worksheet_rows",
        selectors=_parse_csv_env(source, "DPONE_IT_GS_SELECTORS") or ("app.worksheet_rows",),
        spreadsheet_id=spreadsheet_id,
        spreadsheet_url=spreadsheet_url,
        worksheet_title=str(source.get("DPONE_IT_GS_WORKSHEET_TITLE", "")).strip() or None,
        worksheet_index=int(worksheet_index_raw) if worksheet_index_raw else None,
        range_name=str(source.get("DPONE_IT_GS_RANGE_NAME", "")).strip() or None,
        header_row=int(source.get("DPONE_IT_GS_HEADER_ROW", "1")),
        skip_rows=int(source.get("DPONE_IT_GS_SKIP_ROWS", "0")),
        add_metadata_columns=str(source.get("DPONE_IT_GS_ADD_METADATA_COLUMNS", "1")).strip().lower() in TRUTHY,
        timeout=int(source.get("DPONE_IT_GS_TIMEOUT", "60")),
        max_retries=int(source.get("DPONE_IT_GS_MAX_RETRIES", "1")),
        rate_limit_delay=float(source.get("DPONE_IT_GS_RATE_LIMIT_DELAY", "0.2")),
    )


def read_google_ads_live_integration_settings(
    env: Mapping[str, str] | None = None,
) -> GoogleAdsLiveIntegrationSettings:
    source = env or os.environ
    return GoogleAdsLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_GA_VAULT_PATH", "api/google_ads")).strip() or "api/google_ads",
        resource=str(source.get("DPONE_IT_GA_RESOURCE", "ads_stats")).strip() or "ads_stats",
        selectors=_parse_csv_env(source, "DPONE_IT_GA_SELECTORS") or ("app.ads_stats",),
        customer_ids=_parse_csv_env(source, "DPONE_IT_GA_CUSTOMER_IDS"),
        date_from=(str(source.get("DPONE_IT_GA_DATE_FROM", "")).strip() or None),
        date_to=(str(source.get("DPONE_IT_GA_DATE_TO", "")).strip() or None),
        days_back=int(source.get("DPONE_IT_GA_DAYS_BACK", "3")),
        timeout=int(source.get("DPONE_IT_GA_TIMEOUT", "60")),
        max_retries=int(source.get("DPONE_IT_GA_MAX_RETRIES", "1")),
        rate_limit_delay=float(source.get("DPONE_IT_GA_RATE_LIMIT_DELAY", "1.0")),
    )


def read_yandex_webmaster_live_integration_settings(
    env: Mapping[str, str] | None = None,
) -> YandexWebmasterLiveIntegrationSettings:
    source = env or os.environ
    selectors = _parse_csv_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["SELECTORS"]) or ("app.host_metrics_daily",)
    resource = _read_env_alias(
        source, YANDEX_WEBMASTER_ENV_ALIASES["RESOURCE"]
    ) or _yandex_webmaster_resource_from_selectors(selectors)
    host_id = _read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["HOST_ID"]) or "https:travel.example.com:443"
    host_url = _read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["HOST_URL"]) or None

    user_id_raw = _read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["USER_ID"])
    days_back_raw = _read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["DAYS_BACK"])
    return YandexWebmasterLiveIntegrationSettings(
        vault_path=_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["VAULT_PATH"], "api/yandex_webmaster")
        or "api/yandex_webmaster",
        resource=resource,
        selectors=selectors,
        user_id=int(user_id_raw) if user_id_raw else None,
        host_id=host_id,
        host_url=host_url,
        device_types=_parse_csv_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["DEVICE_TYPES"]),
        region_ids=_parse_int_csv_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["REGION_IDS"]),
        day=_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["DAY"]) or None,
        date_from=_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["DATE_FROM"]) or None,
        date_to=_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["DATE_TO"]) or None,
        days_back=int(days_back_raw or str(_default_yandex_webmaster_days_back(resource))),
        timeout=int(_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["TIMEOUT"], "60")),
        max_retries=int(_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["MAX_RETRIES"], "1")),
        rate_limit_delay=float(_read_env_alias(source, YANDEX_WEBMASTER_ENV_ALIASES["RATE_LIMIT_DELAY"], "0.5")),
        pages_in_search_daily_agg=_read_env_alias(
            source,
            YANDEX_WEBMASTER_ENV_ALIASES["PAGES_IN_SEARCH_DAILY_AGG"],
            "last",
        )
        or "last",
    )


def read_cbr_live_integration_settings(env: Mapping[str, str] | None = None) -> CbrLiveIntegrationSettings:
    source = env or os.environ
    return CbrLiveIntegrationSettings(
        resource=str(source.get("DPONE_IT_CBR_RESOURCE", "xml_daily_asp")).strip() or "xml_daily_asp",
        day=(str(source.get("DPONE_IT_CBR_DAY", "")).strip() or None),
        date_from=(str(source.get("DPONE_IT_CBR_DATE_FROM", "")).strip() or None),
        date_to=(str(source.get("DPONE_IT_CBR_DATE_TO", "")).strip() or None),
        days_back=int(source.get("DPONE_IT_CBR_DAYS_BACK", "3")),
        timeout=int(source.get("DPONE_IT_CBR_TIMEOUT", "60")),
        retries=int(source.get("DPONE_IT_CBR_RETRIES", "3")),
        retry_delay=float(source.get("DPONE_IT_CBR_RETRY_DELAY", "2.0")),
    )


def read_openexchangerates_live_integration_settings(
    env: Mapping[str, str] | None = None,
) -> OpenExchangeRatesLiveIntegrationSettings:
    source = env or os.environ
    symbols = _parse_csv_env(source, "DPONE_IT_OXR_SYMBOLS")
    return OpenExchangeRatesLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_OXR_VAULT_PATH", "api/openexchangerates")).strip()
        or "api/openexchangerates",
        resource=str(source.get("DPONE_IT_OXR_RESOURCE", "historical_rates_daily")).strip() or "historical_rates_daily",
        selectors=_parse_csv_env(source, "DPONE_IT_OXR_SELECTORS") or ("default.historical_rates_daily",),
        symbols=symbols or ("ARS", "RUB", "EUR"),
        day=(str(source.get("DPONE_IT_OXR_DAY", "")).strip() or None),
        date_from=(str(source.get("DPONE_IT_OXR_DATE_FROM", "")).strip() or None),
        date_to=(str(source.get("DPONE_IT_OXR_DATE_TO", "")).strip() or None),
        days_back=int(source.get("DPONE_IT_OXR_DAYS_BACK", "1")),
        timeout=int(source.get("DPONE_IT_OXR_TIMEOUT", "60")),
        max_retries=int(source.get("DPONE_IT_OXR_MAX_RETRIES", "1")),
        retry_delay=float(source.get("DPONE_IT_OXR_RETRY_DELAY", "1.0")),
        rate_limit_delay=float(source.get("DPONE_IT_OXR_RATE_LIMIT_DELAY", "1.0")),
    )


@pytest.fixture(scope="session")
def mindbox_live_settings() -> MindboxLiveIntegrationSettings:
    require_vault_live_integration()
    return read_mindbox_live_integration_settings()


@pytest.fixture(scope="session")
def similarweb_live_settings() -> SimilarwebLiveIntegrationSettings:
    require_vault_live_integration()
    return read_similarweb_live_integration_settings()


@pytest.fixture(scope="session")
def google_sheets_live_settings() -> GoogleSheetsLiveIntegrationSettings:
    require_vault_live_integration()
    return read_google_sheets_live_integration_settings()


@pytest.fixture(scope="session")
def google_ads_live_settings() -> GoogleAdsLiveIntegrationSettings:
    require_vault_live_integration()
    return read_google_ads_live_integration_settings()


@pytest.fixture(scope="session")
def yandex_webmaster_live_settings() -> YandexWebmasterLiveIntegrationSettings:
    require_vault_live_integration()
    return read_yandex_webmaster_live_integration_settings()


@pytest.fixture(scope="session")
def cbr_live_settings() -> CbrLiveIntegrationSettings:
    require_live_integration()
    return read_cbr_live_integration_settings()


@pytest.fixture(scope="session")
def openexchangerates_live_settings() -> OpenExchangeRatesLiveIntegrationSettings:
    require_vault_live_integration()
    return read_openexchangerates_live_integration_settings()


@dataclass(frozen=True, slots=True)
class FasttrackLiveIntegrationSettings:
    vault_path: str
    resource: str = "flex_cms_ratings"
    timeout: int = 60
    max_retries: int = 2
    rate_limit_delay: float = 0.2
    dashboard_uuid: str | None = None
    category: str | None = None
    limit: int | None = None
    offset: int | None = None
    page_size: int | None = 1000


def read_fasttrack_live_integration_settings(env: Mapping[str, str] | None = None) -> FasttrackLiveIntegrationSettings:
    source = env or os.environ

    def _optional_int(name: str) -> int | None:
        raw = str(source.get(name, "")).strip()
        return int(raw) if raw else None

    return FasttrackLiveIntegrationSettings(
        vault_path=str(source.get("DPONE_IT_FT_VAULT_PATH", "api/fasttrack")).strip() or "api/fasttrack",
        resource=str(source.get("DPONE_IT_FT_RESOURCE", "flex_cms_ratings")).strip() or "flex_cms_ratings",
        timeout=int(source.get("DPONE_IT_FT_TIMEOUT", "60")),
        max_retries=int(source.get("DPONE_IT_FT_MAX_RETRIES", "2")),
        rate_limit_delay=float(source.get("DPONE_IT_FT_RATE_LIMIT_DELAY", "0.2")),
        dashboard_uuid=(str(source.get("DPONE_IT_FT_DASHBOARD_UUID", "")).strip() or None),
        category=(str(source.get("DPONE_IT_FT_CATEGORY", "")).strip() or None),
        limit=_optional_int("DPONE_IT_FT_LIMIT"),
        offset=_optional_int("DPONE_IT_FT_OFFSET"),
        page_size=_optional_int("DPONE_IT_FT_PAGE_SIZE") or 1000,
    )


@pytest.fixture(scope="session")
def fasttrack_live_settings() -> FasttrackLiveIntegrationSettings:
    require_vault_live_integration()
    return read_fasttrack_live_integration_settings()
