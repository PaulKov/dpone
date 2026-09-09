"""Dependency-free Airflow connection URI parsing for runtime-only pods."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qsl, unquote, urlparse

from dpone.airflow_connection_names import airflow_conn_env_name
from dpone.runtime.credentials.config import CredentialsConfig


class AirflowConnectionEnvError(RuntimeError):
    """Raised when an Airflow env bridge variable is missing or invalid."""


def credentials_from_airflow_env(
    connection_id: str,
    *,
    environ: Mapping[str, str] | None = None,
) -> CredentialsConfig:
    env = environ or os.environ
    env_name = airflow_conn_env_name(connection_id)
    raw = env.get(env_name)
    if not raw:
        raise AirflowConnectionEnvError(f"{env_name} is not set")
    return parse_airflow_connection_uri(raw)


def _normalize_conn_type(scheme: str) -> str:
    """Map Airflow/SQLAlchemy URI schemes to dpone connector families.

    Airflow ``BaseHook.get_uri()`` often emits dialect URLs such as
    ``mssql+pymssql://`` or ``postgres+psycopg2://``. Runtime-only KPO pods
    consume those values through ``AIRFLOW_CONN_*`` without importing Airflow,
    so we strip the ``+driver`` suffix before routing.
    """
    conn_type = scheme.replace("-", "_").lower()
    if "+" in conn_type:
        conn_type = conn_type.split("+", 1)[0]
    return conn_type


def parse_airflow_connection_uri(uri: str) -> CredentialsConfig:
    parsed = urlparse(str(uri or ""))
    conn_type = _normalize_conn_type(parsed.scheme)
    if not conn_type:
        raise AirflowConnectionEnvError("Airflow connection URI must include a scheme")
    extra = _extras(parsed.query)
    host = unquote(parsed.hostname or "") or None
    schema = unquote(parsed.path.lstrip("/")) or None
    port = parsed.port
    username = unquote(parsed.username or "") or None
    password = unquote(parsed.password or "") or None
    if conn_type in {"postgres", "postgresql"}:
        return _postgres(host, port, schema, username, password, extra)
    if conn_type in {"mysql", "mariadb"}:
        return _mysql(host, port, schema, username, password, extra)
    if conn_type in {"mssql", "microsoft_mssql", "sqlserver", "odbc"}:
        return _mssql(host, port, schema, username, password, extra)
    if conn_type in {"clickhouse", "ch"}:
        return _clickhouse(host, port, schema, username, password, extra)
    if conn_type in {"bigquery", "google_cloud_platform", "gcp", "google_cloud"}:
        return _bigquery(schema, extra)
    if conn_type in {"kafka", "confluent"}:
        return _kafka(host, port, username, password, extra)
    if conn_type in {"aws", "s3", "amazon_web_services"}:
        return _aws(host, port, schema, username, password, extra)
    if conn_type in {"http", "https", "rest", "api", "generic_rest"}:
        return _rest(conn_type, host, port, username, password, extra)
    if conn_type == "generic":
        return _generic(host, port, schema, username, password, extra)
    raise NotImplementedError(f"Unsupported Airflow connection type: {conn_type}")


def _postgres(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    return CredentialsConfig(
        host=host,
        port=port,
        database=schema,
        username=username,
        password=password,
        schema=schema,
        additional_params=extra,
    )


def _mysql(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    return CredentialsConfig(
        host=host,
        port=port,
        database=schema,
        username=username,
        password=password,
        schema=schema,
        additional_params=extra,
        connect_timeout=_int(_extra_value(extra, "connect_timeout", "login_timeout", "LoginTimeout"), default=10),
    )


def _mssql(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    return CredentialsConfig(
        host=host,
        port=port,
        database=schema,
        username=username,
        password=password,
        schema=schema,
        additional_params=extra,
        driver=_text(_extra_value(extra, "driver")),
        encrypt=_credential_option(_extra_value(extra, "encrypt")),
        trust_server_certificate=_credential_option(
            _extra_value(extra, "trust_server_certificate", "TrustServerCertificate")
        ),
        connect_timeout=_int(_extra_value(extra, "connect_timeout", "login_timeout", "LoginTimeout"), default=10),
        query_timeout=_optional_int(_extra_value(extra, "query_timeout", "QueryTimeout")),
        bcp_path=_text(_extra_value(extra, "bcp_path")),
    )


def _clickhouse(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    return CredentialsConfig(
        host=host,
        port=port,
        database=schema,
        username=username,
        password=password,
        schema=schema,
        additional_params=extra,
        driver=_text(_extra_value(extra, "driver", "interface", "protocol", "transport", "client")),
        secure=_bool(extra.get("secure"), default=False),
        compression=_bool(extra.get("compression"), default=True),
        connect_timeout=_int(extra.get("connect_timeout"), default=10),
        send_receive_timeout=_int(extra.get("send_receive_timeout"), default=300),
        settings=_settings(extra.get("settings")),
    )


def _bigquery(schema: str | None, extra: dict[str, object]) -> CredentialsConfig:
    service_account = (
        extra.get("service_account")
        or extra.get("service_account_info")
        or extra.get("keyfile_dict")
        or extra.get("extra__google_cloud_platform__keyfile_dict")
    )
    if isinstance(service_account, str):
        service_account = _json_dict(service_account)
    project_id = (
        extra.get("project_id") or extra.get("extra__google_cloud_platform__project") or extra.get("project") or schema
    )
    return CredentialsConfig(
        project_id=_text(project_id),
        service_account_info=service_account if isinstance(service_account, dict) else None,
        service_account_key_file=_text(extra.get("key_path") or extra.get("keyfile_path")),
        additional_params=extra,
    )


def _kafka(
    host: str | None,
    port: int | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    default_bootstrap = f"{host}:{port}" if host and port else host
    return CredentialsConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        additional_params=extra,
        bootstrap_servers=_text(extra.get("bootstrap_servers") or extra.get("bootstrap.servers") or default_bootstrap),
        security_protocol=_text(extra.get("security_protocol") or extra.get("security.protocol")),
        sasl_mechanism=_text(extra.get("sasl_mechanism") or extra.get("sasl.mechanism")),
        sasl_username=_text(extra.get("sasl_username") or extra.get("sasl.username") or username),
        sasl_password=_text(extra.get("sasl_password") or extra.get("sasl.password") or password),
        ssl_ca_location=_text(extra.get("ssl_ca_location") or extra.get("ssl.ca.location")),
        client_id=_text(extra.get("client_id")),
        schema_registry_url=_text(extra.get("schema_registry_url")),
        schema_registry_username=_text(extra.get("schema_registry_username")),
        schema_registry_password=_text(extra.get("schema_registry_password")),
    )


def _rest(
    conn_type: str,
    host: str | None,
    port: int | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    endpoint = _text(extra.get("endpoint") or extra.get("base_url"))
    if not endpoint and host:
        endpoint = f"{conn_type}://{host}{':' + str(port) if port else ''}"
    return CredentialsConfig(
        host=host,
        port=port,
        username=username,
        password=password,
        endpoint=endpoint,
        token=_text(extra.get("token") or extra.get("bearer_token") or password),
        api_key=_text(extra.get("api_key")),
        additional_params=extra,
    )


def _generic(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    return CredentialsConfig(
        host=host,
        port=port,
        database=schema,
        username=username,
        password=password,
        schema=schema,
        endpoint=_text(extra.get("endpoint") or extra.get("endpoint_url") or extra.get("base_url")),
        token=_text(extra.get("token") or extra.get("bearer_token")),
        api_key=_text(extra.get("api_key")),
        additional_params=extra,
        driver=_text(_extra_value(extra, "driver", "interface", "protocol", "transport", "client")),
        secure=_bool(extra.get("secure"), default=False),
        compression=_bool(extra.get("compression"), default=True),
        connect_timeout=_int(extra.get("connect_timeout"), default=10),
        send_receive_timeout=_int(extra.get("send_receive_timeout"), default=300),
        settings=_settings(extra.get("settings")),
    )


def _aws(
    host: str | None,
    port: int | None,
    schema: str | None,
    username: str | None,
    password: str | None,
    extra: dict[str, object],
) -> CredentialsConfig:
    endpoint = _text(extra.get("endpoint_url") or extra.get("endpoint") or extra.get("base_url"))
    if not endpoint and host and host.startswith(("http://", "https://")):
        endpoint = host
    return CredentialsConfig(
        host=host,
        port=port,
        username=username or _text(extra.get("aws_access_key_id")),
        password=password or _text(extra.get("aws_secret_access_key")),
        schema=schema,
        endpoint=endpoint,
        token=_text(extra.get("aws_session_token") or extra.get("session_token")),
        additional_params=extra,
    )


def _extras(query: str) -> dict[str, object]:
    values: dict[str, object] = {}
    for key, value in parse_qsl(query, keep_blank_values=True):
        decoded = unquote(value)
        values[key] = _json_value(decoded)
    for extra_key in ("extra", "__extra__"):
        raw_extra = values.pop(extra_key, None)
        if isinstance(raw_extra, dict):
            values.update(raw_extra)
    return values


def _extra_value(extra: Mapping[str, object], *names: str) -> object:
    for name in names:
        if name in extra:
            return extra[name]
    wanted = {_token(name) for name in names}
    for key, value in extra.items():
        if _token(key) in wanted:
            return value
    return None


def _token(value: object) -> str:
    return "".join(character for character in str(value or "").lower() if character.isalnum())


def _json_value(value: str) -> object:
    if not value:
        return value
    if value[:1] in {"{", "["}:
        parsed = _json_dict(value)
        if parsed is not None:
            return parsed
    return value


def _json_dict(value: str) -> dict[str, object] | None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _int(value: object, *, default: int) -> int:
    parsed = _optional_int(value)
    return default if parsed is None else parsed


def _optional_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _credential_option(value: object) -> str | bool | None:
    if isinstance(value, bool):
        return value
    return _text(value)


def _settings(value: object) -> dict[str, Any] | None:
    return value if isinstance(value, dict) else None


def _text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = [
    "AirflowConnectionEnvError",
    "airflow_conn_env_name",
    "credentials_from_airflow_env",
    "parse_airflow_connection_uri",
]
