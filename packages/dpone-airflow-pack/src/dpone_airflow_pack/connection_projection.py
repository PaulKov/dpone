from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

from dpone_airflow_pack.connection_names import require_airflow_connection_id


class ConnectionUriReader(Protocol):
    def read_uri(self, connection_id: str) -> str: ...


class AirflowBaseHookConnectionReader:
    """Read Airflow connection URIs lazily at task execution time."""

    def read_uri(self, connection_id: str) -> str:
        from airflow.hooks.base import BaseHook

        return BaseHook.get_connection(connection_id).get_uri()


class RuntimeAirflowConnectionUriReader:
    def __init__(self, *, reader: ConnectionUriReader, scheme_overrides: Mapping[str, str]) -> None:
        self._reader = reader
        self._scheme_overrides = dict(scheme_overrides)

    def read_uri(self, connection_id: str) -> str:
        uri = self._reader.read_uri(connection_id)
        scheme = self._scheme_overrides.get(connection_id)
        return replace_uri_scheme(uri, scheme) if scheme else uri


class ProjectedAirflowConnectionUriReader:
    """Apply closed projection URI overrides at Secret materialization time.

    ``kubernetes_secret_volume`` must honor the same scheme/database/query
    overrides as the legacy ``unsafe_airflow_env`` path so Airflow Connection
    ids such as ``ClickHouse`` can be rewritten to a dpone-parseable URI before
    the attempt Secret is published.
    """

    def __init__(
        self,
        *,
        reader: ConnectionUriReader,
        scheme_overrides: Mapping[str, str] | None = None,
        database_overrides: Mapping[str, str] | None = None,
        query_overrides: Mapping[str, Mapping[str, str]] | None = None,
    ) -> None:
        self._reader = reader
        self._scheme_overrides = dict(scheme_overrides or {})
        self._database_overrides = dict(database_overrides or {})
        self._query_overrides = {
            connection_id: dict(values) for connection_id, values in dict(query_overrides or {}).items()
        }

    def read_uri(self, connection_id: str) -> str:
        uri = self._reader.read_uri(connection_id)
        scheme = self._scheme_overrides.get(connection_id)
        if scheme:
            uri = replace_uri_scheme(uri, scheme)
        database = self._database_overrides.get(connection_id)
        if database:
            uri = replace_uri_database(uri, database)
        query = self._query_overrides.get(connection_id)
        if query:
            uri = merge_uri_query(uri, query)
        return uri


class UnsafeAirflowConnectionEnvBuilder:
    """Build AIRFLOW_CONN_* values from Airflow connections.

    This is intentionally an execution-time adapter, not a scheduler parse-time
    reader, so DAG parsing never touches secrets.
    """

    def __init__(self, *, reader: ConnectionUriReader) -> None:
        self._reader = reader

    def build(self, connection_ids: Sequence[str]) -> dict[str, str]:
        safe_ids = tuple(
            require_airflow_connection_id(connection_id, context="unsafe Airflow connection id")
            for connection_id in connection_ids
        )
        return {
            airflow_conn_env_name(connection_id): self._reader.read_uri(connection_id) for connection_id in safe_ids
        }


def airflow_conn_env_name(connection_id: str) -> str:
    return "AIRFLOW_CONN_" + "".join(ch if ch.isalnum() else "_" for ch in connection_id.upper())


def apply_database_overrides(env_vars: Mapping[str, str], database_overrides: Mapping[str, str]) -> dict[str, str]:
    patched = dict(env_vars)
    for connection_id, database in database_overrides.items():
        env_name = airflow_conn_env_name(connection_id)
        if env_name in patched:
            patched[env_name] = replace_uri_database(patched[env_name], database)
    return patched


def apply_query_overrides(
    env_vars: Mapping[str, str],
    query_overrides: Mapping[str, Mapping[str, str]],
) -> dict[str, str]:
    patched = dict(env_vars)
    for connection_id, values in query_overrides.items():
        env_name = airflow_conn_env_name(connection_id)
        if env_name in patched:
            patched[env_name] = merge_uri_query(patched[env_name], values)
    return patched


def replace_uri_database(uri: str, database: str) -> str:
    normalized = str(database or "").strip()
    if not normalized:
        raise ValueError("Runtime database override must be non-empty")
    parts = urlsplit(uri)
    return urlunsplit((parts.scheme, parts.netloc, f"/{quote(normalized, safe='')}", parts.query, parts.fragment))


def replace_uri_scheme(uri: str, scheme: str) -> str:
    normalized = str(scheme or "").strip()
    if not normalized:
        raise ValueError("Runtime scheme override must be non-empty")
    parts = urlsplit(uri)
    return urlunsplit((normalized, parts.netloc, parts.path, parts.query, parts.fragment))


def merge_uri_query(uri: str, values: Mapping[str, str]) -> str:
    normalized = {str(key): str(value) for key, value in values.items() if str(key) and value not in (None, "")}
    parts = urlsplit(uri)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    query.update(normalized)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), parts.fragment))


__all__ = [
    "AirflowBaseHookConnectionReader",
    "ConnectionUriReader",
    "ProjectedAirflowConnectionUriReader",
    "RuntimeAirflowConnectionUriReader",
    "UnsafeAirflowConnectionEnvBuilder",
    "airflow_conn_env_name",
    "apply_database_overrides",
    "apply_query_overrides",
    "merge_uri_query",
    "replace_uri_database",
    "replace_uri_scheme",
]
