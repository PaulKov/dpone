"""Import-safe built-in connector declarations used by discovery surfaces."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ConnectorDeclaration:
    """Stable connector identity without importing a vendor SDK or runtime."""

    id: str
    endpoint_types: tuple[str, ...]
    roles: tuple[str, ...]
    install_extras: tuple[str, ...]
    capability_ids: tuple[str, ...]
    maturity: str
    release_phase: str
    docs_link: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "endpoint_types": list(self.endpoint_types),
            "roles": list(self.roles),
            "install_extras": list(self.install_extras),
            "capability_ids": list(self.capability_ids),
            "maturity": self.maturity,
            "release_phase": self.release_phase,
            "docs_link": self.docs_link,
        }


_BUILT_IN_CONNECTORS = (
    ConnectorDeclaration(
        id="bigquery",
        endpoint_types=("bigquery",),
        roles=("sink",),
        install_extras=("gcp",),
        capability_ids=("full_refresh", "incremental_append", "incremental_merge", "replace"),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink-matrix.md",
    ),
    ConnectorDeclaration(
        id="clickhouse",
        endpoint_types=("clickhouse",),
        roles=("source", "sink"),
        install_extras=("clickhouse",),
        capability_ids=("full_refresh", "incremental_append", "replace", "http_bulk_ingest"),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink-matrix.md",
    ),
    ConnectorDeclaration(
        id="kafka",
        endpoint_types=("kafka",),
        roles=("source", "sink"),
        install_extras=("kafka",),
        capability_ids=("bounded_batch", "incremental_append", "state"),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink-matrix.md",
    ),
    ConnectorDeclaration(
        id="mssql",
        endpoint_types=("mssql",),
        roles=("source", "sink"),
        install_extras=("mssql",),
        capability_ids=(
            "full_refresh",
            "incremental_append",
            "incremental_merge",
            "replace",
            "state",
            "bcp_import",
            "bcp_queryout",
            "schema_evolution",
        ),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink/mssql-to-clickhouse.md",
    ),
    ConnectorDeclaration(
        id="mysql",
        endpoint_types=("mysql",),
        roles=("source",),
        install_extras=("mysql",),
        capability_ids=("full_refresh", "incremental_append", "incremental_merge", "replace"),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink-matrix.md",
    ),
    ConnectorDeclaration(
        id="postgres",
        endpoint_types=("postgres",),
        roles=("source", "sink"),
        install_extras=("postgres",),
        capability_ids=(
            "full_refresh",
            "incremental_append",
            "incremental_merge",
            "replace",
            "xmin_state",
            "schema_evolution",
        ),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/source-sink/postgres-to-clickhouse.md",
    ),
    ConnectorDeclaration(
        id="rest",
        endpoint_types=("api",),
        roles=("source",),
        install_extras=(),
        capability_ids=("auth", "pagination", "incremental_cursor", "state"),
        maturity="experimental",
        release_phase="beta",
        docs_link="docs/rest-api.md",
    ),
)

_ENDPOINT_TYPE_ALIASES = {
    "microsoft mssql": "mssql",
    "microsoft_mssql": "mssql",
    "odbc": "mssql",
    "postgresql": "postgres",
    "sqlserver": "mssql",
    "sql_server": "mssql",
}


def built_in_connector_declarations() -> tuple[ConnectorDeclaration, ...]:
    """Return immutable declarations in deterministic connector-id order."""

    return _BUILT_IN_CONNECTORS


def canonical_endpoint_type(value: str) -> str:
    """Normalize endpoint aliases without changing the public endpoint family.

    For example, ``api`` remains an API endpoint while SQL Server and
    PostgreSQL spelling variants collapse to ``mssql`` and ``postgres``.
    """

    normalized = str(value).strip().lower().replace("-", "_")
    return _ENDPOINT_TYPE_ALIASES.get(normalized, normalized)


def canonical_connector_id(value: str) -> str:
    """Resolve an endpoint family or provider alias to one connector identity."""

    normalized = canonical_endpoint_type(value)
    for declaration in _BUILT_IN_CONNECTORS:
        if normalized == declaration.id or normalized in declaration.endpoint_types:
            return declaration.id
    return normalized


__all__ = [
    "ConnectorDeclaration",
    "built_in_connector_declarations",
    "canonical_connector_id",
    "canonical_endpoint_type",
]
