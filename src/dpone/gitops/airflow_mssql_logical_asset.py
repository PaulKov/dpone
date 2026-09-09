"""Environment-neutral logical MSSQL Asset references for dbt release packs.

Physical ``mssql://host:port/...`` URIs are deployment-owned and must not enter
immutable release-set bytes. Release packs retain ``asset_ref``; deployment
projection materializes the canonical URI against the environment registry.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.gitops.airflow_mssql_asset_authority import (
    AssetUriIssue,
    AssetUriResolution,
    MssqlAssetAuthority,
    is_templated_value,
    issue,
)

_MSSQL_ENGINE = "mssql"


@dataclass(frozen=True, slots=True)
class MssqlLogicalAssetRef:
    """Portable MSSQL Asset identity without deployment-owned host/port."""

    connection_ref: str
    database: str
    schema: str
    table: str
    engine: str = _MSSQL_ENGINE

    def to_mapping(self) -> dict[str, str]:
        return {
            "engine": self.engine,
            "connection_ref": self.connection_ref,
            "database": self.database,
            "schema": self.schema,
            "table": self.table,
        }

    def identity_key(self) -> str:
        """Stable ``asset_ref_sha256`` digest for deployment outlet projections."""

        from dpone_airflow_pack.mssql_asset_ref_codec import require_asset_ref_sha256

        return require_asset_ref_sha256(self.to_mapping())


def parse_mssql_logical_asset_ref(
    raw: object,
    *,
    path: str | None = None,
) -> tuple[MssqlLogicalAssetRef | None, tuple[AssetUriIssue, ...]]:
    """Parse one logical ``asset_ref`` mapping."""

    if not isinstance(raw, Mapping):
        return None, (issue("mssql asset_ref must be a mapping", path=path),)
    engine = str(raw.get("engine") or "").strip().lower()
    if engine != _MSSQL_ENGINE:
        return None, (issue("mssql asset_ref.engine must be 'mssql'", path=path),)
    fields = {
        "connection_ref": str(raw.get("connection_ref") or "").strip(),
        "database": str(raw.get("database") or "").strip(),
        "schema": str(raw.get("schema") or "").strip(),
        "table": str(raw.get("table") or "").strip(),
    }
    problems: list[AssetUriIssue] = []
    for name, value in fields.items():
        if not value:
            problems.append(issue(f"mssql asset_ref.{name} is required", path=path))
        elif is_templated_value(value):
            problems.append(issue(f"mssql asset_ref.{name} must not be templated", path=path))
    if problems:
        return None, tuple(problems)
    return (
        MssqlLogicalAssetRef(
            engine=_MSSQL_ENGINE,
            connection_ref=fields["connection_ref"],
            database=fields["database"],
            schema=fields["schema"],
            table=fields["table"],
        ),
        (),
    )


def materialize_mssql_logical_asset_uri(
    asset_ref: MssqlLogicalAssetRef,
    *,
    authority: MssqlAssetAuthority,
    path: str | None = None,
) -> AssetUriResolution:
    """Materialize one canonical physical URI from logical ref + authority."""

    from dpone.gitops.airflow_asset_uri import canonicalize_mssql_asset_uri

    return canonicalize_mssql_asset_uri(
        authority=authority,
        database=asset_ref.database,
        schema=asset_ref.schema,
        table=asset_ref.table,
        path=path,
    )


def logical_asset_ref_from_sink_block(
    block: Mapping[str, Any],
    *,
    path: str | None = None,
    default_database: str | None = None,
) -> tuple[MssqlLogicalAssetRef | None, tuple[AssetUriIssue, ...]]:
    """Build a logical asset_ref from a managed MSSQL sink/source block."""

    engine = canonical_endpoint_type(str(block.get("type") or ""))
    if engine != _MSSQL_ENGINE:
        return None, ()
    from dpone.gitops.airflow_mssql_asset_authority import connection_ref_from_block

    connection_ref = connection_ref_from_block(block)
    raw_table = block.get("table")
    table: Mapping[str, Any] = raw_table if isinstance(raw_table, Mapping) else {}
    database = str(table.get("database") or "").strip() or (default_database or "").strip()
    schema = str(table.get("schema") or "").strip()
    name = str(table.get("name") or "").strip()
    return parse_mssql_logical_asset_ref(
        {
            "engine": _MSSQL_ENGINE,
            "connection_ref": connection_ref or "",
            "database": database,
            "schema": schema,
            "table": name,
        },
        path=path,
    )


__all__ = [
    "MssqlLogicalAssetRef",
    "logical_asset_ref_from_sink_block",
    "materialize_mssql_logical_asset_uri",
    "parse_mssql_logical_asset_ref",
]
