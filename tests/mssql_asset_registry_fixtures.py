"""Shared MSSQL connection-registry fixtures for Airflow asset URI tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Literal

import yaml

_Layout = Literal["platform", "dpone"]


def _mssql_connection(
    *,
    host: str,
    port: int = 1433,
    database: str = "AppDB",
    connection_id: str | None = None,
) -> dict[str, object]:
    entry: dict[str, object] = {
        "type": "mssql",
        "connection": {
            "asset_authority": {"host": host, "port": port},
            "database": database,
        },
    }
    if connection_id is not None:
        entry["credentials"] = {
            "resolver": "airflow_connection",
            "connection_id": connection_id,
        }
    return entry


DEFAULT_MSSQL_CONNECTIONS: dict[str, dict[str, object]] = {
    "mssql_dev": _mssql_connection(host="sql-dev.internal", connection_id="mssql_dev"),
    "source_dev": _mssql_connection(host="sql-source-dev.internal", connection_id="source_dev"),
    "mssql_dwh_stage": _mssql_connection(
        host="mssql.internal",
        database="DWH_Stage",
        connection_id="mssql_dwh_stage",
    ),
}


def write_mssql_connection_registry(
    repo_root: Path,
    *,
    env: str = "dev",
    layout: _Layout = "platform",
    connections: Mapping[str, Mapping[str, object]] | None = None,
    include: Sequence[str] | None = None,
) -> Path:
    """Write an env-bound connection registry with MSSQL ``asset_authority``.

    Authoring/recipe/selector fixtures historically used ``mssql_dev`` /
    ``source_dev`` without a registry. Compact pack now fail-closes on missing
    deployment-owned authority, so tests that expect a successful pack must
    materialize this SoT.
    """

    if layout == "platform":
        path = repo_root / "platform" / "connection-registries" / f"{env}.yaml"
    else:
        path = repo_root / ".dpone" / "registry" / "connection-registries" / f"{env}.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)

    if connections is not None:
        selected: dict[str, object] = dict(connections)
    elif include is not None:
        selected = {ref: DEFAULT_MSSQL_CONNECTIONS[ref] for ref in include}
    else:
        selected = {
            "mssql_dev": DEFAULT_MSSQL_CONNECTIONS["mssql_dev"],
            "source_dev": DEFAULT_MSSQL_CONNECTIONS["source_dev"],
        }

    payload = {
        "schema": "dpone.connection-registry.v1",
        "environment": env,
        "connections": selected,
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path
