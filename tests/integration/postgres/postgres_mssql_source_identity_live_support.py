"""Real-vendor topology and evidence helpers for PostgreSQL source authority."""

from __future__ import annotations

import hashlib
import os
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any

from dpone.contracts.run_context import RunContext
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.etl.processor import ETLProcessor
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    STAGING_SCHEMA,
    TARGET_SCHEMA,
    VendorCoordinates,
)

AUTHORITY_DATABASE = "dpone_authority"
AUTHORITY_SCHEMA = "dpone_authority"
AUTHORITY_USER = "dpone"
AUTHORITY_PASSWORD = "dpone"
AUTHORITY_PRIMARY_PORT = 55434
AUTHORITY_STANDBY_PORT = 55435


def source_coordinates(
    baseline: VendorCoordinates,
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
) -> VendorCoordinates:
    """Replace only the PostgreSQL endpoint in verified vendor coordinates."""

    return replace(
        baseline,
        postgres_host=host,
        postgres_port=port,
        postgres_database=database,
        postgres_username=username,
        postgres_password=password,
    )


def authority_coordinates(baseline: VendorCoordinates, *, standby: bool) -> VendorCoordinates:
    """Return the pinned primary/standby route-live source coordinates."""

    role = "STANDBY" if standby else "PRIMARY"
    port_name = (
        "DPONE_IT_PG_AUTHORITY_STANDBY_PORT_FORWARD" if standby else "DPONE_IT_PG_AUTHORITY_PRIMARY_PORT_FORWARD"
    )
    default_port = AUTHORITY_STANDBY_PORT if standby else AUTHORITY_PRIMARY_PORT
    return source_coordinates(
        baseline,
        host=os.environ.get(
            f"DPONE_IT_PG_AUTHORITY_{role}_HOST",
            os.environ.get("DPONE_IT_PG_AUTHORITY_HOST", "127.0.0.1"),
        ),
        port=int(
            os.environ.get(
                f"DPONE_IT_PG_AUTHORITY_{role}_PORT",
                os.environ.get(port_name, str(default_port)),
            )
        ),
        database=AUTHORITY_DATABASE,
        username=AUTHORITY_USER,
        password=AUTHORITY_PASSWORD,
    )


def postgis_coordinates(baseline: VendorCoordinates) -> VendorCoordinates:
    """Return the independent pinned PostGIS cluster as PostgreSQL endpoint."""

    return source_coordinates(
        baseline,
        host=os.environ.get("DPONE_IT_POSTGIS_HOST", "127.0.0.1"),
        port=int(os.environ.get("DPONE_IT_POSTGIS_PORT", "55433")),
        database=os.environ.get("DPONE_IT_POSTGIS_DATABASE", "dpone_it"),
        username=os.environ.get("DPONE_IT_POSTGIS_USER", "dpone"),
        password=os.environ.get("DPONE_IT_POSTGIS_PASSWORD", "dpone"),
    )


def postgres_at(coordinates: VendorCoordinates) -> PostgresConnector:
    """Open one explicit PostgreSQL vendor connector from signed coordinates."""

    return PostgresConnector(
        host=coordinates.postgres_host,
        port=coordinates.postgres_port,
        database=coordinates.postgres_database,
        user=coordinates.postgres_username,
        password=coordinates.postgres_password,
    )


def install_source(postgres: Any, *, schema: str, table: str) -> None:
    """Install the exact four-column source relation used by hydration proof."""

    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    postgres.execute_query(f'DROP TABLE IF EXISTS "{schema}"."{table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{schema}"."{table}" ('
        '"id" integer NOT NULL PRIMARY KEY, '
        '"metric_code" text NOT NULL, '
        '"metric_value" double precision NOT NULL, '
        '"note" text NULL)'
    )
    postgres.execute_query(
        f'INSERT INTO "{schema}"."{table}" '
        '("id", "metric_code", "metric_value", "note") VALUES '
        "(1, 'alpha', 1.25, 'first'), "
        "(2, 'beta', -2.5, NULL), "
        "(3, 'gamma', 0.0, 'third')"
    )


def wait_for_replicated_source(postgres: Any, *, schema: str, table: str) -> None:
    """Wait a bounded interval for all source rows to replay on the standby."""

    deadline = time.monotonic() + 45
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            rows = postgres.get_records(
                f'SELECT count(*) AS row_count FROM "{schema}"."{table}"',
                as_dict=True,
            )
            if len(rows) == 1 and int(rows[0]["row_count"]) == 3:
                return
        except Exception as exc:  # noqa: BLE001 - bounded vendor readiness
            last_error = exc
        time.sleep(1)
    raise RuntimeError("postgres authority standby did not replay the source relation") from last_error


def processor(bindings: Any) -> ETLProcessor:
    """Compose the production ETL processor from hydrated runtime bindings."""

    return ETLProcessor(
        source=bindings.source_obj,
        sink=bindings.sink_obj,
        etl_logger=bindings.etl_logger,
        run_state_storage=bindings.run_state_storage,
        load_identity_service=bindings.load_identity_service,
    )


def run_context(case_id: str) -> RunContext:
    """Issue one immutable scheduler invocation identity per reviewed case."""

    return RunContext(
        run_id=f"route-live-source-identity-{case_id}",
        config={"pipeline_id": "source-identity-authority", "task_id": case_id},
    )


def route_image(live: Any, *, target: Any) -> dict[str, Any]:
    """Hash only target, staging, and owned transfer artifacts."""

    exists = bool(
        target.get_records(
            "SELECT CASE WHEN OBJECT_ID(?, N'U') IS NULL THEN 0 ELSE 1 END AS exists_flag",
            (f"{TARGET_SCHEMA}.{live.target_table}",),
            as_dict=True,
        )[0]["exists_flag"]
    )
    rows: tuple[dict[str, Any], ...] = ()
    if exists:
        rows = tuple(
            target.get_records(
                f"SELECT [id], [metric_code], [metric_value], [note] "
                f"FROM [{TARGET_SCHEMA}].[{live.target_table}] ORDER BY [id]",
                as_dict=True,
            )
        )
    staging = tuple(
        target.get_records(
            "SELECT t.name AS table_name, SUM(p.rows) AS row_count "
            "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id=t.schema_id "
            "INNER JOIN sys.partitions AS p ON p.object_id=t.object_id AND p.index_id IN (0,1) "
            "WHERE s.name=? GROUP BY t.name ORDER BY t.name",
            (STAGING_SCHEMA,),
            as_dict=True,
        )
    )
    return {
        "target_exists": exists,
        "target_rows": rows,
        "staging": staging,
        "owned_artifacts": artifact_image(live.transfer_root),
    }


def artifact_image(root: Path) -> tuple[dict[str, Any], ...]:
    """Return deterministic identity facts for runtime-owned transfer files."""

    if not root.exists():
        return ()
    return tuple(
        {
            "path": path.relative_to(root).as_posix(),
            "bytes": path.stat().st_size,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        }
        for path in sorted(root.rglob("*"))
        if path.is_file()
    )


def apply_environment(monkeypatch: Any, environment: Mapping[str, str]) -> None:
    """Project one verified runtime context into the process environment."""

    for name, value in environment.items():
        monkeypatch.setenv(name, value)


def drop_role(postgres: Any, role: str) -> None:
    """Remove one disposable login and all grants without masking test errors."""

    with suppress(Exception):
        postgres.execute_query(f'DROP OWNED BY "{role}"')
    with suppress(Exception):
        postgres.execute_query(f'DROP ROLE IF EXISTS "{role}"')


__all__ = [
    "AUTHORITY_DATABASE",
    "AUTHORITY_SCHEMA",
    "apply_environment",
    "authority_coordinates",
    "drop_role",
    "install_source",
    "postgis_coordinates",
    "postgres_at",
    "processor",
    "route_image",
    "run_context",
    "source_coordinates",
    "wait_for_replicated_source",
]
