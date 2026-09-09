"""Bounded, fail-closed cleanup for disposable production-hydration proofs.

The live fixture deliberately creates isolated PostgreSQL and SQL Server
objects.  This module owns their teardown so that setup/readback concerns stay
separate from process and vendor-resource lifecycle management.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from tests.integration.postgres.postgres_live_support import mssql_connector

MSSQL_ADMIN_CONNECT_TIMEOUT_SECONDS = 5
MSSQL_ADMIN_QUERY_TIMEOUT_SECONDS = 5
_MSSQL_DROP_ATTEMPTS = 2


def cleanup_production_hydration_resources(
    *,
    postgres: Any,
    target: Any | None,
    state: Any | None,
    master: Any,
    source_schema: str,
    source_table: str,
    target_database: str,
    state_database: str,
) -> None:
    """Attempt every teardown step and reject any unproven cleanup."""

    errors: list[Exception] = []
    try:
        postgres.execute_query("SET statement_timeout = '5s'")
        postgres.execute_query("SET lock_timeout = '2s'")
        postgres.execute_query(f'DROP TABLE IF EXISTS "{source_schema}"."{source_table}" CASCADE')
    except Exception as exc:
        errors.append(_cleanup_error("postgres_source_drop", exc))
    _close_connector(postgres, label="postgres", errors=errors)

    closed: set[int] = set()
    for label, connector in (("target", target), ("state", state), ("master", master)):
        if connector is None or id(connector) in closed:
            continue
        closed.add(id(connector))
        _close_connector(connector, label=label, errors=errors)

    for database in (target_database, state_database):
        try:
            drop_mssql_database(database)
        except Exception as exc:
            errors.append(_cleanup_error(f"mssql_database_drop:{database}", exc))

    if errors:
        raise ExceptionGroup("production hydration cleanup failed", errors)


def close_runtime_bindings(bindings: Any | None) -> None:
    """Close every connector created by the production hydrator exactly once."""

    if bindings is None:
        return
    connectors = (
        getattr(getattr(bindings, "source_obj", None), "connector", None),
        getattr(getattr(bindings, "sink_obj", None), "connector", None),
        getattr(getattr(getattr(bindings, "sink_obj", None), "state_storage", None), "connector", None),
    )
    closed: set[int] = set()
    errors: list[Exception] = []
    for connector in connectors:
        if connector is None or id(connector) in closed:
            continue
        closed.add(id(connector))
        closer = getattr(connector, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception as exc:
                errors.append(_cleanup_error("runtime_binding_close", exc))
    if errors:
        raise ExceptionGroup("runtime binding cleanup failed", errors)


def drop_mssql_database(
    database: str,
    *,
    connector_factory: Callable[..., Any] = mssql_connector,
) -> None:
    """Drop one disposable database with bounded retries and exact readback.

    An attempt succeeds only when both catalog absence and closure of its
    bounded administrative connection are confirmed.  Connector creation is
    inside the retry boundary because login failures are transient vendor
    failures too.
    """

    failures: list[str] = []
    unresolved_close_failure = False
    for attempt in range(1, _MSSQL_DROP_ATTEMPTS + 1):
        admin: Any | None = None
        database_absent = False
        connection_closed = False
        try:
            admin = connector_factory(
                database="master",
                connect_timeout=MSSQL_ADMIN_CONNECT_TIMEOUT_SECONDS,
                query_timeout=MSSQL_ADMIN_QUERY_TIMEOUT_SECONDS,
            )
            admin.execute_query(
                f"IF DB_ID(N'{database}') IS NOT NULL BEGIN "
                f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; "
                f"DROP DATABASE [{database}]; END"
            )
            rows = admin.get_records(
                "SELECT CASE WHEN DB_ID(?) IS NULL THEN 1 ELSE 0 END",
                (database,),
            )
            database_absent = rows == [(1,)]
            if not database_absent:
                failures.append(f"attempt={attempt}:database_remains")
        except Exception as exc:
            failures.append(f"attempt={attempt}:{type(exc).__name__}")
        finally:
            if admin is not None:
                try:
                    admin.close()
                    connection_closed = True
                except Exception as exc:
                    unresolved_close_failure = True
                    failures.append(f"attempt={attempt}:close:{type(exc).__name__}")
        if database_absent and connection_closed and not unresolved_close_failure:
            return

    detail = ",".join(failures) if failures else "no_cleanup_evidence"
    raise RuntimeError(f"disposable MSSQL cleanup not proven: {database} ({detail})")


def _close_connector(connector: Any, *, label: str, errors: list[Exception]) -> None:
    try:
        connector.close()
    except Exception as exc:
        errors.append(_cleanup_error(f"connector_close:{label}", exc))


def _cleanup_error(operation: str, exc: Exception) -> RuntimeError:
    return RuntimeError(f"{operation} failed with {type(exc).__name__}")


__all__ = [
    "MSSQL_ADMIN_CONNECT_TIMEOUT_SECONDS",
    "MSSQL_ADMIN_QUERY_TIMEOUT_SECONDS",
    "cleanup_production_hydration_resources",
    "close_runtime_bindings",
    "drop_mssql_database",
]
