"""Deterministic prepared-table ownership under a caller-held SQL writer fence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, replace
from hashlib import sha256
from typing import Any

from dpone.runtime.sinks.mssql_native_object_authority import (
    read_exact_object_id,
    require_unfiltered_object_metadata,
)


def planned_stage(context: Any, config: Any) -> dict[str, str]:
    """Reserve one stable physical object before CREATE, including recovery."""

    binding = sha256(repr(asdict(context.plan)).encode()).hexdigest()
    database = config.staging_database or config.target_database
    if not database:
        raise ValueError("mssql_native.staging_database_required")
    return {
        "database": database,
        "schema": config.staging_schema or config.target_schema,
        "table": "dpone_native_prepared_" + binding[:40],
        "binding": binding,
    }


def create_prepared(
    strategy: Any, config: Any, schema: Any, planned: dict[str, str], *, before_create: Callable[[], None]
) -> Any:
    """Create table and ownership property atomically, or settle our prior table.

    The caller must hold its lease-backed preparation scope throughout this
    operation. An existing table without the exact property is never dropped.
    """

    connector = strategy.connector
    qualified = connector.qualified_name(planned["schema"], planned["table"], database=planned["database"])
    rows = connector.get_records("SELECT OBJECT_ID(?)", (qualified,))
    if rows and rows[0][0] is not None:
        require_prepared_owner(connector, planned)
        connector.execute_query(f"DROP TABLE {qualified}")
    before_create()
    connector.begin()
    try:
        stage = strategy.staging_manager.create(
            replace(
                config,
                staging_table=planned["table"],
                staging_database=planned["database"],
                staging_schema=planned["schema"],
            ),
            schema,
        )
        database = connector.quote_identifier(planned["database"])
        connector.execute_query(
            f"EXEC {database}.sys.sp_addextendedproperty @name=N'dpone_native_owner', @value=?, "
            "@level0type=N'SCHEMA', @level0name=?, @level1type=N'TABLE', @level1name=?",
            (planned["binding"], planned["schema"], planned["table"]),
        )
    except BaseException:
        try:
            connector.rollback()
        except Exception:
            connector.close()
        raise
    # A lost ACK leaves a deterministically reserved object. Recovery inspects
    # its transactional ownership property before any attempt at settlement.
    connector.commit_transaction()
    return stage


def require_prepared_owner(connector: Any, planned: dict[str, str]) -> None:
    database = connector.quote_identifier(planned["database"])
    qualified = connector.qualified_name(planned["schema"], planned["table"], database=planned["database"])
    rows = connector.get_records(
        f"SELECT CONVERT(nvarchar(128), value) FROM {database}.sys.extended_properties "
        "WHERE class=1 AND major_id=OBJECT_ID(?) AND minor_id=0 AND name=N'dpone_native_owner'",
        (qualified,),
    )
    if len(rows) != 1 or rows[0][0] != planned["binding"]:
        raise ValueError("mssql_native.prepared_owner_mismatch")


def retire_exact_prepared(
    connector: Any,
    planned: dict[str, str],
    object_id: int,
    drop: Callable[[], None],
    *,
    timeout_seconds: int,
) -> None:
    """Drop only the bound prepared object and prove authoritative absence.

    SQL Server may return ``NULL`` from ``OBJECT_ID`` when metadata is hidden.
    Consequently, both an already-absent replay and the post-drop check require
    an unfiltered metadata principal before cleanup may release custody.
    """
    qualified = connector.qualified_name(planned["schema"], planned["table"], database=planned["database"])

    with connector.bounded_query_timeout(timeout_seconds):
        connector.begin()
        try:
            current = read_exact_object_id(connector, qualified)
            if current is None:
                require_unfiltered_object_metadata(
                    connector, diagnostic="mssql_native.prepared_absence_visibility_unproved"
                )
            else:
                if current != object_id:
                    raise ValueError("mssql_native.prepared_object_identity_changed")
                require_prepared_owner(connector, planned)
                connector.get_records(f"SELECT TOP (1) 1 FROM {qualified} WITH (TABLOCKX, HOLDLOCK)")
                if read_exact_object_id(connector, qualified) != object_id:
                    raise ValueError("mssql_native.prepared_object_identity_changed")
                require_prepared_owner(connector, planned)
                drop()
            if read_exact_object_id(connector, qualified) is not None:
                raise ValueError("mssql_native.prepared_retirement_unproved")
            require_unfiltered_object_metadata(
                connector, diagnostic="mssql_native.prepared_absence_visibility_unproved"
            )
            connector.commit_transaction()
        except BaseException:
            connector.rollback()
            raise
