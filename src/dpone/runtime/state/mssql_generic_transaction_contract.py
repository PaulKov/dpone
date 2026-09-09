"""Exact external SQL Server catalog for generic transaction governance."""

from __future__ import annotations

from typing import Any

from dpone.runtime.state.mssql_catalog_integrity import (
    MssqlCheckContract,
    MssqlDefaultContract,
    MssqlForeignKeyContract,
    MssqlIndexContract,
    MssqlTableIntegrityContract,
    require_table_catalog_integrity,
)
from dpone.runtime.state.mssql_contract import MssqlExternalTableContract, require_external_table_shape
from dpone.runtime.state.mssql_generic_fence_trigger import render_fence_trigger_body
from dpone.runtime.state.mssql_generic_operation_trigger import render_operation_trigger_body
from dpone.runtime.state.mssql_generic_table_contracts import (
    ATTEMPT_CONTRACT,
    FENCE_CONTRACT,
    OPERATION_CONTRACT,
    OPTIONAL_METRICS,
    RECEIPT_CONTRACT,
    REQUIRED_METRICS,
)
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    ATTEMPT_TRIGGER,
    FENCE_TABLE,
    FENCE_TRIGGER,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
    RECEIPT_TRIGGER,
)
from dpone.runtime.state.mssql_generic_trigger_contract import require_governance_trigger
from dpone.runtime.support.mssql_trigger_integrity import (
    require_exact_immutable_trigger,
    require_exact_table_trigger_set,
)


def require_generic_transaction_catalog(connector: Any, *, database: str, schema: str) -> None:
    """Fail before source I/O unless every fence/receipt object is exact."""

    for table, shape, integrity in _bindings(schema):
        require_external_table_shape(connector, database=database, schema=schema, table=table, contract=shape)
        require_table_catalog_integrity(
            connector,
            database=database,
            schema=schema,
            table=table,
            contract=integrity,
        )
    for table, trigger in (
        (FENCE_TABLE, FENCE_TRIGGER),
        (ATTEMPT_TABLE, ATTEMPT_TRIGGER),
        (OPERATION_TABLE, OPERATION_TRIGGER),
        (RECEIPT_TABLE, RECEIPT_TRIGGER),
    ):
        require_exact_table_trigger_set(
            connector,
            database=database,
            schema=schema,
            table=table,
            triggers=frozenset({trigger}),
            error_code="mssql_generic_transaction_trigger_set",
        )
    for table, trigger, token in (
        (ATTEMPT_TABLE, ATTEMPT_TRIGGER, "DPONE_LOAD_ATTEMPT_IMMUTABLE"),
        (RECEIPT_TABLE, RECEIPT_TRIGGER, "DPONE_LOAD_RECEIPT_IMMUTABLE"),
    ):
        require_exact_immutable_trigger(
            connector,
            database=database,
            schema=schema,
            table=table,
            trigger=trigger,
            throw_token=token,
            error_code="mssql_generic_transaction_immutable_trigger",
        )
    require_governance_trigger(
        connector,
        database=database,
        schema=schema,
        table=FENCE_TABLE,
        trigger=FENCE_TRIGGER,
        body=render_fence_trigger_body(),
        error_label="fence",
    )
    require_governance_trigger(
        connector,
        database=database,
        schema=schema,
        table=OPERATION_TABLE,
        trigger=OPERATION_TRIGGER,
        body=render_operation_trigger_body(),
        error_label="operation",
    )


def _bindings(
    schema: str,
) -> tuple[tuple[str, MssqlExternalTableContract, MssqlTableIntegrityContract], ...]:
    return (
        (
            FENCE_TABLE,
            FENCE_CONTRACT,
            MssqlTableIntegrityContract(
                indexes=(MssqlIndexContract("primary_key", ("target_identity",), True),),
                foreign_keys=(
                    MssqlForeignKeyContract(
                        (
                            "current_attempt_key",
                            "target_identity",
                            "current_generation",
                            "current_route_fingerprint",
                        ),
                        schema,
                        ATTEMPT_TABLE,
                        ("attempt_key", "target_identity", "generation", "route_fingerprint"),
                    ),
                ),
                checks=(MssqlCheckContract("generation", "[current_generation] >= 1"),),
                defaults=(MssqlDefaultContract("updated_at_utc", "SYSUTCDATETIME()"),),
                exact_indexes=True,
                exact_foreign_keys=True,
                exact_checks=True,
                exact_defaults=True,
            ),
        ),
        (
            ATTEMPT_TABLE,
            ATTEMPT_CONTRACT,
            MssqlTableIntegrityContract(
                indexes=(
                    MssqlIndexContract("primary_key", ("attempt_key",), True),
                    MssqlIndexContract("unique_constraint", ("invocation_digest",), False),
                    MssqlIndexContract("unique_constraint", ("target_identity", "generation"), False),
                    MssqlIndexContract(
                        "unique_constraint",
                        ("attempt_key", "target_identity", "generation", "route_fingerprint"),
                        False,
                    ),
                ),
                checks=(MssqlCheckContract("generation", "[generation] >= 1"),),
                defaults=(MssqlDefaultContract("created_at_utc", "SYSUTCDATETIME()"),),
                exact_indexes=True,
                exact_foreign_keys=True,
                exact_checks=True,
                exact_defaults=True,
            ),
        ),
        (
            OPERATION_TABLE,
            OPERATION_CONTRACT,
            MssqlTableIntegrityContract(
                indexes=(
                    MssqlIndexContract("primary_key", ("operation_key",), True),
                    MssqlIndexContract("unique_constraint", ("attempt_key", "scope_hash"), False),
                    MssqlIndexContract("unique_constraint", ("operation_key", "attempt_key"), False),
                    MssqlIndexContract(
                        "unique_constraint",
                        (
                            "operation_key",
                            "attempt_key",
                            "scope_hash",
                            "current_epoch",
                            "current_owner_digest",
                        ),
                        False,
                    ),
                ),
                foreign_keys=(MssqlForeignKeyContract(("attempt_key",), schema, ATTEMPT_TABLE, ("attempt_key",)),),
                checks=(MssqlCheckContract("epoch", "[current_epoch] >= 1"),),
                defaults=(MssqlDefaultContract("updated_at_utc", "SYSUTCDATETIME()"),),
                exact_indexes=True,
                exact_foreign_keys=True,
                exact_checks=True,
                exact_defaults=True,
            ),
        ),
        (
            RECEIPT_TABLE,
            RECEIPT_CONTRACT,
            MssqlTableIntegrityContract(
                indexes=(
                    MssqlIndexContract("primary_key", ("receipt_id",), True),
                    MssqlIndexContract("unique_constraint", ("operation_key",), False),
                ),
                foreign_keys=(
                    MssqlForeignKeyContract(
                        ("operation_key", "attempt_key", "scope_hash", "operation_epoch", "owner_digest"),
                        schema,
                        OPERATION_TABLE,
                        (
                            "operation_key",
                            "attempt_key",
                            "scope_hash",
                            "current_epoch",
                            "current_owner_digest",
                        ),
                    ),
                ),
                checks=(
                    MssqlCheckContract("operation_epoch", "[operation_epoch] >= 1"),
                    MssqlCheckContract("payload_rows", _payload_rows_check()),
                    MssqlCheckContract("lifecycle", _lifecycle_check()),
                    MssqlCheckContract("metrics", _metric_check()),
                ),
                defaults=(MssqlDefaultContract("committed_at_utc", "SYSUTCDATETIME()"),),
                exact_indexes=True,
                exact_foreign_keys=True,
                exact_checks=True,
                exact_defaults=True,
            ),
        ),
    )


def _metric_check() -> str:
    required = " AND ".join(f"[{name}] >= 0" for name in REQUIRED_METRICS)
    optional = " AND ".join(f"([{name}] IS NULL OR [{name}] >= 0)" for name in OPTIONAL_METRICS)
    return f"{required} AND {optional}"


def _payload_rows_check() -> str:
    return "[declared_rows] >= 0 AND [declared_rows] = [actual_raw_rows] AND [actual_raw_rows] = [actual_native_rows]"


def _lifecycle_check() -> str:
    return (
        "[extraction_started_at_utc] <= [extraction_completed_at_utc] AND "
        "(([snapshot_acquired_at_utc] IS NULL AND [snapshot_authority] IS NULL "
        "AND [source_token_sha256] IS NULL) OR ([snapshot_acquired_at_utc] IS NOT NULL "
        "AND [snapshot_authority] IS NOT NULL "
        "AND [extraction_started_at_utc] <= [snapshot_acquired_at_utc] "
        "AND [snapshot_acquired_at_utc] <= [extraction_completed_at_utc])) "
        "AND [loaded_at_utc] <= [committed_at_utc]"
    )


__all__ = [
    "ATTEMPT_CONTRACT",
    "ATTEMPT_TABLE",
    "ATTEMPT_TRIGGER",
    "FENCE_CONTRACT",
    "FENCE_TABLE",
    "FENCE_TRIGGER",
    "OPERATION_CONTRACT",
    "OPERATION_TABLE",
    "OPERATION_TRIGGER",
    "RECEIPT_CONTRACT",
    "RECEIPT_TABLE",
    "RECEIPT_TRIGGER",
    "require_generic_transaction_catalog",
]
