"""Complete fail-closed catalog contract for target-atomic MSSQL state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.state.mssql_catalog_integrity import (
    MssqlCheckContract,
    MssqlDefaultContract,
    MssqlForeignKeyContract,
    MssqlIndexContract,
    MssqlTableIntegrityContract,
    require_table_catalog_integrity,
)
from dpone.runtime.state.mssql_contract import (
    COMMIT_RECEIPT_CONTRACT,
    LOAD_AUDIT_CONTRACT,
    RUN_STATE_CONTRACT,
    SOURCE_STATE_CONTRACT,
    MssqlExternalTableContract,
    require_external_table_shape,
)
from dpone.runtime.state.mssql_repair_contract import (
    REPAIR_AUTHORITY_CONTRACT,
    REPAIR_AUTHORITY_TRIGGER,
    REPAIR_CONSUMPTION_CONTRACT,
    require_immutable_authority_trigger,
)
from dpone.runtime.support.mssql_trigger_integrity import require_exact_table_trigger_set


@dataclass(frozen=True, slots=True)
class MssqlAtomicStateTables:
    """Physical names of the six external target-atomic state objects."""

    state: str
    receipt: str
    repair_authority: str
    repair_consumption: str
    run: str
    audit: str


@dataclass(frozen=True, slots=True)
class _TableBinding:
    name: str
    shape: MssqlExternalTableContract
    integrity: MssqlTableIntegrityContract


def require_external_atomic_state_catalog(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    tables: MssqlAtomicStateTables,
) -> None:
    """Validate all six objects before a target-atomic route reads its source."""

    for binding in _bindings(schema, tables):
        require_external_table_shape(
            connector,
            database=database,
            schema=schema,
            table=binding.name,
            contract=binding.shape,
        )
        require_table_catalog_integrity(
            connector,
            database=database,
            schema=schema,
            table=binding.name,
            contract=binding.integrity,
        )
    for table in (
        tables.state,
        tables.receipt,
        tables.repair_authority,
        tables.repair_consumption,
        tables.run,
        tables.audit,
    ):
        require_exact_table_trigger_set(
            connector,
            database=database,
            schema=schema,
            table=table,
            triggers=(frozenset({REPAIR_AUTHORITY_TRIGGER}) if table == tables.repair_authority else frozenset()),
            error_code="mssql_external_state_trigger_set",
        )
    require_immutable_authority_trigger(
        connector,
        database=database,
        schema=schema,
        table=tables.repair_authority,
    )


def _bindings(schema: str, tables: MssqlAtomicStateTables) -> tuple[_TableBinding, ...]:
    return (
        _TableBinding(
            tables.state,
            SOURCE_STATE_CONTRACT,
            _exact_integrity(
                indexes=(
                    _primary_key("state_key"),
                    MssqlIndexContract(
                        kind="unique_index",
                        columns=("target_identity",),
                        clustered=False,
                        filter_expression="[superseded_at_utc] IS NULL",
                    ),
                ),
                checks=(
                    MssqlCheckContract(
                        "supersession",
                        """
                        ([superseded_at_utc] IS NULL AND [superseded_by_state_key] IS NULL)
                        OR ([superseded_at_utc] IS NOT NULL AND [superseded_by_state_key] IS NOT NULL
                            AND [superseded_by_state_key] <> [state_key])
                        """,
                    ),
                ),
                defaults=(MssqlDefaultContract("state_revision", "0"),),
            ),
        ),
        _TableBinding(
            tables.receipt,
            COMMIT_RECEIPT_CONTRACT,
            _exact_integrity(
                indexes=(_primary_key("receipt_id"), _unique_constraint("state_key", "load_id")),
                foreign_keys=(_foreign_key(schema, ("state_key",), tables.state, ("state_key",)),),
            ),
        ),
        _TableBinding(
            tables.repair_authority,
            REPAIR_AUTHORITY_CONTRACT,
            _exact_integrity(
                # Authority is provisioned before a baseline/transfer creates its
                # destination state row, so state_key intentionally has no FK.
                indexes=(_primary_key("authority_id"), _unique_constraint("authority_digest")),
                checks=(
                    MssqlCheckContract(
                        "checkpoint",
                        """
                        ([expected_checkpoint_absent] = 1 AND [expected_xmin] IS NULL
                            AND [expected_revision] IS NULL)
                        OR ([expected_checkpoint_absent] = 0 AND [expected_xmin] IS NOT NULL
                            AND [expected_revision] IS NOT NULL)
                        """,
                    ),
                    MssqlCheckContract(
                        "transfer",
                        """
                        ([transfer_from_state_key] IS NULL AND [transfer_from_xmin] IS NULL
                            AND [transfer_from_revision] IS NULL)
                        OR ([transfer_from_state_key] IS NOT NULL AND [transfer_from_xmin] IS NOT NULL
                            AND [transfer_from_revision] IS NOT NULL
                            AND [transfer_from_state_key] <> [state_key]
                            AND [expected_checkpoint_absent] = 1 AND [allow_full_baseline] = 1)
                        """,
                    ),
                    MssqlCheckContract(
                        "delete_rows",
                        "[allow_max_delete_rows] IS NULL OR [allow_max_delete_rows] >= 0",
                    ),
                    MssqlCheckContract(
                        "delete_ratio",
                        "[allow_max_delete_ratio] IS NULL OR [allow_max_delete_ratio] BETWEEN 0.0 AND 1.0",
                    ),
                ),
                defaults=(MssqlDefaultContract("created_at_utc", "SYSUTCDATETIME()"),),
            ),
        ),
        _TableBinding(
            tables.repair_consumption,
            REPAIR_CONSUMPTION_CONTRACT,
            _exact_integrity(
                indexes=(_primary_key("authority_id"), _unique_constraint("receipt_id")),
                foreign_keys=(
                    _foreign_key(schema, ("authority_id",), tables.repair_authority, ("authority_id",)),
                    _foreign_key(schema, ("state_key",), tables.state, ("state_key",)),
                    _foreign_key(schema, ("receipt_id",), tables.receipt, ("receipt_id",)),
                ),
                checks=(
                    MssqlCheckContract("rows", "[observed_delete_rows] >= 0"),
                    MssqlCheckContract("ratio", "[observed_delete_ratio] BETWEEN 0.0 AND 1.0"),
                ),
            ),
        ),
        _TableBinding(
            tables.run,
            RUN_STATE_CONTRACT,
            _exact_integrity(
                indexes=(_primary_key("id"), _unique_constraint("run_state_key")),
                defaults=(
                    MssqlDefaultContract("__dpone__loaded_at", "SYSUTCDATETIME()"),
                    MssqlDefaultContract("__dpone__updated_at", "SYSUTCDATETIME()"),
                ),
            ),
        ),
        _TableBinding(
            tables.audit,
            LOAD_AUDIT_CONTRACT,
            _exact_integrity(
                indexes=(_primary_key("load_id"),),
                defaults=(MssqlDefaultContract("__dpone__loaded_at", "SYSUTCDATETIME()"),),
            ),
        ),
    )


def _exact_integrity(
    *,
    indexes: tuple[MssqlIndexContract, ...] = (),
    foreign_keys: tuple[MssqlForeignKeyContract, ...] = (),
    checks: tuple[MssqlCheckContract, ...] = (),
    defaults: tuple[MssqlDefaultContract, ...] = (),
) -> MssqlTableIntegrityContract:
    """Build one fail-closed relational catalog contract."""

    return MssqlTableIntegrityContract(
        indexes=indexes,
        foreign_keys=foreign_keys,
        checks=checks,
        defaults=defaults,
        exact_indexes=True,
        exact_foreign_keys=True,
        exact_checks=True,
        exact_defaults=True,
    )


def _primary_key(*columns: str) -> MssqlIndexContract:
    return MssqlIndexContract(kind="primary_key", columns=columns, clustered=True)


def _unique_constraint(*columns: str) -> MssqlIndexContract:
    return MssqlIndexContract(kind="unique_constraint", columns=columns, clustered=False)


def _foreign_key(
    schema: str,
    columns: tuple[str, ...],
    referenced_table: str,
    referenced_columns: tuple[str, ...],
) -> MssqlForeignKeyContract:
    return MssqlForeignKeyContract(
        columns=columns,
        referenced_schema=schema,
        referenced_table=referenced_table,
        referenced_columns=referenced_columns,
    )


__all__ = ["MssqlAtomicStateTables", "require_external_atomic_state_catalog"]
