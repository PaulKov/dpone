"""Exact external MSSQL table contracts for one-shot repair authority."""

from typing import Any

from dpone.runtime.state.mssql_contract import (
    MssqlColumnShape,
    exact_external_table_contract,
)
from dpone.runtime.support.mssql_trigger_integrity import require_exact_immutable_trigger

REPAIR_AUTHORITY_TRIGGER = "trg_dpone_repair_authority_immutable"

REPAIR_AUTHORITY_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "authority_id",
            "authority_digest",
            "state_key",
            "transfer_from_state_key",
            "transfer_from_xmin",
            "transfer_from_revision",
            "expected_checkpoint_absent",
            "expected_xmin",
            "expected_revision",
            "scope_hash",
            "reason",
            "expires_at_utc",
            "allow_full_baseline",
            "allow_max_delete_rows",
            "allow_max_delete_ratio",
            "created_at_utc",
        }
    ),
    unique_indexes=(("authority_id",), ("authority_digest",)),
    shapes=(
        MssqlColumnShape("authority_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("authority_digest", "char", 71, None, None, False),
        MssqlColumnShape("state_key", "binary", 32, None, None, False),
        MssqlColumnShape("transfer_from_state_key", "binary", 32, None, None, True),
        MssqlColumnShape("transfer_from_xmin", "bigint", 8, 19, 0, True),
        MssqlColumnShape("transfer_from_revision", "bigint", 8, 19, 0, True),
        MssqlColumnShape("expected_checkpoint_absent", "bit", 1, 1, 0, False),
        MssqlColumnShape("expected_xmin", "bigint", 8, 19, 0, True),
        MssqlColumnShape("expected_revision", "bigint", 8, 19, 0, True),
        MssqlColumnShape("scope_hash", "char", 71, None, None, False),
        MssqlColumnShape("reason", "nvarchar", 4096, None, None, False),
        MssqlColumnShape("expires_at_utc", "datetime2", 8, None, 7, False),
        MssqlColumnShape("allow_full_baseline", "bit", 1, 1, 0, False),
        MssqlColumnShape("allow_max_delete_rows", "bigint", 8, 19, 0, True),
        MssqlColumnShape("allow_max_delete_ratio", "float", 8, 53, 0, True),
        MssqlColumnShape("created_at_utc", "datetime2", 8, None, 7, False),
    ),
)

REPAIR_CONSUMPTION_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "authority_id",
            "authority_digest",
            "state_key",
            "load_id",
            "receipt_id",
            "used_full_baseline",
            "observed_delete_rows",
            "observed_delete_ratio",
            "consumed_at_utc",
        }
    ),
    unique_indexes=(("authority_id",), ("receipt_id",)),
    shapes=(
        MssqlColumnShape("authority_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("authority_digest", "char", 71, None, None, False),
        MssqlColumnShape("state_key", "binary", 32, None, None, False),
        MssqlColumnShape("load_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("receipt_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("used_full_baseline", "bit", 1, 1, 0, False),
        MssqlColumnShape("observed_delete_rows", "bigint", 8, 19, 0, False),
        MssqlColumnShape("observed_delete_ratio", "float", 8, 53, 0, False),
        MssqlColumnShape("consumed_at_utc", "datetime2", 8, None, 7, False),
    ),
)


def require_immutable_authority_trigger(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    table: str,
) -> None:
    """Require the exact enabled INSTEAD OF UPDATE/DELETE immutability guard."""

    require_exact_immutable_trigger(
        connector,
        database=database,
        schema=schema,
        table=table,
        trigger=REPAIR_AUTHORITY_TRIGGER,
        throw_token="DPONE_REPAIR_AUTHORITY_IMMUTABLE",
        error_code="mssql_external_repair_authority_immutable_trigger",
    )


__all__ = [
    "REPAIR_AUTHORITY_CONTRACT",
    "REPAIR_AUTHORITY_TRIGGER",
    "REPAIR_CONSUMPTION_CONTRACT",
    "require_immutable_authority_trigger",
]
