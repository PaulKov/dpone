"""Observe a ClickHouse table after an opaque native load.

Character-wire files are scanned before insert. Native BCP cannot be scanned,
so the contract is checked on the table that just received the rows: exported
row count and columns the contract marks non-nullable. ClickHouse itself
rejects values that do not fit the declared column types during insert.
"""

from __future__ import annotations

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.governance.acceptance_snapshot import AcceptanceMetricRequest, AcceptanceMetricSnapshot
from dpone.runtime.governance.clickhouse_acceptance_metrics import ClickHouseAcceptanceMetricProbe


class ClickHouseLoadedContractError(RuntimeError):
    """The loaded table does not satisfy the source contract."""

    code = "DPONE_CLICKHOUSE_LOADED_CONTRACT_BLOCKED"

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}")


def require_loaded_contract(
    connector: object,
    *,
    database: str,
    table: str,
    contract: SchemaContract,
    rows_exported: object,
) -> None:
    """Fail the load unless the inserted table matches the export contract."""

    if type(rows_exported) is not int or rows_exported < 0:
        raise ClickHouseLoadedContractError("rows_exported_required")
    required = tuple(
        name for name, column in (contract.columns or {}).items() if column.nullable is False
    )
    columns = tuple((contract.columns or {}).keys())
    request = AcceptanceMetricRequest(
        side="staged",
        columns=columns,
        dataset_identity=f"{database}.{table}",
        schema=database,
        table=table,
        include_row_count=True,
        null_count_columns=required,
    )
    snapshot = ClickHouseAcceptanceMetricProbe(connector).collect(request)
    blocker = loaded_contract_blocker(snapshot, rows_exported=rows_exported, required_columns=required)
    if blocker is not None:
        raise ClickHouseLoadedContractError(blocker)


def loaded_contract_blocker(
    snapshot: AcceptanceMetricSnapshot,
    *,
    rows_exported: int,
    required_columns: tuple[str, ...],
) -> str | None:
    """Return a stable blocker, or None when the loaded table matches."""

    if snapshot.row_count != rows_exported:
        return "row_count_mismatch"
    for column in required_columns:
        if int(snapshot.null_counts.get(column, 0)) != 0:
            return f"not_null_violation:{column}"
    return None


__all__ = [
    "ClickHouseLoadedContractError",
    "loaded_contract_blocker",
    "require_loaded_contract",
]
