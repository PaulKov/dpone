"""Contract proof for opaque SQL Server native files loaded into ClickHouse staging.

A character wire is scanned before insert and carries a source receipt. Native
BCP bytes cannot be scanned, so they never get that receipt. The staged
ClickHouse load is the one place that can still prove the contract: after the
whole payload is in the staging table, and before validation or publication,
the table must hold exactly the exported rows and no NULL in columns the
contract declares non-nullable. Every other path keeps failing closed on the
missing receipt.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.etl.contract_artifacts import ContractValidationSummary
from dpone.runtime.etl.file_contract_validation import FileContractValidationError
from dpone.runtime.etl.validated_file_artifact import ContractValidatedFileArtifact
from dpone.runtime.governance.acceptance_snapshot import AcceptanceMetricRequest, AcceptanceMetricSnapshot
from dpone.runtime.governance.clickhouse_acceptance_metrics import ClickHouseAcceptanceMetricProbe

_OPAQUE_NATIVE_FORMATS = frozenset({"mssql-bcp-native", "mssql-native"})
_RECEIPT_REQUIRED = "file_contract_receipt.required"
OBSERVED_VALIDATION_MODE = "clickhouse_staging_observed"


class ClickHouseLoadedContractError(RuntimeError):
    """The staging table does not satisfy the source contract."""

    code = "DPONE_CLICKHOUSE_LOADED_CONTRACT_BLOCKED"

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}")


@dataclass(frozen=True, slots=True)
class NativeContractObservation:
    """A pending contract proof for one unscannable native file."""

    wrapper: ContractValidatedFileArtifact
    payload: Any
    contract: SchemaContract

    def require(self, connector: Any, *, database: str, table: str, staged_rows: int) -> None:
        """Observe the fully loaded staging table, then record the evidence."""

        rows_exported = getattr(self.payload.artifact, "rows_exported", None)
        if type(rows_exported) is not int or rows_exported < 0:
            raise ClickHouseLoadedContractError("rows_exported_required")
        if staged_rows != rows_exported:
            raise ClickHouseLoadedContractError("staged_row_count_mismatch")
        required = required_columns(self.contract)
        request = AcceptanceMetricRequest(
            side="staged",
            columns=tuple((self.contract.columns or {}).keys()),
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
        self.wrapper.validation_summary = ContractValidationSummary(
            accepted_rows=rows_exported,
            validation_mode=OBSERVED_VALIDATION_MODE,
        )


def pending_native_observation(load_config: Any, payload: Any) -> NativeContractObservation | None:
    """Return the observation a staged load owes, or None for any other payload."""

    wrapper = getattr(payload, "artifact", None)
    if not isinstance(wrapper, ContractValidatedFileArtifact):
        return None
    inner = wrapper.completed_source_authority_artifact
    if _wire_format(inner) not in _OPAQUE_NATIVE_FORMATS:
        return None
    try:
        wrapper.validated_file_contract_artifact
    except FileContractValidationError as error:
        if error.blocker != _RECEIPT_REQUIRED:
            raise
    else:
        return None
    contract = _schema_contract(load_config)
    if not contract.columns:
        return None
    return NativeContractObservation(wrapper=wrapper, payload=payload.rebind(artifact=inner), contract=contract)


def required_columns(contract: SchemaContract) -> tuple[str, ...]:
    return tuple(name for name, column in (contract.columns or {}).items() if column.nullable is False)


def loaded_contract_blocker(
    snapshot: AcceptanceMetricSnapshot,
    *,
    rows_exported: int,
    required_columns: tuple[str, ...],
) -> str | None:
    """Return a stable blocker, or None when the staging table matches."""

    if snapshot.row_count != rows_exported:
        return "row_count_mismatch"
    for column in required_columns:
        if int(snapshot.null_counts.get(column, 0)) != 0:
            return f"not_null_violation:{column}"
    return None


def _schema_contract(load_config: Any) -> SchemaContract:
    options = getattr(load_config, "options", None) or {}
    raw = options.get("schema_contract") if isinstance(options, Mapping) else None
    return SchemaContract.from_config(dict(raw) if isinstance(raw, Mapping) else {})


def _wire_format(artifact: Any) -> str:
    return str(getattr(artifact, "format", "")).replace("_", "-").lower()


__all__ = [
    "ClickHouseLoadedContractError",
    "NativeContractObservation",
    "OBSERVED_VALIDATION_MODE",
    "loaded_contract_blocker",
    "pending_native_observation",
    "required_columns",
]
