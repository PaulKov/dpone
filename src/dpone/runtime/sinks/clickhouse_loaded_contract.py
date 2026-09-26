"""Prove an unscannable native file from the staging table that received it.

A character file is scanned before insert and carries a source receipt. SQL
Server native bytes cannot be scanned, so they have no receipt. After the whole
payload is in a local staging table, and before publication, that table is
the contract proof: exported row count, and no NULL in columns the contract or
a physical fail-fast rule forbids. ClickHouse stores a column default instead
of NULL when input_format_null_as_default is left on, so fail-fast inserts turn
that setting off. Cell values are not scanned. A sink that cannot read its
staging table still fails closed on the missing receipt. Cluster-external
publication proves each member by its own row hash, not by this local query.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.etl.contract_artifacts import ContractValidationSummary
from dpone.runtime.etl.validated_file_artifact import ContractValidatedFileArtifact
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricProbe
from dpone.runtime.governance.acceptance_snapshot import AcceptanceMetricRequest, AcceptanceMetricSnapshot
from dpone.runtime.physical_design.nullability import NullabilityOptions

_OPAQUE_NATIVE_FORMATS = frozenset({"mssql-bcp-native", "mssql-native"})
OBSERVED_VALIDATION_MODE = "staging_table"


class StagingContractError(RuntimeError):
    """The staging table does not satisfy the source contract."""

    code = "DPONE_STAGING_CONTRACT_BLOCKED"

    def __init__(self, blocker: str) -> None:
        self.blocker = blocker
        super().__init__(f"{self.code}:{blocker}")


@dataclass(frozen=True, slots=True)
class NativeContractObservation:
    """A pending contract proof for one unscannable native file."""

    wrapper: ContractValidatedFileArtifact
    payload: Any
    contract: SchemaContract
    null_columns: tuple[str, ...]

    def require(self, probe: AcceptanceMetricProbe, *, database: str, table: str) -> int:
        """Observe the fully loaded staging table, then record the evidence.

        The insert call's return value is not a row count. Only the probe
        query against the staging table is.
        """

        rows_exported = getattr(self.payload.artifact, "rows_exported", None)
        if type(rows_exported) is not int or rows_exported < 0:
            raise StagingContractError("rows_exported_required")
        required = self.null_columns
        request = AcceptanceMetricRequest(
            side="staged",
            columns=tuple(dict.fromkeys((*((self.contract.columns or {}).keys()), *required))),
            dataset_identity=f"{database}.{table}",
            schema=database,
            table=table,
            include_row_count=True,
            null_count_columns=required,
        )
        snapshot = probe.collect(request)
        blocker = loaded_contract_blocker(snapshot, rows_exported=rows_exported, required_columns=required)
        if blocker is not None:
            raise StagingContractError(blocker)
        self.wrapper.validation_summary = ContractValidationSummary(
            accepted_rows=int(snapshot.row_count or 0),
            validation_mode=OBSERVED_VALIDATION_MODE,
        )
        return int(snapshot.row_count or 0)


def pending_native_observation(load_config: Any, payload: Any) -> NativeContractObservation | None:
    """Return the observation a staged load owes, or None for any other payload."""

    wrapper = getattr(payload, "artifact", None)
    if not isinstance(wrapper, ContractValidatedFileArtifact):
        return None
    if not wrapper.lacks_source_contract_receipt():
        return None
    inner = wrapper.completed_source_authority_artifact
    if _wire_format(inner) not in _OPAQUE_NATIVE_FORMATS:
        return None
    contract = _schema_contract(load_config)
    if not contract.columns and not _fail_fast_columns(load_config, contract):
        return None
    return NativeContractObservation(
        wrapper=wrapper,
        payload=payload.rebind(artifact=inner),
        contract=contract,
        null_columns=null_check_columns(contract, load_config),
    )


def null_check_columns(contract: SchemaContract, load_config: Any) -> tuple[str, ...]:
    """Columns whose stored NULLs violate either the contract or physical design."""

    names = [name for name, column in (contract.columns or {}).items() if column.nullable is False]
    for name in _fail_fast_columns(load_config, contract):
        if name not in names:
            names.append(name)
    return tuple(names)


def _fail_fast_columns(load_config: Any, contract: SchemaContract) -> tuple[str, ...]:
    options = getattr(load_config, "options", None) or {}
    physical = options.get("physical_design") if isinstance(options, Mapping) else None
    storage = physical.get("storage") if isinstance(physical, Mapping) else None
    if not isinstance(storage, Mapping):
        return ()
    found: list[str] = []
    candidates = tuple((contract.columns or {}).keys())
    for target_storage in storage.values():
        if not isinstance(target_storage, Mapping):
            continue
        raw = target_storage.get("nullability")
        if not isinstance(raw, Mapping):
            continue
        policy = NullabilityOptions.from_config(raw)
        for name in candidates:
            if (
                policy.effective_mode(name) == "non_nullable_by_default"
                and policy.effective_null_handling(name) == "fail_fast"
                and name not in found
            ):
                found.append(name)
    return tuple(found)


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
    "StagingContractError",
    "NativeContractObservation",
    "OBSERVED_VALIDATION_MODE",
    "loaded_contract_blocker",
    "pending_native_observation",
    "null_check_columns",
]
