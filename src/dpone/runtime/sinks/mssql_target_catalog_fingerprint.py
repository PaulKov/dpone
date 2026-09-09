"""Canonical before/after catalog expectations for governed SQL Server DDL."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalIntrospector
from dpone.runtime.sinks.mssql_target_catalog_model import MssqlSchemaCatalogSnapshot
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot

ExpectationKind = Literal["schema_columns", "physical_design"]
ExpectationBoundary = Literal["before", "after"]
ExpectationRepresentation = Literal["exact_sys_catalog_v5", "legacy_columns_v1", "physical_state_v1"]


@dataclass(frozen=True, slots=True)
class MssqlTargetCatalogExpectation:
    """Exact phase-local catalog transition consumed under the target lock."""

    kind: ExpectationKind
    before_sha256: bytes
    after_sha256: bytes
    representation: ExpectationRepresentation

    def __post_init__(self) -> None:
        for value in (self.before_sha256, self.after_sha256):
            if not isinstance(value, bytes) or len(value) != 32:
                raise ValueError("mssql_transaction.target_catalog_fingerprint_invalid")

    @property
    def sha256(self) -> bytes:
        """Backward-compatible alias for the immutable before-image."""

        return self.before_sha256

    def at(self, boundary: ExpectationBoundary) -> bytes:
        return self.before_sha256 if boundary == "before" else self.after_sha256


def exact_schema_catalog_transition(
    before: MssqlSchemaCatalogSnapshot,
    after: MssqlSchemaCatalogSnapshot,
) -> MssqlTargetCatalogExpectation:
    """Bind complete ``sys`` catalog before/after images for one schema phase."""

    return MssqlTargetCatalogExpectation(
        "schema_columns",
        _digest(before.to_schema_dict()),
        _digest(after.to_schema_dict()),
        "exact_sys_catalog_v5",
    )


def schema_catalog_expectation(
    *,
    target_exists: bool,
    columns: Sequence[ColumnDef],
    expected_after_columns: Sequence[ColumnDef] | None = None,
    expected_after_exists: bool | None = None,
) -> MssqlTargetCatalogExpectation:
    """Build a compatibility transition for legacy non-catalog adapters.

    Governed production preplanning uses :func:`exact_schema_catalog_transition`;
    this helper remains for adapters that expose only ``ColumnDef`` values.
    """

    before = _legacy_columns_payload(target_exists, columns)
    after = _legacy_columns_payload(
        target_exists if expected_after_exists is None else expected_after_exists,
        columns if expected_after_columns is None else expected_after_columns,
    )
    return MssqlTargetCatalogExpectation(
        "schema_columns",
        _digest(before),
        _digest(after),
        "legacy_columns_v1",
    )


def physical_catalog_expectation(
    state: PhysicalTableState,
    *,
    expected_after: PhysicalTableState | None = None,
) -> MssqlTargetCatalogExpectation:
    """Fingerprint exact physical before/after states from the reconciler."""

    return MssqlTargetCatalogExpectation(
        "physical_design",
        _digest(state.to_dict()),
        _digest((expected_after or state).to_dict()),
        "physical_state_v1",
    )


def assert_target_catalog_expectations(
    strategy: Any,
    load_config: Any,
    expectations: Sequence[MssqlTargetCatalogExpectation],
    *,
    boundary: ExpectationBoundary = "before",
    kinds: frozenset[ExpectationKind] | None = None,
) -> None:
    """Re-read and compare target catalog evidence under the operation lock."""

    selected = tuple(item for item in expectations if kinds is None or item.kind in kinds)
    for expectation in selected:
        actual = _current_digest(strategy, load_config, expectation)
        if actual != expectation.at(boundary):
            raise RuntimeError(f"mssql_transaction.target_catalog_{boundary}_mismatch:{expectation.kind}")


def aggregate_expectations(
    expectations: Sequence[MssqlTargetCatalogExpectation],
    *,
    boundary: ExpectationBoundary = "before",
) -> bytes:
    """Bind ordered phase expectations into one durable receipt digest."""

    return _digest(
        [
            {
                "kind": item.kind,
                "representation": item.representation,
                "sha256": item.at(boundary).hex(),
            }
            for item in expectations
        ]
    )


def _current_digest(
    strategy: Any,
    load_config: Any,
    expectation: MssqlTargetCatalogExpectation,
) -> bytes:
    if expectation.representation == "physical_state_v1":
        state = MssqlPhysicalIntrospector(strategy.connector).inspect(load_config)
        return _digest(state.to_dict())
    if expectation.representation == "exact_sys_catalog_v5":
        return _digest(read_schema_catalog_snapshot(strategy, load_config).to_schema_dict())
    return _legacy_schema_digest(strategy, load_config)


def _legacy_schema_digest(strategy: Any, load_config: Any) -> bytes:
    target = strategy._target_object(load_config)
    exists = bool(strategy._connector_table_exists(target))
    if not exists:
        return _digest(_legacy_columns_payload(False, ()))
    fetch = getattr(strategy.connector, "fetch_schema_columns", None)
    if not callable(fetch):
        raise RuntimeError("mssql_transaction.target_catalog_revalidation_unavailable")
    try:
        raw = fetch(target.schema, target.table, database=target.database)
    except TypeError:
        raw = fetch(target.schema_label, target.table)
    columns = tuple(
        ColumnDef(
            str(item.name),
            str(item.dtype),
            nullable=bool(item.nullable),
            collation=str(item.collation) if item.collation else None,
        )
        for item in raw
    )
    return _digest(_legacy_columns_payload(True, columns))


def _legacy_columns_payload(
    exists: bool,
    columns: Sequence[ColumnDef],
) -> dict[str, object]:
    return {
        "exists": bool(exists),
        "columns": [
            {
                "name": column.name,
                "dtype": column.dtype,
                "nullable": column.nullable,
                "collation": column.collation,
            }
            for column in columns
        ],
    }


def _digest(value: Mapping[str, Any] | Sequence[Any]) -> bytes:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        allow_nan=False,
    )
    return hashlib.sha256(encoded.encode("utf-8")).digest()


__all__ = [
    "MssqlTargetCatalogExpectation",
    "aggregate_expectations",
    "assert_target_catalog_expectations",
    "exact_schema_catalog_transition",
    "physical_catalog_expectation",
    "schema_catalog_expectation",
]
