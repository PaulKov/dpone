"""Explain schema evolution decisions with pair-specific type matrix context."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.readiness.schema_type_compatibility import types_equal
from dpone.services.schema_type_matrix import PairTypeMatrixService


class SchemaEvolutionExplainService:
    """Build operator-friendly diagnostics for schema evolution plans."""

    def __init__(self, type_matrix: PairTypeMatrixService | None = None) -> None:
        self._type_matrix = type_matrix or PairTypeMatrixService()

    def explain(
        self,
        *,
        source_system: str,
        sink_system: str,
        source: Sequence[ColumnDef],
        target: Sequence[ColumnDef],
        policy: SchemaEvolutionPolicy | None = None,
    ) -> dict[str, Any]:
        source_key = source_system.strip().lower()
        sink_key = sink_system.strip().lower()
        resolved_policy = policy or SchemaEvolutionPolicy()
        plan = SchemaComparator(resolved_policy).compare(list(source), list(target))
        target_by_name = {column.name.lower(): column for column in target}
        type_decisions = [
            self._explain_column(source_key, sink_key, source_column, target_by_name.get(source_column.name.lower()))
            for source_column in source
        ]
        return {
            "source_system": source_key,
            "sink_system": sink_key,
            "has_breaking_changes": plan.has_breaking_changes,
            "schema_plan": plan.to_dict(),
            "type_decisions": type_decisions,
        }

    def _explain_column(
        self,
        source_system: str,
        sink_system: str,
        source: ColumnDef,
        target: ColumnDef | None,
    ) -> dict[str, Any]:
        target_type = target.dtype if target else None
        raw_types_equal = bool(target and types_equal(source.dtype, target.dtype))
        matrix_entry = self._matrix_entry(source_system, sink_system, source)
        expected_target = str(matrix_entry.get("target_type")) if matrix_entry else None
        matrix_matches_target = bool(target and expected_target and types_equal(expected_target, target.dtype))
        return {
            "column": source.name,
            "source_type": source.dtype,
            "target_type": target_type,
            "raw_types_equal": raw_types_equal,
            "matrix_profile": matrix_entry.get("profile") if matrix_entry else None,
            "matrix_expected_target_type": expected_target,
            "matrix_canonical_type": matrix_entry.get("canonical_type") if matrix_entry else None,
            "matrix_decision_category": matrix_entry.get("decision_category") if matrix_entry else None,
            "matrix_decision_source": matrix_entry.get("decision_source") if matrix_entry else None,
            "matrix_native_transport": matrix_entry.get("native_transport") if matrix_entry else None,
            "matrix_matches_target": matrix_matches_target,
            "diagnostic": self._diagnostic(target, raw_types_equal, matrix_matches_target, expected_target),
        }

    def _matrix_entry(self, source_system: str, sink_system: str, source: ColumnDef) -> dict[str, Any] | None:
        try:
            payload = self._type_matrix.build(
                source=source_system,
                sink=sink_system,
                source_types=(f"{source.name}:{source.dtype}{' nullable' if source.nullable else ''}",),
            )
        except ValueError:
            return None
        entries = payload.get("entries", [])
        if not entries or not isinstance(entries[0], dict):
            return None
        entry = dict(entries[0])
        entry["profile"] = payload.get("profile")
        return entry

    def _diagnostic(
        self,
        target: ColumnDef | None,
        raw_types_equal: bool,
        matrix_matches_target: bool,
        expected_target: str | None,
    ) -> str:
        if not target:
            return "source column is missing in target"
        if matrix_matches_target:
            return "target type matches the source -> sink type matrix profile"
        if raw_types_equal:
            return "raw type compatibility check already accepts this pair"
        if expected_target:
            return f"target differs from expected source -> sink profile type {expected_target}"
        return "no pair-specific type matrix profile is available"


__all__ = ["SchemaEvolutionExplainService"]
