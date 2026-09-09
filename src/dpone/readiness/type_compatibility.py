"""Target type compatibility gates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass

from dpone.type_system.models import ConflictPolicy

_VARIANT_PREFIX = "__dpone__nc__"


@dataclass(frozen=True, slots=True)
class TypeCompatibilityDecision:
    column: str
    source_type: str
    target_type: str | None
    compatible: bool
    action: str
    reason: str
    target_column: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class TypeCompatibilityReport:
    sink_type: str
    passed: bool
    decisions: tuple[TypeCompatibilityDecision, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "sink_type": self.sink_type,
            "passed": self.passed,
            "decisions": [item.to_dict() for item in self.decisions],
        }


class TypeCompatibilityGate:
    """Detect unsafe target type changes before a finalizer writes data."""

    def evaluate(
        self,
        *,
        sink_type: str,
        source_columns: Mapping[str, str],
        target_columns: Mapping[str, str],
        conflict_policy: ConflictPolicy = "fail",
    ) -> TypeCompatibilityReport:
        decisions = tuple(
            self._decision(
                column=column,
                source_type=str(source_type),
                target_type=_case_insensitive_get(target_columns, column),
                conflict_policy=conflict_policy,
            )
            for column, source_type in source_columns.items()
        )
        return TypeCompatibilityReport(
            sink_type=_normalize_sink(sink_type),
            passed=all(item.action != "fail" for item in decisions),
            decisions=decisions,
        )

    def _decision(
        self,
        *,
        column: str,
        source_type: str,
        target_type: str | None,
        conflict_policy: ConflictPolicy,
    ) -> TypeCompatibilityDecision:
        if target_type is None:
            return TypeCompatibilityDecision(
                column=column,
                source_type=source_type,
                target_type=None,
                compatible=True,
                action="add_column",
                reason="target column does not exist",
                target_column=column,
            )
        if _compatible(source_type, target_type):
            return TypeCompatibilityDecision(
                column=column,
                source_type=source_type,
                target_type=target_type,
                compatible=True,
                action="keep",
                reason="source and target type families are compatible",
                target_column=column,
            )
        if conflict_policy == "variant_column":
            return TypeCompatibilityDecision(
                column=column,
                source_type=source_type,
                target_type=target_type,
                compatible=False,
                action="variant_column",
                reason="incompatible type routed to framework variant column",
                target_column=f"{_VARIANT_PREFIX}{column}",
            )
        if conflict_policy == "quarantine":
            return TypeCompatibilityDecision(
                column=column,
                source_type=source_type,
                target_type=target_type,
                compatible=False,
                action="quarantine",
                reason="incompatible type should be quarantined before staging",
            )
        return TypeCompatibilityDecision(
            column=column,
            source_type=source_type,
            target_type=target_type,
            compatible=False,
            action="fail",
            reason="unsafe narrowing or incompatible target type",
        )


def _case_insensitive_get(values: Mapping[str, str], key: str) -> str | None:
    if key in values:
        return str(values[key])
    normalized = key.lower()
    for name, value in values.items():
        if str(name).lower() == normalized:
            return str(value)
    return None


def _compatible(source_type: str, target_type: str) -> bool:
    source_family = _family(source_type)
    target_family = _family(target_type)
    if source_family == target_family:
        return True
    if source_family in {"integer", "decimal"} and target_family in {"decimal", "float"}:
        return True
    if source_family == "date" and target_family == "timestamp":
        return True
    if target_family in {"string", "json"}:
        return True
    return False


def _family(raw_type: str) -> str:
    normalized = str(raw_type).strip().lower()
    if any(token in normalized for token in ("decimal", "numeric", "number")):
        return "decimal"
    if any(token in normalized for token in ("bigint", "int64", "int32", "integer", " int", "int")):
        return "integer"
    if any(token in normalized for token in ("float", "double", "real")):
        return "float"
    if any(token in normalized for token in ("bool", "bit")):
        return "boolean"
    if "timestamp" in normalized or "datetime" in normalized:
        return "timestamp"
    if normalized == "date" or normalized.endswith(" date"):
        return "date"
    if any(token in normalized for token in ("json", "variant", "object")):
        return "json"
    if any(token in normalized for token in ("binary", "byte", "bytes")):
        return "binary"
    return "string"


def _normalize_sink(sink_type: str) -> str:
    normalized = str(sink_type).strip().lower()
    if normalized in {"sqlserver", "sql_server"}:
        return "mssql"
    if normalized in {"bq", "google_bigquery"}:
        return "bigquery"
    return normalized


__all__ = ["TypeCompatibilityDecision", "TypeCompatibilityGate", "TypeCompatibilityReport"]
