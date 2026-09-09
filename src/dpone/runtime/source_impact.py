from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class SourceImpactDiagnostic:
    code: str
    severity: str
    message: str
    action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class SourceImpactInspector:
    """Static source-query diagnostics for native transfer planning UX."""

    def inspect(
        self,
        *,
        source_type: str,
        base_query: str,
        partition_column: str | None = None,
        indexed_columns: tuple[str, ...] = (),
        source_kind: str = "table",
    ) -> tuple[SourceImpactDiagnostic, ...]:
        del source_type
        query = str(base_query)
        diagnostics: list[SourceImpactDiagnostic] = []
        if re.search(r"\bselect\s+\*", query, flags=re.IGNORECASE):
            diagnostics.append(
                _diag(
                    "source_select_star",
                    "warning",
                    "Source query exports all columns.",
                    "Configure source.options.columns to reduce transfer width.",
                )
            )
        if source_kind.lower() in {"view", "materialized_view"}:
            diagnostics.append(
                _diag(
                    "source_view_over_view_risk",
                    "warning",
                    "Source is a view; optimizer pushdown may be weaker than on a base table.",
                    "Review generated SQL and use an indexed boundary column.",
                )
            )
        if partition_column:
            normalized_indexes = {item.lower() for item in indexed_columns}
            if partition_column.lower() not in normalized_indexes:
                diagnostics.append(
                    _diag(
                        "source_boundary_index_unknown",
                        "warning",
                        "Partition boundary column is not known to be indexed.",
                        "Use an indexed primary/clustered/keyset column for large native transfers.",
                    )
                )
            if _non_sargable(query, partition_column):
                diagnostics.append(
                    _diag(
                        "source_non_sargable_boundary",
                        "high",
                        "Boundary predicate appears wrapped in a function or conversion.",
                        "Keep partition predicates directly on the boundary column.",
                    )
                )
        return tuple(diagnostics)


def _non_sargable(query: str, column: str) -> bool:
    escaped = re.escape(column)
    patterns = (
        rf"\bcast\s*\([^)]*\b{escaped}\b",
        rf"\bconvert\s*\([^)]*\b{escaped}\b",
        rf"\bisnull\s*\(\s*\b{escaped}\b",
        rf"\bcoalesce\s*\(\s*\b{escaped}\b",
    )
    return any(re.search(pattern, query, flags=re.IGNORECASE) for pattern in patterns)


def _diag(code: str, severity: str, message: str, action: str) -> SourceImpactDiagnostic:
    return SourceImpactDiagnostic(code=code, severity=severity, message=message, action=action)


__all__ = ["SourceImpactDiagnostic", "SourceImpactInspector"]
