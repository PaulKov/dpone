"""Generated SELECT-only plans and exact aggregate result validation."""

from __future__ import annotations

import re
from typing import Any

from dpone.contracts.target_acceptance import TargetAcceptanceError, TargetAcceptanceRequest, require_count

# Conservative native scalar support; selected nested/aggregate types fail before publication.
_SCALAR = re.compile(
    r"^(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|String|FixedString\(\d+\)|UUID|Bool|Date|Date32|DateTime(?:\('[^']+'\))?|DateTime64\(\d+(?:, '[^']+')?\)|Decimal(?:32|64|128|256)?\([\d, ]+\)|Enum(?:8|16)\(.+\))$"
)


def quote(value: str) -> str:
    """Backtick identifiers escape both backslashes and embedded backticks."""
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def metric_plan(request: TargetAcceptanceRequest) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Ordinal aliases cannot collide with payload column names or metric kinds."""
    request.validate()
    selected = set(request.null_columns) | set(request.distinct_columns)
    for name, kind in request.columns:
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            kind = kind[kind.index("(") + 1 : -1]
        if name in selected and not _SCALAR.fullmatch(kind):
            raise TargetAcceptanceError("UNSUPPORTED")
    metrics: list[tuple[str, str]] = []
    expressions: list[str] = []
    if request.row_count:
        metrics.append(("row_count", ""))
        expressions.append("count()")
    for field, columns in (("null_counts", request.null_columns), ("distinct_counts", request.distinct_columns)):
        for column in columns:
            metrics.append((field, column))
            function = "countIf(isNull({}))" if field == "null_counts" else "uniqExact({})"
            expressions.append(function.format(quote(column)))
    # Empty metric selections still require a real aggregate row, never table rows.
    if not expressions:
        expressions.append("count()")
        metrics.append(("_discard", ""))
    fields = ", ".join(f"{expression} AS __dpone_m{index}" for index, expression in enumerate(expressions))
    return f"SELECT {fields} FROM {quote(request.database)}.{quote(request.table)}", tuple(metrics)


def parse_metrics(rows: Any, columns: Any, plan: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    """One row, exact ordinal aliases and exact UInt64 values; no default zero."""
    if not isinstance(rows, (list, tuple)) or len(rows) != 1:
        raise TargetAcceptanceError()
    aliases = [(f"__dpone_m{index}", "UInt64") for index in range(len(plan))]
    if columns != aliases or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != len(plan):
        raise TargetAcceptanceError()
    result: dict[str, Any] = {"row_count": None, "null_counts": {}, "distinct_counts": {}}
    for (field, name), value in zip(plan, rows[0], strict=True):
        require_count(value)
        if field == "row_count":
            result[field] = value
        elif field != "_discard":
            result[field][name] = value
    return result
