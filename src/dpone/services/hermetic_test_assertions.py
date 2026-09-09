"""Pure, metadata-only assertions for hermetic test results."""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any


def infer_json_schema(rows: Sequence[Mapping[str, Any]]) -> dict[str, str]:
    """Infer a small deterministic logical type vocabulary from final rows."""

    columns = sorted({str(column) for row in rows for column in row})
    result: dict[str, str] = {}
    for column in columns:
        observed = {_logical_type(row.get(column)) if column in row else "null" for row in rows}
        non_null = observed - {"null"}
        if not non_null:
            result[column] = "null"
        elif len(non_null) == 1:
            result[column] = next(iter(non_null))
        else:
            result[column] = "mixed"
    return result


def evaluate_expectations(
    rows: Sequence[Mapping[str, Any]],
    *,
    expected_rows: int,
    expected_rejected_rows: int,
    expected_schema: Sequence[tuple[str, str]],
    expected_output: Sequence[Mapping[str, Any]] | None,
) -> tuple[dict[str, Any], ...]:
    """Return safe mismatch metadata without fixture row values or hashes."""

    failures: list[dict[str, Any]] = []
    if len(rows) != expected_rows:
        failures.append(
            _failure(
                "DPONE_TEST_EXPECTED_ROWS_MISMATCH",
                "expect.rows",
                expected=expected_rows,
                observed=len(rows),
            )
        )
    if expected_rejected_rows != 0:
        failures.append(
            _failure(
                "DPONE_TEST_EXPECTED_REJECTED_ROWS_MISMATCH",
                "expect.rejected_rows",
                expected=expected_rejected_rows,
                observed=0,
            )
        )
    observed_schema = infer_json_schema(rows)
    for column, expected_type in expected_schema:
        observed_type = observed_schema.get(column, "missing")
        if observed_type != expected_type:
            failures.append(
                {
                    "code": "DPONE_TEST_EXPECTED_SCHEMA_MISMATCH",
                    "path": f"expect.schema.{column}",
                    "message": "Observed logical type does not match the expectation.",
                    "expected": expected_type,
                    "observed": observed_type,
                }
            )
    if expected_output is not None and _row_multiset(rows) != _row_multiset(expected_output):
        failures.append(
            _failure(
                "DPONE_TEST_EXPECTED_OUTPUT_MISMATCH",
                "expect.output_fixture",
                expected=len(expected_output),
                observed=len(rows),
            )
        )
    return tuple(failures)


def _logical_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int64"
    if isinstance(value, float):
        return "float64"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "mixed"


def _row_multiset(rows: Sequence[Mapping[str, Any]]) -> Counter[str]:
    return Counter(
        json.dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        for row in rows
    )


def _failure(code: str, path: str, *, expected: int, observed: int) -> dict[str, Any]:
    return {
        "code": code,
        "path": path,
        "message": "Observed result does not match the expectation.",
        "expected": expected,
        "observed": observed,
    }


__all__ = ["evaluate_expectations", "infer_json_schema"]
