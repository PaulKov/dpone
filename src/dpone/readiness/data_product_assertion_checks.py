"""Assertion result helpers for data product quality suites."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def evaluate_plan_assertions(
    plan: Mapping[str, Any], runtime: Mapping[str, Any], target: Mapping[str, Any]
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    suites = plan.get("suites", [])
    for suite in suites if isinstance(suites, list) else ():
        if not isinstance(suite, Mapping):
            continue
        assertions = suite.get("assertions", [])
        for assertion in assertions if isinstance(assertions, list) else ():
            if isinstance(assertion, Mapping):
                results.append(_evaluate_assertion(assertion, runtime, target))
    return results


def aggregate_assertion_results(
    assertions: Sequence[Mapping[str, Any]], blockers: list[str], warnings: list[str]
) -> None:
    for assertion in assertions:
        assertion_id = assertion.get("id")
        severity = assertion.get("severity")
        if assertion.get("status") == "failed":
            code = f"data_product_assertions.assertion_failed:{assertion_id}"
            (blockers if severity in {"critical", "high"} else warnings).append(code)
        if assertion.get("status") == "skipped":
            warnings.append(f"data_product_assertions.assertion_skipped:{assertion_id}")


def assertion_summary(assertions: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "passed": sum(1 for item in assertions if item.get("status") == "passed"),
        "failed": sum(1 for item in assertions if item.get("status") == "failed"),
        "warning": sum(1 for item in assertions if item.get("status") == "warning"),
        "skipped": sum(1 for item in assertions if item.get("status") == "skipped"),
    }


def _evaluate_assertion(
    assertion: Mapping[str, Any], runtime: Mapping[str, Any], target: Mapping[str, Any]
) -> dict[str, Any]:
    assertion_type = str(assertion.get("type") or "")
    if assertion_type == "freshness":
        return _threshold(assertion, _number(runtime.get("freshness_lag_seconds")), "max_lag_seconds")
    if assertion_type == "row_count":
        rows = _number(target.get("row_count"), runtime.get("rows_loaded"), runtime.get("row_count"))
        return _row_count(assertion, rows)
    if assertion_type in {"null_key", "duplicate_key"}:
        key = "null_key_failures" if assertion_type == "null_key" else "duplicate_key_failures"
        failures = sum(_number(_mapping(target.get(key)).get(column)) or 0 for column in _columns(assertion))
        return _failure_count(assertion, failures)
    if assertion_type in {"accepted_values", "regex", "range"}:
        failures = _number(_mapping(target.get(f"{assertion_type}_failures")).get(str(assertion.get("column") or "")))
        return _failure_count(assertion, failures)
    if assertion_type == "sql":
        return _sql_result(assertion, target)
    if assertion_type in {"typed_hash", "schema", "imported"}:
        status = str(target.get(f"{assertion_type}_status") or "skipped")
        return _result(assertion, "passed" if status == "passed" else "skipped", [], {"status": status})
    return _result(assertion, "skipped", [f"data_product_assertions.unsupported_type:{assertion.get('id')}"], {})


def _threshold(assertion: Mapping[str, Any], actual: float | None, limit_key: str) -> dict[str, Any]:
    limit = _number(assertion.get(limit_key))
    if actual is None:
        return _missing(assertion)
    failed = limit is not None and actual > limit
    return _result(assertion, "failed" if failed else "passed", [], {"actual": actual, limit_key: limit})


def _row_count(assertion: Mapping[str, Any], rows: float | None) -> dict[str, Any]:
    if rows is None:
        return _missing(assertion)
    min_rows = _number(assertion.get("min_rows"))
    max_rows = _number(assertion.get("max_rows"))
    failed = (min_rows is not None and rows < min_rows) or (max_rows is not None and rows > max_rows)
    return _result(assertion, "failed" if failed else "passed", [], {"rows": rows})


def _failure_count(assertion: Mapping[str, Any], failures: float | None) -> dict[str, Any]:
    if failures is None:
        return _missing(assertion)
    limit = _number(assertion.get("max_failures")) or 0
    return _result(assertion, "failed" if failures > limit else "passed", [], {"failures": failures})


def _sql_result(assertion: Mapping[str, Any], target: Mapping[str, Any]) -> dict[str, Any]:
    row = _mapping(_mapping(target.get("sql_results")).get(str(assertion.get("id") or "")))
    expect = _mapping(assertion.get("expect"))
    column = str(expect.get("column") or "")
    if not row or column not in row:
        return _missing(assertion)
    expected = expect.get("equals")
    failed = expected is not None and row.get(column) != expected
    return _result(assertion, "failed" if failed else "passed", [], {"actual": row.get(column), "expected": expected})


def _missing(assertion: Mapping[str, Any]) -> dict[str, Any]:
    return _result(assertion, "skipped", [f"data_product_assertions.evidence_missing:{assertion.get('id')}"], {})


def _result(
    assertion: Mapping[str, Any], status: str, details: Sequence[str], metrics: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "id": str(assertion.get("id") or ""),
        "type": str(assertion.get("type") or ""),
        "severity": str(assertion.get("severity") or "high"),
        "status": status,
        "details": list(details),
        "metrics": dict(metrics),
    }


def _columns(assertion: Mapping[str, Any]) -> tuple[str, ...]:
    raw = assertion.get("columns") or ([assertion.get("column")] if assertion.get("column") else [])
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _number(*raw_values: object) -> float | None:
    for raw in raw_values:
        if raw in {None, ""}:
            continue
        try:
            return float(raw)
        except (TypeError, ValueError):
            continue
    return None


__all__ = ("aggregate_assertion_results", "assertion_summary", "evaluate_plan_assertions")
