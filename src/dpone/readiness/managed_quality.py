"""Lightweight quality contract evaluator."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.readiness.managed_models import (
    QualityCheckOutcome,
    QualityOutcomeStatus,
    QualityRunResult,
)

_LIVE_CHECK_TYPES = frozenset({"freshness", "row_count_delta", "source_target_count"})


class QualityService:
    """Runs lightweight data quality checks over materialized row samples."""

    def run_checks(self, rows: Iterable[Mapping[str, Any]], checks: Sequence[Mapping[str, Any]]) -> QualityRunResult:
        materialized = [dict(row) for row in rows]
        outcomes = tuple(self._run_one(materialized, check) for check in checks)
        return QualityRunResult(outcomes)

    def _run_one(self, rows: list[dict[str, Any]], check: Mapping[str, Any]) -> QualityCheckOutcome:
        check_type = str(check.get("type", "unknown"))
        mode = str(check.get("mode", "fail"))
        try:
            if mode == "skip":
                return QualityCheckOutcome(
                    check_type,
                    QualityOutcomeStatus.SKIPPED,
                    mode,
                    "check skipped by config",
                    {"executed": False},
                )
            if check_type in _LIVE_CHECK_TYPES:
                return QualityCheckOutcome(
                    check_type,
                    QualityOutcomeStatus.UNVERIFIED,
                    mode,
                    f"{check_type} requires live connector evidence",
                    {
                        "evidence_profile": "plan_only",
                        "executed": False,
                    },
                )
            if not rows:
                return QualityCheckOutcome(
                    check_type,
                    QualityOutcomeStatus.UNVERIFIED,
                    mode,
                    "quality check requires at least one materialized row",
                    {
                        "evidence_profile": "empty_run",
                        "executed": False,
                    },
                )
            ok, message, details = self._evaluate(rows, check_type, check)
            status = QualityOutcomeStatus.PASSED if ok else QualityOutcomeStatus.FAILED
            return QualityCheckOutcome(check_type, status, mode, message, details)
        except Exception:  # noqa: BLE001 - boundary returns a safe failed outcome.
            return QualityCheckOutcome(
                check_type,
                QualityOutcomeStatus.FAILED,
                mode,
                "quality check could not be evaluated safely",
            )

    def _evaluate(
        self, rows: list[dict[str, Any]], check_type: str, check: Mapping[str, Any]
    ) -> tuple[bool, str, dict[str, Any]]:
        if check_type == "not_null":
            column = str(check["column"])
            nulls = sum(1 for row in rows if row.get(column) is None)
            return nulls == 0, f"{column} null_count={nulls}", {"column": column, "null_count": nulls}
        if check_type == "unique":
            columns = tuple(str(item) for item in check.get("columns", [check.get("column")]))
            seen: set[tuple[Any, ...]] = set()
            duplicates = 0
            for row in rows:
                key = tuple(row.get(column) for column in columns)
                if key in seen:
                    duplicates += 1
                seen.add(key)
            return (
                duplicates == 0,
                f"{','.join(columns)} duplicate_count={duplicates}",
                {"columns": columns, "duplicate_count": duplicates},
            )
        if check_type == "accepted_values":
            column = str(check["column"])
            accepted = set(check.get("values", ()))
            bad = [row.get(column) for row in rows if row.get(column) not in accepted]
            return not bad, f"{column} invalid_count={len(bad)}", {"column": column, "invalid_count": len(bad)}
        if check_type == "min_rows":
            minimum = int(check.get("value", 1))
            return len(rows) >= minimum, f"rows={len(rows)} minimum={minimum}", {"rows": len(rows), "minimum": minimum}
        if check_type == "max_null_ratio":
            column = str(check["column"])
            max_ratio = float(check.get("value", 0))
            ratio = (sum(1 for row in rows if row.get(column) is None) / len(rows)) if rows else 0.0
            return (
                ratio <= max_ratio,
                f"{column} null_ratio={ratio:.4f}",
                {"column": column, "null_ratio": ratio, "max_ratio": max_ratio},
            )
        if check_type == "checksum":
            digest = hashlib.sha256(json.dumps(rows, sort_keys=True, default=str).encode("utf-8")).hexdigest()
            expected = check.get("value")
            return expected in (None, digest), "checksum calculated", {"checksum": digest}
        return False, f"unsupported quality check: {check_type}", {}


__all__ = ["QualityService"]
