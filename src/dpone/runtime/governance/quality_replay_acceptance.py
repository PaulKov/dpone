"""Validate every persisted acceptance observation against its original policy."""

from __future__ import annotations

from typing import Any

from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricPolicy
from dpone.runtime.governance.acceptance_snapshot import ACCEPTANCE_SNAPSHOT_KIND, selected_columns
from dpone.runtime.quality_replay_contracts import contracts

_FIELDS = frozenset(
    {
        "kind",
        "side",
        "dataset",
        "columns",
        "row_count",
        "null_counts",
        "distinct_counts",
        "warnings",
        "captured_before_cleanup",
    }
)
_WARNINGS = frozenset(
    {
        "acceptance_metric_unknown_columns",
        "acceptance_metric_probe_failed",
        "acceptance_metric_snapshot_invalid",
        "source_acceptance_metric_probe_unavailable",
        "target_acceptance_metric_probe_unavailable",
        "staged_acceptance_metric_unavailable",
    }
)


def validate_replay_acceptance(
    policy: AcceptanceMetricPolicy, observations: dict[str, Any], *, before_target: bool = False
) -> None:
    """Missing fields never become implicit warn-only observations."""
    expected = set(policy.requested_sides) if policy.enabled else set()
    if before_target:
        expected.discard("target")
    if set(observations) != expected:
        raise contracts.ReplayQualityEvidenceError("INCOMPLETE")
    for side, record in observations.items():
        if not isinstance(record, dict) or set(record) != _FIELDS:
            raise contracts.ReplayQualityEvidenceError("INVALID")
        if (
            record["kind"] != ACCEPTANCE_SNAPSHOT_KIND
            or record["side"] != side
            or record["captured_before_cleanup"] is not True
            or not isinstance(record["dataset"], str)
        ):
            raise contracts.ReplayQualityEvidenceError("INVALID")
        columns, warnings = record["columns"], record["warnings"]
        if (
            not isinstance(columns, list)
            or any(not isinstance(c, str) or not c for c in columns)
            or len(set(columns)) != len(columns)
            or not isinstance(warnings, list)
            or any(not isinstance(w, str) or w not in _WARNINGS for w in warnings)
        ):
            raise contracts.ReplayQualityEvidenceError("INVALID")
        if warnings and policy.mode != "warn_only":
            raise contracts.ReplayQualityEvidenceError("FAILED")
        unavailable = bool(set(warnings) - {"acceptance_metric_unknown_columns"})
        count = record["row_count"]
        if count is not None and (type(count) is not int or count < 0):
            raise contracts.ReplayQualityEvidenceError("INVALID")
        if policy.row_count and count is None and not unavailable:
            raise contracts.ReplayQualityEvidenceError("INCOMPLETE")
        if before_target and count is not None and count > 2**64 - 1:
            raise contracts.ReplayQualityEvidenceError("INVALID")
        if not policy.row_count and count is not None:
            raise contracts.ReplayQualityEvidenceError("INVALID")
        for field, selection in (("null_counts", policy.null_counts), ("distinct_counts", policy.distinct_counts)):
            if isinstance(selection, tuple) and set(selection) - set(columns):
                if policy.mode == "required" or "acceptance_metric_unknown_columns" not in warnings:
                    raise contracts.ReplayQualityEvidenceError("INCOMPLETE")
            metrics = record[field]
            if not isinstance(metrics, dict) or any(type(v) is not int or v < 0 for v in metrics.values()):
                raise contracts.ReplayQualityEvidenceError("INVALID")
            if before_target and any(value > 2**64 - 1 for value in metrics.values()):
                raise contracts.ReplayQualityEvidenceError("INVALID")
            requested = set(selected_columns(selection, columns))
            if set(metrics) - requested or (set(metrics) != requested and not unavailable):
                raise contracts.ReplayQualityEvidenceError("INCOMPLETE")
