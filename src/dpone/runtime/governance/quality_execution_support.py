"""Pure normalization, fingerprint, and evidence helpers for quality execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.governance.quality import QualityGateReport
    from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricPolicy


import hashlib
import json
import math
import re
from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType
from typing import Any, cast

from dpone.contracts.quality_failure import (
    QualityFailureBoundary,
    QualityGateReceiptInvalid,
)
from dpone.governance.quality import GATE_TYPES, QualityGatePolicy, quality_gate_policy_fingerprint

_CANONICAL_FINGERPRINT = re.compile(r"sha256:[0-9a-f]{64}\Z")
_SAFE_GATE_ID = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z")


def _immutable_gate_policy(policy: QualityGatePolicy) -> QualityGatePolicy:
    return QualityGatePolicy(gates=tuple(replace(gate, raw=MappingProxyType({})) for gate in policy.gates))


def _policy_snapshot_id(
    gate_policy: QualityGatePolicy,
    acceptance_policy: AcceptanceMetricPolicy,
) -> str:
    payload = {
        "schema": "dpone.quality.execution-policy.v1",
        "gate_policy_fingerprint": quality_gate_policy_fingerprint(gate_policy),
        "acceptance": {
            "enabled": acceptance_policy.enabled,
            "mode": acceptance_policy.mode,
            "capture_source": acceptance_policy.capture_source,
            "capture_staged": acceptance_policy.capture_staged,
            "capture_target": acceptance_policy.capture_target,
            "row_count": acceptance_policy.row_count,
            "null_counts": acceptance_policy.null_counts,
            "distinct_counts": acceptance_policy.distinct_counts,
        },
    }
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _immutable_safe_report(
    report: QualityGateReport,
    policy: QualityGatePolicy,
) -> QualityGateReport:
    results = tuple(
        replace(
            result,
            metrics=MappingProxyType(_safe_metrics(gate.type, result.metrics)),
            message=f"quality_gate_{result.status}" if result.message else "",
        )
        for gate, result in zip(policy.gates, report.results, strict=True)
    )
    contract = tuple(
        MappingProxyType(
            {
                "gate_id": gate.id,
                "type": gate.type,
                "severity": gate.severity,
            }
        )
        for gate in policy.gates
    )
    return replace(report, results=results, gate_contract=contract)


def _safe_metrics(gate_type: str, metrics: Mapping[str, Any]) -> dict[str, object]:
    if gate_type == "row_count_reconciliation":
        return _numeric_metrics(
            metrics,
            ("source_row_count", "target_row_count", "difference", "allowed_difference"),
        )
    if gate_type == "min_rows":
        projected = _numeric_metrics(metrics, ("row_count", "threshold"))
        side = metrics.get("side")
        if side in {"source", "target"}:
            projected["side"] = side
        return projected
    if gate_type == "typed_hash_reconciliation":
        projected = {}
        for key in ("source_hash", "target_hash"):
            present, value = _typed_hash_metric(metrics.get(key))
            if present:
                projected[key] = value
        mode = metrics.get("mode")
        if mode in {"full", "sample"}:
            projected["mode"] = mode
        return projected
    return {}


def _numeric_metrics(metrics: Mapping[str, Any], keys: tuple[str, ...]) -> dict[str, object]:
    projected: dict[str, object] = {}
    for key in keys:
        value = metrics.get(key)
        if value is None:
            projected[key] = None
        elif isinstance(value, int | float) and not isinstance(value, bool):
            try:
                number = float(value)
            except OverflowError:
                continue
            if value >= 0 and math.isfinite(number):
                projected[key] = value
    return projected


def _typed_hash_metric(value: object) -> tuple[bool, str | None]:
    if value is None:
        return True, None
    if not isinstance(value, str) or not value or len(value) > 4_096:
        return False, None
    if _CANONICAL_FINGERPRINT.fullmatch(value):
        return True, value
    return True, f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _immutable_scope(value: Mapping[str, object] | None) -> Mapping[str, object] | None:
    return MappingProxyType(dict(value)) if value is not None else None


def quality_gate_report_evidence(
    report: QualityGateReport,
    policy: QualityGatePolicy,
) -> dict[str, object]:
    """Project a validated quality report into bounded, safe evidence."""

    return {
        "kind": report.kind,
        "passed": report.passed,
        "policy_fingerprint": report.policy_fingerprint,
        "gate_contract": [
            {
                "gate_id": _evidence_gate_id(gate.id, ordinal),
                "type": gate.type if gate.type in GATE_TYPES else "unsupported",
                "severity": gate.severity,
            }
            for ordinal, gate in enumerate(policy.gates)
        ],
        "results": [
            {
                "gate_id": _evidence_gate_id(gate.id, ordinal),
                "type": gate.type if gate.type in GATE_TYPES else "unsupported",
                "status": result.status,
                "severity": result.severity,
                "metrics": _safe_metrics(gate.type, result.metrics),
                "message": f"quality_gate_{result.status}" if result.message else "",
            }
            for ordinal, (gate, result) in enumerate(zip(policy.gates, report.results, strict=True))
        ],
    }


def _evidence_gate_id(value: str, ordinal: int) -> str:
    return value if _SAFE_GATE_ID.fullmatch(value) else f"quality_gate_{ordinal}"


def _quality_boundary(value: object) -> QualityFailureBoundary:
    if not isinstance(value, str) or value not in {"pre_commit", "post_commit", "resume_validation"}:
        raise QualityGateReceiptInvalid
    return cast(QualityFailureBoundary, value)


def _is_bounded_identity(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


def _quality_config(load_config: Any) -> object:
    options = getattr(load_config, "options", None)
    return options.get("quality") if isinstance(options, Mapping) else None
