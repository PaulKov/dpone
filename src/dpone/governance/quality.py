"""Connector-neutral runtime data quality gates."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dpone.governance.quality_normalize import normalize_quality_config

GATE_TYPES = frozenset(
    {
        "row_count_reconciliation",
        "min_rows",
        "typed_hash_reconciliation",
        "custom_sql",
        "not_null",
        "unique",
        "freshness",
        "accepted_values",
    }
)
GATE_SEVERITIES = frozenset({"error", "warning"})
GATE_SIDES = frozenset({"source", "target"})
GATE_STATUSES = frozenset({"passed", "failed", "warning", "skipped"})
TOLERANCE_MODES = frozenset({"absolute", "pct"})
HASH_MODES = frozenset({"full", "sample"})
_GATE_FIELDS = frozenset(
    {
        "id",
        "type",
        "severity",
        "side",
        "threshold",
        "tolerance",
        "mode",
        "sql",
        "_compatibility_unknown_check",
    }
)


@dataclass(frozen=True, slots=True)
class QualityGateDefinition:
    id: str
    type: str
    severity: str = "error"
    side: str = "target"
    tolerance_mode: str = "absolute"
    tolerance_value: float = 0.0
    threshold: int | None = None
    mode: str = "full"
    sql: str | None = None
    raw: Mapping[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_config(cls, raw: Mapping[str, Any]) -> QualityGateDefinition:
        gate_id = str(raw.get("id") or raw.get("type") or "").strip()
        gate_type = str(raw.get("type") or "").strip()
        if not gate_id:
            raise ValueError("quality gate id is required")
        severity = str(raw["severity"]).strip().lower() if "severity" in raw else "error"
        if severity not in GATE_SEVERITIES:
            choices = ", ".join(sorted(GATE_SEVERITIES))
            raise ValueError(f"quality gate {gate_id} severity must be one of: {choices}")
        compatibility_unknown = raw.get("_compatibility_unknown_check") is True
        if gate_type not in GATE_TYPES and not (compatibility_unknown and severity == "warning"):
            raise ValueError(f"quality gate {gate_id} type must be one of: {', '.join(sorted(GATE_TYPES))}")
        _reject_unknown_gate_fields(raw, gate_id=gate_id)
        if gate_type == "min_rows" and "threshold" not in raw:
            raise ValueError(f"quality gate {gate_id} min_rows requires threshold")
        side = str(raw["side"]).strip().lower() if "side" in raw else "target"
        if side not in GATE_SIDES:
            raise ValueError(f"quality gate {gate_id} side must be one of: {', '.join(sorted(GATE_SIDES))}")
        tolerance_raw = raw.get("tolerance")
        if "tolerance" in raw and not isinstance(tolerance_raw, Mapping):
            raise ValueError(f"quality gate {gate_id} tolerance must be a mapping")
        tolerance = tolerance_raw or {}
        tolerance_mode = str(tolerance["mode"]).strip().lower() if "mode" in tolerance else "absolute"
        if tolerance_mode not in TOLERANCE_MODES:
            raise ValueError(
                f"quality gate {gate_id} tolerance mode must be one of: {', '.join(sorted(TOLERANCE_MODES))}"
            )
        mode = str(raw["mode"]).strip().lower() if "mode" in raw else "full"
        if mode not in HASH_MODES:
            raise ValueError(f"quality gate {gate_id} mode must be one of: {', '.join(sorted(HASH_MODES))}")
        threshold = _optional_non_negative_int(
            raw.get("threshold"),
            field=f"quality gate {gate_id} threshold",
        )
        if gate_type == "min_rows" and threshold is None:
            raise ValueError(f"quality gate {gate_id} threshold must be a non-negative integer")
        return cls(
            id=gate_id,
            type=gate_type,
            severity=severity,
            side=side,
            tolerance_mode=tolerance_mode,
            tolerance_value=_non_negative_finite_number(
                tolerance.get("value", 0),
                field=f"quality gate {gate_id} tolerance value",
            ),
            threshold=threshold,
            mode=mode,
            sql=str(raw.get("sql")) if raw.get("sql") is not None else None,
            raw=dict(raw),
        )


@dataclass(frozen=True, slots=True)
class QualityGatePolicy:
    gates: tuple[QualityGateDefinition, ...] = ()

    @classmethod
    def from_config(cls, raw: object) -> QualityGatePolicy:
        values = normalize_quality_config(raw)
        raw_gates = values.get("gates", ())
        if raw_gates is None:
            raw_gates = ()
        if not isinstance(raw_gates, list | tuple):
            raise ValueError("quality.gates must be a list")
        gates: list[QualityGateDefinition] = []
        gate_ids: set[str] = set()
        for index, item in enumerate(raw_gates):
            if not isinstance(item, Mapping):
                raise ValueError(f"quality.gates[{index}] must be a mapping")
            gate = QualityGateDefinition.from_config(item)
            if gate.id in gate_ids:
                raise ValueError(f"duplicate quality gate id: {gate.id}")
            gate_ids.add(gate.id)
            gates.append(gate)
        return cls(gates=tuple(gates))


@dataclass(frozen=True, slots=True)
class QualityProbeSnapshot:
    row_count: int | None = None
    typed_hash: str | None = None
    metrics: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject malformed explicit probe evidence at the typed boundary."""

        if self.row_count is not None and (
            isinstance(self.row_count, bool) or not isinstance(self.row_count, int) or self.row_count < 0
        ):
            raise ValueError("quality probe snapshot row_count must be a non-negative integer or None")
        if self.typed_hash is not None and (not isinstance(self.typed_hash, str) or not self.typed_hash):
            raise ValueError("quality probe snapshot typed_hash must be a non-empty string or None")


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    gate_id: str
    type: str
    status: str
    severity: str
    metrics: Mapping[str, Any] = field(default_factory=dict)
    message: str = ""

    @property
    def blocking(self) -> bool:
        return self.severity == "error" and self.status != "passed"


@dataclass(frozen=True, slots=True)
class QualityGateReport:
    results: tuple[QualityGateResult, ...]
    kind: str = "dpone.data_quality.gates.v1"
    policy_fingerprint: str | None = None
    gate_contract: tuple[Mapping[str, str], ...] = ()

    @property
    def passed(self) -> bool:
        return not any(result.blocking for result in self.results)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "passed": self.passed,
            "policy_fingerprint": self.policy_fingerprint,
            "gate_contract": [dict(item) for item in self.gate_contract],
            "results": [
                {
                    "gate_id": result.gate_id,
                    "type": result.type,
                    "status": result.status,
                    "severity": result.severity,
                    "metrics": dict(result.metrics),
                    "message": result.message,
                }
                for result in self.results
            ],
        }


class QualityGateRunner:
    """Evaluate quality gates against source and target probe snapshots."""

    def run(
        self,
        policy: QualityGatePolicy,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ) -> QualityGateReport:
        return QualityGateReport(
            results=tuple(self._evaluate(gate, source=source, target=target) for gate in policy.gates),
            policy_fingerprint=quality_gate_policy_fingerprint(policy),
            gate_contract=quality_gate_contract(policy),
        )

    def _evaluate(
        self,
        gate: QualityGateDefinition,
        *,
        source: QualityProbeSnapshot,
        target: QualityProbeSnapshot,
    ) -> QualityGateResult:
        if gate.type == "row_count_reconciliation":
            return _row_count(gate, source, target)
        if gate.type == "min_rows":
            snapshot = source if gate.side == "source" else target
            return _min_rows(gate, snapshot)
        if gate.type == "typed_hash_reconciliation":
            return _typed_hash(gate, source, target)
        status = "failed" if gate.severity == "error" else "skipped"
        return QualityGateResult(
            gate_id=gate.id,
            type=gate.type,
            status=status,
            severity=gate.severity,
            message=f"quality gate type {gate.type!r} is not implemented",
        )


def _row_count(
    gate: QualityGateDefinition,
    source: QualityProbeSnapshot,
    target: QualityProbeSnapshot,
) -> QualityGateResult:
    source_count = source.row_count
    target_count = target.row_count
    if source_count is None or target_count is None:
        return QualityGateResult(
            gate_id=gate.id,
            type=gate.type,
            status=_failure_status(gate),
            severity=gate.severity,
            metrics={
                "source_row_count": source_count,
                "target_row_count": target_count,
                "difference": None,
                "allowed_difference": None,
            },
            message="row count probe unavailable",
        )
    difference = abs(source_count - target_count)
    allowed = _allowed_delta(gate, source_count)
    status = "passed" if difference <= allowed else _failure_status(gate)
    return QualityGateResult(
        gate_id=gate.id,
        type=gate.type,
        status=status,
        severity=gate.severity,
        metrics={
            "source_row_count": source_count,
            "target_row_count": target_count,
            "difference": difference,
            "allowed_difference": allowed,
        },
    )


def _min_rows(gate: QualityGateDefinition, snapshot: QualityProbeSnapshot) -> QualityGateResult:
    threshold = gate.threshold or 0
    row_count = snapshot.row_count
    if row_count is None:
        return QualityGateResult(
            gate_id=gate.id,
            type=gate.type,
            status=_failure_status(gate),
            severity=gate.severity,
            metrics={"row_count": None, "threshold": threshold, "side": gate.side},
            message="row count probe unavailable",
        )
    status = "passed" if row_count >= threshold else _failure_status(gate)
    return QualityGateResult(
        gate_id=gate.id,
        type=gate.type,
        status=status,
        severity=gate.severity,
        metrics={"row_count": row_count, "threshold": threshold, "side": gate.side},
    )


def _typed_hash(
    gate: QualityGateDefinition,
    source: QualityProbeSnapshot,
    target: QualityProbeSnapshot,
) -> QualityGateResult:
    if not source.typed_hash or not target.typed_hash:
        status = "warning" if gate.severity == "warning" else "failed"
        return QualityGateResult(
            gate_id=gate.id,
            type=gate.type,
            status=status,
            severity=gate.severity,
            metrics={"source_hash": source.typed_hash, "target_hash": target.typed_hash, "mode": gate.mode},
            message="typed hash unavailable",
        )
    status = (
        "passed" if source.typed_hash == target.typed_hash else ("warning" if gate.severity == "warning" else "failed")
    )
    return QualityGateResult(
        gate_id=gate.id,
        type=gate.type,
        status=status,
        severity=gate.severity,
        metrics={"source_hash": source.typed_hash, "target_hash": target.typed_hash, "mode": gate.mode},
    )


def _allowed_delta(gate: QualityGateDefinition, source_count: int) -> float:
    if gate.tolerance_mode == "pct":
        return abs(source_count) * gate.tolerance_value / 100.0
    return gate.tolerance_value


def _failure_status(gate: QualityGateDefinition) -> str:
    return "warning" if gate.severity == "warning" else "failed"


def _optional_non_negative_int(raw: object, *, field: str) -> int | None:
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return raw


def _non_negative_finite_number(raw: object, *, field: str) -> float:
    if isinstance(raw, bool) or not isinstance(raw, int | float):
        raise ValueError(f"{field} must be a non-negative finite number")
    value = float(raw)
    if value < 0 or not math.isfinite(value):
        raise ValueError(f"{field} must be a non-negative finite number")
    return 0.0 if value == 0 else value


def _reject_unknown_gate_fields(raw: Mapping[str, Any], *, gate_id: str) -> None:
    unknown = sorted(str(key) for key in raw if key not in _GATE_FIELDS)
    if unknown:
        raise ValueError(f"quality gate {gate_id}.{unknown[0]} is an unknown field")


def quality_gate_contract(policy: QualityGatePolicy) -> tuple[dict[str, str], ...]:
    """Build the exact ordered identity expected from gate results."""

    return tuple(
        {
            "gate_id": gate.id,
            "type": gate.type,
            "severity": gate.severity,
        }
        for gate in policy.gates
    )


def quality_gate_policy_fingerprint(policy: QualityGatePolicy) -> str:
    """Fingerprint the ordered, fully normalized policy without raw SQL."""

    gates: list[dict[str, object]] = []
    for ordinal, gate in enumerate(policy.gates):
        normalized: dict[str, object] = {
            "ordinal": ordinal,
            "id": gate.id,
            "type": gate.type,
            "severity": gate.severity,
            "side": gate.side,
            "threshold": gate.threshold,
            "tolerance_mode": gate.tolerance_mode,
            "tolerance_value": gate.tolerance_value,
            "hash_mode": gate.mode,
        }
        if gate.sql is not None:
            normalized["sql_sha256"] = f"sha256:{hashlib.sha256(gate.sql.encode('utf-8')).hexdigest()}"
        gates.append(normalized)
    canonical = json.dumps(
        {"gates": gates},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(canonical).hexdigest()}"
