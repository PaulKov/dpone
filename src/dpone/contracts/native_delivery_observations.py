"""Immutable, bounded diagnostic values; never delivery or recovery authority."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Any

PHASES = frozenset(
    "source_read source_adapt frame_build ipc_submit encode bcp raw_verify prepare_insert metadata_project prepared_verify quality publish evidence checkpoint".split()
)


def diagnostic_token(value: str) -> None:
    """Accept bounded identifiers/reason codes, never free-form exception text."""
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValueError("observation.invalid_token")


def _finite_number(value: float | int | None) -> bool:
    try:
        return type(value) in (float, int) and math.isfinite(value)
    except OverflowError:
        return False


@dataclass(frozen=True)
class ObservationMetric:
    """A finite measurement with units/provenance, or explicit absence."""

    value: float | int | None
    unit: str
    availability: str
    reason: str | None
    provenance: str

    def __post_init__(self) -> None:
        for token in (self.unit, self.provenance):
            diagnostic_token(token)
        if self.reason is not None:
            diagnostic_token(self.reason)
        if self.availability == "unavailable":
            valid = self.value is None and self.reason is not None
        else:
            valid = self.availability == "measured" and _finite_number(self.value) and self.reason is None
        if not valid:
            raise ValueError("observation.invalid_metric")

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible copy."""
        return asdict(self)


@dataclass(frozen=True)
class NativeDeliveryObservation:
    """One phase attempt in an explicitly identified monotonic clock domain.

    Counters describe this span only; nested spans are not additive row totals.
    All text is caller-supplied sanitized identifiers, never dataset contents.
    """

    phase: str
    reason: str | None
    clock_domain: str
    process_id: int
    worker_id: str
    ordinal: int | None
    attempt_id: str | None
    start_monotonic_ns: int
    end_monotonic_ns: int
    outcome: str
    rows: int | None
    encoded_bytes: int | None
    metrics: Mapping[str, ObservationMetric]
    schema_version: int = 1

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != 1 or self.phase not in PHASES:
            raise ValueError("observation.invalid_schema_or_phase")
        for token in (self.clock_domain, self.worker_id):
            diagnostic_token(token)
        for token in (self.reason, self.attempt_id):
            if token is not None:
                diagnostic_token(token)
        for value in (self.process_id, self.start_monotonic_ns, self.end_monotonic_ns):
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError("observation.invalid_counter")
        for value in (self.ordinal, self.rows, self.encoded_bytes):
            if value is not None and (type(value) is not int or not 0 <= value <= 2**63 - 1):
                raise ValueError("observation.invalid_counter")
        if self.end_monotonic_ns < self.start_monotonic_ns or self.outcome not in {"completed", "failed", "cancelled"}:
            raise ValueError("observation.invalid_span")
        if len(self.metrics) > 32:
            raise ValueError("observation.metric_capacity")
        for key, metric in self.metrics.items():
            diagnostic_token(key)
            if not isinstance(metric, ObservationMetric):
                raise ValueError("observation.invalid_metric")
        object.__setattr__(self, "metrics", MappingProxyType(dict(self.metrics)))

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> NativeDeliveryObservation:
        """Reconstruct a serialized worker record through the same validation."""
        values = dict(payload)
        values["metrics"] = {name: ObservationMetric(**metric) for name, metric in values["metrics"].items()}
        return cls(**values)

    def to_dict(self) -> dict[str, Any]:
        """Return the frozen v1 field contract without exposing mutable state."""
        result = {name: getattr(self, name) for name in self.__dataclass_fields__ if name != "metrics"}
        result["metrics"] = {name: metric.to_dict() for name, metric in self.metrics.items()}
        return result


# Feature-local JSON contracts shared with the offline producer/consumer.
# These are additive diagnostics, not changes to runtime journals or receipts.
def _record(**properties: Any) -> dict[str, Any]:
    return {"type": "object", "properties": properties, "required": list(properties), "additionalProperties": False}


def _enum(*values: str) -> dict[str, Any]:
    return {"enum": list(values)}


_TEXT = {"type": "string", "minLength": 1}
_TOKEN = {"type": "string", "pattern": r"^[A-Za-z0-9_.:-]{1,128}$"}
_SHA = {"type": "string", "pattern": "^[0-9a-f]{64}$"}
_COMMIT = {"type": "string", "pattern": "^[0-9a-f]{40}$"}
_BOOL = {"type": "boolean"}
_COUNT = {"type": "integer", "minimum": 0}
_STATUS = _enum("PASS", "FAIL", "SKIP", "UNVERIFIED")
_REASON = {"type": ["string", "null"]}
_LIMITATIONS = {"type": "array", "items": _TEXT}
_METRIC = _record(
    value={"type": ["number", "null"]},
    unit=_TOKEN,
    availability=_enum("measured", "unavailable"),
    reason=_REASON,
    provenance=_TOKEN,
)
_METRIC["allOf"] = [
    {
        "if": {"properties": {"availability": {"const": "unavailable"}}},
        "then": {"properties": {"value": {"type": "null"}, "reason": _TEXT}},
        "else": {"properties": {"value": {"type": "number"}, "reason": {"type": "null"}}},
    }
]
_REF = _record(path=_TEXT, sha256=_SHA, status=_STATUS)
_ROUTE = _record(
    source={"const": "clickhouse"},
    sink={"const": "mssql"},
    strategy=_enum("full_refresh", "partition_replace"),
    mode=_enum("bounded_native", "isolated_switch"),
)
_SAMPLE = _record(
    id=_TOKEN,
    is_warmup=_BOOL,
    status=_STATUS,
    reason=_REASON,
    visibility_seconds=_METRIC,
    pipeline_seconds=_METRIC,
    correctness=_REF,
    observations={"anyOf": [_REF, {"type": "null"}]},
    metrics={"type": "object", "additionalProperties": _METRIC},
)
_CHECK = _record(
    id=_TOKEN,
    status=_enum("PASS", "FAIL", "SKIP", "UNVERIFIED", "N/A"),
    method=_enum(
        "exact_typed_multiset", "versioned_typed_digest", "transaction_fixture", "live_observation", "not_applicable"
    ),
    reason=_REASON,
    expected={},
    observed={},
    evidence={"anyOf": [_REF, {"type": "null"}]},
)
CHECKS_BY_SCOPE = {
    "sample": frozenset(
        "typed_content duplicate_multiplicity metadata_parity commit_receipt_binding outside_window_unchanged".split()
    ),
    "type_fidelity": frozenset(
        "typed_content duplicate_multiplicity metadata_parity commit_receipt_binding outside_window_unchanged".split()
    ),
    "failure_recovery": frozenset("empty_input rollback receipt_first_recovery source_free_resume".split()),
}
RUN_SCHEMA = _record(
    schema_version={"const": 1, "type": "integer"},
    kind={"const": "native-delivery-run"},
    producer=_record(name=_TOKEN, version=_TOKEN, commit=_COMMIT, dirty=_BOOL),
    subject=_record(commit=_COMMIT, dirty=_BOOL),
    route=_ROUTE,
    workload=_record(
        id=_TOKEN, seed={"type": "integer"}, rows=_COUNT, columns={"type": "integer", "minimum": 1}, sha256=_SHA
    ),
    configuration=_record(sha256=_SHA, limits={"type": "object"}),
    environment=_record(
        sha256=_SHA,
        versions={"type": "object", "additionalProperties": _TEXT},
        target_layout_sha256=_SHA,
        resource_profile={"type": "object"},
    ),
    samples={"type": "array", "items": _SAMPLE},
    fidelity_receipt=_REF,
    recovery_receipt=_REF,
    status=_STATUS,
    limitations=_LIMITATIONS,
)
RECEIPT_SCHEMA = _record(
    schema_version={"const": 1, "type": "integer"},
    kind={"const": "native-delivery-correctness"},
    subject_commit=_COMMIT,
    workload_sha256=_SHA,
    configuration_sha256=_SHA,
    environment_sha256=_SHA,
    sample_id=_TOKEN,
    route=_ROUTE,
    scope=_enum(*CHECKS_BY_SCOPE),
    execution=_enum("hermetic", "live"),
    fixture=_record(id=_TOKEN, rows=_COUNT, sha256=_SHA),
    checks={"type": "array", "items": _CHECK},
    status=_STATUS,
)
CAMPAIGN_SCHEMA = _record(
    schema_version={"const": 1, "type": "integer"},
    kind={"const": "native-delivery-campaign"},
    workloads={"type": "array", "items": _TOKEN, "minItems": 1, "uniqueItems": True},
    runs={"type": "array", "items": _REF},
    limitations=_LIMITATIONS,
)
