"""Pre-cleanup runtime acceptance metric evidence.

This module is intentionally connector-neutral.  It defines the small request,
snapshot and recorder contracts used by runtime governance; concrete databases
provide probes behind ``metric_probe.collect(...)``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from dpone.runtime.governance.acceptance_metric_requests import build_acceptance_metric_request
from dpone.runtime.governance.acceptance_snapshot import (
    AcceptanceMetricRequest,
    AcceptanceMetricSnapshot,
    AcceptanceMetricSnapshotError,
    business_columns,
    canonical_snapshot,
)

_CONFIGURATION_ERROR_CODE = "DPONE_ACCEPTANCE_METRIC_CONFIGURATION_INVALID"
_ACCEPTANCE_FIELDS = {"enabled", "mode", "capture", "checks"}
_CAPTURE_FIELDS = {"source", "staged", "target"}
_CHECK_FIELDS = {"row_count", "null_counts", "distinct_counts"}
_COLUMN_SELECTORS = {"off", "none", "all_columns", "business_columns", "source_and_binary_semantics", "strict"}


class AcceptanceMetricProbe(Protocol):
    """Small source/sink port for collecting table/query metric snapshots."""

    def collect(self, request: AcceptanceMetricRequest) -> AcceptanceMetricSnapshot: ...


class AcceptanceMetricConfigurationError(ValueError):
    """Strict runtime rejection of malformed ``quality.acceptance`` authoring."""

    code = _CONFIGURATION_ERROR_CODE

    def __init__(self, field_path: str, reason: str) -> None:
        self.field_path = field_path
        super().__init__(f"{field_path}: {reason}")


class AcceptanceMetricCapabilityError(RuntimeError):
    """A required side or metric probe is unavailable for the selected path."""

    def __init__(self, side: str, code: str) -> None:
        self.side = side
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class AcceptanceMetricPolicy:
    """Resolved runtime acceptance metric policy."""

    enabled: bool = False
    mode: str = "warn_only"
    capture_source: bool = True
    capture_staged: bool = True
    capture_target: bool = True
    row_count: bool = True
    null_counts: str | tuple[str, ...] = "off"
    distinct_counts: str | tuple[str, ...] = "off"

    @classmethod
    def from_load_config(cls, load_config: Any) -> AcceptanceMetricPolicy:
        options = getattr(load_config, "options", None)
        quality = options.get("quality") if isinstance(options, Mapping) else None
        if not isinstance(quality, Mapping):
            return cls()
        if "acceptance" not in quality:
            return cls()
        raw = _strict_object(quality.get("acceptance"), "quality.acceptance", _ACCEPTANCE_FIELDS)
        if "enabled" not in raw:
            raise AcceptanceMetricConfigurationError("quality.acceptance.enabled", "field is required")
        capture = _strict_object(raw.get("capture", {}), "quality.acceptance.capture", _CAPTURE_FIELDS)
        checks = _strict_object(raw.get("checks", {}), "quality.acceptance.checks", _CHECK_FIELDS)
        mode = raw.get("mode", "warn_only")
        if not isinstance(mode, str) or mode not in {"warn_only", "required"}:
            raise AcceptanceMetricConfigurationError("quality.acceptance.mode", "expected 'warn_only' or 'required'")
        policy = cls(
            enabled=_bool_field(raw, "enabled", False, "quality.acceptance.enabled"),
            mode=mode,
            capture_source=_bool_field(capture, "source", True, "quality.acceptance.capture.source"),
            capture_staged=_bool_field(capture, "staged", True, "quality.acceptance.capture.staged"),
            capture_target=_bool_field(capture, "target", True, "quality.acceptance.capture.target"),
            row_count=_bool_field(checks, "row_count", True, "quality.acceptance.checks.row_count"),
            null_counts=_column_selector(checks.get("null_counts", "off"), "quality.acceptance.checks.null_counts"),
            distinct_counts=_column_selector(
                checks.get("distinct_counts", "off"),
                "quality.acceptance.checks.distinct_counts",
            ),
        )
        if policy.enabled and not policy.requested_sides:
            raise AcceptanceMetricConfigurationError("quality.acceptance.capture", "at least one side is required")
        if policy.enabled and not (policy.row_count or policy.null_counts != "off" or policy.distinct_counts != "off"):
            raise AcceptanceMetricConfigurationError("quality.acceptance.checks", "at least one check is required")
        return policy

    @property
    def requested_sides(self) -> tuple[str, ...]:
        selections = (("source", self.capture_source), ("staged", self.capture_staged), ("target", self.capture_target))
        return tuple(side for side, selected in selections if selected)

    def selection_warnings(self, columns: Sequence[str]) -> tuple[str, ...]:
        for selector, field_path in (
            (self.null_counts, "quality.acceptance.checks.null_counts"),
            (self.distinct_counts, "quality.acceptance.checks.distinct_counts"),
        ):
            if not isinstance(selector, tuple):
                continue
            unknown = tuple(column for column in selector if column not in columns)
            if unknown and self.mode == "required":
                raise AcceptanceMetricConfigurationError(
                    field_path,
                    f"unknown payload column: {unknown[0]}",
                )
            if unknown:
                return ("acceptance_metric_unknown_columns",)
        return ()


@dataclass(slots=True)
class AcceptanceMetricRun:
    """One policy evaluation and its at-most-one snapshot per requested side."""

    policy: AcceptanceMetricPolicy
    selection_warnings_by_side: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    snapshots: dict[str, AcceptanceMetricSnapshot] = field(default_factory=dict)

    def add(self, snapshot: AcceptanceMetricSnapshot) -> None:
        if snapshot.side in self.snapshots:
            raise RuntimeError(f"duplicate acceptance snapshot for {snapshot.side}")
        self.snapshots[snapshot.side] = snapshot

    def metrics(self, load_result: Any | None) -> dict[str, Any]:
        if not self.policy.enabled:
            return {}
        ordered = {
            side: self.snapshots[side].to_jsonable() for side in self.policy.requested_sides if side in self.snapshots
        }
        return {
            "acceptance_metrics": {
                "kind": "dpone.acceptance.run_metrics.v1",
                "mode": self.policy.mode,
                "snapshots": ordered,
                "load_result_rows": getattr(load_result, "total_rows", None),
            }
        }

    def selection_warnings(self, side: str) -> tuple[str, ...]:
        return tuple(self.selection_warnings_by_side.get(side, ()))


@dataclass(frozen=True, slots=True)
class AcceptanceEvidenceContext:
    """Safe load-step destination and failure boundary for acceptance evidence."""

    load_record: Any
    governance_service: Any
    boundary: str


class AcceptanceMetricsRecorder:
    """Plan, collect and publish connector-neutral acceptance snapshots."""

    def start(
        self,
        *,
        policy: AcceptanceMetricPolicy,
        physical_sides: tuple[str, ...],
        payload_schema: Sequence[tuple[str, str]] | Sequence[str],
        schemas_by_side: Mapping[str, Sequence[tuple[str, str]] | Sequence[str]] | None = None,
        source: Any | None,
        sink: Any,
        evidence: AcceptanceEvidenceContext,
    ) -> AcceptanceMetricRun:
        try:
            if not policy.enabled:
                return AcceptanceMetricRun(policy)
            side_columns = {
                side: business_columns((schemas_by_side or {}).get(side, payload_schema))
                for side in policy.requested_sides
            }
            warnings_by_side = {side: policy.selection_warnings(columns) for side, columns in side_columns.items()}
            run = AcceptanceMetricRun(policy, selection_warnings_by_side=warnings_by_side)
            for side in policy.requested_sides:
                owner = source if side == "source" else sink
                if side in physical_sides and _probe(owner) is not None:
                    continue
                code = _probe_unavailable_code(side)
                if policy.mode == "required":
                    raise AcceptanceMetricCapabilityError(side, code)
                run.add(
                    AcceptanceMetricSnapshot(
                        side=side,
                        columns=side_columns[side],
                        warnings=_merge_warnings(run.selection_warnings(side), (code,)),
                    )
                )
        except AcceptanceMetricCapabilityError as exc:
            self._record_failure(evidence, exc.side, exc.code)
            raise
        except AcceptanceMetricConfigurationError as exc:
            self._record_failure(evidence, policy.requested_sides[0], exc.code)
            raise
        for snapshot in run.snapshots.values():
            self._record_snapshot(snapshot, evidence)
        return run

    def capture(
        self,
        run: AcceptanceMetricRun,
        *,
        side: str,
        load_config: Any,
        extract_result: Any,
        payload_schema: Sequence[tuple[str, str]] | Sequence[str],
        source: Any | None,
        sink: Any,
        evidence: AcceptanceEvidenceContext,
        staged_handle: Any | None = None,
    ) -> None:
        if not run.policy.enabled or side not in run.policy.requested_sides or side in run.snapshots:
            return
        request = build_acceptance_metric_request(
            run.policy,
            side=side,
            load_config=load_config,
            extract_result=extract_result,
            payload_schema=payload_schema,
            staged_handle=staged_handle,
        )
        probe = _probe(source if side == "source" else sink)
        if probe is None:
            error = AcceptanceMetricCapabilityError(side, _probe_unavailable_code(side))
            self._record_failure(evidence, side, error.code)
            raise error
        try:
            raw_snapshot = probe.collect(request)
        except Exception:
            if run.policy.mode == "required":
                self._record_failure(evidence, side, f"{side}_acceptance_metric_probe_failed")
                raise
            snapshot = AcceptanceMetricSnapshot(
                side=side,
                columns=request.columns,
                dataset=request.dataset_identity,
                warnings=_merge_warnings(
                    run.selection_warnings(side),
                    ("acceptance_metric_probe_failed",),
                ),
            )
        else:
            try:
                snapshot = canonical_snapshot(
                    request,
                    raw_snapshot,
                    mode=run.policy.mode,
                    selection_warnings=run.selection_warnings(side),
                )
            except AcceptanceMetricSnapshotError as exc:
                self._record_failure(evidence, side, exc.code)
                raise
        run.add(snapshot)
        self._record_snapshot(snapshot, evidence)

    @staticmethod
    def _record_snapshot(snapshot: AcceptanceMetricSnapshot, evidence: AcceptanceEvidenceContext) -> None:
        evidence.governance_service.record_acceptance_snapshot(
            load_record=evidence.load_record,
            snapshot=snapshot,
            boundary=evidence.boundary,
        )

    @staticmethod
    def _record_failure(evidence: AcceptanceEvidenceContext, side: str, code: str) -> None:
        evidence.governance_service.record_acceptance_failure(
            load_record=evidence.load_record,
            boundary=evidence.boundary,
            side=side,
            code=code,
        )


def _probe(owner: Any | None) -> AcceptanceMetricProbe | None:
    probe = getattr(owner, "metric_probe", None) or getattr(owner, "acceptance_metric_probe", None)
    return probe if callable(getattr(probe, "collect", None)) else None


def _probe_unavailable_code(side: str) -> str:
    return "staged_acceptance_metric_unavailable" if side == "staged" else f"{side}_acceptance_metric_probe_unavailable"


def _merge_warnings(*groups: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(warning for group in groups for warning in group))


def _column_selector(raw: object, field_path: str) -> str | tuple[str, ...]:
    if raw is False or (isinstance(raw, str) and raw in {"off", "none"}):
        return "off"
    if raw is True:
        return "all_columns"
    if isinstance(raw, str):
        if raw not in _COLUMN_SELECTORS:
            raise AcceptanceMetricConfigurationError(field_path, "unsupported column selector")
        return raw
    if not isinstance(raw, list) or not raw:
        raise AcceptanceMetricConfigurationError(field_path, "expected a supported selector or non-empty list")
    if any(not isinstance(item, str) or not item.strip() for item in raw):
        raise AcceptanceMetricConfigurationError(field_path, "column names must be non-empty strings")
    if len(set(raw)) != len(raw):
        raise AcceptanceMetricConfigurationError(field_path, "column names must be unique")
    return tuple(raw)


def _strict_object(raw: object, field_path: str, allowed: set[str]) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping):
        raise AcceptanceMetricConfigurationError(field_path, "expected an object")
    unknown = sorted(str(field) for field in raw if field not in allowed)
    if unknown:
        raise AcceptanceMetricConfigurationError(f"{field_path}.{unknown[0]}", "unknown field")
    return raw


def _bool_field(
    parent: Mapping[str, Any],
    field: str,
    default: bool,
    field_path: str,
) -> bool:
    value = parent.get(field, default)
    if not isinstance(value, bool):
        raise AcceptanceMetricConfigurationError(field_path, "expected a boolean")
    return value


__all__ = [
    "AcceptanceEvidenceContext",
    "AcceptanceMetricCapabilityError",
    "AcceptanceMetricConfigurationError",
    "AcceptanceMetricPolicy",
    "AcceptanceMetricProbe",
    "AcceptanceMetricRequest",
    "AcceptanceMetricRun",
    "AcceptanceMetricSnapshot",
    "AcceptanceMetricSnapshotError",
    "AcceptanceMetricsRecorder",
]
