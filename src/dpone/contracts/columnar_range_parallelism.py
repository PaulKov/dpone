"""Immutable contracts for bounded columnar range execution.

The models are connector-neutral and contain no credentials, sessions, SQL, or
vendor objects.  Runtime adapters consume the normalized values and report what
actually ran through the evidence model.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint

DEFAULT_MAX_INFLIGHT_ROWS = 100_000
DEFAULT_MAX_INFLIGHT_BYTES = 256 * 1024 * 1024


def columnar_range_fingerprint(payload: Mapping[str, Any]) -> str:
    """Return the canonical public identity for a range-policy payload."""

    return canonical_fingerprint(payload)


@dataclass(frozen=True, slots=True)
class RangeParallelismPolicy:
    """Normalized resource, consistency, and staging policy."""

    mode: str = "off"
    reader_workers: int = 1
    upload_workers: int = 1
    load_workers: int = 1
    max_inflight_ranges: int = 1
    max_inflight_rows: int = DEFAULT_MAX_INFLIGHT_ROWS
    max_inflight_bytes: int = DEFAULT_MAX_INFLIGHT_BYTES
    gap_policy: str = "reject"
    consistency: str = "immutable"
    staging_topology: str = "shared_per_run"
    group_key: tuple[str, ...] = ()
    consistency_authority: tuple[tuple[str, str], ...] = ()

    @classmethod
    def from_mapping(
        cls,
        value: Mapping[str, Any] | None,
        *,
        reader_workers: int,
        load_workers: int,
    ) -> RangeParallelismPolicy:
        raw = dict(value or {})
        requested_reader_workers = raw.get("reader_workers", reader_workers)
        requested_load_workers = raw.get("load_workers", load_workers)
        consistency = _choice(
            raw,
            "consistency",
            {"immutable", "database_snapshot", "temporal_as_of", "write_exclusion"},
            "immutable",
        )
        policy = cls(
            mode=_choice(raw, "mode", {"off", "auto", "required"}, "off"),
            reader_workers=_positive("reader_workers", requested_reader_workers),
            upload_workers=_positive("upload_workers", raw.get("upload_workers", 1)),
            load_workers=_positive("load_workers", requested_load_workers),
            max_inflight_ranges=_positive(
                "max_inflight_ranges", raw.get("max_inflight_ranges", max(1, reader_workers))
            ),
            max_inflight_rows=_positive("max_inflight_rows", raw.get("max_inflight_rows", DEFAULT_MAX_INFLIGHT_ROWS)),
            max_inflight_bytes=_positive(
                "max_inflight_bytes", raw.get("max_inflight_bytes", DEFAULT_MAX_INFLIGHT_BYTES)
            ),
            gap_policy=_choice(raw, "gap_policy", {"reject", "allow_explicit"}, "reject"),
            consistency=consistency,
            staging_topology=_choice(raw, "staging_topology", {"shared_per_run", "per_partition"}, "shared_per_run"),
            group_key=_group_key(raw.get("group_key")),
            consistency_authority=_authority(raw.get("consistency_authority"), consistency=consistency),
        )
        if policy.mode == "off":
            if set(raw) - {"mode"}:
                raise ValueError("range_parallelism settings require mode=auto or mode=required.")
            return replace(policy, reader_workers=1, upload_workers=1, load_workers=1, max_inflight_ranges=1)
        return policy

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "reader_workers": self.reader_workers,
            "upload_workers": self.upload_workers,
            "load_workers": self.load_workers,
            "max_inflight_ranges": self.max_inflight_ranges,
            "max_inflight_rows": self.max_inflight_rows,
            "max_inflight_bytes": self.max_inflight_bytes,
            "gap_policy": self.gap_policy,
            "consistency": self.consistency,
            "staging_topology": self.staging_topology,
            "group_key": list(self.group_key),
            "consistency_authority": dict(self.consistency_authority),
        }

    @property
    def fingerprint(self) -> str:
        return canonical_fingerprint(self.to_dict())


@dataclass(frozen=True, slots=True)
class ColumnarRangeDescriptor:
    """One sanitized, typed range in an immutable execution plan."""

    range_id: str
    ordinal: int
    boundary_family: str
    lower: str | int | float | None
    upper: str | int | float | None
    include_lower: bool
    include_upper: bool
    is_null: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_id": self.range_id,
            "ordinal": self.ordinal,
            "boundary_family": self.boundary_family,
            "lower": self.lower,
            "upper": self.upper,
            "include_lower": self.include_lower,
            "include_upper": self.include_upper,
            "is_null": self.is_null,
        }


@dataclass(frozen=True, slots=True)
class ColumnarRangePlan:
    """Content-addressed range plan safe to expose in plan output."""

    policy: RangeParallelismPolicy
    ranges: tuple[ColumnarRangeDescriptor, ...]
    query_identity: str
    plan_fingerprint: str

    @classmethod
    def create(
        cls,
        *,
        policy: RangeParallelismPolicy,
        ranges: Sequence[ColumnarRangeDescriptor],
        query_identity: str,
    ) -> ColumnarRangePlan:
        ordered = tuple(ranges)
        if not ordered or tuple(item.ordinal for item in ordered) != tuple(range(len(ordered))):
            raise ValueError("Columnar range ordinals must be contiguous and start at zero.")
        if len({item.range_id for item in ordered}) != len(ordered):
            raise ValueError("Columnar range identifiers must be unique.")
        payload = {
            "schema": "dpone.native_transfer.columnar_range_plan.v1",
            "policy": policy.to_dict(),
            "ranges": [item.to_dict() for item in ordered],
            "query_identity": query_identity,
        }
        return cls(policy, ordered, query_identity, canonical_fingerprint(payload))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.native_transfer.columnar_range_plan.v1",
            "policy": self.policy.to_dict(),
            "policy_fingerprint": self.policy.fingerprint,
            "ranges": [item.to_dict() for item in self.ranges],
            "query_identity": self.query_identity,
            "plan_fingerprint": self.plan_fingerprint,
        }


@dataclass(frozen=True, slots=True)
class RangeEvidenceItem:
    range_id: str
    rows: int
    retained_bytes: int
    eof_confirmed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_id": self.range_id,
            "rows": self.rows,
            "retained_bytes": self.retained_bytes,
            "eof_confirmed": self.eof_confirmed,
        }


@dataclass(frozen=True, slots=True)
class ColumnarRangeExecutionEvidence:
    """Actual bounded execution facts; construction fails on partial success."""

    plan_fingerprint: str
    policy_fingerprint: str
    ranges: tuple[RangeEvidenceItem, ...]
    observed_reader_concurrency: int
    rows_high_water: int
    bytes_high_water: int
    all_ranges_confirmed: bool = True
    schema: str = "dpone.native_transfer.columnar_range_parallelism.v1"

    @classmethod
    def from_results(
        cls,
        *,
        plan: ColumnarRangePlan,
        results: Sequence[Any],
        observed_reader_concurrency: int,
        rows_high_water: int,
        bytes_high_water: int,
    ) -> ColumnarRangeExecutionEvidence:
        by_id = {str(item.range_id): item for item in results}
        expected = tuple(item.range_id for item in plan.ranges)
        if len(results) != len(expected) or set(by_id) != set(expected):
            raise ValueError("Range results must cover every planned range exactly once.")
        ordered = tuple(
            RangeEvidenceItem(
                range_id=range_id,
                rows=int(by_id[range_id].rows),
                retained_bytes=int(by_id[range_id].retained_bytes),
                eof_confirmed=bool(by_id[range_id].eof_confirmed),
            )
            for range_id in expected
        )
        if not all(item.eof_confirmed for item in ordered):
            raise ValueError("Every planned range must confirm EOF before success evidence.")
        return cls(
            plan.plan_fingerprint,
            plan.policy.fingerprint,
            ordered,
            int(observed_reader_concurrency),
            int(rows_high_water),
            int(bytes_high_water),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "plan_fingerprint": self.plan_fingerprint,
            "policy_fingerprint": self.policy_fingerprint,
            "ranges": [item.to_dict() for item in self.ranges],
            "observed_reader_concurrency": self.observed_reader_concurrency,
            "rows_high_water": self.rows_high_water,
            "bytes_high_water": self.bytes_high_water,
            "all_ranges_confirmed": self.all_ranges_confirmed,
        }


def _positive(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"range_parallelism.{name} must be an integer greater than zero.")
    if value <= 0:
        raise ValueError(f"range_parallelism.{name} must be greater than zero.")
    return value


def _choice(raw: Mapping[str, Any], key: str, allowed: set[str], default: str) -> str:
    raw_value = raw.get(key, default)
    if not isinstance(raw_value, str):
        raise ValueError(f"range_parallelism.{key} must be a string.")
    value = raw_value.strip().lower()
    if value not in allowed:
        choices = ", ".join(sorted(allowed))
        raise ValueError(f"range_parallelism.{key} must be one of: {choices}.")
    return value


def _group_key(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list | tuple) or any(not str(item).strip() for item in value):
        raise ValueError("range_parallelism.group_key must be a list of nonempty column names.")
    return tuple(str(item).strip() for item in value)


def _authority(value: object, *, consistency: str) -> tuple[tuple[str, str], ...]:
    if consistency == "immutable":
        if value is None:
            return ()
    if not isinstance(value, Mapping):
        raise ValueError(f"range_parallelism.consistency={consistency} requires consistency_authority.")
    expected_key = {
        "database_snapshot": "database_snapshot",
        "temporal_as_of": "as_of",
        "write_exclusion": "write_exclusion_ref",
    }.get(consistency)
    if expected_key is not None and not str(value.get(expected_key, "")).strip():
        raise ValueError(f"range_parallelism.consistency={consistency} requires consistency_authority.{expected_key}.")
    normalized: list[tuple[str, str]] = []
    for key, item in value.items():
        if not isinstance(key, str) or not key.strip() or not isinstance(item, str) or not item.strip():
            raise ValueError("range_parallelism.consistency_authority must contain nonempty string fields.")
        normalized.append((key.strip(), item.strip()))
    return tuple(sorted(normalized))


__all__ = [
    "ColumnarRangeDescriptor",
    "ColumnarRangeExecutionEvidence",
    "ColumnarRangePlan",
    "RangeEvidenceItem",
    "RangeParallelismPolicy",
    "columnar_range_fingerprint",
]
