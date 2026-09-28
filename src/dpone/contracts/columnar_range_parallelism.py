"""Connector-neutral contracts for bounded range plans and execution evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest

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
        cls, value: Mapping[str, Any] | None, *, reader_workers: int, load_workers: int
    ) -> RangeParallelismPolicy:
        raw = dict(value or {})
        requested_reader_workers = raw.get("reader_workers", reader_workers)
        requested_load_workers = raw.get("load_workers", load_workers)
        consistency = _choice(
            raw, "consistency", {"immutable", "database_snapshot", "temporal_as_of", "write_exclusion"}, "immutable"
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
        payload = asdict(self)
        payload["group_key"] = list(self.group_key)
        payload["consistency_authority"] = dict(self.consistency_authority)
        return payload

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
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ColumnarRangePlan:
    """Content-addressed range plan safe to expose in plan output."""

    policy: RangeParallelismPolicy
    ranges: tuple[ColumnarRangeDescriptor, ...]
    query_identity: str
    execution_identity: str | None
    plan_fingerprint: str
    range_set_fingerprint: str
    execution_plan_fingerprint: str | None

    @classmethod
    def create(
        cls,
        *,
        policy: RangeParallelismPolicy,
        ranges: Sequence[ColumnarRangeDescriptor],
        query_identity: str,
        execution_identity: str | None = None,
    ) -> ColumnarRangePlan:
        ordered = tuple(ranges)
        if not ordered or tuple(item.ordinal for item in ordered) != tuple(range(len(ordered))):
            raise ValueError("Columnar range ordinals must be contiguous and start at zero.")
        if len({item.range_id for item in ordered}) != len(ordered):
            raise ValueError("Columnar range identifiers must be unique.")
        logical_payload = {
            "schema": "dpone.native_transfer.columnar_range_plan.v1",
            "policy": policy.to_dict(),
            "ranges": [item.to_dict() for item in ordered],
            "logical_identity": query_identity,
        }
        range_set = {"schema": "dpone.native_transfer.columnar_range_set.v1", "ranges": logical_payload["ranges"]}
        range_set_fingerprint = canonical_fingerprint(range_set)
        plan_fingerprint = canonical_fingerprint(logical_payload)
        execution_fingerprint = None
        if execution_identity is not None:
            execution_fingerprint = canonical_fingerprint(
                {
                    "schema": "dpone.native_transfer.columnar_range_execution_plan.v1",
                    "plan": plan_fingerprint,
                    "execution_identity": execution_identity,
                }
            )
        return cls(
            policy,
            ordered,
            query_identity,
            execution_identity,
            plan_fingerprint,
            range_set_fingerprint,
            execution_fingerprint,
        )

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema": "dpone.native_transfer.columnar_range_plan.v1",
            "policy": self.policy.to_dict(),
            "policy_fingerprint": self.policy.fingerprint,
            "ranges": [item.to_dict() for item in self.ranges],
            "query_identity": self.query_identity,
            "range_set_fingerprint": self.range_set_fingerprint,
            "plan_fingerprint": self.plan_fingerprint,
        }
        if self.execution_identity is not None:
            payload["execution_identity"] = self.execution_identity
            payload["execution_plan_fingerprint"] = self.execution_plan_fingerprint
        return payload


@dataclass(frozen=True, slots=True)
class RangeChunkReceipt:
    """One immutable object/chunk owned by a source range."""

    ordinal: int
    object_identity: str
    checksum_sha256: str
    rows: int
    bytes: int

    def __post_init__(self) -> None:
        _nonnegative("chunk.ordinal", self.ordinal)
        _nonnegative("chunk.rows", self.rows)
        _nonnegative("chunk.bytes", self.bytes)
        _nonempty("chunk.object_identity", self.object_identity)
        if not is_canonical_sha256_digest(self.checksum_sha256):
            raise ValueError("chunk.checksum_sha256 must be a canonical SHA-256 digest.")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RangeStageReceipt:
    """Authoritative sink confirmation linked to one extracted range."""

    range_id: str
    stage_identity: str
    receipt_sha256: str
    staged_rows: int

    def __post_init__(self) -> None:
        _nonempty("stage.range_id", self.range_id)
        _nonempty("stage.stage_identity", self.stage_identity)
        _nonnegative("stage.staged_rows", self.staged_rows)
        if not is_canonical_sha256_digest(self.receipt_sha256):
            raise ValueError("stage.receipt_sha256 must be a canonical SHA-256 digest.")

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RangeEvidenceItem:
    """Measured extraction and optional staging facts for one range."""

    range_id: str
    rows: int
    retained_bytes: int
    eof_confirmed: bool
    chunks: tuple[RangeChunkReceipt, ...]
    stage: RangeStageReceipt | None = None

    def __post_init__(self) -> None:
        _nonempty("range_id", self.range_id)
        _nonnegative("range.rows", self.rows)
        _nonnegative("range.retained_bytes", self.retained_bytes)
        if not isinstance(self.eof_confirmed, bool):
            raise ValueError("range.eof_confirmed must be a boolean.")
        if not isinstance(self.chunks, tuple) or any(not isinstance(chunk, RangeChunkReceipt) for chunk in self.chunks):
            raise ValueError("range.chunks must be a tuple of RangeChunkReceipt values.")
        if self.stage is not None and not isinstance(self.stage, RangeStageReceipt):
            raise ValueError("range.stage must be a RangeStageReceipt when supplied.")
        if tuple(chunk.ordinal for chunk in self.chunks) != tuple(range(len(self.chunks))):
            raise ValueError("Chunk ordinals must be unique, contiguous, and start at zero.")
        if self.rows > 0 and not self.chunks:
            raise ValueError("A nonempty range requires at least one chunk receipt.")
        if sum(chunk.rows for chunk in self.chunks) != self.rows:
            raise ValueError("Chunk row counts must reconcile to the extracted range row count.")
        if len({chunk.object_identity for chunk in self.chunks}) != len(self.chunks):
            raise ValueError("Chunk object identities must be unique within a range.")
        if self.stage and (self.stage.range_id != self.range_id or self.stage.staged_rows != self.rows):
            raise ValueError("Stage receipt identity and row count must reconcile to its extracted range.")

    def to_dict(self) -> dict[str, Any]:
        return {
            "range_id": self.range_id,
            "rows": self.rows,
            "retained_bytes": self.retained_bytes,
            "eof_confirmed": self.eof_confirmed,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "stage": self.stage.to_dict() if self.stage else None,
        }


def _positive(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"range_parallelism.{name} must be an integer greater than zero.")
    if value <= 0:
        raise ValueError(f"range_parallelism.{name} must be greater than zero.")
    return value


def _nonnegative(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer.")
    return value


def _nonempty(name: str, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a nonempty string.")
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
    "ColumnarRangePlan",
    "RangeChunkReceipt",
    "RangeEvidenceItem",
    "RangeParallelismPolicy",
    "columnar_range_fingerprint",
    "RangeStageReceipt",
]
