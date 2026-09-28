"""Bounded durable quality evidence; integrity is distinct from store authority.

Capsules are public values, not capabilities. Only an injected trusted store may
authorize their use. The immutable prepared core survives appended completion
records, so post-commit observations never invalidate the publication receipt.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

VERSION = "dpone.quality.replay.v1"
TARGET_VERSION = "dpone.quality.replay.v2"
MAX_CAPSULE_BYTES = 256 * 1024
_CORE_FIELDS = frozenset(
    {
        "policy_snapshot_id",
        "admission_digest",
        "effective_plan",
        "run_id",
        "load_id",
        "source_probe",
        "target_probe",
        "report",
        "acceptance",
        "binding",
    }
)
_REASONS = frozenset({"REQUIRED", "MISMATCH", "INVALID", "FAILED", "INCOMPLETE", "UNSUPPORTED"})


class ReplayQualityEvidenceError(RuntimeError):
    """A proven target commit cannot substitute for exact quality evidence."""

    blocks_committed_success = True

    def __init__(self, reason: str = "INVALID") -> None:
        if reason not in _REASONS:
            reason = "INVALID"
        self.code = f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}"
        self.replay_details: dict[str, Any] = {}
        super().__init__(self.code)


def canonical_quality_json(value: Any) -> str:
    """Canonical finite JSON with a bounded encoded representation."""
    try:
        result = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(result.encode("utf-8")) > MAX_CAPSULE_BYTES:
            raise ValueError("oversize")
        return result
    except (TypeError, ValueError, OverflowError, RecursionError):
        raise ReplayQualityEvidenceError from None


def quality_digest(value: Any) -> str:
    return hashlib.sha256(canonical_quality_json(value).encode("utf-8")).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ReplayQualityEvidenceError
        result[key] = value
    return result


def _is_digest(value: Any) -> bool:
    if isinstance(value, str) and value.startswith("sha256:"):
        value = value[7:]
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


@dataclass(frozen=True, slots=True)
class QualityReplayCapsule:
    """An immutable canonical envelope with at most two completion transitions."""

    payload: str

    def __post_init__(self) -> None:
        self._validated()

    @classmethod
    def prepare(cls, core: Mapping[str, Any]) -> QualityReplayCapsule:
        return cls(
            canonical_quality_json(
                {
                    "kind": TARGET_VERSION if "target_plan" in core else VERSION,
                    "core": dict(core),
                    "core_digest": quality_digest(core),
                    "completion": [],
                }
            )
        )

    @classmethod
    def parse(cls, raw: str) -> QualityReplayCapsule:
        return cls(raw)

    @property
    def version(self) -> str:
        return str(self._validated()["kind"])

    @property
    def target(self) -> dict[str, Any]:
        records = self._validated()["completion"]
        return records[-1]["target"] if records else {}

    @property
    def core(self) -> dict[str, Any]:
        return self._validated()["core"]

    @property
    def core_digest(self) -> str:
        return str(self._validated()["core_digest"])

    @property
    def state(self) -> str:
        records = self._validated()["completion"]
        return str(records[-1]["state"]) if records else "PREPARED"

    @property
    def completion_authority_version(self) -> int | None:
        """Latest durable transition version, absent for a prepared capsule."""
        records = self._validated()["completion"]
        return int(records[-1]["authority_version"]) if records else None

    def advance(
        self,
        state: str,
        *,
        authority_version: int,
        target: Mapping[str, Any] | None = None,
    ) -> QualityReplayCapsule:
        envelope = self._validated()
        records = envelope["completion"]
        records.append(
            {
                "state": state,
                "authority_version": authority_version,
                "previous_digest": quality_digest(records[-1]) if records else envelope["core_digest"],
                "target": dict(target or {}),
            }
        )
        return type(self)(canonical_quality_json(envelope))

    def _validated(self) -> dict[str, Any]:
        try:
            if not isinstance(self.payload, str) or len(self.payload.encode("utf-8")) > MAX_CAPSULE_BYTES:
                raise ReplayQualityEvidenceError
            value = json.loads(self.payload, object_pairs_hook=_unique_object)
            if not isinstance(value, dict) or set(value) != {"kind", "core", "core_digest", "completion"}:
                raise ReplayQualityEvidenceError
            if value["kind"] not in {VERSION, TARGET_VERSION} or canonical_quality_json(value) != self.payload:
                raise ReplayQualityEvidenceError
            core = value["core"]
            fields = _CORE_FIELDS | ({"target_plan"} if value["kind"] == TARGET_VERSION else set())
            if not isinstance(core, dict) or set(core) != fields:
                raise ReplayQualityEvidenceError
            if value["core_digest"] != quality_digest(core):
                raise ReplayQualityEvidenceError
            for field in ("policy_snapshot_id", "admission_digest"):
                if not _is_digest(core[field]):
                    raise ReplayQualityEvidenceError
            for field in ("run_id", "load_id"):
                if not isinstance(core[field], str) or not 1 <= len(core[field]) <= 1024:
                    raise ReplayQualityEvidenceError
            for field in fields - {"policy_snapshot_id", "admission_digest", "run_id", "load_id"}:
                if not isinstance(core[field], dict):
                    raise ReplayQualityEvidenceError
            for field in ("source_probe", "target_probe"):
                probe = core[field]
                if set(probe) != {"row_count", "typed_hash"}:
                    raise ReplayQualityEvidenceError
                count, typed_hash = probe["row_count"], probe["typed_hash"]
                if count is not None and (type(count) is not int or count < 0):
                    raise ReplayQualityEvidenceError
                if typed_hash is not None and (not isinstance(typed_hash, str) or not typed_hash):
                    raise ReplayQualityEvidenceError
            self._validate_completion(value)
            if value["kind"] == TARGET_VERSION:
                self._validate_target(value)
            return value
        except (TypeError, ValueError, KeyError, OverflowError, RecursionError):
            raise ReplayQualityEvidenceError from None

    @staticmethod
    def _validate_completion(value: dict[str, Any]) -> None:
        records = value["completion"]
        if not isinstance(records, list) or len(records) > 2:
            raise ReplayQualityEvidenceError
        previous_digest, state, version = value["core_digest"], "PREPARED", -1
        for record in records:
            if not isinstance(record, dict) or set(record) != {
                "state",
                "authority_version",
                "previous_digest",
                "target",
            }:
                raise ReplayQualityEvidenceError
            if state in {"COMPLETE", "FAILED"} or record["state"] not in {"TARGET_PENDING", "COMPLETE", "FAILED"}:
                raise ReplayQualityEvidenceError
            if state == "TARGET_PENDING" and record["state"] == "TARGET_PENDING":
                raise ReplayQualityEvidenceError
            if value["kind"] == TARGET_VERSION:
                if record["state"] == "COMPLETE":
                    if state != "TARGET_PENDING" or not record["target"]:
                        raise ReplayQualityEvidenceError
                elif record["target"]:
                    raise ReplayQualityEvidenceError
            next_version = record["authority_version"]
            if type(next_version) is not int or next_version <= version or not isinstance(record["target"], dict):
                raise ReplayQualityEvidenceError
            if value["kind"] == TARGET_VERSION and next_version > 2**64 - 1:
                raise ReplayQualityEvidenceError
            if record["previous_digest"] != previous_digest:
                raise ReplayQualityEvidenceError
            state, version, previous_digest = record["state"], next_version, quality_digest(record)

    @staticmethod
    def _validate_target(value: dict[str, Any]) -> None:
        """Terminal v2 evidence remains strict even when inspected for retirement."""
        records = value["completion"]
        if not records or records[-1]["state"] != "COMPLETE":
            return
        try:
            core = value["core"]
            plan = dict(core["target_plan"])
            replicas = plan.pop("replicas")
            mode = plan.pop("mode")
            if mode not in {"required", "warn_only"} or not isinstance(replicas, list) or not replicas:
                raise ReplayQualityEvidenceError
            plan["columns"] = tuple(tuple(pair) for pair in plan["columns"])
            plan["null_columns"] = tuple(plan["null_columns"])
            plan["distinct_columns"] = tuple(plan["distinct_columns"])
            request = TargetAcceptanceRequest(
                **plan, binding=core["binding"], core_digest=value["core_digest"], reader_token="sealed-record"
            )
            target = records[-1]["target"]
            validate_target_observation(request, target, allow_unavailable=mode == "warn_only")
            if target["replica"] != sorted(replicas)[0]:
                raise ReplayQualityEvidenceError("MISMATCH")
        except (TargetAcceptanceError, KeyError, TypeError, ValueError):
            raise ReplayQualityEvidenceError from None


def require_quality_retired(payload: str | None, reader: str | None, *, authority_version: int) -> None:
    """Slot reuse cannot erase pending governance or a held read guard."""
    if reader is not None:
        raise ReplayQualityEvidenceError("INCOMPLETE")
    if payload is not None:
        capsule = QualityReplayCapsule.parse(payload)
        version = capsule.completion_authority_version
        if version is not None and version > authority_version:
            raise ReplayQualityEvidenceError("MISMATCH")
        if capsule.state != "COMPLETE":
            raise ReplayQualityEvidenceError("INCOMPLETE")


MAX_FRAME_BYTES = 256 * 1024
MAX_UINT64 = 2**64 - 1
OBSERVATION_PROFILE = "clickhouse-immutable-generation-v1"
UNAVAILABLE_WARNING = "target_acceptance_metric_probe_unavailable"


class TargetAcceptanceError(RuntimeError):
    """Safe failure plus independently verified local lifecycle facts.

    Remote SELECT termination is never inferred from local process termination.
    Unknown local lifecycle requires the owner to retain its durable reader guard.
    """

    blocks_committed_success = True
    remote_termination = "UNVERIFIED"

    def __init__(
        self,
        reason: str = "INVALID",
        *,
        quiescent: bool = True,
        output_revoked: bool = True,
        probe_unavailable: bool = False,
    ) -> None:
        self.code = f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}"
        self.quiescent = quiescent
        self.output_revoked = output_revoked
        self.probe_unavailable = probe_unavailable
        super().__init__(self.code)


@dataclass(frozen=True, slots=True)
class TargetAcceptanceRequest:
    """A generated metric plan bound to the caller's exact guarded generation.

    ``columns`` preserves payload order and ClickHouse type names. The reader
    token is transmitted privately and is never returned as observation evidence.
    """

    cluster: str
    database: str
    table: str
    dataset: str
    columns: tuple[tuple[str, str], ...]
    selection_digest: str
    schema_digest: str
    binding: dict[str, Any]
    reader_token: str
    core_digest: str
    row_count: bool
    null_columns: tuple[str, ...]
    distinct_columns: tuple[str, ...]

    def validate(self) -> None:
        location = (self.cluster, self.database, self.table, self.dataset)
        proof = (self.selection_digest, self.schema_digest, self.reader_token, self.core_digest)
        if any(not isinstance(item, str) or not item or len(item) > 4096 for item in (*location, *proof)):
            raise TargetAcceptanceError()
        names = [name for name, _ in self.columns]
        if len(names) != len(set(names)) or any(not name or not kind for name, kind in self.columns):
            raise TargetAcceptanceError()
        if type(self.row_count) is not bool or not isinstance(self.binding, dict):
            raise TargetAcceptanceError()
        for selected in (self.null_columns, self.distinct_columns):
            if len(selected) != len(set(selected)) or not set(selected) <= set(names):
                raise TargetAcceptanceError()
        if len(set(self.null_columns) | set(self.distinct_columns)) > 256:
            raise TargetAcceptanceError("UNSUPPORTED")


def unavailable_observation(request: TargetAcceptanceRequest, *, replica: str, attempt_id: str) -> dict[str, Any]:
    """Construct only the explicit warn-only unavailable representation."""
    return {
        "kind": "dpone.quality.target-observation.v1",
        "side": "target",
        "dataset": request.dataset,
        "columns": [name for name, _ in request.columns],
        "selection_digest": request.selection_digest,
        "schema_digest": request.schema_digest,
        "binding": request.binding,
        "replica": replica,
        "observation_profile": OBSERVATION_PROFILE,
        "attempt_id": attempt_id,
        "row_count": None,
        "null_counts": {},
        "distinct_counts": {},
        "warnings": [UNAVAILABLE_WARNING],
        "capture_boundary": "committed_generation_before_governance_complete",
    }


def validate_target_observation(
    request: TargetAcceptanceRequest, observation: Any, *, allow_unavailable: bool = False
) -> dict[str, Any]:
    """Reject missing/extra metrics, coercions, stale identity and partial output."""
    request.validate()
    if not isinstance(observation, dict):
        raise TargetAcceptanceError()
    baseline = unavailable_observation(request, replica="", attempt_id="")
    if set(observation) != set(baseline):
        raise TargetAcceptanceError()
    variable = {"replica", "attempt_id", "row_count", "null_counts", "distinct_counts", "warnings"}
    if any(observation[key] != value for key, value in baseline.items() if key not in variable):
        raise TargetAcceptanceError("MISMATCH")
    if not isinstance(observation["replica"], str) or not 1 <= len(observation["replica"]) <= 4096:
        raise TargetAcceptanceError()
    if not isinstance(observation["attempt_id"], str) or not re.fullmatch(r"[0-9a-f]{32}", observation["attempt_id"]):
        raise TargetAcceptanceError()
    if observation["warnings"] == [UNAVAILABLE_WARNING] and allow_unavailable:
        if (
            observation["row_count"] is not None
            or observation["null_counts"] != {}
            or observation["distinct_counts"] != {}
        ):
            raise TargetAcceptanceError()
        return observation
    if observation["warnings"] != []:
        raise TargetAcceptanceError()
    if request.row_count:
        require_count(observation["row_count"])
    elif observation["row_count"] is not None:
        raise TargetAcceptanceError()
    for field, selected in (("null_counts", request.null_columns), ("distinct_counts", request.distinct_columns)):
        values = observation[field]
        if not isinstance(values, dict) or set(values) != set(selected):
            raise TargetAcceptanceError()
        for value in values.values():
            require_count(value)
    return observation


def require_count(value: Any) -> int:
    """UInt64 counts have no string, bool, negative or overflow coercion."""
    if type(value) is not int or not 0 <= value <= MAX_UINT64:
        raise TargetAcceptanceError()
    return value
