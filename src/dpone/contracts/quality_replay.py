"""Bounded durable quality evidence; integrity is distinct from store authority.

Capsules are public values, not capabilities. Only an injected trusted store may
authorize their use. The immutable prepared core survives appended completion
records, so post-commit observations never invalidate the publication receipt.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

VERSION = "dpone.quality.replay.v1"
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
                    "kind": VERSION,
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
            if value["kind"] != VERSION or canonical_quality_json(value) != self.payload:
                raise ReplayQualityEvidenceError
            core = value["core"]
            if not isinstance(core, dict) or set(core) != _CORE_FIELDS:
                raise ReplayQualityEvidenceError
            if value["core_digest"] != quality_digest(core):
                raise ReplayQualityEvidenceError
            for field in ("policy_snapshot_id", "admission_digest"):
                if not _is_digest(core[field]):
                    raise ReplayQualityEvidenceError
            for field in ("run_id", "load_id"):
                if not isinstance(core[field], str) or not 1 <= len(core[field]) <= 1024:
                    raise ReplayQualityEvidenceError
            for field in _CORE_FIELDS - {"policy_snapshot_id", "admission_digest", "run_id", "load_id"}:
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
            next_version = record["authority_version"]
            if type(next_version) is not int or next_version <= version or not isinstance(record["target"], dict):
                raise ReplayQualityEvidenceError
            if record["previous_digest"] != previous_digest:
                raise ReplayQualityEvidenceError
            state, version, previous_digest = record["state"], next_version, quality_digest(record)


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
