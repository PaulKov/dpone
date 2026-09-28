"""Bounded target observations are values, never publication authority or permits."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

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
        strings = (
            self.cluster,
            self.database,
            self.table,
            self.dataset,
            self.selection_digest,
            self.schema_digest,
            self.reader_token,
            self.core_digest,
        )
        if any(not isinstance(item, str) or not item or len(item) > 4096 for item in strings):
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
