"""Version-one identities and durable records for opt-in bounded windows.

Identity uses only sanitized fingerprints, never connection URLs or row payloads.
Existing native-transfer checkpoints are deliberately a separate contract.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import cast

from dpone.contracts.process_errors import WindowContractError


class PublicationStatus(str, Enum):  # noqa: UP042 - Python 3.10 typing compatibility.
    """Observed target generation outcome; UNKNOWN never permits exchange replay."""

    PUBLISHED = "published"
    ABSENT = "absent"
    UNKNOWN = "unknown"


def _identity(values: object) -> str:
    return hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _timestamp(value: datetime) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise WindowContractError("Window timestamps must be timezone aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")  # noqa: UP017


@dataclass(frozen=True)
class WindowChunk:
    """One contiguous half-open extraction range."""

    chunk_id: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class WindowPlan:
    """Frozen source version and exact range; boundaries include both endpoints.

    The source adapter must verify that source_version is immutable and readable
    by every worker, including after a restart. A changed version creates a new
    run and never authorizes reuse of old chunk staging.
    """

    route_id: str
    target_id: str
    start: datetime
    end: datetime
    boundaries: tuple[datetime, ...]
    schema_fingerprint: str
    source_version: str
    parameters_fingerprint: str
    workers: int = 1
    window_column: str = field(kw_only=True)
    _run_id: str = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value.strip()
            for value in (
                self.route_id,
                self.target_id,
                self.schema_fingerprint,
                self.source_version,
                self.parameters_fingerprint,
            )
        ):
            raise WindowContractError("Identity fingerprints must be nonempty")
        if not isinstance(self.window_column, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.window_column):
            raise WindowContractError("Window column must be one unqualified identifier")
        if not isinstance(self.boundaries, (tuple, list)):
            raise WindowContractError("Window boundaries must be a finite sequence")
        object.__setattr__(self, "boundaries", tuple(self.boundaries))
        for boundary in (self.start, self.end, *self.boundaries):
            _timestamp(boundary)
        object.__setattr__(self, "start", datetime.fromisoformat(_timestamp(self.start)))
        object.__setattr__(self, "end", datetime.fromisoformat(_timestamp(self.end)))
        object.__setattr__(self, "boundaries", tuple(datetime.fromisoformat(_timestamp(x)) for x in self.boundaries))
        if (
            self.start >= self.end
            or len(self.boundaries) < 2
            or self.boundaries[0] != self.start
            or self.boundaries[-1] != self.end
            or any(a >= b for a, b in zip(self.boundaries, self.boundaries[1:]))
        ):
            raise WindowContractError("Chunks must cover the nonempty window without gaps or overlap")
        if isinstance(self.workers, bool) or not isinstance(self.workers, int) or not 1 <= self.workers <= 64:
            raise WindowContractError("workers must be an integer between 1 and 64")
        object.__setattr__(self, "_run_id", self._compute_identity())

    @property
    def run_id(self) -> str:
        """Canonical v1 identity, independent of worker scheduling and owner."""
        return self._run_id

    def _compute_identity(self) -> str:
        return _identity(
            [
                1,
                self.route_id,
                self.target_id,
                self.window_column,
                _timestamp(self.start),
                _timestamp(self.end),
                [_timestamp(x) for x in self.boundaries],
                self.schema_fingerprint,
                self.source_version,
                self.parameters_fingerprint,
            ]
        )

    @property
    def chunks(self) -> tuple[WindowChunk, ...]:
        """Return deterministic ranges in ascending boundary order."""
        return tuple(
            WindowChunk(_identity([self.run_id, _timestamp(a), _timestamp(b)]), a, b)
            for a, b in zip(self.boundaries, self.boundaries[1:])
        )


@dataclass(frozen=True)
class WindowLease:
    """Target-scoped exclusive writer epoch; every mutation must check it."""

    target_id: str
    owner: str
    fence: int


@dataclass(frozen=True)
class ChunkReceipt:
    """Adapter-verified source/staging parity, including duplicate multiplicity.

    digest identifies a documented typed multiset reconciliation. A digest is
    probabilistic evidence, not an exact-equality claim. Adapter staging must
    persist this receipt atomically with its verification completion marker.
    """

    chunk_id: str
    attempt_id: str
    row_count: int
    digest: str

    def __post_init__(self) -> None:
        if (
            any(not isinstance(value, str) or not value for value in (self.chunk_id, self.attempt_id, self.digest))
            or type(self.row_count) is not int
            or self.row_count < 0
        ):
            raise WindowContractError("Invalid verified chunk receipt")


@dataclass(frozen=True)
class WindowRecord:
    """CAS record: JSON payload contains metadata only, with version-one encoding."""

    revision: int
    payload: str


@dataclass(frozen=True)
class WindowResult:
    """Success exists only after publication, durable evidence, and source state."""

    run_id: str
    generation: str
    receipts: tuple[ChunkReceipt, ...]


def decode_receipt(value: object) -> ChunkReceipt:
    """Reject malformed receipts instead of coercing untrusted durable metadata."""
    if not isinstance(value, dict) or set(value) != {"chunk_id", "attempt_id", "row_count", "digest"}:
        raise WindowContractError("Malformed persisted chunk receipt")
    return ChunkReceipt(**value)


def validate_window_record(name: str, value: object) -> dict[str, object]:
    if not isinstance(value, dict) or type(value.get("version")) is not int or value["version"] != 1:
        raise WindowContractError("Unsupported bounded-window journal version")
    phase = value.get("phase")
    if name == "run":
        if phase == "planned":
            required = {"version", "phase"}
        elif phase in ("publishing", "published", "evidence-complete", "succeeded"):
            required = {"version", "phase", "generation", "receipts"}
            if not isinstance(value.get("generation"), str) or not value["generation"]:
                raise WindowContractError("Malformed persisted generation")
            if not isinstance(value.get("receipts"), list):
                raise WindowContractError("Missing persisted publication receipts")
            for receipt in value["receipts"]:
                decode_receipt(receipt)
        else:
            raise WindowContractError("Unsupported run phase")
    else:
        if phase not in ("staging", "verified", "failed"):
            raise WindowContractError("Unsupported chunk phase")
        if type(value.get("attempt")) is not int or not 0 <= value["attempt"] <= 2:
            raise WindowContractError("Invalid persisted attempt ordinal")
        required = {"version", "phase", "attempt"}
        if phase == "verified":
            required.add("receipt")
            decode_receipt(value.get("receipt"))
    if set(value) != required:
        raise WindowContractError("Unexpected persisted journal fields")
    return cast(dict[str, object], value)


def receipt_data(receipt: ChunkReceipt) -> dict[str, object]:
    """Serialize only sanitized verification metadata, never rows."""
    return asdict(receipt)


_PLAN_FIELDS = {
    "route_id",
    "target_id",
    "start",
    "end",
    "boundaries",
    "schema_fingerprint",
    "source_version",
    "parameters_fingerprint",
    "workers",
    "window_column",
    "run_id",
}


def invocation_fingerprint(*values: object) -> str:
    """Hash canonical, sanitized request identity supplied by the composition root."""
    return hashlib.sha256(
        json.dumps(values, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def encode_window_plan(plan: WindowPlan) -> dict[str, object]:
    return {
        "route_id": plan.route_id,
        "target_id": plan.target_id,
        "start": plan.start.isoformat(timespec="microseconds"),
        "end": plan.end.isoformat(timespec="microseconds"),
        "boundaries": [x.isoformat(timespec="microseconds") for x in plan.boundaries],
        "schema_fingerprint": plan.schema_fingerprint,
        "source_version": plan.source_version,
        "parameters_fingerprint": plan.parameters_fingerprint,
        "workers": plan.workers,
        "window_column": plan.window_column,
        "run_id": plan.run_id,
    }


def decode_window_plan(value: object) -> WindowPlan:
    if not isinstance(value, dict) or set(value) != _PLAN_FIELDS:
        raise WindowContractError("rolling_window_registry_plan_invalid")
    if not isinstance(value["boundaries"], list):
        raise WindowContractError("rolling_window_registry_boundaries_invalid")
    try:
        plan = WindowPlan(
            value["route_id"],
            value["target_id"],
            datetime.fromisoformat(value["start"]),
            datetime.fromisoformat(value["end"]),
            tuple(datetime.fromisoformat(x) for x in value["boundaries"]),
            value["schema_fingerprint"],
            value["source_version"],
            value["parameters_fingerprint"],
            value["workers"],
            window_column=value["window_column"],
        )
    except (TypeError, ValueError) as error:
        raise WindowContractError("rolling_window_registry_plan_invalid") from error
    if encode_window_plan(plan) != value:
        raise WindowContractError("rolling_window_registry_identity_invalid")
    return plan
