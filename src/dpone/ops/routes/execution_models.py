"""Generic route execution ledger models and report contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from dpone.ops.routes.models import RouteKey

SCHEMA_VERSION = "dpone.route_execution_ledger.v1"


class RouteExecutionStage(str, Enum):  # noqa: UP042
    """Ordered route execution phases used by every source -> sink strategy."""

    PLANNED = "planned"
    EXTRACTING = "extracting"
    LOADED_TO_STAGING = "loaded_to_staging"
    FINALIZED = "finalized"
    QUALITY_CHECKED = "quality_checked"
    STATE_COMMITTED = "state_committed"

    @classmethod
    def of(cls, value: str | RouteExecutionStage) -> RouteExecutionStage:
        if isinstance(value, RouteExecutionStage):
            return value
        return cls(str(value).strip().lower().replace("-", "_"))

    @property
    def order(self) -> int:
        return _STAGE_ORDER[self]

    def can_transition_to(self, next_stage: RouteExecutionStage) -> bool:
        return self.order <= next_stage.order


_STAGE_ORDER: Mapping[RouteExecutionStage, int] = {
    RouteExecutionStage.PLANNED: 10,
    RouteExecutionStage.EXTRACTING: 20,
    RouteExecutionStage.LOADED_TO_STAGING: 30,
    RouteExecutionStage.FINALIZED: 40,
    RouteExecutionStage.QUALITY_CHECKED: 50,
    RouteExecutionStage.STATE_COMMITTED: 60,
}


class RouteExecutionStatus(str, Enum):  # noqa: UP042
    """Status for one recorded execution stage."""

    PLANNED = "planned"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"
    COMMITTED = "committed"

    @classmethod
    def of(cls, value: str | RouteExecutionStatus) -> RouteExecutionStatus:
        if isinstance(value, RouteExecutionStatus):
            return value
        return cls(str(value).strip().lower().replace("-", "_"))

    @property
    def is_terminal(self) -> bool:
        return self in {
            RouteExecutionStatus.SUCCEEDED,
            RouteExecutionStatus.FAILED,
            RouteExecutionStatus.SKIPPED,
            RouteExecutionStatus.COMMITTED,
        }

    @property
    def is_successful(self) -> bool:
        return self in {RouteExecutionStatus.SUCCEEDED, RouteExecutionStatus.COMMITTED}


@dataclass(frozen=True, slots=True)
class RouteArtifactHash:
    """Checksum metadata for an artifact referenced by a ledger step."""

    name: str
    path: str
    sha256: str
    missing: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "path": self.path,
            "sha256": self.sha256,
            "missing": self.missing,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RouteArtifactHash:
        return cls(
            name=str(payload.get("name", "")),
            path=str(payload.get("path", "")),
            sha256=str(payload.get("sha256", "")),
            missing=bool(payload.get("missing", False)),
        )


@dataclass(frozen=True, slots=True)
class RouteExecutionStep:
    """One append-only route execution step."""

    stage: RouteExecutionStage
    status: RouteExecutionStatus
    runner_id: str
    created_at: str
    source_boundary: str
    sink_boundary: str
    idempotency_key: str
    artifact_hashes: Mapping[str, RouteArtifactHash]
    metadata: Mapping[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "runner_id": self.runner_id,
            "created_at": self.created_at,
            "source_boundary": self.source_boundary,
            "sink_boundary": self.sink_boundary,
            "idempotency_key": self.idempotency_key,
            "artifact_hashes": {name: artifact.to_dict() for name, artifact in self.artifact_hashes.items()},
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RouteExecutionStep:
        artifacts = payload.get("artifact_hashes", {})
        artifact_hashes: dict[str, RouteArtifactHash] = {}
        if isinstance(artifacts, Mapping):
            artifact_hashes = {
                str(name): RouteArtifactHash.from_dict(value)
                for name, value in artifacts.items()
                if isinstance(value, Mapping)
            }
        metadata = payload.get("metadata", {})
        return cls(
            stage=RouteExecutionStage.of(str(payload.get("stage", RouteExecutionStage.PLANNED.value))),
            status=RouteExecutionStatus.of(str(payload.get("status", RouteExecutionStatus.PLANNED.value))),
            runner_id=str(payload.get("runner_id", "")),
            created_at=str(payload.get("created_at", "")),
            source_boundary=str(payload.get("source_boundary", "")),
            sink_boundary=str(payload.get("sink_boundary", "")),
            idempotency_key=str(payload.get("idempotency_key", "")),
            artifact_hashes=artifact_hashes,
            metadata=metadata if isinstance(metadata, Mapping) else {},
        )

    def fingerprint(self) -> dict[str, object]:
        payload = self.to_dict()
        payload.pop("created_at", None)
        payload.pop("runner_id", None)
        return payload


@dataclass(frozen=True, slots=True)
class RouteExecutionLease:
    """Local commit-fencing lease for one route and dataset."""

    lease_key: str
    owner: str
    acquired_at: str
    expires_at: str
    fencing_token: str

    def to_dict(self) -> dict[str, object]:
        return {
            "lease_key": self.lease_key,
            "owner": self.owner,
            "acquired_at": self.acquired_at,
            "expires_at": self.expires_at,
            "fencing_token": self.fencing_token,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RouteExecutionLease:
        return cls(
            lease_key=str(payload.get("lease_key", "")),
            owner=str(payload.get("owner", "")),
            acquired_at=str(payload.get("acquired_at", "")),
            expires_at=str(payload.get("expires_at", "")),
            fencing_token=str(payload.get("fencing_token", "")),
        )


@dataclass(frozen=True, slots=True)
class RouteExecutionDecision:
    """Pure policy decision for one ledger write."""

    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RouteExecutionLedgerReport:
    """Stable JSON/Markdown route execution ledger contract."""

    route: RouteKey
    dataset: str
    run_id: str
    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    steps: tuple[RouteExecutionStep, ...]
    lease: RouteExecutionLease | None
    output_dir: str
    ledger_path: str
    json_path: str
    markdown_path: str
    store_backend: str = "local_json"

    @property
    def step_count(self) -> int:
        return len(self.steps)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "dataset": self.dataset,
            "run_id": self.run_id,
            "passed": self.passed,
            "level": self.level,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "step_count": self.step_count,
            "steps": [step.to_dict() for step in self.steps],
            "lease": self.lease.to_dict() if self.lease else None,
            "output_dir": self.output_dir,
            "ledger_path": self.ledger_path,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "store_backend": self.store_backend,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone route execution ledger",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Run ID: `{self.run_id}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- Store backend: `{self.store_backend}`",
            f"- Step count: `{self.step_count}`",
            "",
            "| stage | status | runner | source boundary | sink boundary | idempotency key |",
            "|---|---|---|---|---|---|",
        ]
        for step in self.steps:
            lines.append(
                f"| `{step.stage.value}` | `{step.status.value}` | `{step.runner_id}` | "
                f"`{step.source_boundary}` | `{step.sink_boundary}` | `{step.idempotency_key}` |"
            )
        if self.lease:
            lines.extend(
                [
                    "",
                    "## Lease",
                    "",
                    f"- Owner: `{self.lease.owner}`",
                    f"- Fencing token: `{self.lease.fencing_token}`",
                    f"- Expires at: `{self.lease.expires_at}`",
                ]
            )
        lines.extend(["", "## Blockers", ""])
        lines.extend(f"- `{item}`" for item in self.blockers)
        if not self.blockers:
            lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings)
        if not self.warnings:
            lines.append("- none")
        lines.extend(["", "## Operator runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Re-run with the same idempotency key to prove safe replay.",
                    "- Attach this ledger to route readiness, release evidence, or incident review.",
                    "- Commit source state only after durable sink success and quality evidence are recorded.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")
