from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class GitOpsIssue:
    code: str
    message: str
    path: str
    source: str

    def to_jsonable(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class GitOpsSparsePath:
    path: str
    kind: str
    source: str
    required: bool
    exists: bool
    is_dir: bool
    reason: str

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "source": self.source,
            "required": self.required,
            "exists": self.exists,
            "is_dir": self.is_dir,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsRunnerContract:
    kind: str
    requires_sparse_checkout: bool = True

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "requires_sparse_checkout": self.requires_sparse_checkout,
        }


@dataclass(frozen=True, slots=True)
class GitOpsCommandContract:
    text: str

    def to_jsonable(self) -> dict[str, str]:
        return {"text": self.text}


@dataclass(frozen=True, slots=True)
class GitOpsProvenance:
    schema_version: str = "1"
    producer: str = "dpone gitops plan"

    def to_jsonable(self) -> dict[str, str]:
        return {
            "schema_version": self.schema_version,
            "producer": self.producer,
        }


@dataclass(frozen=True, slots=True)
class GitOpsLockEntry:
    path: str
    exists: bool
    is_dir: bool
    sha256: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "exists": self.exists,
            "is_dir": self.is_dir,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class GitOpsLockReport:
    entries: tuple[GitOpsLockEntry, ...]
    schema_version: str = "1"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "entries": [entry.to_jsonable() for entry in self.entries],
        }


@dataclass(frozen=True, slots=True)
class GitOpsPlanReport:
    manifest: str
    workload_root: str
    sparse_paths: tuple[GitOpsSparsePath, ...]
    runner: GitOpsRunnerContract
    command: GitOpsCommandContract
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    provenance: GitOpsProvenance = GitOpsProvenance()
    lock: GitOpsLockReport = GitOpsLockReport(entries=())
    kind: str = "gitops.plan"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "manifest": self.manifest,
            "workload_root": self.workload_root,
            "runner": self.runner.to_jsonable(),
            "command": self.command.to_jsonable(),
            "sparse_paths": [entry.to_jsonable() for entry in self.sparse_paths],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
            "provenance": self.provenance.to_jsonable(),
            "lock": self.lock.to_jsonable(),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsPathCheck:
    path: str
    required: bool
    exists: bool
    is_dir: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "required": self.required,
            "exists": self.exists,
            "is_dir": self.is_dir,
        }


@dataclass(frozen=True, slots=True)
class GitOpsLockCheck:
    path: str
    exists: bool
    is_dir: bool
    expected_sha256: str | None
    actual_sha256: str | None
    passed: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "exists": self.exists,
            "is_dir": self.is_dir,
            "expected_sha256": self.expected_sha256,
            "actual_sha256": self.actual_sha256,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsVerifyReport:
    plan: str
    worktree: str
    manifest: str
    checked_paths: tuple[GitOpsPathCheck, ...]
    lock_checks: tuple[GitOpsLockCheck, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.verify"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "plan": self.plan,
            "worktree": self.worktree,
            "manifest": self.manifest,
            "checked_paths": [check.to_jsonable() for check in self.checked_paths],
            "lock_checks": [check.to_jsonable() for check in self.lock_checks],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsBundlePolicy:
    profile: str = "custom"
    verify_lock: bool = False
    fail_on_empty_impact: bool = False
    fail_on_warnings: bool = False
    require_lock: bool = False

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "verify_lock": self.verify_lock,
            "fail_on_empty_impact": self.fail_on_empty_impact,
            "fail_on_warnings": self.fail_on_warnings,
            "require_lock": self.require_lock,
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleEntry:
    manifest: str
    plan_path: str
    verify_path: str
    passed: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "manifest": self.manifest,
            "plan_path": self.plan_path,
            "verify_path": self.verify_path,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleArtifactDigest:
    path: str
    sha256: str
    bytes: int

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "sha256": self.sha256,
            "bytes": self.bytes,
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleAttestation:
    artifacts: tuple[GitOpsBundleArtifactDigest, ...]
    bundle_digest: str
    provenance: dict[str, Any]
    schema_version: str = "1"
    producer: str = "dpone gitops bundle"
    hash_algorithm: str = "sha256"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "producer": self.producer,
            "hash_algorithm": self.hash_algorithm,
            "bundle_digest": self.bundle_digest,
            "provenance": dict(self.provenance),
            "artifacts": [artifact.to_jsonable() for artifact in self.artifacts],
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleReport:
    output_dir: str
    affected_path: str
    summary_path: str
    entries: tuple[GitOpsBundleEntry, ...]
    policy: GitOpsBundlePolicy
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    attestation: GitOpsBundleAttestation | None = None
    kind: str = "gitops.bundle"

    @property
    def passed(self) -> bool:
        return not self.blockers and all(entry.passed for entry in self.entries)

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "output_dir": self.output_dir,
            "affected_path": self.affected_path,
            "summary_path": self.summary_path,
            "entries": [entry.to_jsonable() for entry in self.entries],
            "policy": self.policy.to_jsonable(),
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }
        if self.attestation is not None:
            payload["attestation"] = self.attestation.to_jsonable()
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class GitOpsImpactReason:
    changed_path: str
    matched_path: str
    kind: str
    source: str
    reason: str

    def to_jsonable(self) -> dict[str, str]:
        return {
            "changed_path": self.changed_path,
            "matched_path": self.matched_path,
            "kind": self.kind,
            "source": self.source,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class GitOpsImpactedManifest:
    manifest: str
    workload_root: str
    reasons: tuple[GitOpsImpactReason, ...]
    suggested_commands: tuple[str, ...]
    emitted_plan: str | None = None

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "manifest": self.manifest,
            "workload_root": self.workload_root,
            "reasons": [reason.to_jsonable() for reason in self.reasons],
            "suggested_commands": list(self.suggested_commands),
        }
        if self.emitted_plan is not None:
            payload["emitted_plan"] = self.emitted_plan
        return payload


@dataclass(frozen=True, slots=True)
class GitOpsAffectedReport:
    changed_files: tuple[str, ...]
    impacted_manifests: tuple[GitOpsImpactedManifest, ...]
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.affected"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "changed_files": list(self.changed_files),
            "impacted_manifests": [item.to_jsonable() for item in self.impacted_manifests],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


__all__ = [
    "GitOpsAffectedReport",
    "GitOpsBundleArtifactDigest",
    "GitOpsBundleAttestation",
    "GitOpsBundleEntry",
    "GitOpsBundlePolicy",
    "GitOpsBundleReport",
    "GitOpsCommandContract",
    "GitOpsIssue",
    "GitOpsImpactReason",
    "GitOpsImpactedManifest",
    "GitOpsLockEntry",
    "GitOpsLockCheck",
    "GitOpsLockReport",
    "GitOpsPathCheck",
    "GitOpsPlanReport",
    "GitOpsProvenance",
    "GitOpsRunnerContract",
    "GitOpsSparsePath",
    "GitOpsVerifyReport",
]
