from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path


@dataclass(frozen=True, slots=True)
class GitOpsBundleSchemaCheck:
    expected_kind: str
    actual_kind: str | None
    passed: bool
    issues: tuple[GitOpsIssue, ...] = ()

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "expected_kind": self.expected_kind,
            "actual_kind": self.actual_kind,
            "passed": self.passed,
            "issues": [issue.to_jsonable() for issue in self.issues],
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleArtifactCheck:
    path: str
    exists: bool
    expected_sha256: str | None
    actual_sha256: str | None
    expected_bytes: int | None
    actual_bytes: int | None
    passed: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "exists": self.exists,
            "expected_sha256": self.expected_sha256,
            "actual_sha256": self.actual_sha256,
            "expected_bytes": self.expected_bytes,
            "actual_bytes": self.actual_bytes,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleAttestationCheck:
    present: bool
    required: bool
    expected_bundle_digest: str | None
    actual_bundle_digest: str | None
    passed: bool

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "present": self.present,
            "required": self.required,
            "expected_bundle_digest": self.expected_bundle_digest,
            "actual_bundle_digest": self.actual_bundle_digest,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class GitOpsBundleVerifyReport:
    bundle_path: str
    schema_check: GitOpsBundleSchemaCheck
    attestation_check: GitOpsBundleAttestationCheck
    artifact_checks: tuple[GitOpsBundleArtifactCheck, ...] = ()
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.bundle_verify"

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "bundle_path": self.bundle_path,
            "schema_check": self.schema_check.to_jsonable(),
            "attestation_check": self.attestation_check.to_jsonable(),
            "artifact_checks": [check.to_jsonable() for check in self.artifact_checks],
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


@dataclass(frozen=True, slots=True)
class BundleVerificationResult:
    attestation_check: GitOpsBundleAttestationCheck
    artifact_checks: tuple[GitOpsBundleArtifactCheck, ...]
    warnings: tuple[GitOpsIssue, ...]
    blockers: tuple[GitOpsIssue, ...]


class GitOpsBundleVerifier:
    """Verifies bundle attestation digests against local repo-relative artifacts."""

    def verify(
        self,
        *,
        repo_root: Path,
        bundle: Mapping[str, Any],
        require_attestation: bool,
    ) -> BundleVerificationResult:
        attestation = bundle.get("attestation")
        if not isinstance(attestation, Mapping):
            issue = GitOpsIssue(
                code="attestation_required" if require_attestation else "attestation_missing",
                message="Bundle does not contain attestation evidence",
                path="attestation",
                source="dpone gitops bundle verify",
            )
            return BundleVerificationResult(
                attestation_check=GitOpsBundleAttestationCheck(
                    present=False,
                    required=require_attestation,
                    expected_bundle_digest=None,
                    actual_bundle_digest=None,
                    passed=not require_attestation,
                ),
                artifact_checks=(),
                warnings=() if require_attestation else (issue,),
                blockers=(issue,) if require_attestation else (),
            )

        expected_bundle_digest = _optional_string(attestation.get("bundle_digest"))
        artifact_items = attestation.get("artifacts")
        if not isinstance(artifact_items, list):
            blocker = GitOpsIssue(
                code="attestation_artifacts_invalid",
                message="Bundle attestation artifacts must be a list",
                path="attestation.artifacts",
                source="dpone gitops bundle verify",
            )
            return BundleVerificationResult(
                attestation_check=GitOpsBundleAttestationCheck(
                    present=True,
                    required=require_attestation,
                    expected_bundle_digest=expected_bundle_digest,
                    actual_bundle_digest=None,
                    passed=False,
                ),
                artifact_checks=(),
                warnings=(),
                blockers=(blocker,),
            )

        artifact_checks: list[GitOpsBundleArtifactCheck] = []
        blockers: list[GitOpsIssue] = []
        actual_records: list[dict[str, object]] = []
        for index, raw_artifact in enumerate(artifact_items):
            if not isinstance(raw_artifact, Mapping):
                blockers.append(
                    GitOpsIssue(
                        code="attestation_artifact_invalid",
                        message="Attestation artifact must be an object",
                        path=f"attestation.artifacts[{index}]",
                        source="dpone gitops bundle verify",
                    )
                )
                continue
            check, blocker = _verify_artifact(repo_root=repo_root, artifact=raw_artifact)
            artifact_checks.append(check)
            if blocker is not None:
                blockers.append(blocker)
            if check.exists and check.actual_sha256 is not None and check.actual_bytes is not None:
                actual_records.append({"path": check.path, "sha256": check.actual_sha256, "bytes": check.actual_bytes})

        actual_bundle_digest = _bundle_digest(actual_records) if len(actual_records) == len(artifact_items) else None
        if expected_bundle_digest and actual_bundle_digest and actual_bundle_digest != expected_bundle_digest:
            blockers.append(
                GitOpsIssue(
                    code="bundle_digest_mismatch",
                    message="Attestation bundle digest does not match current artifact digests",
                    path="attestation.bundle_digest",
                    source="dpone gitops bundle verify",
                )
            )
        attestation_passed = not blockers and bool(expected_bundle_digest)
        return BundleVerificationResult(
            attestation_check=GitOpsBundleAttestationCheck(
                present=True,
                required=require_attestation,
                expected_bundle_digest=expected_bundle_digest,
                actual_bundle_digest=actual_bundle_digest,
                passed=attestation_passed,
            ),
            artifact_checks=tuple(artifact_checks),
            warnings=(),
            blockers=tuple(blockers),
        )


def _verify_artifact(
    *, repo_root: Path, artifact: Mapping[str, Any]
) -> tuple[GitOpsBundleArtifactCheck, GitOpsIssue | None]:
    raw_path = artifact.get("path")
    expected_path = str(raw_path or "")
    expected_sha = _optional_string(artifact.get("sha256"))
    expected_bytes = _optional_int(artifact.get("bytes"))
    try:
        rel_path = safe_relative_path(expected_path, source="attestation.artifacts[].path")
    except GitOpsPathValidationError as exc:
        check = GitOpsBundleArtifactCheck(
            path=expected_path,
            exists=False,
            expected_sha256=expected_sha,
            actual_sha256=None,
            expected_bytes=expected_bytes,
            actual_bytes=None,
            passed=False,
        )
        return check, GitOpsIssue(
            code="invalid_artifact_path",
            message=str(exc),
            path=expected_path,
            source="dpone gitops bundle verify",
        )

    path = repo_root / rel_path
    if not path.exists():
        check = GitOpsBundleArtifactCheck(
            path=rel_path.as_posix(),
            exists=False,
            expected_sha256=expected_sha,
            actual_sha256=None,
            expected_bytes=expected_bytes,
            actual_bytes=None,
            passed=False,
        )
        return check, GitOpsIssue(
            code="artifact_missing",
            message="Attested artifact is missing",
            path=rel_path.as_posix(),
            source="dpone gitops bundle verify",
        )

    data = path.read_bytes()
    actual_sha = hashlib.sha256(data).hexdigest()
    actual_bytes = len(data)
    passed = expected_sha == actual_sha and expected_bytes == actual_bytes
    check = GitOpsBundleArtifactCheck(
        path=rel_path.as_posix(),
        exists=True,
        expected_sha256=expected_sha,
        actual_sha256=actual_sha,
        expected_bytes=expected_bytes,
        actual_bytes=actual_bytes,
        passed=passed,
    )
    if passed:
        return check, None
    code = "artifact_digest_mismatch" if expected_sha != actual_sha else "artifact_size_mismatch"
    return check, GitOpsIssue(
        code=code,
        message="Attested artifact digest or size does not match current file",
        path=rel_path.as_posix(),
        source="dpone gitops bundle verify",
    )


def _bundle_digest(records: list[dict[str, object]]) -> str:
    encoded = json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


__all__ = [
    "BundleVerificationResult",
    "GitOpsBundleArtifactCheck",
    "GitOpsBundleAttestationCheck",
    "GitOpsBundleSchemaCheck",
    "GitOpsBundleVerifier",
    "GitOpsBundleVerifyReport",
]
