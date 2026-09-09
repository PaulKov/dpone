"""Strict workspace promotion v3 identity; not cryptographic authorization."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from pathlib import PurePosixPath

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_promotion import (
    DBT_MIRROR_TRANSACTION_DIRECTORY,
    DBT_PROMOTION_SOURCE_DRIFT,
    DBT_PROMOTION_SOURCE_VERIFIED,
    DbtProdMirrorError,
    DbtPromotionTrustDescriptor,
    dbt_json_object,
    require_dbt_digest,
    safe_dbt_reference,
)

DBT_WORKSPACE_PROMOTION_SCHEMA = "dpone.dbt-prod-promotion.v3"
MAX_DBT_WORKSPACE_PROMOTION_BYTES = 64 * 1024
_KEYS = frozenset(
    {
        "schema",
        "promotion_id",
        "release_id",
        "mirror_root",
        "source_snapshot_path",
        "source_snapshot_sha256",
        "dev_deployment_id",
        "dev_evidence_ref",
        "dev_evidence_subject_sha256",
        "dev_evidence_artifact_name",
        "dev_evidence_producer_workflow",
        "dev_evidence_source_commit",
        "dev_evidence_set_id",
        "dev_evidence_campaign_request_sha256",
    }
)


@dataclass(frozen=True, slots=True)
class DbtWorkspacePromotionDescriptor:
    """Immutable complete source and accepted DEV identity for an audit mirror."""

    release_id: str
    mirror_root: str
    source_snapshot_path: str
    source_snapshot_sha256: str
    dev_deployment_id: str
    dev_evidence_ref: str
    trust: DbtPromotionTrustDescriptor

    def __post_init__(self) -> None:
        for field in ("release_id", "source_snapshot_sha256", "dev_deployment_id"):
            require_dbt_digest(getattr(self, field), field)
        safe_dbt_reference(self.dev_evidence_ref)
        _disjoint_paths((self.mirror_root, self.source_snapshot_path))
        if not isinstance(self.trust, DbtPromotionTrustDescriptor):
            raise DbtProdMirrorError("workspace promotion trust identity is invalid")
        validated = DbtPromotionTrustDescriptor.validated(
            subject_sha256=self.trust.subject_sha256,
            artifact_name=self.trust.artifact_name,
            producer_workflow=self.trust.producer_workflow,
            source_commit=self.trust.source_commit,
            evidence_set_id=self.trust.evidence_set_id,
            campaign_request_sha256=self.trust.campaign_request_sha256,
        )
        if validated.evidence_set_id is None or validated.campaign_request_sha256 is None:
            raise DbtProdMirrorError("workspace promotion requires complete DEV campaign identity")

    @property
    def promotion_id(self) -> str:
        return canonical_fingerprint(self._unsigned())

    def to_dict(self) -> dict[str, str]:
        unsigned = self._unsigned()
        return {**unsigned, "promotion_id": canonical_fingerprint(unsigned)}

    def _unsigned(self) -> dict[str, str]:
        return {
            "schema": DBT_WORKSPACE_PROMOTION_SCHEMA,
            "release_id": self.release_id,
            "mirror_root": self.mirror_root,
            "source_snapshot_path": self.source_snapshot_path,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "dev_deployment_id": self.dev_deployment_id,
            "dev_evidence_ref": self.dev_evidence_ref,
            **self.trust.to_dict(),
        }

    @classmethod
    def from_payload(cls, payload: bytes) -> DbtWorkspacePromotionDescriptor:
        if not isinstance(payload, bytes) or len(payload) > MAX_DBT_WORKSPACE_PROMOTION_BYTES:
            raise DbtProdMirrorError("workspace promotion descriptor exceeds its byte bound")
        try:
            value = dbt_json_object(payload)
        except RecursionError as exc:
            raise DbtProdMirrorError("workspace promotion descriptor nesting is invalid") from exc
        if set(value) != _KEYS or value.get("schema") != DBT_WORKSPACE_PROMOTION_SCHEMA:
            raise DbtProdMirrorError("workspace promotion descriptor fields or schema are invalid")

        def text(key: str) -> str:
            item = value[key]
            if not isinstance(item, str):
                raise DbtProdMirrorError(f"workspace promotion {key} must be text")
            return item

        result = cls(
            release_id=text("release_id"),
            mirror_root=text("mirror_root"),
            source_snapshot_path=text("source_snapshot_path"),
            source_snapshot_sha256=text("source_snapshot_sha256"),
            dev_deployment_id=text("dev_deployment_id"),
            dev_evidence_ref=text("dev_evidence_ref"),
            trust=DbtPromotionTrustDescriptor.validated(
                subject_sha256=value["dev_evidence_subject_sha256"],
                artifact_name=value["dev_evidence_artifact_name"],
                producer_workflow=value["dev_evidence_producer_workflow"],
                source_commit=value["dev_evidence_source_commit"],
                evidence_set_id=value["dev_evidence_set_id"],
                campaign_request_sha256=value["dev_evidence_campaign_request_sha256"],
            ),
        )
        if result.promotion_id != text("promotion_id"):
            raise DbtProdMirrorError("workspace promotion fingerprint differs from its fields")
        return result


@dataclass(frozen=True, slots=True)
class DbtWorkspaceProjectVerification:
    """One pinned project; unavailable observations remain explicit failed rows."""

    project_path: str
    expected_project_bundle_sha256: str
    observed_project_bundle_sha256: str | None
    passed: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "project_path": self.project_path,
            "expected_project_bundle_sha256": self.expected_project_bundle_sha256,
            "observed_project_bundle_sha256": self.observed_project_bundle_sha256,
            "passed": self.passed,
            "code": DBT_PROMOTION_SOURCE_VERIFIED if self.passed else DBT_PROMOTION_SOURCE_DRIFT,
        }


@dataclass(frozen=True, slots=True)
class DbtWorkspacePromotionVerificationReport:
    """Complete source-only report; not an attestation or deployment approval."""

    release_id: str
    source_snapshot_sha256: str
    projects: tuple[DbtWorkspaceProjectVerification, ...]
    metadata_passed: bool
    tree_passed: bool

    @property
    def passed(self) -> bool:
        return (
            self.metadata_passed and self.tree_passed and bool(self.projects) and all(p.passed for p in self.projects)
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-workspace-promotion-verification.v1",
            "status": "passed" if self.passed else "failed",
            "passed": self.passed,
            "code": DBT_PROMOTION_SOURCE_VERIFIED if self.passed else DBT_PROMOTION_SOURCE_DRIFT,
            "release_id": self.release_id,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "projects": [project.to_dict() for project in self.projects],
        }


def validate_workspace_mirror_paths(
    mirror_root: str, source_snapshot_path: str, descriptor_path: str
) -> tuple[PurePosixPath, PurePosixPath, PurePosixPath]:
    """Reject equal, ancestor, reserved and nonportable transaction destinations."""

    _disjoint_paths((mirror_root, source_snapshot_path, descriptor_path))
    return PurePosixPath(mirror_root), PurePosixPath(source_snapshot_path), PurePosixPath(descriptor_path)


def _disjoint_paths(values: tuple[str, ...]) -> None:
    paths = tuple(_relative_path(value) for value in values)
    portable = tuple(path.as_posix().casefold() for path in paths)
    for index, path in enumerate(portable):
        for other in portable[index + 1 :]:
            if path == other or path.startswith(other + "/") or other.startswith(path + "/"):
                raise DbtProdMirrorError("workspace promotion destinations overlap")
    components = {item.as_posix() for path in paths for item in (path, *path.parents)}
    if len({path.casefold() for path in components}) != len(components):
        raise DbtProdMirrorError("workspace promotion destinations have portable case collisions")


def _relative_path(value: str) -> PurePosixPath:
    if not isinstance(value, str):
        raise DbtProdMirrorError("workspace promotion path must be text")
    path = PurePosixPath(value)
    try:
        byte_length = len(value.encode("utf-8"))
    except UnicodeError as exc:
        raise DbtProdMirrorError("workspace promotion path is not valid UTF-8") from exc
    if (
        not path.parts
        or path.is_absolute()
        or path.as_posix() != value
        or "\\" in value
        or byte_length > 1024
        or len(path.parts) > 32
        or unicodedata.normalize("NFC", value) != value
        or any(part.casefold() in {"..", ".git", ".worktrees", DBT_MIRROR_TRANSACTION_DIRECTORY} for part in path.parts)
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise DbtProdMirrorError("workspace promotion path is unsafe")
    return path


__all__ = [
    "DBT_MIRROR_TRANSACTION_DIRECTORY",
    "DBT_WORKSPACE_PROMOTION_SCHEMA",
    "MAX_DBT_WORKSPACE_PROMOTION_BYTES",
    "DbtWorkspacePromotionDescriptor",
    "DbtWorkspaceProjectVerification",
    "DbtWorkspacePromotionVerificationReport",
    "validate_workspace_mirror_paths",
]
