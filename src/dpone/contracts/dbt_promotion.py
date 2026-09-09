"""Shared validation primitives for bot-owned dbt promotion metadata."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import PurePosixPath

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.contracts.dbt_source_snapshot import LegacyDbtSourceSnapshot
from dpone.contracts.strict_json import StrictJsonError, strict_json_object

_MAX_TRUST_TEXT_LENGTH = 2048
_FULL_GIT_COMMIT = re.compile(r"[0-9a-f]{40}")
DBT_MIRROR_TRANSACTION_DIRECTORY = ".dpone-dbt-promotion.transaction"
DBT_PROMOTION_SOURCE_VERIFIED = "DPONE_DBT_PROMOTION_SOURCE_VERIFIED"
DBT_PROMOTION_SOURCE_DRIFT = "DPONE_DBT_PROMOTION_SOURCE_DRIFT"


class DbtProdMirrorError(ValueError):
    """The prod audit mirror violates its immutable promotion authority."""

    code = "DPONE_DBT_PROMOTION_SOURCE_DRIFT"


@dataclass(frozen=True, slots=True)
class DbtPromotionTrustDescriptor:
    """Immutable identity of the dev evidence authorized for promotion."""

    subject_sha256: str
    artifact_name: str
    producer_workflow: str
    source_commit: str
    evidence_set_id: str | None = None
    campaign_request_sha256: str | None = None

    @classmethod
    def validated(
        cls,
        *,
        subject_sha256: object,
        artifact_name: object,
        producer_workflow: object,
        source_commit: object,
        evidence_set_id: object | None = None,
        campaign_request_sha256: object | None = None,
    ) -> DbtPromotionTrustDescriptor:
        """Validate one complete fail-closed dev evidence identity."""

        if (evidence_set_id is None) != (campaign_request_sha256 is None):
            raise DbtProdMirrorError("dev evidence campaign identity is incomplete")
        return cls(
            subject_sha256=require_dbt_digest(
                subject_sha256,
                "dev evidence subject identity",
            ),
            artifact_name=_safe_trust_text(
                artifact_name,
                "dev evidence artifact name",
            ),
            producer_workflow=_safe_trust_text(
                producer_workflow,
                "dev evidence producer workflow",
            ),
            source_commit=_validated_source_commit(source_commit),
            evidence_set_id=(
                require_dbt_digest(
                    evidence_set_id,
                    "dev evidence set identity",
                )
                if evidence_set_id is not None
                else None
            ),
            campaign_request_sha256=(
                require_dbt_digest(
                    campaign_request_sha256,
                    "dev evidence campaign request identity",
                )
                if campaign_request_sha256 is not None
                else None
            ),
        )

    def to_dict(self) -> dict[str, str]:
        """Return descriptor fields using the published wire names."""

        payload = {
            "dev_evidence_subject_sha256": self.subject_sha256,
            "dev_evidence_artifact_name": self.artifact_name,
            "dev_evidence_producer_workflow": self.producer_workflow,
            "dev_evidence_source_commit": self.source_commit,
        }
        if self.evidence_set_id is not None and self.campaign_request_sha256 is not None:
            payload["dev_evidence_set_id"] = self.evidence_set_id
            payload["dev_evidence_campaign_request_sha256"] = self.campaign_request_sha256
        return payload


def validated_relative_path(value: str, field: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or "\\" in value
        or path.is_absolute()
        or path.as_posix() != value
        or any(part in {"", ".", "..", ".git"} for part in path.parts)
        or any(ord(character) < 32 for character in value)
    ):
        raise DbtProdMirrorError(f"{field} is unsafe")
    return path


def require_dbt_digest(value: object, field: str) -> str:
    if not isinstance(value, str) or not is_canonical_sha256_digest(value):
        raise DbtProdMirrorError(f"{field} is invalid")
    return value


def safe_dbt_reference(value: str) -> str:
    return _safe_trust_text(value, "dev evidence reference")


def _safe_trust_text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_TRUST_TEXT_LENGTH
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise DbtProdMirrorError(f"{field} is invalid")
    return value


def _validated_source_commit(value: object) -> str:
    if not isinstance(value, str) or _FULL_GIT_COMMIT.fullmatch(value) is None:
        raise DbtProdMirrorError("dev evidence source commit is invalid")
    return value


def dbt_json_object(payload: bytes) -> dict[str, object]:
    try:
        value = strict_json_object(payload)
    except StrictJsonError as exc:
        raise DbtProdMirrorError("dbt promotion metadata is invalid JSON") from exc
    return value


_REPORT_SCHEMA = "dpone.dbt-promotion-verification.v1"
_RELEASE_SCHEMA = "dpone.release-set.v2"
_PROJECT_PAYLOAD_ID = "dbt_project"
_PROJECT_PAYLOAD_KIND = "dbt_project_bundle"
_PROJECT_PAYLOAD_PATH = "runtime/dbt/project.tar.gz"
_PROJECT_PAYLOAD_MEDIA_TYPE = "application/vnd.dpone.dbt-project-bundle+gzip"
_MANIFEST_PAYLOAD_ID = "dbt_manifest"
_MANIFEST_PAYLOAD_KIND = "dbt_manifest"
_MANIFEST_PAYLOAD_PATH = "runtime/dbt/manifest.json"
_MANIFEST_PAYLOAD_MEDIA_TYPE = "application/vnd.dbt.manifest+json"


@dataclass(frozen=True, slots=True)
class DbtPromotionVerificationReport:
    """Machine-readable proof that one prod mirror matches one pinned release."""

    code: str
    release_id: str | None = None
    source_snapshot_sha256: str | None = None
    expected_project_bundle_sha256: str | None = None
    observed_project_bundle_sha256: str | None = None

    @property
    def passed(self) -> bool:
        """Return whether byte-identical promotion source was proven."""

        return self.code == DBT_PROMOTION_SOURCE_VERIFIED

    @property
    def status(self) -> str:
        """Return the stable human- and machine-readable status."""

        return "passed" if self.passed else "failed"

    def to_dict(self) -> dict[str, object]:
        """Serialize the deterministic promotion report."""

        return {
            "schema": _REPORT_SCHEMA,
            "status": self.status,
            "passed": self.passed,
            "code": self.code,
            "release_id": self.release_id,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "expected_project_bundle_sha256": self.expected_project_bundle_sha256,
            "observed_project_bundle_sha256": self.observed_project_bundle_sha256,
        }


@dataclass(frozen=True, slots=True)
class _PinnedPromotionSource:
    release_id: str
    source_snapshot_sha256: str
    project_bundle_sha256: str
    project_bundle_bytes: int


def _validated_promotion_source(
    release_set: Mapping[str, object],
    source_snapshot: Mapping[str, object],
) -> _PinnedPromotionSource:
    release_identity = _validated_release_identity(release_set)
    project_sha256, project_bytes = _runtime_payload_metadata(
        release_set,
        payload_id=_PROJECT_PAYLOAD_ID,
        kind=_PROJECT_PAYLOAD_KIND,
        path=_PROJECT_PAYLOAD_PATH,
        media_type=_PROJECT_PAYLOAD_MEDIA_TYPE,
    )
    manifest_sha256, _ = _runtime_payload_metadata(
        release_set,
        payload_id=_MANIFEST_PAYLOAD_ID,
        kind=_MANIFEST_PAYLOAD_KIND,
        path=_MANIFEST_PAYLOAD_PATH,
        media_type=_MANIFEST_PAYLOAD_MEDIA_TYPE,
    )
    pinned_snapshot_sha256 = _release_source_snapshot_sha256(release_set)
    snapshot_sha256, snapshot_project_sha256, snapshot_manifest_sha256 = _validated_source_snapshot(source_snapshot)
    if (
        snapshot_sha256 != pinned_snapshot_sha256
        or snapshot_project_sha256 != project_sha256
        or snapshot_manifest_sha256 != manifest_sha256
    ):
        raise ValueError("source snapshot differs from pinned release metadata")
    return _PinnedPromotionSource(
        release_id=release_identity,
        source_snapshot_sha256=snapshot_sha256,
        project_bundle_sha256=project_sha256,
        project_bundle_bytes=project_bytes,
    )


def _validated_release_identity(release_set: Mapping[str, object]) -> str:
    if not isinstance(release_set, Mapping) or release_set.get("schema") != _RELEASE_SCHEMA:
        raise ValueError("release-set v2 is required")
    release_identity = _canonical_digest(release_set.get("release_id"), "release identity")
    if compute_release_id(release_set) != release_identity:
        raise ValueError("release identity is invalid")
    return release_identity


def _runtime_payload_metadata(
    release_set: Mapping[str, object],
    *,
    payload_id: str,
    kind: str,
    path: str,
    media_type: str,
) -> tuple[str, int]:
    artifacts = release_set.get("artifacts")
    if not isinstance(artifacts, Mapping):
        raise ValueError("release artifacts are invalid")
    runtime_payloads = artifacts.get("runtime_payloads")
    if not isinstance(runtime_payloads, list):
        raise ValueError("release runtime payloads are invalid")
    matches = [item for item in runtime_payloads if isinstance(item, Mapping) and item.get("id") == payload_id]
    if len(matches) != 1:
        raise ValueError("release must contain exactly one required runtime payload")
    payload = matches[0]
    if payload.get("kind") != kind or payload.get("path") != path or payload.get("media_type") != media_type:
        raise ValueError("required runtime payload metadata is invalid")
    payload_sha256 = _canonical_digest(payload.get("sha256"), "required runtime payload digest")
    payload_bytes = payload.get("bytes")
    if isinstance(payload_bytes, bool) or not isinstance(payload_bytes, int) or payload_bytes <= 0:
        raise ValueError("required runtime payload byte count is invalid")
    return payload_sha256, payload_bytes


def _release_source_snapshot_sha256(release_set: Mapping[str, object]) -> str:
    provenance = release_set.get("provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("release provenance is invalid")
    return _canonical_digest(
        provenance.get("source_snapshot_sha256"),
        "release source snapshot digest",
    )


def _validated_source_snapshot(
    source_snapshot: Mapping[str, object],
) -> tuple[str, str, str]:
    snapshot = LegacyDbtSourceSnapshot.from_mapping(source_snapshot)
    return snapshot.snapshot_sha256, snapshot.project_bundle_sha256, snapshot.manifest_sha256


def _canonical_digest(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not is_canonical_sha256_digest(value):
        raise ValueError(f"{field_name} is invalid")
    return value


def _drift_report(
    pinned: _PinnedPromotionSource | None = None,
    *,
    observed_sha256: str | None = None,
) -> DbtPromotionVerificationReport:
    return DbtPromotionVerificationReport(
        code=DBT_PROMOTION_SOURCE_DRIFT,
        release_id=pinned.release_id if pinned is not None else None,
        source_snapshot_sha256=pinned.source_snapshot_sha256 if pinned is not None else None,
        expected_project_bundle_sha256=(pinned.project_bundle_sha256 if pinned is not None else None),
        observed_project_bundle_sha256=observed_sha256,
    )


__all__ = [
    "DBT_MIRROR_TRANSACTION_DIRECTORY",
    "DBT_PROMOTION_SOURCE_DRIFT",
    "DBT_PROMOTION_SOURCE_VERIFIED",
    "DbtProdMirrorError",
    "DbtPromotionTrustDescriptor",
    "DbtPromotionVerificationReport",
    "dbt_json_object",
    "require_dbt_digest",
    "safe_dbt_reference",
    "validated_relative_path",
]
