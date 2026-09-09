"""Provider-neutral schema migration evidence bundle contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import yaml

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_bundle_relationships import (
    approval_blockers,
    bundle_summary,
    relationship_blockers,
    relationship_warnings,
    required_approvals,
)
from dpone.readiness.schema_migration_review import REVIEW_SCHEMA, MigrationReviewRenderer

BUNDLE_SCHEMA = "dpone.schema_migration_bundle.v1"
BUNDLE_VERIFICATION_SCHEMA = "dpone.schema_migration_bundle_verification.v1"


@dataclass(frozen=True, slots=True)
class MigrationEvidenceArtifact:
    kind: str
    path: str
    content: bytes
    payload: dict[str, Any]
    required: bool = False

    @classmethod
    def from_bytes(
        cls,
        *,
        kind: str,
        path: str,
        content: bytes,
        required: bool = False,
    ) -> MigrationEvidenceArtifact:
        return cls(kind=kind, path=path, content=content, payload=_decode_mapping(content, path), required=required)

    @property
    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.content).hexdigest()

    @property
    def schema_version(self) -> str | None:
        value = self.payload.get("schema_version")
        return str(value) if value else None

    @property
    def pack_id(self) -> str | None:
        value = self.payload.get("pack_id")
        return str(value) if value else None

    def to_ref(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "path": self.path,
            "sha256": self.sha256,
            "required": self.required,
            "schema_version": self.schema_version,
            "pack_id": self.pack_id,
        }


class MigrationBundleBuilder:
    """Builds immutable evidence bundles from already-loaded artifacts."""

    def build(self, *, artifacts: tuple[MigrationEvidenceArtifact, ...], attest: bool = False) -> dict[str, Any]:
        by_kind = {artifact.kind: artifact for artifact in artifacts}
        pack_artifact = by_kind.get("migration_pack")
        if pack_artifact is None:
            raise ValueError("migration bundle requires a migration_pack artifact")
        pack = MigrationPack.from_mapping(pack_artifact.payload)
        blockers = [*pack.blockers, *relationship_blockers(pack=pack, artifacts=by_kind)]
        warnings = [*pack.warnings, *relationship_warnings(artifacts=by_kind)]
        required = required_approvals(by_kind.get("impact_plan"))
        summary = bundle_summary(pack=pack, artifacts=by_kind, required_approvals=required)
        blocker_set = tuple(dict.fromkeys(blockers + list(approval_blockers(required, by_kind.get("approval")))))
        warning_set = tuple(dict.fromkeys(warnings))
        status = "blocked" if blocker_set else "ready"
        artifact_refs = tuple(artifact.to_ref() for artifact in sorted(artifacts, key=lambda item: item.kind))
        payload: dict[str, Any] = {
            "schema_version": BUNDLE_SCHEMA,
            "bundle_id": stable_fingerprint(
                {
                    "pack_id": pack.pack_id,
                    "artifact_digests": artifact_refs,
                    "summary": summary,
                    "blockers": blocker_set,
                    "warnings": warning_set,
                }
            ),
            "pack_id": pack.pack_id,
            "status": status,
            "target": pack.target.to_dict(),
            "artifacts": list(artifact_refs),
            "summary": summary,
            "blockers": list(blocker_set),
            "warnings": list(warning_set),
            "recommendations": _recommendations(blocker_set, warning_set),
        }
        if attest:
            payload["attestation"] = _attestation(payload["bundle_id"], artifact_refs)
        return payload


class MigrationBundleVerifier:
    """Offline verifier for bundle integrity and cross-artifact consistency."""

    def verify(
        self,
        *,
        bundle: Mapping[str, Any],
        artifact_bytes: Mapping[str, bytes],
        require_attestation: bool = False,
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        artifacts = _bundle_artifacts(bundle)
        loaded: list[MigrationEvidenceArtifact] = []
        for artifact in artifacts:
            path = str(artifact.get("path", ""))
            content = artifact_bytes.get(path)
            if content is None:
                blockers.append(f"migration_bundle.artifact_missing:{artifact.get('kind')}")
                continue
            actual = "sha256:" + hashlib.sha256(content).hexdigest()
            if actual != artifact.get("sha256"):
                blockers.append(f"migration_bundle.artifact_digest_mismatch:{artifact.get('kind')}")
            try:
                loaded.append(
                    MigrationEvidenceArtifact.from_bytes(
                        kind=str(artifact.get("kind")),
                        path=path,
                        content=content,
                        required=bool(artifact.get("required", False)),
                    )
                )
            except ValueError as exc:
                blockers.append(f"migration_bundle.artifact_invalid:{artifact.get('kind')}:{exc}")
        if loaded:
            try:
                rebuilt = MigrationBundleBuilder().build(
                    artifacts=tuple(loaded), attest=bool(bundle.get("attestation"))
                )
                blockers.extend(str(item) for item in rebuilt.get("blockers", []))
                warnings.extend(str(item) for item in rebuilt.get("warnings", []))
                if rebuilt.get("bundle_id") != bundle.get("bundle_id"):
                    blockers.append("migration_bundle.bundle_id_mismatch")
            except (KeyError, ValueError) as exc:
                blockers.append(f"migration_bundle.bundle_invalid:{exc}")
        blockers.extend(_attestation_blockers(bundle=bundle, require_attestation=require_attestation))
        if bundle.get("status") == "blocked":
            blockers.append("migration_bundle.bundle_blocked")
        blockers = list(dict.fromkeys(blockers))
        warnings = list(dict.fromkeys(warnings))
        return {
            "schema_version": BUNDLE_VERIFICATION_SCHEMA,
            "status": "blocked" if blockers else "passed",
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "artifact_checks": _artifact_checks(artifacts, artifact_bytes),
            "attestation_required": require_attestation,
            "blockers": blockers,
            "warnings": warnings,
        }


def _recommendations(blockers: tuple[str, ...], warnings: tuple[str, ...]) -> list[str]:
    if blockers:
        return ["Resolve bundle blockers before migration apply or PR/MR approval."]
    if warnings:
        return ["Review warnings before promoting this migration to the next environment."]
    return ["Upload bundle.json, verification.json, and review.md as SCM review artifacts."]


def _attestation(bundle_id: str, artifact_refs: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    digests = [{"kind": item["kind"], "path": item["path"], "sha256": item["sha256"]} for item in artifact_refs]
    return {
        "hash_algorithm": "sha256",
        "artifact_digests": digests,
        "bundle_digest": _attestation_digest(bundle_id, digests),
    }


def _attestation_digest(bundle_id: object, digests: object) -> str:
    return stable_fingerprint({"bundle_id": bundle_id, "artifact_digests": digests})


def _attestation_blockers(*, bundle: Mapping[str, Any], require_attestation: bool) -> tuple[str, ...]:
    attestation = bundle.get("attestation")
    if not isinstance(attestation, Mapping):
        return ("migration_bundle.attestation_missing",) if require_attestation else ()
    expected = _attestation_digest(bundle.get("bundle_id"), attestation.get("artifact_digests", []))
    if attestation.get("bundle_digest") != expected:
        return ("migration_bundle.attestation_digest_mismatch",)
    return ()


def _bundle_artifacts(bundle: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    artifacts = bundle.get("artifacts", [])
    return tuple(dict(item) for item in artifacts if isinstance(item, Mapping)) if isinstance(artifacts, list) else ()


def _artifact_checks(
    artifacts: tuple[dict[str, Any], ...], artifact_bytes: Mapping[str, bytes]
) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for artifact in artifacts:
        content = artifact_bytes.get(str(artifact.get("path", "")))
        actual = "sha256:" + hashlib.sha256(content).hexdigest() if content is not None else None
        checks.append(
            {
                "kind": artifact.get("kind"),
                "path": artifact.get("path"),
                "expected_sha256": artifact.get("sha256"),
                "actual_sha256": actual,
                "passed": actual == artifact.get("sha256"),
            }
        )
    return checks


def _decode_mapping(content: bytes, path: str) -> dict[str, Any]:
    text = content.decode("utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


__all__ = [
    "BUNDLE_SCHEMA",
    "BUNDLE_VERIFICATION_SCHEMA",
    "REVIEW_SCHEMA",
    "MigrationBundleBuilder",
    "MigrationBundleVerifier",
    "MigrationEvidenceArtifact",
    "MigrationReviewRenderer",
]
