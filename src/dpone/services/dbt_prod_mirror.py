"""Prepare a byte-identical dbt audit mirror for a bot-owned prod PR."""

from __future__ import annotations

import json
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_publishing import (
    DbtPublishingError,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_publish_release_materializer import (
    DbtReleaseMaterializationError,
    DbtReleaseMaterializer,
)
from dpone.services.dbt_prod_mirror_transaction import (
    DbtProdMirrorTransaction,
    DbtProjectBundleOperations,
)
from dpone.services.dbt_prod_promotion_contract import (
    DbtProdMirrorError,
    DbtPromotionTrustDescriptor,
    dbt_json_object,
    require_dbt_digest,
    safe_dbt_reference,
    validated_relative_path,
    validated_repository_root,
)
from dpone.services.dbt_release_integrity import (
    DbtReleaseIntegrityError,
    DbtReleaseIntegrityService,
)

_MAX_SNAPSHOT_BYTES = 1024 * 1024
_MAX_PROJECT_BUNDLE_BYTES = 256 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class DbtProdMirrorReport:
    release_id: str
    promotion_id: str
    mirror_path: str
    source_snapshot_path: str
    descriptor_path: str
    no_op: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": "dpone.dbt-prod-mirror-prepare.v1",
            "passed": True,
            "release_id": self.release_id,
            "promotion_id": self.promotion_id,
            "mirror_path": self.mirror_path,
            "source_snapshot_path": self.source_snapshot_path,
            "descriptor_path": self.descriptor_path,
            "no_op": self.no_op,
        }


class DbtProdMirrorService:
    """Extract an attested release bundle and update only bot-owned paths."""

    def __init__(self, *, bundle_operations: DbtProjectBundleOperations) -> None:
        self._transaction = DbtProdMirrorTransaction(
            bundle_operations=bundle_operations,
        )

    def prepare(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        mirror_path: str,
        source_snapshot_path: str,
        descriptor_path: str,
        expected_release_id: str,
        dev_deployment_id: str,
        dev_evidence_ref: str,
        dev_evidence_subject_sha256: str,
        dev_evidence_artifact_name: str,
        dev_evidence_producer_workflow: str,
        dev_evidence_source_commit: str,
        dev_evidence_set_id: str | None = None,
        dev_evidence_campaign_request_sha256: str | None = None,
    ) -> DbtProdMirrorReport:
        root = validated_repository_root(repository_root)
        mirror = validated_relative_path(mirror_path, "mirror path")
        snapshot = validated_relative_path(
            source_snapshot_path,
            "source snapshot path",
        )
        descriptor = validated_relative_path(
            descriptor_path,
            "promotion descriptor path",
        )
        _require_separate_paths(mirror, snapshot, descriptor)
        require_dbt_digest(expected_release_id, "release identity")
        require_dbt_digest(dev_deployment_id, "dev deployment identity")
        evidence_ref = safe_dbt_reference(dev_evidence_ref)
        trust_descriptor = DbtPromotionTrustDescriptor.validated(
            subject_sha256=dev_evidence_subject_sha256,
            artifact_name=dev_evidence_artifact_name,
            producer_workflow=dev_evidence_producer_workflow,
            source_commit=dev_evidence_source_commit,
            evidence_set_id=dev_evidence_set_id,
            campaign_request_sha256=(dev_evidence_campaign_request_sha256),
        )
        try:
            DbtReleaseIntegrityService().verify(compiled_root)
            with tempfile.TemporaryDirectory(prefix="dpone-dbt-release-validation-") as cache:
                materialized = DbtReleaseMaterializer().materialize(
                    compiled_root=compiled_root,
                    cache_root=Path(cache),
                    expected_release_id=expected_release_id,
                )
            source_snapshot = dbt_json_object(
                read_confined_file(
                    compiled_root,
                    "_dbt/dbt-source-snapshot.json",
                    max_bytes=_MAX_SNAPSHOT_BYTES,
                )
            )
            project_bundle = _read_project_bundle(compiled_root)
        except (
            DbtReleaseIntegrityError,
            DbtReleaseMaterializationError,
            DbtPublishingError,
            OSError,
        ) as exc:
            raise DbtProdMirrorError("compiled release cannot be used as a prod source mirror") from exc
        if materialized.release_id != expected_release_id:
            raise DbtProdMirrorError("compiled release identity changed")
        snapshot_digest = source_snapshot.get("snapshot_sha256")
        require_dbt_digest(snapshot_digest, "source snapshot identity")
        snapshot_bytes = _json_bytes(source_snapshot)
        unsigned_descriptor = {
            "schema": (
                "dpone.dbt-prod-promotion.v2" if dev_evidence_set_id is not None else "dpone.dbt-prod-promotion.v1"
            ),
            "release_id": expected_release_id,
            "source_snapshot_path": snapshot.as_posix(),
            "source_snapshot_sha256": snapshot_digest,
            "dev_deployment_id": dev_deployment_id,
            "dev_evidence_ref": evidence_ref,
            **trust_descriptor.to_dict(),
        }
        descriptor_payload = {
            **unsigned_descriptor,
            "promotion_id": canonical_fingerprint(unsigned_descriptor),
        }
        descriptor_bytes = _json_bytes(descriptor_payload)
        try:
            installed = self._transaction.install(
                repository_root=root,
                mirror_path=mirror,
                project_bundle=project_bundle,
                source_snapshot_path=snapshot,
                source_snapshot_bytes=snapshot_bytes,
                descriptor_path=descriptor,
                descriptor_bytes=descriptor_bytes,
            )
        except (DbtPublishingError, OSError) as exc:
            raise DbtProdMirrorError("compiled project bundle cannot be installed safely") from exc
        return DbtProdMirrorReport(
            release_id=expected_release_id,
            promotion_id=str(descriptor_payload["promotion_id"]),
            mirror_path=mirror.as_posix(),
            source_snapshot_path=snapshot.as_posix(),
            descriptor_path=descriptor.as_posix(),
            no_op=installed.no_op,
        )


def _require_separate_paths(
    mirror: PurePosixPath,
    snapshot: PurePosixPath,
    descriptor: PurePosixPath,
) -> None:
    for metadata in (snapshot, descriptor):
        if metadata.parts[: len(mirror.parts)] == mirror.parts:
            raise DbtProdMirrorError("promotion metadata must be outside the mirrored dbt subtree")
    if snapshot == descriptor:
        raise DbtProdMirrorError("promotion metadata paths must be distinct")


def _read_project_bundle(compiled_root: Path) -> bytes:
    try:
        return read_confined_file(
            compiled_root,
            "runtime/dbt/project.tar.gz",
            max_bytes=_MAX_PROJECT_BUNDLE_BYTES,
        )
    except OSError as exc:
        raise DbtProdMirrorError("compiled release project bundle is unavailable") from exc


def _json_bytes(payload: dict[str, object]) -> bytes:
    return (
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


__all__ = [
    "DbtProdMirrorError",
    "DbtProdMirrorReport",
    "DbtProdMirrorService",
    "DbtProjectBundleOperations",
]
