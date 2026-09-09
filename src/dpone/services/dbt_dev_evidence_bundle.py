"""Atomic, attestation-ready bundle for release-bound dev runtime evidence."""

from __future__ import annotations

import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from dpone.services.bounded_tree_integrity import (
    BoundedTreeIntegrityError,
    BoundedTreeIntegrityPolicy,
    BoundedTreeIntegrityReport,
    BoundedTreeIntegrityService,
)
from dpone.services.dbt_dev_evidence_campaign_receipt import (
    DbtDevEvidenceCampaignReceiptError,
    read_campaign_receipt,
)
from dpone.services.dbt_dev_evidence_provenance import (
    DBT_DEV_EVIDENCE_PROVENANCE_FILENAME,
    DBT_DEV_EVIDENCE_REQUEST_FILENAME,
    DbtDevEvidenceProvenanceError,
    campaign_request_sha256,
    read_campaign_request,
    read_provenance,
    require_provenance_identity,
    validate_campaign_argument,
    validate_identity,
    validate_provenance,
    write_provenance,
)
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
)
from dpone.services.dbt_dev_evidence_source import (
    DbtDevEvidenceSourceError,
    copy_dev_evidence_tree,
)
from dpone.services.dbt_dev_evidence_verification import (
    DbtDevEvidenceVerificationReport,
    DbtDevEvidenceVerificationService,
)

DBT_DEV_EVIDENCE_SUBJECTS_FILENAME = "evidence-subjects.sha256"
_MAX_BUNDLE_FILE_BYTES = 16 * 1024 * 1024
_POLICY = BoundedTreeIntegrityPolicy(
    subject_filename=DBT_DEV_EVIDENCE_SUBJECTS_FILENAME,
    schema_header="# dpone.dbt-dev-evidence-subjects.v1",
    required_paths=("provenance.json",),
    max_files=1_501,
    max_file_bytes=_MAX_BUNDLE_FILE_BYTES,
    max_inventory_bytes=1024 * 1024,
    max_total_bytes=256 * 1024 * 1024,
)


class DbtDevEvidenceBundleError(ValueError):
    """Dev evidence could not be finalized or verified without ambiguity."""

    code = "DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID"


@dataclass(frozen=True, slots=True)
class DbtDevEvidenceBundleReport:
    """Safe output from evidence finalization or integrity verification."""

    release_id: str
    deployment_id: str
    evidence_set_id: str | None
    campaign_request_sha256: str | None
    output_root: str
    subject_path: str
    subject_sha256: str
    file_count: int
    total_bytes: int
    verified_workloads: tuple[str, ...]
    no_op: bool

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": (
                "dpone.dbt-dev-evidence-bundle.v2"
                if self.evidence_set_id is not None
                else "dpone.dbt-dev-evidence-bundle.v1"
            ),
            "passed": True,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "output_root": self.output_root,
            "subject_path": self.subject_path,
            "subject_sha256": self.subject_sha256,
            "file_count": self.file_count,
            "total_bytes": self.total_bytes,
            "verified_workloads": list(self.verified_workloads),
            "no_op": self.no_op,
        }
        if self.evidence_set_id is not None:
            payload["evidence_set_id"] = self.evidence_set_id
            payload["campaign_request_sha256"] = self.campaign_request_sha256
        return payload


class DbtDevEvidenceBundleService:
    """Copy, validate and atomically publish one immutable evidence bundle."""

    def __init__(
        self,
        *,
        verifier: DbtDevEvidenceVerificationService | None = None,
    ) -> None:
        self._verifier = verifier or DbtDevEvidenceVerificationService()
        self._integrity = BoundedTreeIntegrityService(_POLICY)

    def finalize(
        self,
        *,
        compiled_root: Path,
        source_evidence_root: Path,
        output_root: Path,
        expected_release_id: str,
        expected_deployment_id: str,
        expected_evidence_set_id: str | None = None,
        expected_activation_id: str | None = None,
        campaign_request: DbtDevEvidenceRequest | None = None,
        producer_repository: str,
        producer_workflow: str,
        source_commit: str,
    ) -> DbtDevEvidenceBundleReport:
        try:
            validate_identity(expected_release_id, "release")
            validate_identity(expected_deployment_id, "deployment")
            if expected_evidence_set_id is not None:
                validate_identity(expected_evidence_set_id, "evidence set")
            validate_campaign_argument(
                campaign_request,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                evidence_set_id=expected_evidence_set_id,
            )
            validate_provenance(
                producer_repository,
                producer_workflow,
                source_commit,
            )
        except DbtDevEvidenceProvenanceError as exc:
            raise DbtDevEvidenceBundleError("dev evidence finalization identity is invalid") from exc
        destination = Path(output_root).absolute()
        parent = destination.parent
        parent.mkdir(parents=True, exist_ok=True)
        staging = Path(
            tempfile.mkdtemp(
                dir=parent,
                prefix=f".{destination.name}.",
            )
        )
        installed = False
        try:
            copy_dev_evidence_tree(
                Path(source_evidence_root).absolute(),
                staging,
                campaign_bound=campaign_request is not None,
            )
            source_request = read_campaign_request(
                staging,
                required=campaign_request is not None,
            )
            if source_request != campaign_request:
                raise DbtDevEvidenceBundleError("source campaign request differs from the expected request")
            _require_passed_campaign_receipt(staging, source_request)
            semantic = self._require_semantic_evidence(
                compiled_root=compiled_root,
                evidence_root=staging,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                evidence_set_id=expected_evidence_set_id,
                expected_activation_id=expected_activation_id,
                campaign_request=source_request,
            )
            write_provenance(
                staging,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                evidence_set_id=semantic.evidence_set_id,
                campaign_request=source_request,
                producer_repository=producer_repository,
                producer_workflow=producer_workflow,
                source_commit=source_commit,
            )
            integrity = self._integrity.write(staging)
            if destination.exists():
                existing = self.verify(
                    compiled_root=compiled_root,
                    evidence_root=destination,
                    expected_release_id=expected_release_id,
                    expected_deployment_id=expected_deployment_id,
                    expected_evidence_set_id=expected_evidence_set_id,
                    expected_activation_id=expected_activation_id,
                    expected_campaign_request=campaign_request,
                )
                if existing.subject_sha256 != integrity.subject_sha256:
                    raise DbtDevEvidenceBundleError("existing dev evidence bundle conflicts with finalized bytes")
                return _bundle_report(
                    destination,
                    semantic,
                    integrity,
                    campaign_request=source_request,
                    no_op=True,
                )
            try:
                os.rename(staging, destination)
            except OSError as exc:
                if destination.exists():
                    existing = self.verify(
                        compiled_root=compiled_root,
                        evidence_root=destination,
                        expected_release_id=expected_release_id,
                        expected_deployment_id=expected_deployment_id,
                        expected_evidence_set_id=expected_evidence_set_id,
                        expected_activation_id=expected_activation_id,
                        expected_campaign_request=campaign_request,
                    )
                    if existing.subject_sha256 == integrity.subject_sha256:
                        return _bundle_report(
                            destination,
                            semantic,
                            integrity,
                            campaign_request=source_request,
                            no_op=True,
                        )
                raise DbtDevEvidenceBundleError("dev evidence bundle could not be installed atomically") from exc
            installed = True
            _fsync_directory(parent)
            return _bundle_report(
                destination,
                semantic,
                integrity,
                campaign_request=source_request,
                no_op=False,
            )
        except (
            BoundedTreeIntegrityError,
            DbtDevEvidenceCampaignReceiptError,
            DbtDevEvidenceProvenanceError,
            DbtDevEvidenceSourceError,
            OSError,
        ) as exc:
            raise DbtDevEvidenceBundleError("dev evidence bundle is unsafe or incomplete") from exc
        finally:
            if not installed:
                shutil.rmtree(staging, ignore_errors=True)

    def verify(
        self,
        *,
        compiled_root: Path,
        evidence_root: Path,
        expected_release_id: str,
        expected_deployment_id: str,
        expected_evidence_set_id: str | None = None,
        expected_activation_id: str | None = None,
        expected_campaign_request: DbtDevEvidenceRequest | None = None,
    ) -> DbtDevEvidenceBundleReport:
        root = Path(evidence_root).absolute()
        try:
            integrity = self._integrity.verify(root)
            provenance = read_provenance(root)
            campaign_request = read_campaign_request(
                root,
                required=(
                    expected_evidence_set_id is not None
                    or provenance.get("schema") == "dpone.dbt-dev-evidence-provenance.v2"
                ),
            )
            _require_passed_campaign_receipt(root, campaign_request)
            if expected_campaign_request is not None and campaign_request != expected_campaign_request:
                raise DbtDevEvidenceBundleError("dev evidence campaign request differs")
            semantic = self._require_semantic_evidence(
                compiled_root=compiled_root,
                evidence_root=root,
                release_id=expected_release_id,
                deployment_id=expected_deployment_id,
                evidence_set_id=expected_evidence_set_id,
                expected_activation_id=expected_activation_id,
                campaign_request=campaign_request,
            )
            require_provenance_identity(
                provenance,
                expected_release_id=expected_release_id,
                expected_deployment_id=expected_deployment_id,
                expected_evidence_set_id=semantic.evidence_set_id,
                campaign_request=campaign_request,
            )
        except (
            BoundedTreeIntegrityError,
            DbtDevEvidenceCampaignReceiptError,
            DbtDevEvidenceProvenanceError,
            DbtDevEvidenceSourceError,
            OSError,
        ) as exc:
            raise DbtDevEvidenceBundleError("dev evidence integrity verification failed") from exc
        return _bundle_report(
            root,
            semantic,
            integrity,
            campaign_request=campaign_request,
            no_op=True,
        )

    def _require_semantic_evidence(
        self,
        *,
        compiled_root: Path,
        evidence_root: Path,
        release_id: str,
        deployment_id: str,
        evidence_set_id: str | None,
        expected_activation_id: str | None,
        campaign_request: DbtDevEvidenceRequest | None,
    ) -> DbtDevEvidenceVerificationReport:
        report = self._verifier.verify(
            compiled_root=compiled_root,
            evidence_root=evidence_root,
            expected_release_id=release_id,
            expected_deployment_id=deployment_id,
            expected_evidence_set_id=evidence_set_id,
            expected_campaign_request=campaign_request,
            expected_activation_id=expected_activation_id,
            require_exact_activation=(evidence_set_id is not None or campaign_request is not None),
        )
        if not report.passed:
            raise DbtDevEvidenceBundleError("dev evidence does not prove the complete release workflow")
        return report


def _bundle_report(
    root: Path,
    semantic: DbtDevEvidenceVerificationReport,
    integrity: BoundedTreeIntegrityReport,
    *,
    campaign_request: DbtDevEvidenceRequest | None,
    no_op: bool,
) -> DbtDevEvidenceBundleReport:
    if semantic.evidence_set_id is not None and campaign_request is None:
        raise DbtDevEvidenceBundleError("dev evidence campaign request is missing")
    return DbtDevEvidenceBundleReport(
        release_id=semantic.release_id,
        deployment_id=semantic.deployment_id,
        evidence_set_id=semantic.evidence_set_id,
        campaign_request_sha256=(campaign_request_sha256(campaign_request) if campaign_request is not None else None),
        output_root=root.as_posix(),
        subject_path=integrity.subject_path,
        subject_sha256=integrity.subject_sha256,
        file_count=integrity.file_count,
        total_bytes=integrity.total_bytes,
        verified_workloads=semantic.verified_workloads,
        no_op=no_op,
    )


def _require_passed_campaign_receipt(
    root: Path,
    request: DbtDevEvidenceRequest | None,
) -> None:
    if request is None:
        return
    receipt = read_campaign_receipt(root, request=request)
    if not receipt.passed:
        raise DbtDevEvidenceBundleError("dev evidence campaign did not close with terminal success")


def _fsync_directory(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


__all__ = [
    "DBT_DEV_EVIDENCE_PROVENANCE_FILENAME",
    "DBT_DEV_EVIDENCE_REQUEST_FILENAME",
    "DBT_DEV_EVIDENCE_SUBJECTS_FILENAME",
    "DbtDevEvidenceBundleError",
    "DbtDevEvidenceBundleReport",
    "DbtDevEvidenceBundleService",
]
