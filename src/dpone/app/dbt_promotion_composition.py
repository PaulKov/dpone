"""Composition root for deterministic dbt promotion and evidence services."""

from __future__ import annotations

import os
from collections.abc import Mapping
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.adapters.airflow_rest_dbt_evidence import (
    AirflowRestDbtEvidenceAdapter,
)
from dpone.adapters.dbt_dev_evidence_campaign_journal import (
    ConfinedDbtDevEvidenceCampaignJournal,
)
from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.dbt_project_bundle import (
    build_dbt_project_bundle,
    extract_dbt_project_bundle,
    verify_dbt_project_bundle_tree,
)
from dpone.services.dbt_ci_report import render_dbt_ci_report
from dpone.services.dbt_dev_evidence_bundle import (
    DbtDevEvidenceBundleError,
    DbtDevEvidenceBundleService,
)
from dpone.services.dbt_dev_evidence_campaign import (
    DbtDevEvidenceCampaignError,
    DbtDevEvidenceCampaignService,
)
from dpone.services.dbt_dev_evidence_release import DbtExpectedReleaseLoader, load_expected_dbt_release
from dpone.services.dbt_dev_evidence_request import (
    DbtDevEvidenceRequest,
    DbtDevEvidenceRequestError,
)
from dpone.services.dbt_dev_evidence_verification import (
    DbtDevEvidenceVerificationService,
)
from dpone.services.dbt_prod_mirror import DbtProdMirrorService
from dpone.services.dbt_prod_mirror_transaction import DbtProdMirrorTransaction
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError
from dpone.services.dbt_prod_promotion_metadata import (
    DbtProdPromotionMetadataVerifier,
)
from dpone.services.dbt_promotion_verification import (
    DbtPromotionVerificationService,
)
from dpone.services.dbt_release_integrity import (
    DbtReleaseIntegrityError,
    DbtReleaseIntegrityService,
)
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from dpone.services.dbt_workspace_mirror import DbtWorkspaceMirrorService
from dpone.services.dbt_workspace_mirror_source import DbtWorkspaceMirrorSourceLoader
from dpone.services.dbt_workspace_promotion_verification import DbtWorkspacePromotionVerificationService

if TYPE_CHECKING:
    from dpone.contracts.dbt_project_bundle import (
        DbtProjectBundle,
        DbtProjectBundleArtifact,
    )


class RuntimeDbtProjectBundleOperations:
    """Bind application bundle ports to the verified runtime adapter."""

    def __init__(self, *, package_environment: Mapping[str, str] | None = None) -> None:
        self._package_environment = dict(os.environ if package_environment is None else package_environment)

    def build(self, project_root: Path) -> DbtProjectBundleArtifact:
        return build_dbt_project_bundle(
            project_root,
            package_environment=self._package_environment,
        )

    def extract(self, archive: bytes, destination: Path) -> DbtProjectBundle:
        return extract_dbt_project_bundle(archive, destination)

    def verify(self, archive: bytes, destination: Path) -> DbtProjectBundle:
        return verify_dbt_project_bundle_tree(archive, destination)


def build_dbt_prod_mirror_service() -> DbtProdMirrorService:
    return DbtProdMirrorService(
        bundle_operations=RuntimeDbtProjectBundleOperations(),
    )


def build_dbt_workspace_mirror_service() -> DbtWorkspaceMirrorService:
    bundles = RuntimeDbtProjectBundleOperations()
    return DbtWorkspaceMirrorService(
        read_file=read_confined_file,
        candidate_loader=_workspace_mirror_source_loader(bundles),
        transaction=DbtProdMirrorTransaction(bundle_operations=bundles),
    )


def _workspace_mirror_source_loader(bundles: RuntimeDbtProjectBundleOperations) -> DbtWorkspaceMirrorSourceLoader:
    return DbtWorkspaceMirrorSourceLoader(
        read_file=read_confined_file,
        bundle_operations=bundles,
        source_reader=DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file),
        verify_integrity=DbtReleaseIntegrityService().verify,
    )


def build_dbt_workspace_promotion_verification_service() -> DbtWorkspacePromotionVerificationService:
    return DbtWorkspacePromotionVerificationService(
        read_file=read_confined_file,
        candidate_loader=_workspace_mirror_source_loader(RuntimeDbtProjectBundleOperations()),
    )


def build_dbt_dev_evidence_verification_service() -> DbtDevEvidenceVerificationService:
    return DbtDevEvidenceVerificationService(release_loader=build_dbt_expected_release_loader())


def build_dbt_expected_release_loader() -> DbtExpectedReleaseLoader:
    """Wire complete workspace source verification into request and evidence gates."""

    return partial(
        load_expected_dbt_release,
        source_reader=build_dbt_release_source_reader(),
    )


def build_dbt_release_source_reader() -> DbtReleaseSourceReader:
    """Use the same complete-source authority for evidence and cache capture."""

    return DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=read_confined_file)


def build_dbt_dev_evidence_bundle_service() -> DbtDevEvidenceBundleService:
    return DbtDevEvidenceBundleService(
        verifier=build_dbt_dev_evidence_verification_service(),
    )


def build_dbt_dev_evidence_campaign_service(
    *,
    airflow_api_url: str,
    airflow_api_version: str,
    bearer_token: str,
    evidence_root: Path,
    request_timeout_seconds: int = 10,
) -> DbtDevEvidenceCampaignService:
    return DbtDevEvidenceCampaignService(
        airflow=AirflowRestDbtEvidenceAdapter(
            base_url=airflow_api_url,
            api_version=airflow_api_version,
            bearer_token=bearer_token,
            request_timeout_seconds=request_timeout_seconds,
        ),
        journal=ConfinedDbtDevEvidenceCampaignJournal(evidence_root),
    )


def build_dbt_prod_promotion_metadata_verifier() -> DbtProdPromotionMetadataVerifier:
    return DbtProdPromotionMetadataVerifier()


def build_dbt_release_integrity_service() -> DbtReleaseIntegrityService:
    return DbtReleaseIntegrityService()


def build_dbt_promotion_verification_service() -> DbtPromotionVerificationService:
    bundle_operations = RuntimeDbtProjectBundleOperations()
    return DbtPromotionVerificationService(
        bundle_builder=bundle_operations,
    )


def render_dbt_promotion_ci_report(
    payload: Mapping[str, object],
    *,
    airflow_base_url: str | None,
) -> str:
    return render_dbt_ci_report(
        payload,
        airflow_base_url=airflow_base_url,
    )


__all__ = [
    "DbtDevEvidenceBundleError",
    "DbtDevEvidenceCampaignError",
    "DbtDevEvidenceRequest",
    "DbtDevEvidenceRequestError",
    "DbtProdMirrorError",
    "DbtReleaseIntegrityError",
    "RuntimeDbtProjectBundleOperations",
    "build_dbt_dev_evidence_bundle_service",
    "build_dbt_dev_evidence_campaign_service",
    "build_dbt_dev_evidence_verification_service",
    "build_dbt_expected_release_loader",
    "build_dbt_prod_mirror_service",
    "build_dbt_workspace_mirror_service",
    "build_dbt_workspace_promotion_verification_service",
    "build_dbt_prod_promotion_metadata_verifier",
    "build_dbt_promotion_verification_service",
    "build_dbt_release_integrity_service",
    "build_dbt_release_source_reader",
    "render_dbt_promotion_ci_report",
]
