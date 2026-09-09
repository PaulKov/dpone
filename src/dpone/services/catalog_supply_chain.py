"""Composition service for signed catalog and conformance application flows."""

from __future__ import annotations

from pathlib import Path

from dpone.adapters.cosign_blob_signature import CosignBlobSignatureVerifier
from dpone.contracts.catalog_supply_chain import (
    CatalogBundleBuildRequest,
    CatalogBundleBuildResult,
    CatalogBundleVerification,
    CatalogBundleVerifyRequest,
    ExtensionConformanceReport,
)
from dpone.services.catalog_bundle_builder import CatalogBundleBuilder
from dpone.services.catalog_bundle_verification import CatalogBundleVerificationService
from dpone.services.extension_conformance import ExtensionConformanceService


class CatalogSupplyChainService:
    """Compose application policies with the certified infrastructure adapter."""

    def build(
        self,
        *,
        project_root: str,
        kind: str,
        source: str,
        bundle_root: str,
        publisher_id: str,
        environment: str | None,
    ) -> CatalogBundleBuildResult:
        return CatalogBundleBuilder().build(
            CatalogBundleBuildRequest(
                project_root=project_root,
                kind=kind,
                source=source,
                bundle_root=bundle_root,
                publisher_id=publisher_id,
                environment=environment,
            )
        )

    def verify(
        self,
        *,
        bundle_dir: str,
        sigstore_bundle: str,
        policy: str,
        trusted_root: str,
        output: str,
    ) -> CatalogBundleVerification:
        return CatalogBundleVerificationService(blob_verifier=CosignBlobSignatureVerifier()).verify(
            CatalogBundleVerifyRequest(
                bundle_dir=bundle_dir,
                sigstore_bundle=sigstore_bundle,
                policy=policy,
                trusted_root=trusted_root,
                output=output,
            )
        )

    def conformance(
        self,
        *,
        request_path: Path,
        output_dir: Path,
        project_root: Path,
    ) -> ExtensionConformanceReport:
        service = ExtensionConformanceService()
        report = service.evaluate_file(request_path, project_root=project_root)
        service.write(report, output_dir=output_dir, project_root=project_root)
        return report


__all__ = ["CatalogSupplyChainService"]
