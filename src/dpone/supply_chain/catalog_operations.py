"""Platform-facing facade for signed catalog supply-chain operations."""

from __future__ import annotations

from pathlib import Path

from dpone.contracts.catalog_supply_chain import (
    CatalogBundleBuildResult,
    CatalogBundleVerification,
    ExtensionConformanceReport,
)
from dpone.services.catalog_supply_chain import CatalogSupplyChainService


class CatalogSupplyChainOperations:
    """Keep command adapters independent from application service modules."""

    def __init__(self, *, service: CatalogSupplyChainService | None = None) -> None:
        self._service = service or CatalogSupplyChainService()

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
        return self._service.build(
            project_root=project_root,
            kind=kind,
            source=source,
            bundle_root=bundle_root,
            publisher_id=publisher_id,
            environment=environment,
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
        return self._service.verify(
            bundle_dir=bundle_dir,
            sigstore_bundle=sigstore_bundle,
            policy=policy,
            trusted_root=trusted_root,
            output=output,
        )

    def conformance(
        self,
        *,
        request_path: Path,
        output_dir: Path,
        project_root: Path,
    ) -> ExtensionConformanceReport:
        return self._service.conformance(
            request_path=request_path,
            output_dir=output_dir,
            project_root=project_root,
        )


__all__ = ["CatalogSupplyChainOperations"]
