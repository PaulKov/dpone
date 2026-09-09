"""Documentation and deployment artifact service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.deploy_profiles import DeploymentProfileRenderer
from dpone.ops.docs_publish_pack import DocsPublishPackService
from dpone.ops.manifest_bundle import ManifestBundleService
from dpone.ops.runbook_pack import RunbookPackService


@dataclass(frozen=True, slots=True)
class ArtifactDocumentationCatalog:
    """Factory catalog for documentation, manifest and runbook artifacts."""

    @classmethod
    def default(cls) -> ArtifactDocumentationCatalog:
        return cls()

    def deployment_profiles(self) -> DeploymentProfileRenderer:
        return DeploymentProfileRenderer()

    def docs_publish_pack(self) -> DocsPublishPackService:
        return DocsPublishPackService()

    def manifest_bundle(self) -> ManifestBundleService:
        return ManifestBundleService()

    def runbook_pack(self) -> RunbookPackService:
        return RunbookPackService()


__all__ = ["ArtifactDocumentationCatalog"]
