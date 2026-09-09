"""Lineage and catalog-publication artifact service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.catalog_publish import CatalogPublicationService
from dpone.ops.dbt_lineage import DbtLineageService
from dpone.ops.openlineage_export import OpenLineageExportService
from dpone.ops.run_registry import RunRegistryService


@dataclass(frozen=True, slots=True)
class ArtifactLineageCatalog:
    """Factory catalog for lineage, run registry and catalog-publication services."""

    @classmethod
    def default(cls) -> ArtifactLineageCatalog:
        return cls()

    def catalog_publication(self) -> CatalogPublicationService:
        return CatalogPublicationService()

    def dbt_lineage(self) -> DbtLineageService:
        return DbtLineageService()

    def openlineage_export(self) -> OpenLineageExportService:
        return OpenLineageExportService()

    def run_registry(self) -> RunRegistryService:
        return RunRegistryService()


__all__ = ["ArtifactLineageCatalog"]
