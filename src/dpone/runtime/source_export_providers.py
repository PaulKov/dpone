"""Connector-neutral source export provider descriptors."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SourceExportProviderDescriptor:
    """Static capabilities for a source export provider.

    Runtime implementations can expose probes through this identity without
    coupling the optimizer to a concrete connector or sink.
    """

    provider_id: str
    source_type: str
    artifact_kind: str
    preserves_types: bool
    supports_probe: bool = True
    supports_full_export: bool = True
    supports_physical_chunking: bool = False
    requires_seekable_boundary: bool = False
    requires_source_escaping: bool = False
    worker_disk_pressure: str = "bounded"
    provider_version: str = "1"


class SourceExportProviderCatalog:
    """Base catalog for connector-specific provider descriptors."""

    def providers(self) -> tuple[SourceExportProviderDescriptor, ...]:
        raise NotImplementedError


__all__ = ["SourceExportProviderCatalog", "SourceExportProviderDescriptor"]
