"""MSSQL source export provider descriptors for adaptive benchmarking."""

from __future__ import annotations

from dpone.runtime.source_export_providers import (
    SourceExportProviderCatalog,
    SourceExportProviderDescriptor,
)


class MssqlExportProviderCatalog(SourceExportProviderCatalog):
    """Static MSSQL provider catalog consumed by benchmark/probe runtime."""

    def providers(self) -> tuple[SourceExportProviderDescriptor, ...]:
        return (
            SourceExportProviderDescriptor(
                provider_id="mssql_bcp_native",
                source_type="mssql",
                artifact_kind="bcp_native_file",
                preserves_types=True,
                supports_physical_chunking=False,
                worker_disk_pressure="file",
            ),
            SourceExportProviderDescriptor(
                provider_id="mssql_bcp_character_raw",
                source_type="mssql",
                artifact_kind="character_raw_file",
                preserves_types=False,
                supports_physical_chunking=True,
                requires_source_escaping=False,
                worker_disk_pressure="chunked_file",
            ),
            SourceExportProviderDescriptor(
                provider_id="mssql_odbc_array",
                source_type="mssql",
                artifact_kind="rowset_batch",
                preserves_types=True,
                supports_physical_chunking=True,
                worker_disk_pressure="memory_bounded",
            ),
            SourceExportProviderDescriptor(
                provider_id="mssql_driver_rowset",
                source_type="mssql",
                artifact_kind="row_stream",
                preserves_types=True,
                supports_physical_chunking=True,
                worker_disk_pressure="memory_bounded",
            ),
            SourceExportProviderDescriptor(
                provider_id="range_partitioned",
                source_type="mssql",
                artifact_kind="partitioned_artifacts",
                preserves_types=True,
                supports_physical_chunking=True,
                requires_seekable_boundary=True,
                worker_disk_pressure="bounded",
            ),
            SourceExportProviderDescriptor(
                provider_id="single_scan_chunks",
                source_type="mssql",
                artifact_kind="physical_chunks",
                preserves_types=True,
                supports_physical_chunking=True,
                worker_disk_pressure="chunked_file",
            ),
        )


__all__ = ["MssqlExportProviderCatalog"]
