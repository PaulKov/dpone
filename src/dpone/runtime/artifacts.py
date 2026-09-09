"""Compatibility facade for source-to-sink extraction artifacts."""

from __future__ import annotations

from dpone.runtime.artifact_models import BaseExtractionArtifact, StagingTableArtifact
from dpone.runtime.artifact_protocols import ExtractionArtifact, TerminalExtractionArtifact
from dpone.runtime.cloud_artifacts import GCSExportArtifact
from dpone.runtime.extraction_lifecycle import (
    ArtifactTerminalOutcome,
    ArtifactTerminalReceipt,
    ExtractionLifecycleAuthority,
    ExtractionLifecycleReceipt,
)
from dpone.runtime.file_artifacts import BatchedFileExportArtifact, FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.native_transfer_artifacts import PartitionedTransferPlanArtifact
from dpone.runtime.staging import StagingManager
from dpone.runtime.streaming_rows import StreamingRowsArtifact

__all__ = [
    "ExtractionArtifact",
    "TerminalExtractionArtifact",
    "ArtifactTerminalOutcome",
    "ArtifactTerminalReceipt",
    "ExtractionLifecycleAuthority",
    "ExtractionLifecycleReceipt",
    "BaseExtractionArtifact",
    "StagingTableArtifact",
    "InMemoryRowsArtifact",
    "StreamingRowsArtifact",
    "InternalQueryArtifact",
    "FileExportArtifact",
    "PartitionedFileExportArtifact",
    "PartitionedTransferPlanArtifact",
    "GCSExportArtifact",
    "BatchedFileExportArtifact",
    "StagingManager",
]
