"""Compatibility entry point for the historical file exchange helper."""

from __future__ import annotations

from typing import Any

from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


def load_with_exchange(loader: Any, load_config: Any, payload: LoadPayload, artifact: FileExportArtifact) -> LoadResult:
    """Delegate to the configured strategy through the transaction-owning loader."""
    return loader.load_with_exchange(load_config, payload, artifact)
