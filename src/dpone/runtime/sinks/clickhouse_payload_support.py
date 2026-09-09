"""Artifact classification and optional loader factories for ClickHouse ingestion."""

from __future__ import annotations

from importlib import import_module
from typing import Any


def is_source_native_artifact(artifact: Any) -> bool:
    return getattr(artifact, "native_wire_contract", None) is not None


def native_wire_transcoder() -> Any:
    return import_module("dpone.runtime.native_wire_transcoder").NativeWireTranscoder()


def columnar_pull_loader(sink: Any) -> Any:
    module = import_module("dpone.runtime.sinks.clickhouse_columnar_pull")
    return module.ClickHouseColumnarPullLoader(
        connector=sink.connector,
        table_name=sink._table,
        count_rows=sink._count,
    )


def columnar_direct_push_loader(sink: Any) -> Any:
    module = import_module("dpone.runtime.sinks.clickhouse_columnar_direct_push")
    return module.ClickHouseColumnarDirectPushLoader(
        table_name=sink._table,
        count_rows=sink._count,
        client_runner_factory=sink._build_client_runner,
        http_runner_factory=sink._build_http_runner,
    )


def is_object_storage_staging_manifest(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "ObjectStorageStagingManifest" and hasattr(artifact, "chunks")


def is_object_storage_columnar_chunked_artifact(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "ObjectStorageColumnarChunkedArtifact" and hasattr(artifact, "iter_windows")


def is_local_columnar_staging_manifest(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "LocalColumnarStagingManifest" and hasattr(artifact, "chunks")


def is_local_columnar_chunked_artifact(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "LocalColumnarChunkedArtifact" and hasattr(artifact, "iter_chunks")
