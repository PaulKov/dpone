"""Artifact rendering for MSSQL -> ClickHouse refresh chunks."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from dpone.ops.checksums import sha256_file
from dpone.ops.routes.refresh_execution_models import RouteRefreshChunkExecutionRequest

from .mssql_clickhouse_adapters import ClickHouseChunkLoadResult, ClickHouseChunkPrepareResult, MssqlChunkExportResult
from .mssql_clickhouse_config import MssqlClickHouseRefreshConfig

CHUNK_SCHEMA_VERSION = "dpone.mssql_clickhouse.route_refresh_chunk.v1"


def chunk_artifact_path(request: RouteRefreshChunkExecutionRequest) -> Path:
    return Path(request.output_dir) / "chunks" / f"mssql_clickhouse_refresh_chunk_{request.ordinal:04d}.json"


def transfer_path(request: RouteRefreshChunkExecutionRequest) -> Path:
    return Path(request.output_dir) / "chunks" / f"mssql_clickhouse_refresh_chunk_{request.ordinal:04d}.tsv"


def write_success_artifact(
    *,
    request: RouteRefreshChunkExecutionRequest,
    config: MssqlClickHouseRefreshConfig,
    query: str,
    transfer_file: Path,
    export_result: MssqlChunkExportResult,
    prepare_result: ClickHouseChunkPrepareResult,
    load_result: ClickHouseChunkLoadResult,
) -> Path:
    path = chunk_artifact_path(request)
    write_json(
        path,
        artifact_payload(
            request=request,
            config=config,
            query=query,
            transfer_file=transfer_file,
            export_result=export_result,
            prepare_result=prepare_result,
            load_result=load_result,
            passed=True,
            blockers=(),
        ),
    )
    return path


def write_failure_artifact(
    *,
    request: RouteRefreshChunkExecutionRequest,
    config: MssqlClickHouseRefreshConfig,
    exc: Exception,
) -> Path:
    path = chunk_artifact_path(request)
    write_json(
        path,
        {
            "schema_version": CHUNK_SCHEMA_VERSION,
            "route": request.route.to_dict(),
            "dataset": request.dataset,
            "runner_id": request.runner_id,
            "chunk": chunk_payload(request),
            "source_dataset": config.source_dataset,
            "target_dataset": config.target_dataset,
            "columns": list(config.columns),
            "query_sha256": "0" * 64,
            "transfer_path": str(transfer_path(request)),
            "artifact_sha256": "0" * 64,
            "export": {"rows_read": 0, "redacted_command": [], "stdout_tail": "", "stderr_tail": ""},
            "prepare": {"rows_deleted": 0, "redacted_command": [], "stdout_tail": "", "stderr_tail": ""},
            "load": {"rows_written": 0, "redacted_command": [], "stdout_tail": "", "stderr_tail": ""},
            "passed": False,
            "blockers": ["mssql_clickhouse_refresh_executor.chunk_failed"],
            "error": str(exc),
        },
    )
    return path


def artifact_payload(
    *,
    request: RouteRefreshChunkExecutionRequest,
    config: MssqlClickHouseRefreshConfig,
    query: str,
    transfer_file: Path,
    export_result: MssqlChunkExportResult,
    prepare_result: ClickHouseChunkPrepareResult,
    load_result: ClickHouseChunkLoadResult,
    passed: bool,
    blockers: Sequence[str],
) -> dict[str, object]:
    artifact_sha256 = sha256_file(transfer_file) if transfer_file.is_file() else "0" * 64
    return {
        "schema_version": CHUNK_SCHEMA_VERSION,
        "route": request.route.to_dict(),
        "dataset": request.dataset,
        "runner_id": request.runner_id,
        "chunk": chunk_payload(request),
        "source_dataset": config.source_dataset,
        "target_dataset": config.target_dataset,
        "columns": list(config.columns),
        "query_sha256": hashlib.sha256(query.encode("utf-8")).hexdigest(),
        "transfer_path": str(transfer_file),
        "artifact_sha256": artifact_sha256,
        "export": {
            "rows_read": export_result.rows_read,
            "redacted_command": list(export_result.redacted_command),
            "stdout_tail": export_result.stdout_tail,
            "stderr_tail": export_result.stderr_tail,
        },
        "prepare": {
            "rows_deleted": prepare_result.rows_deleted,
            "redacted_command": list(prepare_result.redacted_command),
            "stdout_tail": prepare_result.stdout_tail,
            "stderr_tail": prepare_result.stderr_tail,
        },
        "load": {
            "rows_written": load_result.rows_written,
            "redacted_command": list(load_result.redacted_command),
            "stdout_tail": load_result.stdout_tail,
            "stderr_tail": load_result.stderr_tail,
        },
        "passed": passed,
        "blockers": list(blockers),
    }


def chunk_payload(request: RouteRefreshChunkExecutionRequest) -> dict[str, object]:
    return {
        "ordinal": request.ordinal,
        "start": request.start,
        "end": request.end,
        "partition": request.partition,
        "source_boundary": request.source_boundary,
        "sink_boundary": request.sink_boundary,
        "idempotency_key": request.idempotency_key,
    }


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")


__all__ = [
    "CHUNK_SCHEMA_VERSION",
    "artifact_payload",
    "chunk_artifact_path",
    "chunk_payload",
    "transfer_path",
    "write_failure_artifact",
    "write_json",
    "write_success_artifact",
]
