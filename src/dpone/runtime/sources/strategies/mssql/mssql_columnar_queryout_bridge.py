"""Bridge MSSQL queryout selection to the columnar snapshot provider port."""

from __future__ import annotations

import re
import uuid
from typing import TYPE_CHECKING, Any

from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_SIZE_UNITS = {
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
}


def columnar_snapshot_artifact(
    *,
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
    provider: Any | None,
    logger: Any,
) -> Any | None:
    """Return a columnar snapshot artifact when the manifest requests this route."""

    columnar_options = _columnar_fast_path_options(load_config.options)
    if not columnar_options:
        return None
    mode = _option(columnar_options, "mode", "auto")
    if mode in {"off", "benchmark_only"}:
        return None
    if provider is None:
        if mode == "required":
            raise RuntimeError("columnar_snapshot_provider_missing")
        return None

    request = _snapshot_request(load_config, query, schema, columnar_options)
    capability = provider.capabilities(request)
    if not capability.supports(request):
        if mode == "required":
            raise RuntimeError(", ".join(capability.blockers or ("columnar_snapshot_provider_uncertified",)))
        _log_fallback(logger, capability.blockers or ("columnar_snapshot_provider_uncertified",))
        return None
    return provider.snapshot(request)


def build_columnar_snapshot_request(
    *,
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
    run_id: str | None = None,
) -> ColumnarSnapshotRequest:
    """Build the provider request shared by legacy queryout and route runtime."""

    columnar_options = _columnar_fast_path_options(load_config.options)
    if not columnar_options:
        raise RuntimeError("columnar_fast_path_options_missing")
    return _snapshot_request(load_config, query, schema, columnar_options, run_id=run_id)


def _snapshot_request(
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
    columnar_options: dict[str, Any],
    *,
    run_id: str | None = None,
) -> ColumnarSnapshotRequest:
    request_run_id = str(run_id or load_config.options.get("run_id") or uuid.uuid4().hex)
    object_storage = columnar_options.get("object_storage")
    execution = _execution_options(columnar_options)
    if not isinstance(object_storage, dict):
        return _local_snapshot_request(
            load_config,
            query,
            schema,
            columnar_options,
            run_id=request_run_id,
        )
    uri_prefix = str(object_storage.get("uri_prefix") or "").strip()
    if not uri_prefix:
        raise RuntimeError("columnar_object_storage_uri_prefix_missing")
    request_options = {
        **columnar_options,
        "execution": execution,
        "object_storage": dict(object_storage),
        "batch_size": load_config.options.get("batch_size", load_config.batch_size),
        "cleanup_policy": execution.get("cleanup_policy", object_storage.get("cleanup_policy", "eager")),
    }
    if isinstance(object_storage.get("clickhouse_read_access"), dict):
        request_options["clickhouse_read_access"] = dict(object_storage["clickhouse_read_access"])
    if isinstance(object_storage.get("retention"), dict):
        request_options["retention"] = dict(object_storage["retention"])
    return ColumnarSnapshotRequest(
        query=query,
        schema=schema,
        uri_prefix=uri_prefix,
        run_id=request_run_id,
        target_chunk_bytes=_size_bytes(
            execution.get("target_chunk_bytes", object_storage.get("target_chunk_bytes")),
            512 * 1024 * 1024,
        ),
        max_chunk_bytes=_size_bytes(
            execution.get("max_chunk_bytes", object_storage.get("max_chunk_bytes")),
            1024 * 1024 * 1024,
        ),
        format=str(object_storage.get("format") or "parquet"),
        compression=str(object_storage.get("compression") or "zstd"),
        options=request_options,
    )


def _local_snapshot_request(
    load_config: LoadConfig,
    query: str,
    schema: list[tuple[str, str]],
    columnar_options: dict[str, Any],
    *,
    run_id: str,
) -> ColumnarSnapshotRequest:
    execution = _execution_options(columnar_options)
    request_options = {
        **columnar_options,
        "execution": execution,
        "batch_size": load_config.options.get("batch_size", load_config.batch_size),
        "cleanup_policy": execution.get("cleanup_policy", columnar_options.get("cleanup_policy", "eager")),
    }
    return ColumnarSnapshotRequest(
        query=query,
        schema=schema,
        uri_prefix=f"local://dpone-columnar/{run_id}/",
        run_id=run_id,
        target_chunk_bytes=_size_bytes(
            execution.get("target_chunk_bytes", columnar_options.get("target_chunk_bytes")),
            512 * 1024 * 1024,
        ),
        max_chunk_bytes=_size_bytes(
            execution.get("max_chunk_bytes", columnar_options.get("max_chunk_bytes")),
            1024 * 1024 * 1024,
        ),
        format=str(columnar_options.get("format") or "parquet"),
        compression=str(columnar_options.get("compression") or "zstd"),
        options=request_options,
    )


def _columnar_fast_path_options(options: dict[str, Any]) -> dict[str, Any]:
    native_transfer = options.get("native_transfer")
    if isinstance(native_transfer, dict):
        snapshot = native_transfer.get("snapshot")
        if isinstance(snapshot, dict) and isinstance(snapshot.get("columnar_fast_path"), dict):
            return dict(snapshot["columnar_fast_path"])
    if isinstance(options.get("columnar_fast_path"), dict):
        return dict(options["columnar_fast_path"])
    return {}


def _execution_options(columnar_options: dict[str, Any]) -> dict[str, Any]:
    execution = columnar_options.get("execution")
    return dict(execution) if isinstance(execution, dict) else {}


def _size_bytes(value: object, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        match = re.fullmatch(r"\s*(\d+)\s*([KMGT]?i?B)?\s*", value, flags=re.IGNORECASE)
        if match:
            unit = (match.group(2) or "B").upper()
            return int(match.group(1)) * _SIZE_UNITS.get(unit, 1)
    raise ValueError(f"Invalid byte size: {value!r}")


def _option(options: dict[str, Any], key: str, default: str) -> str:
    return str(options.get(key) or default).strip().lower() or default


def _log_fallback(logger: Any, blockers: tuple[str, ...]) -> None:
    if hasattr(logger, "log_etl_progress"):
        logger.log_etl_progress("MSSQL_COLUMNAR_SNAPSHOT_FALLBACK", {"Blockers": list(blockers)})


__all__ = ["build_columnar_snapshot_request", "columnar_snapshot_artifact"]
