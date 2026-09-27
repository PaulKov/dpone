"""Stage a foreign SQL query into SQL Server through the existing BCP spool."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.staging import owned_staging_handle

_SOURCE_CONNECTOR_OPTION = "_source_connector"


def stage_sql_query_artifact(
    staging_manager: Any,
    load_config: Any,
    artifact: SqlQueryArtifact,
    schema: Sequence[tuple[str, str]],
) -> StagingTableArtifact:
    """Read the source query and import it with one SQL Server bulk load."""

    source = _source_connector(load_config)
    _require_source_stream(source, artifact)
    lifecycle = getattr(artifact, "extraction_lifecycle", None)
    if lifecycle is not None and getattr(lifecycle, "receipt", None) is None:
        lifecycle.acquire()
    batch_size = int(getattr(load_config, "batch_size", None) or 10_000)
    with owned_staging_handle(staging_manager, load_config, schema) as handle:
        inserted = int(
            staging_manager.insert_streaming_rows(
                handle,
                _flatten_batches(
                    source.get_records_streaming(artifact.sql, batch_size=batch_size, as_dict=True)
                ),
            )
            or 0
        )
        handle.row_count = inserted
        if lifecycle is not None:
            lifecycle.complete()
        return handle


def _source_connector(load_config: Any) -> Any:
    options = getattr(load_config, "options", {}) or {}
    source = options.get(_SOURCE_CONNECTOR_OPTION)
    if source is None or not callable(getattr(source, "get_records_streaming", None)):
        raise RuntimeError("mssql_sql_query_source_stream_required")
    return source


def _require_source_stream(source: Any, artifact: SqlQueryArtifact) -> None:
    connector_dialect = str(getattr(source, "dialect", "") or "").strip().lower()
    if connector_dialect and connector_dialect != artifact.dialect:
        raise RuntimeError("mssql_sql_query_source_dialect_mismatch")


def _flatten_batches(batches: Any) -> Iterator[Mapping[str, object]]:
    for batch in batches:
        if isinstance(batch, Mapping):
            yield batch
            continue
        yield from batch


__all__ = ["stage_sql_query_artifact"]
