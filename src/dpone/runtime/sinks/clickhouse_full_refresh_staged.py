"""Staged-load integration helpers for recoverable ClickHouse full refresh."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import CLUSTER_RECEIPT_VERSION
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult


def finalize_full_refresh(sink: Any, load_config: Any, handle: StagedLoadHandle) -> LoadResult:
    """Publish the finalization table and bind its receipt to staged ownership."""

    candidate = handle.finalization_config or handle.staging_config
    publication = sink._swap_table_into_target(load_config, candidate)
    if publication is not None:
        if not isinstance(handle.metadata, MutableMapping):
            raise TypeError("clickhouse_staged_metadata_not_mutable")
        handle.metadata["full_refresh_publication"] = publication.to_dict()
    return LoadResult(
        inserted_rows=handle.staged_rows,
        updated_rows=0,
        total_rows=sink._count(load_config),
        staging_rows=handle.staged_rows,
        commit_receipt_id=publication.marker.operation_id if publication is not None else None,
        commit_outcome=AtomicCommitOutcome.COMMITTED if publication is not None else None,
        reconciliation_metrics=(
            {"clickhouse_cluster_full_refresh": publication.to_dict()}
            if publication is not None and getattr(publication, "schema_version", None) == CLUSTER_RECEIPT_VERSION
            else None
        ),
    )


def publication_cleanup_plan(
    handle: StagedLoadHandle,
) -> tuple[tuple[Any | None, ...], Mapping[str, Any] | None]:
    """Keep the UUID-bound predecessor out of generic name-based cleanup."""

    publication = dict(getattr(handle, "metadata", {}) or {}).get("full_refresh_publication")
    configs = (handle.finalization_config, handle.decoded_config, handle.staging_config)
    if not isinstance(publication, Mapping):
        return configs, None
    marker = publication.get("marker")
    candidate = marker.get("candidate") if isinstance(marker, Mapping) else None
    return tuple(config for config in configs if getattr(config, "target_table", None) != candidate), publication


__all__ = ["finalize_full_refresh", "publication_cleanup_plan"]
