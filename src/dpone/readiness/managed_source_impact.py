"""Source-impact planning for managed dry-run manifests."""

from __future__ import annotations

from typing import Any

from dpone.runtime.source_impact import SourceImpactInspector


def source_impact(
    *,
    source_type: str,
    base_query: str,
    partition_column: str | None,
    indexed_columns: tuple[str, ...],
    source_kind: str,
) -> list[dict[str, Any]]:
    """Inspect the bounded source query selected by managed planning."""

    return [
        item.to_dict()
        for item in SourceImpactInspector().inspect(
            source_type=source_type,
            base_query=base_query,
            partition_column=partition_column,
            indexed_columns=indexed_columns,
            source_kind=source_kind,
        )
    ]
