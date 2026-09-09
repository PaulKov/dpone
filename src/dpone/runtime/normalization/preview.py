"""Preview and inference helpers for nested normalization."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from dpone.runtime.normalization.normalizer import NestedNormalizationService
from dpone.runtime.normalization.options import NestedNormalizationOptions


class NormalizationPreviewService:
    """Render self-service preview payloads for nested normalization."""

    def __init__(self, normalizer: NestedNormalizationService | None = None) -> None:
        self._normalizer = normalizer or NestedNormalizationService()

    def preview_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        root_table: str,
        options: NestedNormalizationOptions,
    ) -> dict[str, Any]:
        result = self._normalizer.normalize_rows(rows, root_table=root_table, options=options)
        return {
            "root_table": root_table,
            "enabled": options.enabled,
            "nested_level": options.nested_level,
            "tables": {
                table.name: {
                    "row_count": table.row_count,
                    "columns": [name for name, _ in table.schema],
                    "schema": list(table.schema),
                }
                for table in result.tables
            },
            "row_counts": result.row_counts(),
        }
