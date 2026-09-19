"""Best-effort multi-table cleanup for ClickHouse staging resources."""

from __future__ import annotations

from typing import Any

from dpone.runtime.process_io import add_exception_note


def drop_staging_configs(sink: Any, *configs: Any | None) -> None:
    seen: set[str] = set()
    first_error: Exception | None = None
    for config in configs:
        if config is None or not getattr(config, "target_table", None):
            continue
        table = sink._table(config)
        if table in seen:
            continue
        seen.add(table)
        try:
            sink._drop_table(table, config)
        except Exception as error:
            if first_error is None:
                first_error = error
            else:
                add_exception_note(first_error, f"additional staging cleanup failed: {type(error).__name__}")
    if first_error is not None:
        raise first_error


__all__ = ["drop_staging_configs"]
