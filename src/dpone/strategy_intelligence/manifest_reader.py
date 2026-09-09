from __future__ import annotations

from collections.abc import Callable
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.manifest.advisory import AdvisoryManifestProcessReader


class StrategyManifestReader:
    """Read raw manifest hints for advisory commands without touching live systems."""

    def __init__(
        self,
        context_factory: Callable[..., Any] | None = None,
        process_reader: AdvisoryManifestProcessReader | None = None,
    ) -> None:
        self._context_factory = context_factory or _default_context_factory
        self._process_reader = process_reader or AdvisoryManifestProcessReader()

    def read_context(
        self,
        path: str | Path,
        *,
        estimated_rows: int | None = None,
        changed_percent: float | None = None,
        delete_percent: float | None = None,
        cdc_available: bool | None = None,
    ) -> Any:
        raw = self._process_reader.read(Path(path))
        source = raw.get("source", {}) if isinstance(raw.get("source"), dict) else {}
        sink = raw.get("sink", {}) if isinstance(raw.get("sink"), dict) else {}
        source_options = source.get("options", {}) if isinstance(source.get("options"), dict) else {}
        strategy = sink.get("strategy", {}) if isinstance(sink.get("strategy"), dict) else {}
        return self._context_factory(
            source_type=str(source.get("type", "unknown")),
            sink_type=str(sink.get("type", "unknown")),
            requested_mode=str(strategy.get("mode", "auto")),
            requested_merge_policy=str(strategy.get("merge_policy", "auto") or "auto"),
            unique_key=_unique_key_tuple(strategy.get("unique_key")),
            estimated_rows=estimated_rows or _optional_int(source_options.get("estimated_rows")),
            changed_percent=changed_percent,
            delete_percent=delete_percent,
            partition_column=source_options.get("partition_column") or strategy.get("partition", {}).get("column")
            if isinstance(strategy.get("partition"), dict)
            else source_options.get("partition_column"),
            cdc_available=bool(cdc_available) if cdc_available is not None else bool(source_options.get("cdc_enabled")),
            source_cursor=source_options.get("incremental_column"),
            source_options=source_options,
            sink_options=sink.get("options", {}) if isinstance(sink.get("options"), dict) else {},
        )


def _default_context_factory(**values: Any) -> Any:
    context_type = import_module("dpone.strategy_intelligence.advisor").StrategyContext
    return context_type(**values)


def _unique_key_tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, list | tuple):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return (str(value),)


def _optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)
