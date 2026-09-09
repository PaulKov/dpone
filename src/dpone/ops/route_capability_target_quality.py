"""Target quality metadata probes for route certification."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


def collect_target_quality(*, sink: Any, load_config: Any) -> dict[str, object]:
    """Collect non-invasive target metadata through the sink public facade."""

    schema = _target_schema(sink, load_config)
    if schema is None:
        return {}
    return {
        "lineage_columns": _lineage_columns(schema),
    }


def _target_schema(sink: Any, load_config: Any) -> Sequence[tuple[str, str]] | None:
    get_target_schema = getattr(sink, "get_target_schema", None)
    if not callable(get_target_schema):
        return None
    return tuple((str(name), str(dtype)) for name, dtype in get_target_schema(load_config))


def _lineage_columns(schema: Sequence[tuple[str, str]]) -> list[str]:
    return [name for name, _ in schema if name.startswith("__dpone__")]


__all__ = ["collect_target_quality"]
