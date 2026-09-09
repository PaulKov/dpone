"""Shared helpers for route ops command handlers."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .command_handlers_release_context import ReleaseOpsCatalog, release_ops


def ops_catalog(ctx: object) -> ReleaseOpsCatalog:
    """Resolve route operations services from the CLI context."""

    return release_ops(ctx)


def read_rows(path: str | Path) -> tuple[Mapping[str, Any], ...]:
    """Read reconciliation rows from a list payload or an object with a rows field."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    rows = payload.get("rows") if isinstance(payload, Mapping) else payload
    if not isinstance(rows, list | tuple):
        raise ValueError("Route reconciliation repair rows JSON must be a list or an object with rows")
    result: list[Mapping[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("Route reconciliation repair row entries must be JSON objects")
        result.append(row)
    return tuple(result)


__all__ = ["ops_catalog", "read_rows"]
