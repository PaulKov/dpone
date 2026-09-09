"""Staged-load detection and load-governance config helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def supports_staged_load(sink: Any) -> bool:
    """Return True when the sink exposes the staged load abort port."""

    return all(
        callable(getattr(sink, name, None)) for name in ("stage_payload", "finalize_staged_load", "abort_staged_load")
    )


def requested_finalization_phase(load_config: Any) -> str:
    options = getattr(load_config, "options", {}) or {}
    governance = options.get("load_governance") if isinstance(options, dict) else None
    if isinstance(governance, dict):
        return str(governance.get("finalization_phase") or "pre_finalize")
    return "pre_finalize"


def quality_config(load_config: Any) -> object:
    options = getattr(load_config, "options", None)
    return options.get("quality") if isinstance(options, Mapping) else None
