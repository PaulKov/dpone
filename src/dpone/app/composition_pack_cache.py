"""Shared worker cache root for pack-exec plan and snapshot reopen."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.composition_activation import CompositionAdmissionError

CACHE_ROOT_ENV = "DPONE_CACHE_ROOT"
SCHEDULER_CACHE_ROOT_ENV = "DPONE_SCHEDULER_CACHE_ROOT"


def cache_root_from_environment(environment: Mapping[str, str]) -> Path:
    """Return the shared release cache the worker must reopen, or fail closed."""

    raw = environment.get(CACHE_ROOT_ENV) or environment.get(SCHEDULER_CACHE_ROOT_ENV)
    if not isinstance(raw, str) or not raw.strip():
        raise CompositionAdmissionError("transfer_source_plan")
    root = Path(raw).expanduser()
    try:
        resolved = root.resolve(strict=True)
    except OSError:
        raise CompositionAdmissionError("transfer_source_plan") from None
    if not resolved.is_dir():
        raise CompositionAdmissionError("transfer_source_plan")
    return resolved


__all__ = ["CACHE_ROOT_ENV", "SCHEDULER_CACHE_ROOT_ENV", "cache_root_from_environment"]
