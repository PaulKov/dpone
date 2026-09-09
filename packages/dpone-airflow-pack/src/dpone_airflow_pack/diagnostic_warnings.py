"""Warnings that must never replace a cache operation's primary outcome."""

from __future__ import annotations

import warnings


def emit_nonfatal_runtime_warning(message: str, *, stacklevel: int = 2) -> None:
    """Emit best-effort diagnostics even when callers promote warnings to errors."""

    try:
        warnings.warn(message, RuntimeWarning, stacklevel=stacklevel)
    except Exception:  # noqa: BLE001 - diagnostics cannot own control flow.
        pass


__all__ = ["emit_nonfatal_runtime_warning"]
