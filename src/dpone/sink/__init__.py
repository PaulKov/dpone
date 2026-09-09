"""DEPRECATED shim package.

This package is kept for backward compatibility.
Use `dpone.runtime.sinks` instead of `dpone.sink`.

This file is hand-written (not auto-generated) to keep imports lazy.
"""

from __future__ import annotations

import warnings as _warnings
from importlib import import_module
from typing import Any

_warnings.warn(
    "`dpone.sink` is deprecated; use `dpone.runtime.sinks`",
    DeprecationWarning,
    stacklevel=2,
)

_RUNTIME = None


def _runtime() -> Any:
    global _RUNTIME
    if _RUNTIME is None:
        _RUNTIME = import_module("dpone.runtime.sinks")
    return _RUNTIME


def __getattr__(name: str) -> Any:
    return getattr(_runtime(), name)


def __dir__() -> list[str]:
    try:
        return sorted(set(list(globals().keys()) + list(dir(_runtime()))))
    except Exception:
        return sorted(globals().keys())
