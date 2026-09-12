"""Compatibility entry point for runtime implementations and lazy registration.

Default composition lives in :mod:`dpone.app.runtime_bootstrap`. Importing this
legacy entry point retains registration through the public runtime ports.
"""

from __future__ import annotations

from dpone.ports.process_runner import ensure_process_runner
from dpone.ports.runtime_hydrator import ensure_runtime_hydrator
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_runner import DefaultProcessRunner

ensure_runtime_hydrator()
ensure_process_runner()

__all__ = ["DefaultProcessRunner", "DefaultRuntimeHydrator"]
