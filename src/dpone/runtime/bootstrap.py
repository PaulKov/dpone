from __future__ import annotations

from dpone.ports.process_runner import register_process_runner
from dpone.ports.runtime_hydrator import register_runtime_hydrator
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.bootstrap_runner import DefaultProcessRunner

register_runtime_hydrator(DefaultRuntimeHydrator())
register_process_runner(DefaultProcessRunner())

__all__ = ["DefaultProcessRunner", "DefaultRuntimeHydrator"]
