"""Execution port for ETL processes.

The DAG layer uses this port instead of importing runtime executors directly.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.process_errors import ETLProcessError

if TYPE_CHECKING:
    from dpone.contracts.process_types import ProcessResult
    from dpone.contracts.run_context import RunContext
    from dpone.dag.process import ETLProcess

if False:  # pragma: no cover
    pass


class ProcessRunner(Protocol):
    def run(
        self,
        process: ETLProcess,
        *,
        context: RunContext | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
    ) -> ProcessResult:  # pragma: no cover - protocol
        ...


_PROCESS_RUNNER: ProcessRunner | None = None
_DEFAULT_RUNTIME_BOOTSTRAP = "dpone.app.runtime_bootstrap"


def register_process_runner(runner: ProcessRunner) -> None:
    global _PROCESS_RUNNER
    _PROCESS_RUNNER = runner


def get_process_runner() -> ProcessRunner | None:
    return _PROCESS_RUNNER


def ensure_process_runner() -> ProcessRunner:
    runner = get_process_runner()
    if runner is not None:
        return runner

    importlib.import_module(_DEFAULT_RUNTIME_BOOTSTRAP)
    runner = get_process_runner()
    if runner is None:
        raise ETLProcessError(
            "Process runner is not registered. Import dpone.app.runtime_bootstrap or "
            "register a custom ProcessRunner via dpone.ports.process_runner."
        )
    return runner
