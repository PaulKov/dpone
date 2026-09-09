"""Pure DAG process node.

Execution is delegated to the ProcessRunner port so that ``dpone.dag`` stays
independent from ``dpone.runtime`` imports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dpone.contracts.process_types import ProcessResult
from dpone.contracts.run_context import RunContext
from dpone.dag.config import ETLProcessConfig
from dpone.ports.process_runner import ensure_process_runner


@dataclass
class ETLProcess:
    """ETL process with config and graph relationships."""

    config: ETLProcessConfig
    config_path: str | None = None
    upstream: ETLProcess | None = None
    siblings: list[ETLProcess] = field(default_factory=list)
    current_state: ProcessResult | None = None

    def run(
        self,
        context: RunContext | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
    ) -> ProcessResult:
        """Execute through the registered ProcessRunner."""

        runner = ensure_process_runner()
        return runner.run(
            self,
            context=context,
            dag_id=dag_id,
            execution_date=execution_date,
        )
