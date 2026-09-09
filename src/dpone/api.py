"""Public Python convenience API for dpone users."""

from __future__ import annotations

import os
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dpone.manifest.loader import ManifestLoaderRouter
from dpone.services.airflow_mapping_context import AirflowMappingContextService
from dpone.services.interval_context import IntervalContextService
from dpone.services.manifest import ManifestCommandContext
from dpone.services.run_invocation_context import RunInvocationContextService
from dpone.services.run_manifest import RunManifestResult, RunManifestService


def run(
    path: str | Path,
    *,
    selector: str | None = None,
    run_id: str | None = None,
    registry_paths: Sequence[str | Path] = (),
    dag_id: str | None = None,
    execution_date: Any | None = None,
    retry_attempts: int = 0,
    retry_backoff_seconds: float = 0.0,
    repair_authority_ref: str | None = None,
) -> RunManifestResult:
    """Execute one dpone manifest process from Python.

    This is the Python equivalent of ``dpone run``. It intentionally returns the
    same report object used by the CLI so callers can inspect status, row
    counts, errors, and render JSON/Markdown/Text if needed.
    """

    normalized_registry_paths = tuple(Path(item) for item in registry_paths)
    manifest_ctx = ManifestCommandContext(
        registry_paths=normalized_registry_paths,
        loader=ManifestLoaderRouter(registry_paths=normalized_registry_paths),
    )
    invocation = RunInvocationContextService(
        mapping_context_service=AirflowMappingContextService(),
        interval_context_factory=IntervalContextService,
    ).resolve(
        environ=os.environ,
        dag_id=dag_id,
        execution_date=execution_date,
        repair_authority_ref=repair_authority_ref,
    )
    return RunManifestService().run(
        path=path,
        manifest_ctx=manifest_ctx,
        selector=selector,
        run_id=run_id or invocation.run_id,
        dag_id=invocation.dag_id,
        execution_date=invocation.execution_date,
        retry_attempts=retry_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        load_config_mutator=invocation.load_config_mutator,
        run_context_config=invocation.run_context_config,
    )


__all__ = ["run", "RunManifestResult"]
