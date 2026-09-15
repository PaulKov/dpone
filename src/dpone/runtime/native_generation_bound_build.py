"""Bind managed build inputs to the existing dbt engine without a second runner.

The bootstrap supplies a held profile store and the native attempt lifecycle.
This composition seam neither qualifies a runtime nor grants repeat dispatch.
The outer bridge owns authentication before credentials and command admission.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path

from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.contracts.dbt_execution_pack import DbtExecutionPack
from dpone.contracts.dbt_runtime import DbtExecutionInterval
from dpone.ports.dbt_publishing import (
    DbtCommandRunner,
    DbtExecutionEvidenceWriter,
    DbtExecutionOutcome,
    DbtManifestSchemaValidator,
    DbtProfileRenderer,
    DbtProfileStore,
    DbtRunResultsReader,
    DbtRunResultsSchemaValidator,
    DbtToolchainInspector,
)
from dpone.runtime.dbt_execution_service import DbtExecutionService
from dpone.runtime.dbt_preflight import DbtRuntimePreflight
from dpone.runtime.dbt_workspace_attempt_lifecycle import DbtExecutionAttemptLifecycle


class BoundNativeGenerationBuild:
    """Reuse the existing service with fixed inputs and three explicit dependencies.

    Construction validates and snapshots value inputs but performs no filesystem,
    credential or command I/O. A required lifecycle prevents silently selecting
    the legacy terminalizing owner. The supplied lifecycle's authority and the
    profile store's lifetime are established by the qualified bootstrap, not here.
    """

    def __init__(
        self,
        *,
        pack: DbtExecutionPack,
        runtime_root: Path,
        run_output_root: Path,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        interval: DbtExecutionInterval,
        toolchain_inspector: DbtToolchainInspector,
        profile_store: DbtProfileStore,
        run_results_reader: DbtRunResultsReader,
        run_results_validator: DbtRunResultsSchemaValidator,
        manifest_validator: DbtManifestSchemaValidator,
        workspace_attempt_lifecycle: DbtExecutionAttemptLifecycle,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if workspace_attempt_lifecycle is None:
            raise ValueError("native build requires an explicit attempt lifecycle")
        self._pack = DbtExecutionPack.from_mapping(pack.to_dict())
        self._identity = AirflowRunIdentity.from_mapping(run_identity.to_dict())
        self._attempt = AirflowAttemptCorrelation.from_mapping(airflow_attempt.to_dict())
        self._interval = DbtExecutionInterval(start=interval.start, end=interval.end)
        self._root, self._output = Path(runtime_root), Path(run_output_root)
        self._inspector, self._store = toolchain_inspector, profile_store
        self._reader, self._results_validator = run_results_reader, run_results_validator
        self._manifest_validator = manifest_validator
        self._lifecycle, self._clock = workspace_attempt_lifecycle, clock

    def execute(
        self,
        *,
        command_runner: DbtCommandRunner,
        profile_renderer: DbtProfileRenderer,
        evidence_writer: DbtExecutionEvidenceWriter,
    ) -> DbtExecutionOutcome:
        """Run the existing service once, preserving its outcome and exceptions."""
        preflight = DbtRuntimePreflight(
            command_runner=command_runner,
            artifact_reader=self._reader,
            manifest_validator=self._manifest_validator,
        )
        service = DbtExecutionService(
            command_runner=command_runner,
            toolchain_inspector=self._inspector,
            profile_renderer=profile_renderer,
            profile_store=self._store,
            run_results_reader=self._reader,
            run_results_validator=self._results_validator,
            preflight=preflight,
            evidence_writer=evidence_writer,
            workspace_attempt_lifecycle=self._lifecycle,
            clock=self._clock,
        )
        return service.execute(
            self._pack,
            runtime_root=self._root,
            run_output_root=self._output,
            run_identity=self._identity,
            airflow_attempt=self._attempt,
            interval=self._interval,
        )
