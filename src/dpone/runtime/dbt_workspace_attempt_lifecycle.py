"""Fail-closed admission and terminalization for one dbt workspace attempt."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.contracts.dbt_runtime import DBT_EXECUTION_PACK_SCHEMA_V2, DbtPublishingError
from dpone.runtime.dbt_preflight import MAX_DBT_PREFLIGHT_MANIFEST_BYTES

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import AirflowAttemptCorrelation, AirflowRunIdentity, DbtExecutionPack
    from dpone.ports.composition_dbt import CompositionDbtBuildAuthority
    from dpone.ports.dbt_publishing import DbtRunResultsReader
    from dpone.ports.dbt_workspace_attempt import (
        DbtWorkspaceAttemptAdmissionPort,
        DbtWorkspaceAttemptRequest,
        DbtWorkspaceAttemptRequestFactoryPort,
        DbtWorkspaceAttemptTerminalState,
    )
    from dpone.runtime.dbt_execution_policy import DbtExecutionOutputPaths


class DbtWorkspaceAttemptLifecycle:
    """Keep workspace fencing policy separate from dbt process orchestration."""

    def __init__(
        self,
        *,
        run_results_reader: DbtRunResultsReader,
        request_factory: DbtWorkspaceAttemptRequestFactoryPort | None,
        admission: DbtWorkspaceAttemptAdmissionPort | None,
        composition_attempt: CompositionDbtBuildAuthority | None = None,
    ) -> None:
        if composition_attempt is not None and (request_factory is not None or admission is not None):
            raise ValueError("dbt execution must use exactly one parent or native workspace authority")
        self._composition_attempt = composition_attempt
        self._run_results_reader = run_results_reader
        self._request_factory = request_factory
        self._admission = admission

    def admit(
        self,
        pack: DbtExecutionPack,
        *,
        output_paths: DbtExecutionOutputPaths,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> DbtWorkspaceAttemptRequest | None:
        """Verify parent authority or admit native V2 immediately before mutation.

        Parent terminalization belongs to its enclosing worker. Returning no
        native request prevents duplicate native receipts for that execution.
        """

        if self._composition_attempt is not None:
            self._composition_attempt.verify_before_build(
                pack=pack,
                manifest=self._run_results_reader.read(
                    output_paths.preflight_target / "manifest.json",
                    root=output_paths.root,
                    max_bytes=MAX_DBT_PREFLIGHT_MANIFEST_BYTES,
                ),
                run_identity=run_identity,
                airflow_attempt=airflow_attempt,
            )
            return None

        if pack.schema != DBT_EXECUTION_PACK_SCHEMA_V2:
            return None
        if self._request_factory is None or self._admission is None:
            raise DbtPublishingError(
                "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
                "Workspace task-attempt admission is required before dbt mutation",
            )
        manifest = self._run_results_reader.read(
            output_paths.preflight_target / "manifest.json",
            root=output_paths.root,
            max_bytes=MAX_DBT_PREFLIGHT_MANIFEST_BYTES,
        )
        request = self._request_factory.build(
            pack=pack,
            manifest=manifest,
            run_identity=run_identity,
            airflow_attempt=airflow_attempt,
        )
        receipt = self._admission.admit(request)
        if receipt.state != "RUNNING" or receipt.request_sha256 != request.request_sha256:
            raise DbtPublishingError(
                "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
                "Workspace task-attempt admission readback differs",
            )
        return request

    def terminalize(
        self,
        request: DbtWorkspaceAttemptRequest | None,
        *,
        state: DbtWorkspaceAttemptTerminalState,
        fallback_code: str,
        build_started: bool,
    ) -> str:
        """Persist a terminal receipt; ambiguity after mutation is COMMIT_UNKNOWN."""

        if request is None:
            return fallback_code
        if self._admission is None:
            return "COMMIT_UNKNOWN" if build_started else fallback_code
        try:
            receipt = self._admission.terminalize(request, state=state)
            if receipt.state != state or receipt.request_sha256 != request.request_sha256:
                raise DbtPublishingError(
                    "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE",
                    "Workspace attempt terminal readback differs",
                )
        except Exception:  # noqa: BLE001 - a lost terminal ACK after mutation is ambiguous
            return "COMMIT_UNKNOWN" if build_started else fallback_code
        return fallback_code


__all__ = ["DbtWorkspaceAttemptLifecycle"]
