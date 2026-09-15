"""Fail-closed admission and terminalization for one dbt workspace attempt."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dpone.contracts.dbt_runtime import DBT_EXECUTION_PACK_SCHEMA_V2, DbtPublishingError
from dpone.runtime.dbt_preflight import MAX_DBT_PREFLIGHT_MANIFEST_BYTES

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import AirflowAttemptCorrelation, AirflowRunIdentity, DbtExecutionPack
    from dpone.ports.dbt_publishing import DbtRunResultsReader
    from dpone.ports.dbt_workspace_attempt import (
        DbtWorkspaceAttemptAdmissionPort,
        DbtWorkspaceAttemptRequest,
        DbtWorkspaceAttemptRequestFactoryPort,
        DbtWorkspaceAttemptTerminalState,
    )
    from dpone.runtime.dbt_execution_policy import DbtExecutionOutputPaths


class DbtExecutionAttemptLifecycle(Protocol):
    """Invocation owner for admission and execution outcome reporting."""

    def admit(
        self,
        pack: DbtExecutionPack,
        *,
        output_paths: DbtExecutionOutputPaths,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> DbtWorkspaceAttemptRequest | None: ...

    def record_execution_outcome(
        self,
        request: DbtWorkspaceAttemptRequest | None,
        *,
        state: DbtWorkspaceAttemptTerminalState,
        fallback_code: str,
        build_started: bool,
    ) -> str: ...


class DbtWorkspaceAttemptLifecycle:
    """Keep workspace fencing policy separate from dbt process orchestration."""

    def __init__(
        self,
        *,
        run_results_reader: DbtRunResultsReader,
        request_factory: DbtWorkspaceAttemptRequestFactoryPort | None,
        admission: DbtWorkspaceAttemptAdmissionPort | None,
    ) -> None:
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
        """Admit V2 immediately before mutation; V1 remains unchanged."""

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

    def record_execution_outcome(
        self,
        request: DbtWorkspaceAttemptRequest | None,
        *,
        state: DbtWorkspaceAttemptTerminalState,
        fallback_code: str,
        build_started: bool,
    ) -> str:
        """Ordinary execution retains its immediate durable terminal decision."""
        return self.terminalize(
            request,
            state=state,
            fallback_code=fallback_code,
            build_started=build_started,
        )

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


def build_execution_attempt_lifecycle(
    *,
    lifecycle: DbtExecutionAttemptLifecycle | None,
    run_results_reader: DbtRunResultsReader,
    request_factory: DbtWorkspaceAttemptRequestFactoryPort | None,
    admission: DbtWorkspaceAttemptAdmissionPort | None,
) -> DbtExecutionAttemptLifecycle:
    """Select one explicit owner without silently combining admission policies."""
    if lifecycle is not None:
        if request_factory is not None or admission is not None:
            raise ValueError("explicit lifecycle cannot be combined with workspace admission dependencies")
        return lifecycle
    return DbtWorkspaceAttemptLifecycle(
        run_results_reader=run_results_reader,
        request_factory=request_factory,
        admission=admission,
    )


def record_execution_outcome(
    lifecycle: DbtExecutionAttemptLifecycle,
    request: DbtWorkspaceAttemptRequest | None,
    *,
    state: DbtWorkspaceAttemptTerminalState,
    fallback_code: str,
    build_started: bool,
) -> str:
    """Normalize owner failure without losing post-dispatch ambiguity evidence."""
    try:
        return lifecycle.record_execution_outcome(
            request,
            state=state,
            fallback_code=fallback_code,
            build_started=build_started,
        )
    except Exception:  # noqa: BLE001 - post-dispatch ambiguity still needs evidence
        return "COMMIT_UNKNOWN" if build_started else fallback_code


__all__ = ["DbtWorkspaceAttemptLifecycle"]
