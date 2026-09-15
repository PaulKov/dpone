"""Retain the native outer owner's physical attempt during one dbt execution."""

from __future__ import annotations

from collections.abc import Callable
from threading import Lock
from typing import TYPE_CHECKING

from dpone.contracts.dbt_runtime import DBT_EXECUTION_PACK_SCHEMA_V2, DbtPublishingError
from dpone.contracts.dbt_workspace_attempt import require_attempt_receipt
from dpone.runtime.dbt_preflight import MAX_DBT_PREFLIGHT_MANIFEST_BYTES

if TYPE_CHECKING:
    from dpone.contracts.dbt_runtime import AirflowAttemptCorrelation, AirflowRunIdentity, DbtExecutionPack
    from dpone.ports.dbt_publishing import DbtRunResultsReader
    from dpone.ports.dbt_workspace_attempt import (
        DbtWorkspaceAttemptReceipt,
        DbtWorkspaceAttemptRequest,
        DbtWorkspaceAttemptRequestFactoryPort,
        DbtWorkspaceAttemptTerminalState,
    )
    from dpone.runtime.dbt_execution_policy import DbtExecutionOutputPaths


class NativeWorkspaceAttemptLifecycle:
    """Compare an already admitted P; never terminalize or authorize settlement.

    One instance belongs to one execution. The local outcome is not durable
    evidence or completion authority. The outer owner must require the successful
    service return and protected native completion, and retain its own fencing
    checks for every later mutation. Current readback is not a lifetime lease.
    """

    def __init__(
        self,
        *,
        request: DbtWorkspaceAttemptRequest,
        receipt: DbtWorkspaceAttemptReceipt,
        request_factory: DbtWorkspaceAttemptRequestFactoryPort,
        run_results_reader: DbtRunResultsReader,
        require_current_running: Callable[[DbtWorkspaceAttemptRequest], DbtWorkspaceAttemptReceipt],
    ) -> None:
        request.__post_init__()
        require_attempt_receipt(receipt, request, state="RUNNING")
        self._request, self._receipt = request, receipt
        self._factory, self._reader = request_factory, run_results_reader
        self._require_current = require_current_running
        self._lock = Lock()
        self._attempted = False
        self._admitted = False
        self._outcome: tuple[str, str, bool] | None = None

    def admit(
        self,
        pack: DbtExecutionPack,
        *,
        output_paths: DbtExecutionOutputPaths,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> DbtWorkspaceAttemptRequest:
        with self._lock:
            if self._attempted:
                raise _error("Native execution cannot repeat an admission attempt")
            self._attempted = True
        if pack.schema != DBT_EXECUTION_PACK_SCHEMA_V2:
            raise _error("Native execution requires a workspace V2 pack")
        manifest = self._reader.read(
            output_paths.preflight_target / "manifest.json",
            root=output_paths.root,
            max_bytes=MAX_DBT_PREFLIGHT_MANIFEST_BYTES,
        )
        request = self._factory.build(
            pack=pack,
            manifest=manifest,
            run_identity=run_identity,
            airflow_attempt=airflow_attempt,
        )
        if request != self._request:
            raise _error("Native execution differs from the retained physical attempt")
        current = self._require_current(request)
        require_attempt_receipt(current, request, state="RUNNING")
        if current != self._receipt:
            raise _error("Native physical attempt receipt changed")
        with self._lock:
            self._admitted = True
        return request

    def record_execution_outcome(
        self,
        request: DbtWorkspaceAttemptRequest | None,
        *,
        state: DbtWorkspaceAttemptTerminalState,
        fallback_code: str,
        build_started: bool,
    ) -> str:
        if request is None and not build_started:
            return fallback_code
        with self._lock:
            if not self._admitted or request != self._request:
                raise _error("Execution outcome has no matching native admission")
            if state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
                raise _error("Invalid native execution outcome")
            outcome = (state, fallback_code, build_started)
            if self._outcome is not None and self._outcome != outcome:
                raise _error("Native execution outcome changed")
            self._outcome = outcome
        return fallback_code


def _error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE", message)
