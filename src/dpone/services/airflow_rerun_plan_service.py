"""Application service for deterministic Airflow reproducible rerun plans."""

from __future__ import annotations

from pathlib import Path

from dpone.ports.airflow_rerun import AirflowRerunPlanInputError, AirflowRerunPlanInputPort
from dpone.readiness.airflow_rerun_plan import (
    AirflowRerunPlan,
    AirflowRerunPlanner,
    AirflowRunIdentityError,
    parse_airflow_run_identity,
)


class AirflowRerunPlanService:
    """Validate injected inputs and delegate pure selection policy."""

    def __init__(
        self,
        *,
        input_port: AirflowRerunPlanInputPort,
        planner: AirflowRerunPlanner | None = None,
    ) -> None:
        self._input_port = input_port
        self._planner = planner or AirflowRerunPlanner()

    def plan(
        self,
        *,
        evidence_path: str | Path,
        current_index_path: str | Path,
        cache_root: str | Path | None,
        bundle_selection: str,
        artifact_selection: str,
        critical: bool,
    ) -> AirflowRerunPlan:
        inputs = self._input_port.load(
            evidence_path=evidence_path,
            current_index_path=current_index_path,
            cache_root=cache_root,
        )
        evidence = inputs.evidence
        index = inputs.current_index
        if evidence.get("kind") != "gitops.airflow_evidence_bundle":
            raise AirflowRerunPlanInputError(
                "DPONE_AIRFLOW_EVIDENCE_INVALID",
                "evidence input must be gitops.airflow_evidence_bundle",
                path=inputs.evidence_path.as_posix(),
            )
        try:
            identity = parse_airflow_run_identity(evidence.get("run_identity"))
        except AirflowRunIdentityError as exc:
            raise AirflowRerunPlanInputError(
                exc.code,
                "evidence does not contain a valid composite run identity",
                path=inputs.evidence_path.as_posix(),
            ) from exc
        current_matches_original = (
            index.get("release_id") == identity.release_id and index.get("deployment_id") == identity.deployment_id
        )
        original_available, original_error_code = (
            (True, "")
            if current_matches_original
            else self._input_port.original_artifacts_availability(
                inputs.cache_root,
                release_id=identity.release_id,
                deployment_id=identity.deployment_id,
            )
        )
        attempt = evidence.get("attempt")
        try:
            return self._planner.plan(
                original_identity=identity,
                source_attempt=attempt if isinstance(attempt, dict) else {},
                current_index=index,
                bundle_selection=bundle_selection,
                artifact_selection=artifact_selection,
                critical=critical,
                original_artifacts_available=original_available,
                original_artifacts_error_code=original_error_code or "DPONE_DEPLOYMENT_EXPIRED",
            )
        except (AirflowRunIdentityError, ValueError) as exc:
            code = _safe_error_code(exc, fallback="DPONE_AIRFLOW_INDEX_INVALID")
            raise AirflowRerunPlanInputError(
                code,
                "current Airflow deployment index cannot satisfy the requested rerun selection",
                path=inputs.index_path.as_posix(),
            ) from exc


def _safe_error_code(exc: BaseException, *, fallback: str) -> str:
    prefix = str(exc).partition(":")[0]
    if prefix.startswith("DPONE_") and prefix.replace("_", "").isalnum():
        return prefix
    return fallback


__all__ = ["AirflowRerunPlanInputError", "AirflowRerunPlanService"]
