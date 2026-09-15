"""Construct and persist complete dbt execution evidence behind one owner."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from dpone.contracts.commit_unknown import CommitUnknownOutcome
from dpone.contracts.dbt_runtime import (
    AirflowAttemptCorrelation,
    AirflowRunIdentity,
    DbtCredentialVersion,
    DbtExecutionEvidence,
    DbtExecutionPack,
    DbtNodeOutcome,
    dbt_target_binding_sha256,
    dbt_target_identity_sha256,
)
from dpone.ports.dbt_publishing import DbtExecutionEvidenceWriter, DbtExecutionOutcome
from dpone.runtime.commit_unknown import CommitUnknownError
from dpone.runtime.dbt_execution_policy import aware_timestamp
from dpone.runtime.dbt_run_results import ParsedDbtRunResults, dbt_node_outcomes


class DbtExecutionOutcomeWriter:
    """Own the complete evidence projection, timestamp and persistence boundary."""

    def __init__(self, *, writer: DbtExecutionEvidenceWriter, clock: Callable[[], datetime]) -> None:
        self._evidence_writer = writer
        self._clock = clock

    def write(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        started_at: str,
        dbt_exit_code: int | None,
        code: str,
        parsed: ParsedDbtRunResults | None,
        credential_versions: tuple[DbtCredentialVersion, ...],
        preflight_status: str,
        build_started: bool,
        passed: bool = False,
    ) -> DbtExecutionOutcome:
        """Construct, persist and return one complete execution outcome."""
        evidence = DbtExecutionEvidence(
            status="passed" if passed else "failed",
            code=code,
            workflow_id=pack.workflow_id,
            release_id=run_identity.release_id,
            deployment_id=run_identity.deployment_id,
            workload_pack_sha256=run_identity.workload_pack.sha256,
            project_bundle_sha256=pack.project_bundle_sha256,
            manifest_sha256=pack.selection_lock.manifest_sha256,
            selection_sha256=pack.selection_lock.selection_sha256,
            toolchain_sha256=pack.selection_lock.toolchain_sha256,
            invocation_context_sha256=(pack.invocation_context.invocation_context_sha256),
            logical_target_sha256=dbt_target_identity_sha256(pack.profile),
            target_binding_sha256=dbt_target_binding_sha256(
                pack,
                run_identity,
            ),
            adapter_runtime=pack.adapter_runtime,
            adapter_policy_sha256=pack.adapter_policy.adapter_policy_sha256,
            graph_policy_sha256=pack.selection_lock.graph_policy_sha256,
            preflight_status=preflight_status,
            build_started=build_started,
            dbt_exit_code=dbt_exit_code,
            dbt_warning_policy=pack.dbt_warning_policy,
            dbt_warning_count=(parsed.warning_count if parsed is not None else 0),
            dbt_schema_version=parsed.schema_version if parsed is not None else None,
            dbt_version=parsed.dbt_version if parsed is not None else None,
            invocation_id=parsed.invocation_id if parsed is not None else None,
            started_at=started_at,
            finished_at=aware_timestamp(self._clock()),
            airflow=airflow_attempt.to_dict(),
            credential_versions=credential_versions,
            nodes=dbt_node_outcomes(parsed, factory=DbtNodeOutcome),
            recovery=(
                CommitUnknownOutcome(
                    failure_boundary="target_invocation",
                    checkpoint_state="not_advanced",
                ).to_jsonable()
                if code == "COMMIT_UNKNOWN"
                else None
            ),
        )
        persist_execution_evidence(self._evidence_writer, evidence)
        outcome_exit_code = dbt_exit_code if dbt_exit_code is not None and dbt_exit_code != 0 else (0 if passed else 1)
        return DbtExecutionOutcome(
            exit_code=outcome_exit_code,
            evidence=evidence,
        )


def persist_execution_evidence(writer: DbtExecutionEvidenceWriter, evidence: DbtExecutionEvidence) -> None:
    """Persist once; post-dispatch write failures cannot become retry permission."""
    try:
        writer.write(evidence)
    except Exception as exc:
        if evidence.build_started:
            raise CommitUnknownError(
                CommitUnknownOutcome(failure_boundary="target_invocation", checkpoint_state="not_advanced")
            ) from exc
        raise
