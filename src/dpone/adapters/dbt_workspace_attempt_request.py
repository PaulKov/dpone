"""Build exact workspace attempt requests from authenticated runtime inputs."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.contracts.dbt_workspace_control import (
    AirflowAttemptCorrelation,
    AirflowDeploymentIdentity,
    AirflowRunIdentity,
    DbtExecutionPack,
    DbtWorkspaceActivationError,
    DbtWorkspaceAttemptRequest,
    canonical_fingerprint,
    dbt_relation_write_subject,
    selected_relation_writes,
)


class DbtWorkspaceAttemptRequestFactory:
    """Bind one Airflow attempt to the activation and exact manifest write subset."""

    def __init__(self, deployment_identity: AirflowDeploymentIdentity) -> None:
        self._deployment_identity = deployment_identity

    def build(
        self,
        *,
        pack: DbtExecutionPack,
        manifest: Mapping[str, object],
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
    ) -> DbtWorkspaceAttemptRequest:
        identity = self._deployment_identity
        if (identity.release_id, identity.deployment_id) != (run_identity.release_id, run_identity.deployment_id):
            raise DbtWorkspaceActivationError("attempt_deployment_identity")
        writes = selected_relation_writes(
            project_path=pack.project_subdir,
            execution=pack,
            manifest=manifest,
        )
        write_subjects = tuple(sorted(dbt_relation_write_subject(item) for item in writes))
        attempt_id = canonical_fingerprint(
            {
                "schema": "dpone.dbt-workspace-airflow-attempt.v1",
                "activation_id": identity.activation_id,
                "release_id": identity.release_id,
                "deployment_id": identity.deployment_id,
                "pack_sha256": pack.pack_sha256,
                "airflow_attempt": airflow_attempt.to_dict(),
            }
        )
        return DbtWorkspaceAttemptRequest.build(
            activation_id=identity.activation_id,
            attempt_id=attempt_id,
            workflow_id=pack.workflow_id,
            write_subjects=write_subjects,
        )


__all__ = ["DbtWorkspaceAttemptRequestFactory"]
