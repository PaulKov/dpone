from __future__ import annotations

from dpone.gitops.schema_airflow_connection_bridge_contracts import airflow_connection_bridge_schema
from dpone.gitops.schema_airflow_deployment_identity_contracts import airflow_deployment_identity_schema
from dpone.gitops.schema_airflow_git_sync_contracts import airflow_git_sync_schema
from dpone.gitops.schema_airflow_mapping_contracts import airflow_mapping_summary_schema
from dpone.gitops.schema_airflow_run_identity_contracts import airflow_run_identity_schema
from dpone.gitops.schema_airflow_runtime_components import (
    AIRFLOW_K8S_SMOKE_REQUIRED,
    AIRFLOW_RUN_SPEC_REQUIRED,
    AIRFLOW_RUNTIME_EVIDENCE_REQUIRED,
    AIRFLOW_RUNTIME_PROFILE_REQUIRED,
    airflow_artifact_schema,
    airflow_artifact_sink_schema,
    airflow_check_schema,
    airflow_env_var_schema,
    airflow_image_contract_schema,
    airflow_k8s_smoke_command_schema,
    airflow_k8s_smoke_result_schema,
    airflow_run_spec_entry_schema,
    airflow_run_spec_step_schema,
    airflow_runtime_resources_schema,
    airflow_runtime_step_schema,
)
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    const_schema,
    contract,
    integer_schema,
    issue_ref,
    number_schema,
    object_schema,
    string_schema,
)


def airflow_render_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-render",
        kind="gitops.airflow_render",
        title="dpone GitOps Airflow render contract",
        required=("kind", "bundle_path", "output_dir", "image", "artifacts", "commands"),
        properties={
            "kind": const_schema("gitops.airflow_render"),
            "bundle_path": string_schema(),
            "output_dir": string_schema(),
            "image": string_schema(),
            "artifacts": array_schema(airflow_artifact_schema()),
            "commands": array_schema(string_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_doctor_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-doctor",
        kind="gitops.airflow_doctor",
        title="dpone GitOps Airflow doctor contract",
        required=("kind", "bundle_path", "pod_template", "image_contract", "runner_policy", "checks"),
        properties={
            "kind": const_schema("gitops.airflow_doctor"),
            "bundle_path": string_schema(),
            "pod_template": string_schema(),
            "image_contract": string_schema(),
            "image": string_schema(),
            "runner_policy": string_schema(),
            "checks": array_schema(airflow_check_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_image_contract_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-image-contract",
        kind="gitops.airflow_image_contract",
        title="dpone GitOps Airflow image contract report",
        required=("kind", "output_path", "contract"),
        properties={
            "kind": const_schema("gitops.airflow_image_contract"),
            "output_path": string_schema(),
            "contract": airflow_image_contract_schema(),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_run_spec_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-run-spec",
        kind="gitops.airflow_run_spec",
        title="dpone GitOps Airflow run spec contract",
        required=AIRFLOW_RUN_SPEC_REQUIRED,
        properties={
            "kind": const_schema("gitops.airflow_run_spec"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "bundle_path": string_schema(),
            "bundle_digest": string_schema(),
            "image": string_schema(),
            "image_digest": {"type": ["string", "null"]},
            "worktree": string_schema(),
            "evidence_output": string_schema(),
            "airflow_context_env": array_schema(string_schema()),
            "entries": array_schema(airflow_run_spec_entry_schema()),
            "steps": array_schema(airflow_run_spec_step_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_runtime_evidence_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-runtime-evidence",
        kind="gitops.airflow_runtime_evidence",
        title="dpone GitOps Airflow runtime evidence contract",
        required=AIRFLOW_RUNTIME_EVIDENCE_REQUIRED,
        properties={
            "kind": const_schema("gitops.airflow_runtime_evidence"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "run_spec_path": string_schema(),
            "bundle_path": string_schema(),
            "image": string_schema(),
            "image_digest": {"type": ["string", "null"]},
            "status": string_schema(),
            "started_at": string_schema(),
            "finished_at": string_schema(),
            "duration_seconds": number_schema(),
            "steps": array_schema(airflow_runtime_step_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_runtime_profile_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-runtime-profile",
        kind="gitops.airflow_runtime_profile",
        title="dpone GitOps Airflow runtime profile contract",
        required=AIRFLOW_RUNTIME_PROFILE_REQUIRED,
        properties={
            "kind": const_schema("gitops.airflow_runtime_profile"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "bundle_path": string_schema(),
            "bundle_digest": string_schema(),
            "run_spec_path": string_schema(),
            "runtime_evidence_path": string_schema(),
            "xcom_summary_path": string_schema(),
            "dag_factory_path": string_schema(),
            "outcome_gate_path": string_schema(),
            "image": string_schema(),
            "image_digest": {"type": ["string", "null"]},
            "namespace": string_schema(),
            "service_account": string_schema(),
            "resources": airflow_runtime_resources_schema(),
            "artifact_sink": airflow_artifact_sink_schema(),
            "env": array_schema(airflow_env_var_schema()),
            "labels": object_schema(),
            "annotations": object_schema(),
            "git_sync": airflow_git_sync_schema(),
            "connection_bridge": airflow_connection_bridge_schema(),
            "runner_policy": string_schema(),
            "outcome_mode": string_schema(),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_xcom_summary_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-xcom-summary",
        kind="gitops.airflow_xcom_summary",
        title="dpone GitOps Airflow XCom summary contract",
        required=("kind", "schema_version", "producer", "status", "run_spec_path", "runtime_evidence_path"),
        properties={
            "kind": const_schema("gitops.airflow_xcom_summary"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "status": string_schema(),
            "runtime_profile_path": string_schema(),
            "run_spec_path": string_schema(),
            "runtime_evidence_path": string_schema(),
            "runtime_evidence_sha256": string_schema(),
            "runtime_evidence": object_schema(),
            "failed_step": {"type": ["string", "null"]},
            "step_counts": object_schema(),
            "artifact_paths": object_schema(),
            "warnings": array_schema(issue_ref()),
            "interval": airflow_run_interval_schema(),
            "backfill": airflow_backfill_progress_schema(),
            "recovery": airflow_runtime_recovery_schema(),
            "blockers": array_schema(issue_ref()),
            "run_identity": airflow_run_identity_schema(),
            "deployment_identity": airflow_deployment_identity_schema(),
            "dbt_execution_evidence_ref": {
                "type": "object",
                "additionalProperties": False,
                "required": [
                    "schema",
                    "workflow_id",
                    "sha256",
                    "bytes",
                    "storage_scope",
                ],
                "properties": {
                    "schema": const_schema("dpone.dbt-execution-evidence-ref.v1"),
                    "workflow_id": string_schema(),
                    "sha256": string_schema(),
                    "bytes": {"type": "integer", "minimum": 1, "maximum": 16777216},
                    "storage_scope": const_schema("dbt_spool"),
                },
            },
        },
    )


def airflow_run_interval_schema() -> dict:
    """Logical data interval of the DAG run (DPONE_* run interval contract)."""

    return object_schema(
        properties={
            "interval_start": {"type": ["string", "null"]},
            "interval_end": {"type": ["string", "null"]},
            "logical_date": {"type": ["string", "null"]},
            "dag_id": {"type": ["string", "null"]},
            "dag_run_id": {"type": ["string", "null"]},
            "try_number": {"type": ["string", "null"]},
            "partition_key": {"type": ["string", "null"]},
            "partition_dimension": {"type": ["string", "null"]},
            "partition_mode": {
                "type": ["string", "null"],
                "enum": ["native", "degraded_unpartitioned", None],
            },
        }
    )


def airflow_backfill_progress_schema() -> dict:
    """Bounded chunked-backfill campaign progress (no per-chunk payloads)."""

    return object_schema(
        properties={
            "run_key": string_schema(),
            "dataset": string_schema(),
            "inner_mode": string_schema(),
            "operation_status": string_schema(),
            "retry_policy": string_schema(),
            "state_path": {"type": ["string", "null"]},
            "state_cache_status": {"enum": ["authoritative", "present_non_authoritative", "unavailable"]},
            "state_authority": object_schema(
                properties={
                    "backend": {"enum": ["local_file", "audit_schema"]},
                    "dialect": {"type": ["string", "null"]},
                    "schema": {"type": ["string", "null"]},
                    "campaigns_table": {"type": ["string", "null"]},
                    "chunks_table": {"type": ["string", "null"]},
                    "run_key": string_schema(),
                }
            ),
            "chunks_total": integer_schema(),
            "chunks_selected": integer_schema(),
            "chunks_committed": integer_schema(),
            "chunks_failed": integer_schema(),
            "chunks_skipped_resume": integer_schema(),
            "verification": object_schema(),
            "mapping": airflow_mapping_summary_schema(),
        }
    )


def airflow_runtime_recovery_schema() -> dict[str, object]:
    """Bounded manual-recovery facts for a commit-unknown runtime attempt."""

    return {
        "type": ["object", "null"],
        "additionalProperties": False,
        "required": [
            "code",
            "failure_boundary",
            "target_state",
            "checkpoint_state",
            "source_state",
            "safe_to_retry",
            "operator_verification_required",
            "recovery_action",
        ],
        "properties": {
            "code": {"const": "COMMIT_UNKNOWN"},
            "failure_boundary": {"enum": ["target_invocation", "checkpoint_persistence"]},
            "target_state": {"const": "unknown"},
            "checkpoint_state": {"enum": ["not_advanced", "incomplete"]},
            "source_state": {"const": "not_advanced"},
            "safe_to_retry": {"const": False},
            "operator_verification_required": {"const": True},
            "recovery_action": {"const": "operator_verification_required"},
        },
    }


def airflow_k8s_smoke_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-k8s-smoke",
        kind="gitops.airflow_k8s_smoke",
        title="dpone GitOps Airflow K8s smoke contract",
        required=AIRFLOW_K8S_SMOKE_REQUIRED,
        properties={
            "kind": const_schema("gitops.airflow_k8s_smoke"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "mode": string_schema(),
            "runner_kind": string_schema(),
            "runner_policy": string_schema(),
            "run_spec_path": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_contract_path": string_schema(),
            "image_contract_path": string_schema(),
            "xcom_summary_path": string_schema(),
            "namespace": string_schema(),
            "service_account": string_schema(),
            "image": string_schema(),
            "image_digest": {"type": ["string", "null"]},
            "image_ref": string_schema(),
            "smoke_name": string_schema(),
            "timeout_seconds": integer_schema(),
            "checks": array_schema(airflow_check_schema()),
            "commands": array_schema(airflow_k8s_smoke_command_schema()),
            "results": array_schema(airflow_k8s_smoke_result_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


__all__ = tuple(
    (
        "airflow_doctor_contract airflow_image_contract_contract airflow_k8s_smoke_contract airflow_render_contract "
        "airflow_run_spec_contract airflow_runtime_evidence_contract airflow_runtime_profile_contract airflow_xcom_summary_contract"
    ).split()
)
