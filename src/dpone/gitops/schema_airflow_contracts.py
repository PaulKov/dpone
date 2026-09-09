from __future__ import annotations

from dpone.gitops.schema_airflow_admission_contracts import airflow_admission_check_contract
from dpone.gitops.schema_airflow_artifact_delivery_contracts import airflow_artifact_delivery_schema_contracts
from dpone.gitops.schema_airflow_authoring_contracts import (
    airflow_authoring_fix_contract,
    airflow_authoring_migration_plan_contract,
    selection_schema_contracts,
)
from dpone.gitops.schema_airflow_cluster_doctor_contracts import airflow_cluster_doctor_contract
from dpone.gitops.schema_airflow_connection_bridge_plan_contracts import airflow_connection_bridge_plan_contract
from dpone.gitops.schema_airflow_connection_secret_gc_contracts import (
    airflow_connection_secret_gc_schema_contracts,
)
from dpone.gitops.schema_airflow_correlation_contracts import airflow_correlation_contract
from dpone.gitops.schema_airflow_deployment_attestation_contracts import (
    airflow_deployment_attestation_schema_contracts,
)
from dpone.gitops.schema_airflow_deployment_identity_contracts import (
    airflow_deployment_identity_contract,
)
from dpone.gitops.schema_airflow_desired_state_contracts import airflow_desired_state_schema_contracts
from dpone.gitops.schema_airflow_evidence_bundle_contracts import airflow_evidence_bundle_contract
from dpone.gitops.schema_airflow_k8s_manifest_contracts import airflow_k8s_manifests_contract
from dpone.gitops.schema_airflow_mapping_contracts import airflow_mapping_schema_contracts
from dpone.gitops.schema_airflow_pack_contracts import airflow_pack_contract, airflow_reconcile_contract
from dpone.gitops.schema_airflow_pod_contracts import (
    airflow_outcome_gate_contract,
    airflow_pod_contract_contract,
    airflow_pod_doctor_contract,
)
from dpone.gitops.schema_airflow_pod_launch_evidence_contracts import airflow_pod_launch_evidence_contract
from dpone.gitops.schema_airflow_preflight_contracts import (
    airflow_artifact_index_contract,
    airflow_deployment_index_contract,
    airflow_explain_contract,
    airflow_operator_diagnostics_contract,
    airflow_preflight_contract,
)
from dpone.gitops.schema_airflow_run_identity_contracts import (
    airflow_rerun_plan_contract,
    airflow_run_identity_contract,
)
from dpone.gitops.schema_airflow_runtime_contracts import (
    airflow_doctor_contract,
    airflow_image_contract_contract,
    airflow_k8s_smoke_contract,
    airflow_render_contract,
    airflow_run_spec_contract,
    airflow_runtime_evidence_contract,
    airflow_runtime_profile_contract,
    airflow_xcom_summary_contract,
)
from dpone.gitops.schema_airflow_runtime_init_fetch_contracts import (
    airflow_runtime_init_fetch_schema_contracts,
)
from dpone.gitops.schema_airflow_runtime_pod_retention_contracts import (
    airflow_runtime_pod_retention_schema_contracts,
)
from dpone.gitops.schema_airflow_runtime_ready_v2_contract import runtime_fetch_ready_v2_contract
from dpone.gitops.schema_connection_check_contracts import connection_check_contract, live_preflight_contract
from dpone.gitops.schema_runtime_artifact_trust_policy_contracts import (
    runtime_artifact_attestation_verification_contract,
    runtime_artifact_trust_policy_v2_contract,
)


def airflow_schema_contracts() -> tuple[object, ...]:
    return (
        airflow_render_contract(),
        airflow_doctor_contract(),
        airflow_image_contract_contract(),
        airflow_run_spec_contract(),
        airflow_runtime_evidence_contract(),
        airflow_runtime_profile_contract(),
        airflow_xcom_summary_contract(),
        airflow_correlation_contract(),
        airflow_deployment_identity_contract(),
        *airflow_mapping_schema_contracts(),
        airflow_run_identity_contract(),
        airflow_rerun_plan_contract(),
        *airflow_runtime_init_fetch_schema_contracts(),
        *airflow_runtime_pod_retention_schema_contracts(),
        runtime_artifact_trust_policy_v2_contract(),
        runtime_artifact_attestation_verification_contract(),
        airflow_cluster_doctor_contract(),
        airflow_k8s_manifests_contract(),
        airflow_admission_check_contract(),
        *airflow_artifact_delivery_schema_contracts(),
        *airflow_deployment_attestation_schema_contracts(),
        runtime_fetch_ready_v2_contract(),
        *airflow_desired_state_schema_contracts(),
        airflow_pack_contract(),
        airflow_reconcile_contract(),
        airflow_k8s_smoke_contract(),
        airflow_pod_launch_evidence_contract(),
        airflow_evidence_bundle_contract(),
        airflow_connection_bridge_plan_contract(),
        *airflow_connection_secret_gc_schema_contracts(),
        airflow_artifact_index_contract(),
        airflow_preflight_contract(),
        airflow_deployment_index_contract(),
        airflow_explain_contract(),
        airflow_authoring_migration_plan_contract(),
        airflow_authoring_fix_contract(),
        *selection_schema_contracts(),
        airflow_operator_diagnostics_contract(),
        airflow_outcome_gate_contract(),
        airflow_pod_contract_contract(),
        airflow_pod_doctor_contract(),
        connection_check_contract(),
        live_preflight_contract(),
    )


__all__ = ["airflow_schema_contracts"]
