from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def test_gitops_airflow_user_and_developer_docs_are_self_service() -> None:
    user_doc = (DOCS / "gitops-airflow-runner-pack.md").read_text(encoding="utf-8")
    developer_doc = (DOCS / "developer-gitops-airflow-runner-pack.md").read_text(encoding="utf-8")

    for text in (user_doc, developer_doc):
        assert "dpone gitops airflow render" in text
        assert "dpone gitops airflow run-spec" in text
        assert "dpone gitops airflow runtime-profile" in text
        assert "dpone gitops airflow pod-contract" in text
        assert "dpone gitops airflow pod-doctor" in text
        assert "dpone gitops airflow connection-bridge-plan" in text
        assert "dpone gitops airflow cluster-doctor" in text
        assert "dpone gitops airflow k8s-manifests" in text
        assert "dpone gitops airflow admission-check" in text
        assert "dpone gitops airflow pack" in text
        assert "dpone gitops airflow artifact-index" in text
        assert "dpone gitops airflow preflight" in text
        assert "dpone gitops airflow outcome-gate" in text
        assert "dpone gitops airflow k8s-smoke" in text
        assert "dpone gitops airflow pod-watch" in text
        assert "dpone gitops airflow evidence-bundle" in text
        assert "dpone gitops airflow run-spec-exec" in text
        assert "dpone gitops airflow evidence-verify" in text
        assert "dpone gitops airflow doctor" in text
        assert "dpone gitops airflow image-contract" in text
        assert "--require-attestation" in text
        assert "bundle attestation" in text
        assert "image contract" in text
        assert "custom dpone image" in text
        assert "KubernetesExecutor" in text
        assert "KubernetesPodOperator" in text
        assert "pod_template_file" in text
        assert "base" in text
        assert "executor_config.json" in text
        assert "entrypoint.sh" in text
        assert "run-spec.json" in text
        assert "runtime-profile.json" in text
        assert "pod-contract.json" in text
        assert "pod-spec.yaml" in text
        assert "kpo-kwargs.json" in text
        assert "artifact-index.json" in text
        assert "connection-bridge-plan.json" in text
        assert "airflow-connections-secret.yaml" in text
        assert "airflow-connections-externalsecret.yaml" in text
        assert "airflow-connections.env.example" in text
        assert "xcom-summary.json" in text
        assert "final XCom outcome" in text
        assert "xcom_then_gate" in text
        assert "outcome_gate.py" in text
        assert "/airflow/xcom/return.json" in text
        assert "airflow_dag_factory.py" in text
        assert "build_dpone_gitops_task_from_artifacts" in text
        assert "dpone.airflow.runtime_adapter" in text
        assert "load_dpone_kpo_kwargs" in text
        assert "validate_dpone_artifacts" in text
        assert "next_actions" in text
        assert "stale artifacts" in text
        assert "artifact-loading" in text
        assert "direct KPO" in text
        assert "not the sparse git-sync runtime contract" in text
        assert "--artifact-dir" in text
        assert "runtime-evidence.json" in text
        assert "--runner-policy release" in text
        assert "runner_policy" in text
        assert "git-sync" in text
        assert "initContainer" in text
        assert "--git-sync-repo" in text
        assert "--git-sync-depth" in text
        assert "--git-sync-filter" in text
        assert "blob:none" in text
        assert "git-sync:v4.7.0" in text
        assert "git-sync:v4.4.0" in text
        assert "ssh_secret" in text
        assert "https_secret" in text
        assert "--airflow-connection-bridge" in text
        assert "--airflow-runtime-mode" in text
        assert "AIRFLOW_CONN_" in text
        assert "ExternalSecret" in text
        assert "Kubernetes Secret" in text
        assert "Secret keys" in text
        assert "namespace" in text
        assert "service account" in text
        assert "runtime-only" in text
        assert "connection_bridge" in text
        assert "example-workloads" in text
        assert "manifest discovery" in text
        assert "airflow-pod-contract.schema.json" in text
        assert "airflow-pod-doctor.schema.json" in text
        assert "airflow-artifact-index.schema.json" in text
        assert "airflow-connection-bridge-plan.schema.json" in text
        assert "airflow-cluster-doctor.schema.json" in text
        assert "airflow-k8s-manifests.schema.json" in text
        assert "--gitops-controller" in text
        assert "Argo CD" in text
        assert "Flux" in text
        assert "sync-wave" in text
        assert "airflow-admission-check.schema.json" in text
        assert "airflow-pack.schema.json" in text
        assert "airflow-preflight.schema.json" in text
        assert "airflow-xcom-summary.schema.json" in text
        assert "airflow-outcome-gate.schema.json" in text
        assert "airflow-k8s-smoke.schema.json" in text
        assert "airflow-pod-launch-evidence.schema.json" in text
        assert "airflow-evidence-bundle.schema.json" in text
        assert "airflow-evidence-bundle.json" in text
        assert "airflow-runtime-pack.json" in text
        assert "airflow-doctor.schema.json" in text
        assert "airflow-render.schema.json" in text
        assert "airflow-image-contract.schema.json" in text
        assert "airflow-run-spec.schema.json" in text
        assert "airflow-runtime-evidence.schema.json" in text
        assert "airflow-runtime-profile.schema.json" in text
        assert "Runbook" in text
        assert "opt-in live" in text

    assert "KubernetesPodExecutor" in user_doc
    assert "sparse checkout" in user_doc
    assert "Sparse checkout and partial clone are separate controls" in user_doc
    assert "dpone run" in user_doc
    assert "GitOpsAirflowRenderService" in developer_doc
    assert "GitOpsAirflowRunSpecService" in developer_doc
    assert "GitOpsAirflowRuntimeProfileService" in developer_doc
    assert "GitOpsAirflowRuntimeProfileBuilder" in developer_doc
    assert "GitOpsAirflowRuntimeProfileGitSyncBuilder" in developer_doc
    assert "GitOpsAirflowGitSyncCloneOptions" in developer_doc
    assert "airflow_git_sync_capabilities" in developer_doc
    assert "airflow_pod_doctor_git_sync" in developer_doc
    assert "airflow_connection_bridge" in developer_doc
    assert "airflow_pod_doctor_connection_bridge" in developer_doc
    assert "GitOpsAirflowGitSyncArtifactCollector" in developer_doc
    assert "GitOpsAirflowConnectionBridgeBuilder" in developer_doc
    assert "GitOpsAirflowGitSyncPodPatchBuilder" in developer_doc
    assert "GitOpsAirflowPodContractService" in developer_doc
    assert "GitOpsAirflowPodDoctorService" in developer_doc
    assert "GitOpsAirflowOutcomeGateService" in developer_doc
    assert "GitOpsAirflowK8sSmokeService" in developer_doc
    assert "GitOpsAirflowK8sManifestsService" in developer_doc
    assert "GitOpsAirflowAdmissionCheckService" in developer_doc
    assert "GitOpsAirflowPackService" in developer_doc
    assert "GitOpsAirflowPackPlanner" in developer_doc
    assert "GitOpsAirflowPodLaunchEvidenceService" in developer_doc
    assert "GitOpsAirflowEvidenceBundleService" in developer_doc
    assert "GitOpsAirflowEvidenceBundleCollector" in developer_doc
    assert "GitOpsAirflowPodContractBuilder" in developer_doc
    assert "GitOpsAirflowXComOutcomeBuilder" in developer_doc
    assert "GitOpsAirflowOutcomeGateEvaluator" in developer_doc
    assert "ArtifactSink" in developer_doc
    assert "GitOpsAirflowEvidenceVerifyService" in developer_doc
    assert "GitOpsAirflowDoctorService" in developer_doc
    assert "GitOpsAirflowImageContractService" in developer_doc
    assert "Do not import Airflow" in developer_doc
    assert "CommandRunner" in developer_doc
    assert "AirflowK8sSmokeRunner" in developer_doc
    assert "AirflowAdmissionCheckRunner" in developer_doc
    assert "AirflowPodLaunchEvidenceRunner" in developer_doc


def test_gitops_airflow_docs_are_linked_from_nav_architecture_ci_and_cli_reference() -> None:
    mkdocs = (ROOT / "mkdocs.yml").read_text(encoding="utf-8")
    index = (DOCS / "README.md").read_text(encoding="utf-8")
    architecture = (DOCS / "architecture.md").read_text(encoding="utf-8")
    cicd = (DOCS / "ci-cd.md").read_text(encoding="utf-8")
    cli_reference = (DOCS / "cli-reference.md").read_text(encoding="utf-8")

    assert "GitOps Airflow runner pack: gitops-airflow-runner-pack.md" in mkdocs
    assert "Developer GitOps Airflow runner pack: developer-gitops-airflow-runner-pack.md" in mkdocs
    assert "gitops-airflow-runner-pack.md" in index
    assert "developer-gitops-airflow-runner-pack.md" in index
    assert "GitOps Airflow runner pack" in architecture
    assert "GitOps Airflow runner pack" in cicd
    assert "dpone gitops airflow render" in cli_reference
    assert "dpone gitops airflow run-spec" in cli_reference
    assert "dpone gitops airflow runtime-profile" in cli_reference
    assert "dpone gitops airflow pod-contract" in cli_reference
    assert "--gitops-controller" in cli_reference
    assert "dpone gitops airflow pod-doctor" in cli_reference
    assert "dpone gitops airflow connection-bridge-plan" in cli_reference
    assert "dpone gitops airflow cluster-doctor" in cli_reference
    assert "dpone gitops airflow k8s-manifests" in cli_reference
    assert "dpone gitops airflow admission-check" in cli_reference
    assert "dpone gitops airflow pack" in cli_reference
    assert "dpone gitops airflow artifact-index" in cli_reference
    assert "dpone gitops airflow preflight" in cli_reference
    assert "dpone gitops airflow outcome-gate" in cli_reference
    assert "dpone gitops airflow k8s-smoke" in cli_reference
    assert "dpone gitops airflow pod-watch" in cli_reference
    assert "dpone gitops airflow evidence-bundle" in cli_reference
    assert "dpone gitops airflow run-spec-exec" in cli_reference
    assert "dpone gitops airflow evidence-verify" in cli_reference
    assert "dpone gitops airflow doctor" in cli_reference
    assert "dpone gitops airflow image-contract" in cli_reference
    assert "--pod-template" in cli_reference
    assert "--artifact-dir" in cli_reference
    assert "--artifact-index-path" in cli_reference
    assert "--pod-spec-path" in cli_reference
    assert "--kpo-kwargs-path" in cli_reference
    assert "--xcom-output" in cli_reference
    assert "--outcome-mode" in cli_reference
    assert "--required-status" in cli_reference
    assert "--runner-kind" in cli_reference
    assert "--mode" in cli_reference
    assert "--smoke-name" in cli_reference
    assert "--expected-phase" in cli_reference
    assert "--log-tail-lines" in cli_reference
    assert "--image-contract" in cli_reference
    assert "--runner-policy" in cli_reference
    assert "--git-sync-repo" in cli_reference
    assert "--git-sync-image" in cli_reference
    assert "--git-sync-depth" in cli_reference
    assert "--git-sync-filter" in cli_reference
    assert "--git-sync-auth-mode" in cli_reference
    assert "--airflow-connection-bridge" in cli_reference
    assert "--airflow-connection-secret" in cli_reference
    assert "--airflow-runtime-mode" in cli_reference
    assert "--secret-manifest-path" in cli_reference
    assert "--external-secret-path" in cli_reference
    assert "--env-example-path" in cli_reference
    assert "--external-secret-store" in cli_reference
    assert "--require-external-secret-ready" in cli_reference
    assert "--connection-bridge-plan-path" in cli_reference
    assert "--manifest-output" in cli_reference
    assert "--manifest-path" in cli_reference

    for schema_name in (
        "airflow-render",
        "airflow-doctor",
        "airflow-image-contract",
        "airflow-run-spec",
        "airflow-runtime-evidence",
        "airflow-runtime-profile",
        "airflow-xcom-summary",
        "airflow-outcome-gate",
        "airflow-pod-contract",
        "airflow-pod-doctor",
        "airflow-connection-bridge-plan",
        "airflow-cluster-doctor",
        "airflow-k8s-manifests",
        "airflow-admission-check",
        "airflow-pack",
        "airflow-dag-spec",
        "airflow-artifact-index",
        "airflow-preflight",
        "airflow-k8s-smoke",
        "airflow-pod-launch-evidence",
        "airflow-evidence-bundle",
    ):
        assert (DOCS / "schemas" / "gitops" / f"{schema_name}.schema.json").exists()
