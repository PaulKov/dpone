"""Shared DI capabilities retain public context reflection and port aliases."""

from importlib import import_module
from types import SimpleNamespace
from typing import get_type_hints

import pytest

from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.context import GitOpsFileContext, GitOpsYamlContext

CONTEXTS = (
    ("airflow_admission_check_service", "GitOpsAirflowAdmissionCheckContext", False),
    ("airflow_artifact_index_service", "GitOpsAirflowArtifactIndexContext", True),
    ("airflow_cluster_doctor_service", "GitOpsAirflowClusterDoctorContext", False),
    ("airflow_compact_pack_service", "GitOpsAirflowCompactPackContext", False),
    ("airflow_connection_bridge_plan_preflight", "GitOpsAirflowConnectionBridgePlanPreflightContext", False),
    ("airflow_connection_bridge_plan_service", "GitOpsAirflowConnectionBridgePlanContext", False),
    ("airflow_doctor_service", "GitOpsAirflowDoctorContext", True),
    ("airflow_evidence_bundle_service", "GitOpsAirflowEvidenceBundleContext", False),
    ("airflow_image_contract_service", "GitOpsAirflowImageContractContext", False),
    ("airflow_k8s_smoke_service", "GitOpsAirflowK8sSmokeContext", False),
    ("airflow_outcome_gate_service", "GitOpsAirflowOutcomeGateContext", False),
    ("airflow_pack_service", "GitOpsAirflowPackContext", False),
    ("airflow_pod_contract_service", "GitOpsAirflowPodContractContext", True),
    ("airflow_pod_doctor_service", "GitOpsAirflowPodDoctorContext", True),
    ("airflow_pod_launch_evidence_service", "GitOpsAirflowPodLaunchEvidenceContext", False),
    ("airflow_preflight_service", "GitOpsAirflowPreflightContext", True),
    ("airflow_render_service", "GitOpsAirflowRenderContext", True),
    ("airflow_runtime_exec_service", "GitOpsAirflowRuntimeExecContext", False),
    ("airflow_runtime_profile_service", "GitOpsAirflowRuntimeProfileContext", True),
    ("airflow_runtime_service", "GitOpsAirflowRuntimeContext", False),
    ("bundle_service", "GitOpsBundleContext", True),
    ("bundle_verify_service", "GitOpsBundleVerifyContext", False),
    ("gitlab_child_pipeline_service", "GitOpsGitLabChildPipelineContext", False),
    ("verify_service", "GitOpsVerifyContext", False),
    ("workload_catalog_service", "GitOpsWorkloadCatalogContext", False),
)


@pytest.mark.parametrize(("module_name", "context_name", "yaml"), CONTEXTS)
def test_service_context_preserves_canonical_reflection_and_exports(module_name, context_name, yaml):
    module = import_module(f"dpone.services.gitops.{module_name}")
    context = getattr(module, context_name)
    hints = get_type_hints(context)
    assert (context_name in module.__all__) == (module_name != "airflow_connection_bridge_plan_preflight")
    assert context._is_protocol
    assert module.FileSystem is FileSystem
    assert hints["fs"] is FileSystem
    expected = {"fs"}
    if yaml:
        assert module.YamlCodec is YamlCodec
        assert hints["yaml"] is YamlCodec
        assert GitOpsYamlContext in context.__mro__
        expected.add("yaml")
    else:
        assert "yaml" not in hints
        assert GitOpsFileContext in context.__mro__
    if module_name != "airflow_connection_bridge_plan_preflight":
        expected.add("settings")
        assert hints["settings"] is module._GitOpsSettings
    assert set(hints) == expected


def test_shared_contexts_require_only_their_declared_injected_capabilities():
    assert get_type_hints(GitOpsFileContext) == {"fs": FileSystem}
    assert get_type_hints(GitOpsYamlContext) == {"fs": FileSystem, "yaml": YamlCodec}
    # Service inputs remain structural: no concrete application context or
    # adapter is created by inheriting the capability protocol.
    fs, codec = object(), object()
    injected = SimpleNamespace(fs=fs, yaml=codec)
    from dpone.services.gitops.airflow_runtime_profile_service import GitOpsAirflowRuntimeProfileService

    service = GitOpsAirflowRuntimeProfileService(ctx=injected)
    assert service._ctx.fs is fs
    assert service._ctx.yaml is codec
