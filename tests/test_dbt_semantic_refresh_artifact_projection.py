from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest
from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

from dpone.contracts.dbt_contract_validation import DbtPublishingError, canonical_fingerprint
from dpone.contracts.dbt_invocation import DbtInvocationContext
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    CompiledDbtWorkflow,
    DbtCompileReport,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishProfile,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_publishing import (
    DbtExecutionPack,
    DbtProfileSpec,
    DbtSelectionLock,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
)
from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy
from dpone.readiness.dbt_airflow_artifact_projection import (
    route_certifications,
    semantic_refresh_topology_template,
    workload_definition,
)
from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
    SemanticRefreshTemplateProofAuthority,
    semantic_refresh_template_pack,
)
from dpone.services.dbt_project_artifacts import DbtProjectArtifactProjector
from dpone.services.dbt_project_runtime_payloads import DbtProjectRuntimePayloads
from tests.test_dbt_semantic_refresh_plan_compiler import _pre_release

DIGEST = "sha256:" + "a" * 64


def test_semantic_topology_is_deployment_neutral_and_dependency_ordered() -> None:
    parent = _compiled("model.analytics.parent", "parent", ())
    child = _compiled("model.analytics.child", "child", (parent.model.unique_id,))
    workflow = CompiledDbtWorkflow(
        workflow="daily",
        profile=DbtWorkflowProfile("daily", None, "2026-01-01", "UTC", "data", ()),
        models=(child, parent),
        dag_id="DAG__analytics__daily__refresh",
    )

    template = semantic_refresh_topology_template(workflow)

    assert template["schema"] == "dpone.dbt-semantic-refresh-topology-template.v1"
    assert template["model_unique_ids"] == [
        "model.analytics.parent",
        "model.analytics.child",
    ]
    assert template["dependencies"] == {
        "model.analytics.child": ["model.analytics.parent"],
        "model.analytics.parent": [],
    }
    assert template["logical_output_asset_uris"] == {
        "model.analytics.child": "clickhouse://mart/child",
        "model.analytics.parent": "clickhouse://mart/parent",
    }
    assert template["dag_policy"] == {
        "catchup": False,
        "max_active_runs": 1,
        "max_active_tasks": 2,
        "owner": "data",
        "schedule": None,
        "start_date": "2026-01-01",
        "tags": ["dbt", "dpone", "semantic-refresh-v2"],
        "timezone": "UTC",
    }
    serialized = str(template)
    assert all(
        forbidden not in serialized
        for forbidden in (
            "release_id",
            "deployment_id",
            "operation_id",
            "workflow_execution_binding",
            "attempt_binding",
            "fencing_epoch",
        )
    )
    assert str(template["topology_sha256"]).startswith("sha256:")


def test_semantic_topology_binds_and_requires_explicit_dag_policy() -> None:
    model = _compiled("model.analytics.events", "events", ())
    base_profile = DbtWorkflowProfile(
        "daily",
        "0 0 * * *",
        "2026-01-01",
        "UTC",
        "data",
        ("finance",),
    )
    first = CompiledDbtWorkflow(
        workflow="daily",
        profile=base_profile,
        models=(model,),
        dag_id="DAG__analytics__daily__refresh",
    )
    second = replace(first, profile=replace(base_profile, max_active_runs=2))

    assert (
        semantic_refresh_topology_template(first)["topology_sha256"]
        != (semantic_refresh_topology_template(second)["topology_sha256"])
    )
    with pytest.raises(DbtPublishingError, match="DAG policy must be explicit"):
        semantic_refresh_topology_template(replace(first, profile=replace(base_profile, start_date="")))


def test_semantic_model_cannot_cross_generic_workload_projection_boundary() -> None:
    with pytest.raises(DbtPublishingError, match="dedicated post-deployment projection") as exc:
        workload_definition(_compiled("model.analytics.events", "events", ()), "manifest.yaml")

    assert exc.value.code == "DPONE_DBT_V2_GENERIC_PACK_FORBIDDEN"


def test_semantic_route_projection_binds_exact_protected_receipt() -> None:
    model = _compiled("model.analytics.events", "events", ())
    report = DbtCompileReport("manifest.json", 12, models=(model,))

    assert route_certifications(report) == [
        {
            "certification_level": "exact_live",
            "evidence_refs": ["sha256:" + "b" * 64],
            "evidence_status": "certified",
            "route_id": "semantic_refresh_v2",
            "variant_id": DIGEST,
        }
    ]


def test_semantic_explain_does_not_infer_proofs_from_route_certification() -> None:
    semantic = _compiled("model.analytics.events", "events", ()).to_jsonable()["semantic_refresh"]

    assert semantic["static_dependency_closure"] == "UNVERIFIED"
    assert semantic["adapter_lifecycle_policy"] == "UNVERIFIED"


def test_semantic_route_projection_rejects_unverified_capability() -> None:
    model = replace(
        _compiled("model.analytics.events", "events", ()),
        route_capability={"status": "UNVERIFIED"},
    )

    with pytest.raises(DbtPublishingError) as exc:
        route_certifications(DbtCompileReport("manifest.json", 12, models=(model,)))

    assert exc.value.code == "DPONE_DBT_V2_LIVE_UNVERIFIED"


def test_semantic_template_pack_is_fingerprinted_but_not_executable() -> None:
    model = _compiled("model.analytics.events", "events", ())
    workflow = CompiledDbtWorkflow(
        workflow="daily",
        profile=DbtWorkflowProfile("daily", None, "2026-01-01", "UTC", "data", ()),
        models=(model,),
        dag_id="DAG__analytics__daily__refresh",
    )
    pack = semantic_refresh_template_pack(
        workflow_id="daily",
        execution_pack=_execution_pack(),
        runtime_payload_ids=("dbt_project", "dbt_manifest", "dbt_selection_daily"),
        topology=semantic_refresh_topology_template(workflow),
        proof_authority=_proof_authority(),
    )

    assert verify_pack_fingerprint(pack) == pack["pack_fingerprint"]
    assert pack["executable"] is False
    assert pack["activation"] == "POST_DEPLOYMENT_AUTHORITY_REQUIRED"
    assert pack["semantic_refresh"]["mode"] == "semantic_refresh_v2_template"
    assert pack["semantic_refresh"]["pre_release_bundle_sha256"] == DIGEST
    assert pack["semantic_refresh"]["package_artifacts_sha256"] == DIGEST
    assert "provider_execution" not in pack
    assert "kpo_kwargs" not in pack


def test_semantic_template_does_not_silently_accept_workspace_execution_pack() -> None:
    from dpone.readiness.dbt_semantic_refresh_airflow_validation import validate_pre_release_template

    raw = _execution_pack().to_dict()
    raw["schema"] = "dpone.dbt-execution-pack.v2"
    raw["invocation_target"] = {"database": "DWH", "schema": "base"}
    raw["pack_sha256"] = canonical_fingerprint({key: value for key, value in raw.items() if key != "pack_sha256"})
    with pytest.raises(DbtPublishingError, match="wire version"):
        validate_pre_release_template(DbtExecutionPack.from_mapping(raw), {}, _proof_authority())


def test_projector_emits_only_dedicated_non_executable_v2_pack(monkeypatch, tmp_path) -> None:
    model = _compiled("model.analytics.events", "events", ())
    workflow = CompiledDbtWorkflow(
        workflow="daily",
        profile=DbtWorkflowProfile("daily", None, "2026-01-01", "UTC", "data", ()),
        models=(model,),
        dag_id="DAG__analytics__daily__refresh",
    )
    report = DbtCompileReport(
        manifest_path="manifest.json",
        manifest_schema_version=12,
        manifest_sha256=DIGEST,
        dbt_version="1.12.3",
        dbt_adapter="sqlserver",
        dbt_adapter_version="1.11.1",
        models=(model,),
        workflows=(workflow,),
    )
    from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2

    # Workspace rejection precedes source capture or selection; the standalone
    # semantic template wire is deliberately not a workspace runtime release.
    with pytest.raises(DbtPublishingError, match="non-executable semantic-refresh"):
        DbtProjectArtifactProjector(
            selection_resolver=object(),
            bundle_operations=object(),
            project_policy=object(),
        ).project(report, project_root=tmp_path, wire_contract=DBT_RUNTIME_WIRE_V2)
    execution_pack = SimpleNamespace(selection_lock=SimpleNamespace(manifest_sha256=DIGEST))
    release_inputs = SimpleNamespace(
        execution_packs={"daily": execution_pack},
        selection_locks={"daily": SimpleNamespace(selection_sha256=DIGEST)},
        project_sha256=DIGEST,
        selection_authority="dbt_cli",
    )
    monkeypatch.setattr(
        "dpone.services.dbt_project_artifacts.build_release_inputs",
        lambda *_args, **_kwargs: release_inputs,
    )
    monkeypatch.setattr(
        "dpone.services.dbt_project_artifacts.semantic_refresh_template_pack",
        lambda **_kwargs: {"kind": "semantic_refresh_v2_template"},
    )
    monkeypatch.setattr(
        "dpone.services.dbt_project_artifacts.project_runtime_payloads",
        lambda _inputs, **_kwargs: DbtProjectRuntimePayloads(
            {},
            (),
            {"daily": ("dbt_project", "dbt_manifest", "dbt_selection_daily")},
        ),
    )
    projector = DbtProjectArtifactProjector(
        selection_resolver=object(),
        bundle_operations=object(),
        project_policy=object(),
        pack_builder=_ForbiddenBuilder(),
        dbt_pack_builder=_ForbiddenBuilder(),
    )

    proof = replace(_pre_release(), workflow_name="daily")
    proof_payload = proof.to_dict()
    proof_payload.pop("pre_release_bundle_sha256")
    proof = replace(proof, pre_release_bundle_sha256=canonical_fingerprint(proof_payload))
    project = projector.project(
        report,
        project_root=tmp_path,
        semantic_refresh_pre_release_bundles={"daily": proof},
    )

    files, artifacts = project.files, project.artifacts
    assert "semantic__daily/airflow-pack.json" in files
    assert artifacts["pack:semantic__daily"] == "semantic__daily/airflow-pack.json"
    assert not any(key.startswith("manifest:") for key in artifacts)
    assert not any(key.startswith("dag:") for key in artifacts)
    assert b'"semantic_refresh_v2_template"' in files["semantic__daily/airflow-pack.json"]


class _ForbiddenBuilder:
    def build(self, **_kwargs):
        raise AssertionError("semantic refresh must not invoke a generic pack builder")


def _execution_pack() -> DbtExecutionPack:
    invocation = DbtInvocationContext.canonical()
    lock = DbtSelectionLock.build(
        manifest_sha256=DIGEST,
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        invocation_context_sha256=invocation.invocation_context_sha256,
        graph_contract_sha256=DIGEST,
        graph_policy_id=DBT_SQLSERVER_GRAPH_POLICY_ID,
        graph_policy_sha256=DBT_SQLSERVER_GRAPH_POLICY_SHA256,
        selectors=("fqn:analytics.events",),
        selected_graph_unique_ids=("model.analytics.events",),
        expected_run_result_unique_ids=("model.analytics.events",),
        publish_model_unique_ids=("model.analytics.events",),
    )
    return DbtExecutionPack.build(
        workflow_id="daily",
        project_bundle_sha256=DIGEST,
        project_subdir="dbt-project",
        target_path="target",
        profile=DbtProfileSpec(
            profile_name="dpone_runtime",
            target_name="runtime",
            connection_ref="mssql_prod",
            adapter_type="sqlserver",
            database="DWH",
            schema="mart",
            threads=4,
        ),
        selection_lock=lock,
        invocation_context=invocation,
        adapter_runtime=DbtSqlServerRuntimePolicy.for_process_timeout(900),
        adapter_policy=DbtSqlServerAdapterPolicy.canonical(),
        dbt_warning_policy="fail",
        timeout_seconds=900,
    )


def _proof_authority() -> SemanticRefreshTemplateProofAuthority:
    return SemanticRefreshTemplateProofAuthority(
        workflow_name="daily",
        manifest_sha256=DIGEST,
        profile_sha256=_semantic_profile().profile_sha256,
        toolchain_sha256=DBT_SQLSERVER_1_12_CERTIFIED.sha256,
        model_unique_ids=("model.analytics.events",),
        pre_release_bundle_sha256=DIGEST,
        package_artifacts_sha256=DIGEST,
    )


def _compiled(unique_id: str, alias: str, depends_on: tuple[str, ...]) -> CompiledDbtModel:
    model = DbtModelArtifact(
        unique_id=unique_id,
        name=alias,
        original_file_path=f"models/{alias}.sql",
        database="DWH",
        schema="mart",
        alias=alias,
        materialized="incremental",
        contract_enforced=True,
        columns=("event_id",),
        column_contracts=(),
        group="analytics",
        tags=(),
        meta={},
        unique_key=("event_id",),
        depends_on=depends_on,
        fqn=("analytics", alias),
    )
    profile = DbtPublishProfile(
        name="semantic",
        source_type="mssql",
        source_connection_ref="mssql_prod",
        sink_type="clickhouse",
        sink_connection_ref="clickhouse_prod",
        target_schema="mart",
        staging_schema="staging",
        runtime_image=DIGEST,
        semantic_refresh=_semantic_profile(),
    )
    return CompiledDbtModel(
        model=model,
        intent=DbtPublishIntent(True, "semantic", "daily"),
        profile=profile,
        strategy={
            "mode": "semantic_refresh_v2",
            "project_config_overlay": {
                "schema": "dpone.dbt-semantic-refresh-project-overlay.v1",
                "models": {},
                "project_overlay_sha256": DIGEST,
            },
        },
        physical_design={},
        workload_id=f"dbt_{alias}",
        manifest={"schema": "dpone.dbt-semantic-refresh-model-template.v1"},
        route_capability={
            "capability": "scope_stable_event_fact",
            "status": "CERTIFIED",
            "certification_coordinate_sha256": DIGEST,
            "certification_receipt_sha256": "sha256:" + "b" * 64,
        },
    )


def _semantic_profile() -> SemanticRefreshProfilePolicy:
    return SemanticRefreshProfilePolicy.from_mapping(
        {
            "schema": "dpone.semantic-refresh-profile.v1",
            "enabled": True,
            "capability": "scope_stable_event_fact",
            "scope": {"grain": "day", "timezone": "UTC", "interval": "half_open"},
            "mutation": {"protocol": "update_insert_v1", "deletes": "ignore_missing"},
            "initial_load": "require_existing_complete_relation",
            "concurrency": "exclusive_workflow",
            "source_snapshot": "snapshot",
            "publication": {
                "database_engine": "Atomic",
                "table_engine": "MergeTree",
                "replica_count": 1,
                "strategy": "full_table_exchange",
            },
            "workflow_publish_atomicity": "none",
            "automatic_sql_retry": False,
        }
    )
