"""Actual dbt-report to V2 pre-release proof compilation tests."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.adapters.dbt_semantic_refresh_sql_proof import (
    SqlglotSemanticRefreshSqlProof,
)
from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    CompiledDbtWorkflow,
    DbtColumnArtifact,
    DbtCompileReport,
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishProfile,
    DbtWorkflowProfile,
)
from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    SemanticRefreshCatalogProofReceipt,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
    SemanticRefreshCertificationRequest,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SemanticRefreshLifecycleReport,
)
from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableRole,
)
from dpone.contracts.semantic_refresh_lifecycle_policy import SemanticRefreshSqlServerLifecyclePolicy
from dpone.contracts.semantic_refresh_mutation_closure import SemanticRefreshMutationClosure
from dpone.contracts.semantic_refresh_profile import SemanticRefreshProfilePolicy
from dpone.contracts.semantic_refresh_read_dependency import SemanticRefreshReadDependencyProof
from dpone.contracts.semantic_refresh_types import ClosureStatus
from dpone.readiness.dbt_semantic_refresh_pre_release_compiler import (
    SemanticRefreshPreReleaseCompileError,
    SemanticRefreshPreReleaseCompiler,
    SemanticRefreshPreReleaseCompileRequest,
)

_MODEL_ID = "model.analytics.events"
_DIGESTS = tuple("sha256:" + character * 64 for character in "123456789abcdef")
_SQL = "select event_date, event_id, payload from DWH.raw.events"


def test_pre_release_compiler_derives_key_event_schema_from_actual_dbt_report() -> None:
    request = _request()

    bundle = SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(request)

    model = bundle.models[0]
    assert model.event_time_column == "event_date"
    assert tuple(item.name for item in model.effective_key_templates) == ("event_date", "event_id")
    assert all(item.source_type != "datetime2(6)" for item in model.effective_key_templates)
    assert model.effective_key_template_sha256.startswith("sha256:")
    assert not hasattr(model, "effective_key_mapping_sha256")
    assert tuple(item.writable_role for item in model.writable_columns) == (
        SemanticRefreshWritableRole.EFFECTIVE_KEY_EVENT_TIME,
        SemanticRefreshWritableRole.MUTABLE_VALUE,
        SemanticRefreshWritableRole.EFFECTIVE_KEY,
        SemanticRefreshWritableRole.MUTABLE_VALUE,
    )
    assert model.writable_columns[3].target_type == "Nullable(String)"
    assert model.read_dependency_proof == request.catalog_proofs[0].dependency_proof
    assert "release_id" not in bundle.to_dict()
    assert "deployment_id" not in bundle.to_dict()


def test_datetime_event_key_emits_only_a_utc_assurance_template_requirement() -> None:
    bundle = SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(
        _request(event_time_type="datetime2(6)")
    )

    event_time = bundle.models[0].effective_key_templates[0]
    assert event_time.source_type == "datetime2(6)"
    assert event_time.utc_assurance_required is True
    assert not hasattr(bundle.models[0], "effective_key_mapping_sha256")
    assert "runtime_assurance_receipt_sha256" not in bundle.to_dict()


@pytest.mark.parametrize(
    "unique_key",
    [
        ("event_id",),
        ("event_date", "processed_date", "event_id"),
    ],
)
def test_zero_or_multiple_temporal_dbt_unique_keys_fail_closed(unique_key: tuple[str, ...]) -> None:
    request = _request(unique_key=unique_key)

    with pytest.raises(SemanticRefreshPreReleaseCompileError) as raised:
        SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(request)

    assert raised.value.code == "DPONE_DBT_V2_EVENT_TIME_INVALID"


def test_author_event_time_knob_is_rejected_even_when_standard_unique_key_is_valid() -> None:
    request = _request()
    compiled = request.report.models[0]
    authored_model = replace(compiled.model, meta={"event_time": "event_date"})
    authored = replace(compiled, model=authored_model)
    workflow = replace(request.report.workflows[0], models=(authored,))
    report = replace(request.report, models=(authored,), workflows=(workflow,))

    with pytest.raises(SemanticRefreshPreReleaseCompileError) as raised:
        SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(
            replace(request, report=report)
        )

    assert raised.value.code == "DPONE_DBT_V2_EFFECTIVE_KEY_INVALID"


def test_profile_selected_v2_rejects_incomplete_transitive_macro_closure() -> None:
    request = _request()
    compiled = request.report.models[0]
    unresolved_model = replace(
        compiled.model,
        incremental_strategy=None,
        semantic_refresh_macro_closure_complete=False,
    )
    unresolved = replace(compiled, model=unresolved_model)
    workflow = replace(request.report.workflows[0], models=(unresolved,))
    report = replace(request.report, models=(unresolved,), workflows=(workflow,))

    with pytest.raises(SemanticRefreshPreReleaseCompileError) as raised:
        SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(
            replace(request, report=report)
        )

    assert raised.value.code == "DPONE_DBT_V2_MACRO_CLOSURE_UNVERIFIED"


def test_pre_release_rejects_lifecycle_report_for_a_different_policy() -> None:
    request = _request()
    lifecycle_report = SemanticRefreshLifecycleReport.build(
        status="PROVEN",
        lifecycle_policy_sha256=_DIGESTS[12],
        lifecycle_authority_sha256=request.lifecycle_report.lifecycle_authority_sha256,
        certification_coordinate_sha256=request.lifecycle_report.certification_coordinate_sha256,
        issues=(),
    )

    with pytest.raises(SemanticRefreshPreReleaseCompileError) as raised:
        SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(
            replace(request, lifecycle_report=lifecycle_report)
        )

    assert raised.value.code == "DPONE_DBT_V2_LIFECYCLE_UNVERIFIED"


def _request(
    *,
    unique_key: tuple[str, ...] = ("event_date", "event_id"),
    event_time_type: str = "date",
) -> SemanticRefreshPreReleaseCompileRequest:
    columns = (
        DbtColumnArtifact("event_date", event_time_type, False, ("not_null",)),
        DbtColumnArtifact("processed_date", "date", False, ("not_null",)),
        DbtColumnArtifact("event_id", "bigint", False, ("not_null",)),
        DbtColumnArtifact("payload", "nvarchar(200)", True),
    )
    model = DbtModelArtifact(
        unique_id=_MODEL_ID,
        name="events",
        original_file_path="models/events.sql",
        database="DWH",
        schema="mart",
        alias="events",
        materialized="incremental",
        contract_enforced=True,
        columns=tuple(item.name for item in columns),
        column_contracts=columns,
        group="analytics",
        tags=(),
        meta={},
        unique_key=unique_key,
        depends_on=("source.analytics.events",),
        fqn=("analytics", "events"),
        raw_code="select event_date, event_id, payload from {{ source('raw', 'events') }}",
        compiled_code_by_target={"certified_a": _SQL, "certified_b": _SQL},
    )
    profile = _profile()
    intent = DbtPublishIntent(True, "semantic", "daily_events")
    compiled = CompiledDbtModel(
        model=model,
        intent=intent,
        profile=profile,
        strategy={"mode": "semantic_refresh_v2"},
        physical_design={},
        workload_id="dbt__daily_events__events",
        manifest={"mode": "semantic_refresh_v2"},
        route_capability={
            "status": "CERTIFIED",
            "certification_receipt_sha256": _DIGESTS[9],
        },
    )
    workflow = CompiledDbtWorkflow(
        "daily_events",
        DbtWorkflowProfile("daily_events", "@daily", "2026-01-01", "UTC", "data", ()),
        (compiled,),
        "DAG__analytics__daily_events",
    )
    report = DbtCompileReport(
        manifest_path="target/manifest.json",
        manifest_schema_version=12,
        manifest_sha256=_DIGESTS[0],
        dbt_version="1.12.3",
        dbt_adapter="sqlserver",
        dbt_adapter_version="1.11.1",
        models=(compiled,),
        workflows=(workflow,),
    )
    sql_digest = (
        SqlglotSemanticRefreshSqlProof()
        .prove(
            dict(model.compiled_code_by_target),
            forbidden_relations=(("DWH", "mart", "events"),),
        )
        .compiled_sql_sha256
    )
    assert sql_digest is not None
    dependency = SemanticRefreshReadDependencyProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=_MODEL_ID,
        database_name="DWH",
        compiled_sql_sha256=sql_digest,
        catalog_snapshot_sha256=_DIGESTS[1],
        normalized_definitions_sha256=_DIGESTS[2],
        policy_sha256=_DIGESTS[3],
        dependency_edges=(),
        base_relation_object_ids=(101,),
        max_depth=4,
        max_nodes=20,
        max_edges=40,
        max_definition_bytes=100_000,
    )
    catalog = SemanticRefreshCatalogProofReceipt.build(
        model_unique_id=_MODEL_ID,
        catalog_authority_sha256=_DIGESTS[4],
        catalog_observation_sha256=_DIGESTS[5],
        dependency_proof=dependency,
    )
    certification_request = SemanticRefreshCertificationRequest.build(
        certification_coordinate_sha256=_DIGESTS[6],
        manifest_sha256=_DIGESTS[0],
        profile_sha256=_semantic_profile(profile).profile_sha256,
        toolchain_sha256=_DIGESTS[7],
        verification_time="2026-08-08T00:00:00Z",
    )
    certification_decision = SemanticRefreshCertificationDecision(
        certification_request.request_sha256,
        "CERTIFIED",
        _DIGESTS[9],
        "2026-08-01T00:00:00Z",
        "2026-09-01T00:00:00Z",
    )
    lifecycle = _lifecycle()
    return SemanticRefreshPreReleaseCompileRequest(
        report=report,
        workflow_name="daily_events",
        environment="prod",
        mutation_closure=SemanticRefreshMutationClosure.build(
            status=ClosureStatus.PROVEN,
            selectors=("fqn:analytics.events",),
            selected_node_ids=(_MODEL_ID,),
            selected_mutating_node_ids=(_MODEL_ID,),
            selected_read_only_node_ids=(),
            unclassified_mutating_node_ids=(),
        ),
        lifecycle_policy=lifecycle,
        lifecycle_report=SemanticRefreshLifecycleReport.build(
            status="PROVEN",
            lifecycle_policy_sha256=lifecycle.sqlserver_lifecycle_policy_sha256,
            lifecycle_authority_sha256=_DIGESTS[10],
            certification_coordinate_sha256=certification_request.certification_coordinate_sha256,
            issues=(),
        ),
        certification_request=certification_request,
        certification_decision=certification_decision,
        platform_policy_digest=_DIGESTS[10],
        resource_policy=_resource_policy(),
        catalog_proofs=(catalog,),
    )


def _profile() -> DbtPublishProfile:
    semantic = SemanticRefreshProfilePolicy.from_mapping(
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
    return DbtPublishProfile(
        name="semantic",
        source_type="mssql",
        source_connection_ref="mssql-prod",
        sink_type="clickhouse",
        sink_connection_ref="clickhouse-prod",
        target_schema="mart",
        staging_schema=None,
        runtime_image="registry/dpone@sha256:" + "a" * 64,
        semantic_refresh=semantic,
    )


def _lifecycle() -> SemanticRefreshSqlServerLifecyclePolicy:
    return SemanticRefreshSqlServerLifecyclePolicy.build(
        python_version="3.12.12",
        runtime_image_digest=_DIGESTS[0],
        pyodbc_version="5.2.0",
        odbc_driver="ODBC Driver 18 for SQL Server",
        sqlserver_version="16.0.1000.6",
        compatibility_level=160,
        macro_closure_sha256=_DIGESTS[1],
        adapter_policy_digest=_DIGESTS[2],
        project_policy_digest=_DIGESTS[3],
        profile_policy_digest=_DIGESTS[4],
        invocation_policy_digest=_DIGESTS[5],
        package_artifacts_digest=_DIGESTS[6],
        materialization_closure_digest=_DIGESTS[7],
        dispatch_closure_digest=_DIGESTS[8],
        driver_digest=_DIGESTS[9],
    )


def _semantic_profile(profile: DbtPublishProfile) -> SemanticRefreshProfilePolicy:
    assert profile.semantic_refresh is not None
    return profile.semantic_refresh


def _resource_policy() -> SemanticRefreshResourcePolicy:
    return SemanticRefreshResourcePolicy(
        max_source_scope_rows=1_000_000,
        max_target_scope_rows=1_000_000,
        max_before_image_rows=1_000_000,
        max_before_image_bytes=1_000_000_000,
        max_after_image_rows=1_000_000,
        max_after_image_bytes=1_000_000_000,
        max_transaction_log_bytes=2_000_000_000,
        min_mssql_log_free_bytes=4_000_000_000,
        max_mssql_scope_lock_seconds=300,
        max_mssql_version_store_bytes=1_000_000_000,
        max_scope_image_total_bytes=2_000_000_000,
        max_dbt_temp_rows=1_000_000,
        max_dbt_temp_bytes=2_000_000_000,
        max_statement_seconds=600,
        max_clickhouse_staging_bytes=2_000_000_000,
        max_clickhouse_shadow_bytes=4_000_000_000,
        max_clickhouse_retained_backup_bytes=4_000_000_000,
        max_clickhouse_total_transient_bytes=10_000_000_000,
    )
