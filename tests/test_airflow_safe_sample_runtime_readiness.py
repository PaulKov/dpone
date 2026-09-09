from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder
from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    SourceSamplingCapabilityDetector,
    TemporaryTargetPlan,
)


def _target_plan() -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_dev",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
        ttl_seconds=86400,
        cleanup_required=True,
        pii_policy="masked",
    )


def _init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }


def _preview_execution_plan():
    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="preview",
            runnable=False,
            runtime_artifact_delivery={"mode": "local_preview"},
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )


def _runnable_execution_plan_with_route_certification():
    policy = SafeSamplePolicySet.default().for_environment("production")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production"),
        policy,
        SourceSamplingCapabilityDetector(verified_route_ids=("mssql_clickhouse_incremental_merge_airflow_kpo",)).detect(
            {
                "processes": [
                    {
                        "source": {"type": "mssql"},
                        "sink": {"type": "clickhouse", "strategy": {"mode": "incremental_merge"}},
                    }
                ]
            }
        ),
    )
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="production",
        policy_result=policy_result,
        temporary_target_plan=_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )


def test_safe_sample_runtime_readiness_reports_actual_remaining_blockers() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_readiness import SafeSampleRuntimeReadinessEvaluator

    readiness = SafeSampleRuntimeReadinessEvaluator().evaluate(
        _preview_execution_plan(),
        temporary_target_plan=_target_plan(),
    )
    payload = readiness.to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-readiness.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)
    assert payload["schema"] == "dpone.safe-sample-runtime-readiness.v1"
    assert payload["ready"] is False
    assert payload["blockers"] == ["runnable_deployment_set", "certified_source_data_copier"]
    assert "safe_sample_runtime_runner" in payload["available_contracts"]
    assert "safe_sample_runtime_evidence_writer" in payload["available_contracts"]
    assert "fail_closed_data_copier" in payload["available_contracts"]
    assert "mssql_clickhouse_bounded_copy_executor" in payload["available_contracts"]
    assert "credential_resolving_mssql_clickhouse_copy_executor" in payload["available_contracts"]
    assert "clickhouse_temporary_target_adapter" in payload["available_contracts"]
    assert [error["code"] for error in payload["errors"]] == [
        "DPONE_DEPLOYMENT_NOT_RUNNABLE",
        "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED",
    ]


def test_safe_sample_runtime_readiness_accepts_registered_certified_copier() -> None:
    from dpone.services.safe_sample_data_copier_registry import SafeSampleDataCopierRegistry
    from dpone.services.safe_sample_runtime_readiness import SafeSampleRuntimeReadinessEvaluator

    class FakeCertifiedCopier:
        def copy(self, **kwargs: object) -> dict[str, object]:
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "copied",
                "rows_read": 1,
                "rows_written": 1,
                "bytes_read": 128,
                "pii_policy": "masked",
                "errors": [],
            }

    registry = SafeSampleDataCopierRegistry.with_copiers(
        {"mssql_clickhouse_incremental_merge_airflow_kpo": FakeCertifiedCopier()}
    )
    registry_schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-data-copier-registry.schema.json").read_text(encoding="utf-8")
    )
    pytest.importorskip("jsonschema").validate(registry.to_dict(), registry_schema)

    payload = (
        SafeSampleRuntimeReadinessEvaluator()
        .evaluate(
            _runnable_execution_plan_with_route_certification(),
            temporary_target_plan=_target_plan(),
            data_copier_registry=registry,
        )
        .to_dict()
    )

    assert payload["ready"] is True
    assert payload["blockers"] == []
    assert payload["errors"] == []
    assert "certified_data_copier_registry" in payload["available_contracts"]
    assert "certified_source_data_copier" in payload["available_contracts"]


def test_safe_sample_data_copier_registry_rejects_unknown_certification() -> None:
    from dpone.services.safe_sample_data_copier_registry import SafeSampleDataCopierRegistry

    class FakeCertifiedCopier:
        def copy(self, **kwargs: object) -> dict[str, object]:
            return {"schema": "dpone.safe-sample-data-copy.v1", "status": "copied", "errors": []}

    with pytest.raises(ValueError, match="unknown route certification"):
        SafeSampleDataCopierRegistry.with_copiers({"unknown_route": FakeCertifiedCopier()})


def test_safe_sample_runtime_readiness_maps_executable_v2_index_blocker() -> None:
    from dpone.services.safe_sample_runtime_readiness import SafeSampleRuntimeReadinessEvaluator

    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=_target_plan(),
        deployment_context=None,
        preparation_error={
            "schema": "dpone.error.v1",
            "code": "DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED",
            "stage": "safe_sample_execution_plan",
            "severity": "error",
            "message": "executable indexed KPOs require the v2 Airflow provider path",
            "fixes": [],
        },
    )

    payload = (
        SafeSampleRuntimeReadinessEvaluator()
        .evaluate(plan, temporary_target_plan=_target_plan(), certified_data_copier_available=True)
        .to_dict()
    )

    assert payload["ready"] is False
    assert "airflow_provider_v2_path_required" in payload["blockers"]
    assert [error["code"] for error in payload["errors"]] == ["DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED"]
