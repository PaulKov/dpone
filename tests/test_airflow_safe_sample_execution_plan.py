from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    TemporaryTargetPlan,
)


def _write_current_preview(cache_root: Path) -> tuple[str, str]:
    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    deployment_dir = cache_root / "deployments" / "local-preview" / deployment_id.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.json").write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-set.v1",
                "deployment_id": deployment_id,
                "deployment_type": "preview",
                "runnable": False,
                "environment": "local-preview",
                "release_ref": release_id,
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    (deployment_dir / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [],
                "runtime_artifact_delivery": {"mode": "local_preview"},
            }
        ),
        encoding="utf-8",
    )
    (deployment_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    (cache_root / "current").symlink_to(deployment_dir.relative_to(cache_root), target_is_directory=True)
    return release_id, deployment_id


def _init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-dev-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-dev-artifacts"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }


def _write_current_environment(cache_root: Path) -> tuple[str, str, dict[str, object]]:
    release_id = "sha256:" + "c" * 64
    deployment_id = "sha256:" + "d" * 64
    pack = {
        "id": "orders_daily",
        "artifact_ref": "cache://releases/sha256-c/packs/orders_daily.airflow-pack.json",
        "sha256": "sha256:" + "e" * 64,
        "bytes": 128,
    }
    deployment_dir = cache_root / "deployments" / "dev" / deployment_id.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.json").write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-set.v1",
                "deployment_id": deployment_id,
                "deployment_type": "environment",
                "runnable": True,
                "environment": "dev",
                "release_ref": release_id,
                "runtime_artifact_delivery": _init_fetch_delivery(),
            }
        ),
        encoding="utf-8",
    )
    (deployment_dir / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": release_id,
                "deployment_id": deployment_id,
                "dag_specs": [],
                "workload_packs": [pack],
                "binding_set_ref": "sha256:" + "1" * 64,
                "connection_registry_ref": "sha256:" + "2" * 64,
                "credential_runtime_ref": "sha256:" + "3" * 64,
                "runtime_image_digest": "sha256:" + "4" * 64,
                "airflow_bundle_ref": "git:7ac31f2",
                "runtime_artifact_delivery": _init_fetch_delivery(),
            }
        ),
        encoding="utf-8",
    )
    (deployment_dir / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    (cache_root / "current").symlink_to(deployment_dir.relative_to(cache_root), target_is_directory=True)
    return release_id, deployment_id, pack


def _unverified_projection_loader(cache_root: Path) -> tuple[dict[str, object], dict[str, object]] | None:
    deployment_path = cache_root / "current" / "deployment.json"
    index_path = cache_root / "current" / "airflow-index.json"
    if not deployment_path.exists() or not index_path.exists():
        return None
    return json.loads(deployment_path.read_text()), json.loads(index_path.read_text())


def _temporary_target_plan() -> TemporaryTargetPlan:
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


def test_safe_sample_execution_plan_reads_current_deployment_without_runtime_side_effects(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_execution_plan import load_current_airflow_deployment_context

    release_id, deployment_id = _write_current_preview(tmp_path / ".dpone-cache")

    context = load_current_airflow_deployment_context(
        tmp_path / ".dpone-cache",
        projection_loader=_unverified_projection_loader,
    )
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-airflow-deployment-context.schema.json").read_text(encoding="utf-8")
    )

    assert context is not None
    jsonschema.validate(context.to_dict(), schema)
    assert context.release_id == release_id
    assert context.deployment_id == deployment_id
    assert context.deployment_type == "preview"
    assert context.runnable is False
    assert context.runtime_artifact_delivery == {"mode": "local_preview"}
    assert context.workload_packs == ()
    assert context.to_dict()["parse_side_effects"] == {
        "network": False,
        "metadata_db": False,
        "airflow_variables": False,
        "airflow_connections": False,
        "vault": False,
        "cache_refresh": False,
    }
    assert tmp_path.as_posix() not in str(context.to_dict())
    assert context.index_path == "$CACHE_ROOT/airflow-index.json"
    assert context.deployment_path == "$CACHE_ROOT/deployment.json"


def test_safe_sample_execution_plan_reads_workload_packs_for_runtime_fetch(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_execution_plan import load_current_airflow_deployment_context

    release_id, deployment_id, pack = _write_current_environment(tmp_path / ".dpone-cache")

    context = load_current_airflow_deployment_context(
        tmp_path / ".dpone-cache",
        projection_loader=_unverified_projection_loader,
    )
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-airflow-deployment-context.schema.json").read_text(encoding="utf-8")
    )

    assert context is not None
    jsonschema.validate(context.to_dict(), schema)
    assert context.release_id == release_id
    assert context.deployment_id == deployment_id
    assert context.runnable is True
    assert context.workload_packs == (pack,)
    assert context.to_dict()["workload_packs"] == [pack]
    assert context.to_dict()["binding_set_ref"] == "sha256:" + "1" * 64
    assert context.to_dict()["connection_registry_ref"] == "sha256:" + "2" * 64
    assert context.to_dict()["credential_runtime_ref"] == "sha256:" + "3" * 64
    assert context.to_dict()["runtime_image_digest"] == "sha256:" + "4" * 64
    assert context.to_dict()["airflow_bundle_ref"] == "git:7ac31f2"
    assert context.runtime_artifact_delivery["artifact_registry_ref"] == "dpone-dev-artifacts"


def test_safe_sample_deployment_context_schema_requires_complete_init_fetch_delivery() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-airflow-deployment-context.schema.json").read_text(encoding="utf-8")
    )
    payload = {
        "schema": "dpone.safe-sample-airflow-deployment-context.v1",
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "deployment_type": "environment",
        "runnable": True,
        "runtime_artifact_delivery": {"mode": "init_fetch"},
        "workload_packs": [],
        "binding_set_ref": "sha256:" + "c" * 64,
        "connection_registry_ref": "sha256:" + "d" * 64,
        "credential_runtime_ref": "sha256:" + "e" * 64,
        "runtime_image_digest": "sha256:" + "f" * 64,
        "airflow_bundle_ref": "git:7ac31f2",
        "index_path": ".dpone-cache/current/airflow-index.json",
        "deployment_path": ".dpone-cache/current/deployment.json",
        "parse_side_effects": {"network": False, "vault": False},
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_safe_sample_context_loader_fails_closed_without_verified_adapter(tmp_path: Path) -> None:
    from dpone.services.safe_sample_execution_plan import load_current_airflow_deployment_context

    _write_current_environment(tmp_path / ".dpone-cache")

    assert load_current_airflow_deployment_context(tmp_path / ".dpone-cache") is None


def test_verified_current_adapter_rejects_shallow_current_projection(tmp_path: Path) -> None:
    from dpone.readiness.airflow_verified_current_deployment import load_verified_airflow_deployment_context

    _write_current_environment(tmp_path / ".dpone-cache")

    assert load_verified_airflow_deployment_context(tmp_path / ".dpone-cache") is None


def test_safe_sample_execution_plan_blocks_preview_deployment_but_keeps_pinned_identity(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_execution_plan import (
        SafeSampleExecutionPlanBuilder,
        SafeSampleSourceSnapshot,
        load_current_airflow_deployment_context,
    )

    release_id, deployment_id = _write_current_preview(tmp_path / ".dpone-cache")
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
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=load_current_airflow_deployment_context(
            tmp_path / ".dpone-cache",
            projection_loader=_unverified_projection_loader,
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipelines/orders_daily/pipeline.yaml",
            sha256="sha256:" + "f" * 64,
        ),
    )

    payload = plan.to_dict()
    schema = json.loads(Path("docs/schemas/gitops/safe-sample-execution-plan.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(payload, schema)
    assert payload["schema"] == "dpone.safe-sample-execution-plan.v1"
    assert payload["runnable"] is False
    assert payload["source_snapshot"] == {
        "pipeline_id": "orders_daily",
        "path": "pipelines/orders_daily/pipeline.yaml",
        "sha256": "sha256:" + "f" * 64,
    }
    assert payload["artifact_pinning"] == {
        "release_id": release_id,
        "deployment_id": deployment_id,
        "pinned_workload_uri": f"cached://deployments/{deployment_id}/workloads/orders_daily",
        "workload_packs": [],
    }
    assert payload["blockers"] == [
        {
            "schema": "dpone.error.v1",
            "code": "DPONE_DEPLOYMENT_NOT_RUNNABLE",
            "stage": "safe_sample_execution_plan",
            "severity": "error",
            "message": "Current deployment is a non-runnable preview projection.",
            "fixes": [],
        }
    ]


def test_safe_sample_source_snapshot_round_trips_from_public_plan_payload() -> None:
    from dpone.services.safe_sample_execution_plan import (
        SafeSampleExecutionPlanBuilder,
        SafeSampleSourceSnapshot,
    )
    from dpone.services.safe_sample_execution_plan_io import safe_sample_execution_plan_from_dict

    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    source_snapshot = SafeSampleSourceSnapshot(
        pipeline_id="orders_daily",
        path="pipelines/orders_daily/pipeline.yaml",
        sha256="sha256:" + "f" * 64,
    )
    original = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=None,
        source_snapshot=source_snapshot,
    )

    restored = safe_sample_execution_plan_from_dict(original.to_dict())

    assert restored.source_snapshot == source_snapshot


def test_safe_sample_execution_plan_allows_runnable_deployment_preconditions() -> None:
    from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder

    release_id = "sha256:" + "c" * 64
    deployment_id = "sha256:" + "d" * 64
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
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id=release_id,
            deployment_id=deployment_id,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )

    assert plan.runnable is True
    assert plan.blockers == ()
    assert plan.to_dict()["artifact_pinning"]["pinned_workload_uri"] == (
        f"cached://deployments/{deployment_id}/workloads/orders_daily"
    )


def test_safe_sample_execution_plan_blocks_preparation_failure_without_reusing_current() -> None:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlanBuilder

    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    preparation_error = {
        "schema": "dpone.error.v1",
        "code": "DPONE_LOCAL_SAFE_SAMPLE_DEPLOYMENT_FAILED",
        "stage": "local_safe_sample_deployment",
        "severity": "error",
        "message": "release publication failed",
        "fixes": [],
    }

    plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=None,
        preparation_error=preparation_error,
    )

    assert plan.runnable is False
    assert plan.deployment_context is None
    assert plan.blockers == (preparation_error,)


def test_safe_sample_execution_plan_blocks_deployment_environment_mismatch() -> None:
    from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder
    from dpone.services.safe_sample_policy import SourceSamplingCapabilityDetector

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

    plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="production",
        policy_result=policy_result,
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "c" * 64,
            deployment_id="sha256:" + "d" * 64,
            environment="dev",
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )

    assert plan.runnable is False
    assert [blocker["code"] for blocker in plan.blockers] == ["DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH"]


def test_safe_sample_execution_plan_blocks_incomplete_init_fetch_delivery() -> None:
    from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder

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
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "c" * 64,
            deployment_id="sha256:" + "d" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery={"mode": "init_fetch"},
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )

    assert plan.runnable is False
    assert [blocker["code"] for blocker in plan.blockers] == ["DPONE_AIRFLOW_INDEX_DELIVERY_INVALID"]
    assert "runtime_artifact_delivery.identity" in plan.blockers[0]["message"]


def test_safe_sample_execution_plan_blocks_nested_incomplete_init_fetch_delivery() -> None:
    from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder

    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(supports_pushdown_sampling=True, full_scan_required=False, proof="unit"),
    )
    incomplete_delivery = _init_fetch_delivery()
    incomplete_delivery["identity"] = {"method": "kubernetes_workload_identity"}

    plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="development",
        policy_result=policy_result,
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "c" * 64,
            deployment_id="sha256:" + "d" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=incomplete_delivery,
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )

    assert plan.runnable is False
    assert [blocker["code"] for blocker in plan.blockers] == ["DPONE_AIRFLOW_INDEX_DELIVERY_INVALID"]
    assert "runtime_artifact_delivery.identity.service_account" in plan.blockers[0]["message"]


def test_legacy_v1_init_fetch_without_strict_v2_fields_stays_runnable() -> None:
    from dpone.contracts.runtime_artifact_delivery import (
        STRICT_INIT_FETCH_REQUIRED_FIELDS,
        missing_init_fetch_delivery_fields,
        missing_init_fetch_delivery_paths,
    )
    from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder

    legacy_delivery = _init_fetch_delivery()
    assert missing_init_fetch_delivery_fields(legacy_delivery) == ()
    assert missing_init_fetch_delivery_paths(legacy_delivery) == ()
    assert all(field not in legacy_delivery for field in ("trust_tier", "registry_config_ref", "trust_policy_ref"))
    assert any(field not in legacy_delivery for field in STRICT_INIT_FETCH_REQUIRED_FIELDS)

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
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "c" * 64,
            deployment_id="sha256:" + "d" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=legacy_delivery,
            workload_packs=(
                {
                    "id": "orders_daily",
                    "artifact_ref": "cache://releases/sha256-c/packs/orders_daily.airflow-pack.json",
                    "sha256": "sha256:" + "e" * 64,
                    "bytes": 128,
                },
            ),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )

    assert plan.runnable is True
    assert plan.blockers == ()
    assert plan.deployment_context is not None
    assert plan.deployment_context.runtime_artifact_delivery["mode"] == "init_fetch"


def test_safe_sample_rejects_executable_v2_index_with_explicit_blocker() -> None:
    from dpone.services.safe_sample_execution_plan import (
        SafeSampleExecutionPlanBuilder,
        resolve_airflow_deployment_context_from_projection,
    )

    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    resolution = resolve_airflow_deployment_context_from_projection(
        deployment={
            "schema": "dpone.deployment-set.v2",
            "deployment_id": deployment_id,
            "deployment_type": "environment",
            "runnable": True,
            "environment": "dev",
            "release_ref": release_id,
        },
        index={
            "schema": "dpone.airflow-deployment-index.v2",
            "release_id": release_id,
            "deployment_id": deployment_id,
            "runtime_artifact_delivery": {
                "mode": "init_fetch",
                "trust_tier": "non_production",
            },
        },
        index_path=Path(".dpone-cache/current/airflow-index.json"),
        deployment_path=Path(".dpone-cache/current/deployment.json"),
    )

    assert resolution.context is None
    assert resolution.error is not None
    assert resolution.error["code"] == "DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED"

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
        temporary_target_plan=_temporary_target_plan(),
        deployment_context=resolution.context,
        preparation_error=resolution.error,
    )

    assert plan.runnable is False
    assert [blocker["code"] for blocker in plan.blockers] == ["DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED"]
    assert "DPONE_DEPLOYMENT_CURRENT_NOT_FOUND" not in {blocker["code"] for blocker in plan.blockers}


def test_verified_current_resolver_surfaces_executable_v2_index_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    from dpone.readiness import airflow_verified_current_deployment as verified_module

    release_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    cache_root = tmp_path / ".dpone-cache"
    deployment_dir = cache_root / "deployments" / "dev" / deployment_id.replace(":", "-")
    deployment_dir.mkdir(parents=True)
    (cache_root / "current").symlink_to(deployment_dir.relative_to(cache_root), target_is_directory=True)

    class _Lock:
        def __enter__(self) -> None:
            return None

        def __exit__(self, *args: object) -> None:
            del args

    monkeypatch.setattr(verified_module, "promotion_lock", lambda root: _Lock())
    monkeypatch.setattr(
        verified_module,
        "resolve_relative_current_symlink",
        lambda root: deployment_dir,
    )
    monkeypatch.setattr(
        verified_module,
        "DeploymentCacheRecoveryPlanner",
        lambda root: SimpleNamespace(
            plan=lambda environment: SimpleNamespace(
                status="ok",
                current_deployment_id=deployment_id,
                current_path_deployment_id=deployment_id,
            )
        ),
    )
    monkeypatch.setattr(
        verified_module,
        "DeploymentCacheMaterializer",
        lambda root: SimpleNamespace(
            validate_current_details=lambda target, environment: SimpleNamespace(
                deployment_id=deployment_id,
                deployment={
                    "schema": "dpone.deployment-set.v2",
                    "deployment_id": deployment_id,
                    "deployment_type": "environment",
                    "runnable": True,
                    "environment": "dev",
                    "release_ref": release_id,
                },
                airflow_index={
                    "schema": "dpone.airflow-deployment-index.v2",
                    "release_id": release_id,
                    "deployment_id": deployment_id,
                    "runtime_artifact_delivery": {
                        "mode": "init_fetch",
                        "trust_tier": "non_production",
                    },
                },
            )
        ),
    )

    resolution = verified_module.resolve_verified_airflow_deployment_context(cache_root)

    assert resolution.context is None
    assert resolution.error is not None
    assert resolution.error["code"] == "DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED"
    assert verified_module.load_verified_airflow_deployment_context(cache_root) is None
