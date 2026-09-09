from __future__ import annotations

from pathlib import Path

from dpone.readiness.safe_sample_live_input_discovery import discover_live_safe_sample_inputs
from dpone.services.safe_sample_capabilities import SampleSourceCapabilities
from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlan
from dpone.services.safe_sample_policy import (
    SafeSamplePolicy,
    SafeSamplePolicyResult,
    SampleRunRequest,
    SampleTarget,
    TemporaryTargetPlan,
)


def test_live_input_discovery_keeps_missing_overlay_as_network_free_handoff(tmp_path: Path) -> None:
    result = discover_live_safe_sample_inputs(
        _plan(),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "not_configured"
    assert result.ready is False
    assert result.paths is None
    assert result.errors == ()
    assert result.to_dict() == {
        "status": "not_configured",
        "deployment_id": "sha256:" + "d" * 64,
        "pipeline_id": "orders_daily",
        "authorization_overlay_profile": "deployment_scoped_v1",
        "errors": [],
    }


def test_live_input_discovery_returns_complete_pinned_paths_without_reading_secrets(tmp_path: Path) -> None:
    _write_environment_inputs(tmp_path)
    authorization_dir = _authorization_dir(tmp_path)
    _write_authorization_inputs(authorization_dir)

    result = discover_live_safe_sample_inputs(
        _plan(),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "ready"
    assert result.ready is True
    assert result.errors == ()
    assert result.paths is not None
    assert result.paths.binding_set == tmp_path / "environments" / "dev" / "binding-set.yaml"
    assert result.paths.connection_registry == tmp_path / "platform" / "connection-registries" / "dev.yaml"
    assert result.paths.credential_runtime == tmp_path / "environments" / "dev" / "credential-runtime.yaml"
    assert result.paths.route_attestation == authorization_dir / "route-attestation.json"
    assert result.paths.route_attestation_bundle == authorization_dir / "route-attestation.sigstore.json"
    assert result.paths.route_certification_bundle == authorization_dir / "route-certification-bundle.json"
    assert result.paths.route_attestation_policy == authorization_dir / "route-attestation-policy.json"
    payload = result.to_dict()
    assert payload["status"] == "ready"
    assert "paths" not in payload
    assert "vault" not in str(payload).lower()
    assert "password" not in str(payload).lower()


def test_live_input_discovery_fails_closed_for_partially_materialized_overlay(tmp_path: Path) -> None:
    _write_environment_inputs(tmp_path)
    authorization_dir = _authorization_dir(tmp_path)
    authorization_dir.mkdir(parents=True)
    (authorization_dir / "route-attestation.json").write_text("{}\n", encoding="utf-8")

    result = discover_live_safe_sample_inputs(
        _plan(),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "incomplete"
    assert result.ready is False
    assert result.paths is None
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE"]
    assert "route-attestation.sigstore.json" in result.errors[0]["message"]
    assert "route-certification-bundle.json" in result.errors[0]["message"]
    assert "route-attestation-policy.json" in result.errors[0]["message"]


def test_live_input_discovery_rejects_unsafe_pipeline_identity_before_path_lookup(tmp_path: Path) -> None:
    result = discover_live_safe_sample_inputs(
        _plan(pipeline_id="../orders"),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "invalid"
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"]
    assert not (tmp_path / "orders").exists()


def test_live_input_discovery_rejects_reserved_current_pipeline_identity(tmp_path: Path) -> None:
    result = discover_live_safe_sample_inputs(
        _plan(pipeline_id="current"),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "invalid"
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"]


def test_live_input_discovery_turns_cache_root_symlink_loop_into_structured_blocker(tmp_path: Path) -> None:
    cache_root = tmp_path / ".dpone-cache"
    cache_root.symlink_to(cache_root)

    result = discover_live_safe_sample_inputs(
        _plan(),
        project_root=tmp_path,
        cache_root=cache_root,
    )

    assert result.status == "invalid"
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"]


def test_live_input_discovery_rejects_symlinked_authorization_input(tmp_path: Path) -> None:
    _write_environment_inputs(tmp_path)
    authorization_dir = _authorization_dir(tmp_path)
    _write_authorization_inputs(authorization_dir)
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    (authorization_dir / "route-attestation-policy.json").unlink()
    (authorization_dir / "route-attestation-policy.json").symlink_to(outside)

    result = discover_live_safe_sample_inputs(
        _plan(),
        project_root=tmp_path,
        cache_root=tmp_path / ".dpone-cache",
    )

    assert result.status == "invalid"
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"]
    assert "symlink" in result.errors[0]["message"].lower()


def _plan(*, pipeline_id: str = "orders_daily") -> SafeSampleExecutionPlan:
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development")
    policy = SafeSamplePolicy(
        environment="development",
        require_pushdown=False,
        allow_full_scan=True,
        max_bytes=10 * 1024**3,
        timeout_seconds=300,
    )
    return SafeSampleExecutionPlan(
        sample_rows=1000,
        environment="development",
        runnable=True,
        policy_result=SafeSamplePolicyResult(
            passed=True,
            request=request,
            policy=policy,
            capabilities=SampleSourceCapabilities(
                supports_pushdown_sampling=True,
                full_scan_required=False,
                estimated_read_bytes=1024,
                proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
                mode="pushdown",
            ),
        ),
        temporary_target_plan=TemporaryTargetPlan(
            mode="temporary",
            pipeline_id=pipeline_id,
            process="orders_daily",
            sink_type="clickhouse",
            connection_ref="clickhouse_dev",
            original_table={"schema": "analytics", "name": "orders"},
            temporary_table={"schema": "dpone_tmp_development", "name": "orders_daily_sample"},
            ttl_seconds=3600,
            cleanup_required=True,
            pii_policy="masked",
        ),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "r" * 64,
            deployment_id="sha256:" + "d" * 64,
            environment="dev",
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery={"mode": "init_fetch"},
            workload_packs=(),
            index_path=str(Path(".dpone-cache/current/airflow-index.json")),
            deployment_path=str(Path(".dpone-cache/current/deployment.json")),
        ),
        blockers=(),
    )


def _authorization_dir(root: Path) -> Path:
    return root / ".dpone-cache" / "route-authorizations" / ("sha256-" + "d" * 64) / "orders_daily"


def _write_environment_inputs(root: Path) -> None:
    environment_dir = root / "environments" / "dev"
    environment_dir.mkdir(parents=True)
    (environment_dir / "binding-set.yaml").write_text("schema: dpone.binding-set.v1\n", encoding="utf-8")
    (environment_dir / "credential-runtime.yaml").write_text(
        "schema: dpone.credential-runtime.v1\n",
        encoding="utf-8",
    )
    registry_dir = root / "platform" / "connection-registries"
    registry_dir.mkdir(parents=True)
    (registry_dir / "dev.yaml").write_text("schema: dpone.connection-registry.v1\n", encoding="utf-8")


def _write_authorization_inputs(path: Path) -> None:
    path.mkdir(parents=True)
    for name in (
        "route-attestation.json",
        "route-attestation.sigstore.json",
        "route-certification-bundle.json",
        "route-attestation-policy.json",
    ):
        (path / name).write_text("{}\n", encoding="utf-8")
