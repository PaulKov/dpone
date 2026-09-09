from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.route_attestation import RouteAttestationVerification
from dpone.readiness import safe_sample_live_runtime
from dpone.readiness.safe_sample_live_runtime import (
    LiveSafeSampleRuntimeAssemblyError,
    LiveSafeSampleRuntimeInputLoader,
    build_live_safe_sample_runtime_assembly,
)
from dpone.services.safe_sample_execution_plan import (
    AirflowDeploymentContext,
    SafeSampleExecutionPlanBuilder,
    SafeSampleSourceSnapshot,
)
from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    TemporaryTargetPlan,
)

_CERTIFICATION_ID = "mssql_clickhouse_incremental_merge_airflow_kpo"
_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def _route_verification(
    *,
    decision: str = "verified",
    code: str = "DPONE_ROUTE_ATTESTATION_VERIFIED",
) -> RouteAttestationVerification:
    return RouteAttestationVerification(
        decision=decision,
        code=code,
        message="Route attestation verification test decision.",
        attestation_id="sha256:" + "9" * 64,
        attestation_sha256="sha256:" + "8" * 64,
        certification_bundle_sha256="sha256:" + "7" * 64,
        policy_fingerprint="sha256:" + "6" * 64,
        route_id=_CERTIFICATION_ID,
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        environment="production",
        authorization_profile="safe_sample_production",
        signer={"certificate_identity": "ci://dpone/test", "verifier_version": "3.0.4"},
        validity={},
        verified_at="2026-07-15T12:00:00Z",
    )


@pytest.fixture(autouse=True)
def _verified_route_attestation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        safe_sample_live_runtime,
        "verify_route_attestation_files",
        lambda **_: _route_verification(),
    )


def _write(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _inputs(tmp_path: Path, *, environment: str = "dev") -> dict[str, Path]:
    inputs = {
        "pipeline_source_path": _write(
            tmp_path / "pipeline.yaml",
            {
                "schema": "dpone.pipeline.v1",
                "metadata": {"id": "orders_daily"},
                "processes": [
                    {
                        "name": "orders_daily",
                        "source": {
                            "type": "mssql",
                            "connection_ref": "mssql_runtime",
                            "table": {"schema": "dbo", "name": "orders"},
                        },
                        "sink": {
                            "type": "clickhouse",
                            "connection_ref": "clickhouse_runtime",
                            "table": {"schema": "analytics", "name": "orders"},
                            "strategy": {"mode": "incremental_merge"},
                        },
                    }
                ],
            },
        ),
        "binding_set_path": _write(
            tmp_path / "binding-set.json",
            {
                "schema": "dpone.binding-set.v1",
                "environment": environment,
                "bindings": {
                    "mssql_runtime": {"connection_ref": "mssql_registry"},
                    "clickhouse_runtime": {"connection_ref": "clickhouse_registry"},
                },
                "runtime": {"service_account": "dpone-runtime"},
            },
        ),
        "connection_registry_path": _write(
            tmp_path / "connection-registry.json",
            {
                "schema": "dpone.connection-registry.v1",
                "environment": environment,
                "connections": {
                    "mssql_registry": {
                        "type": "mssql",
                        "connection": {"host": "mssql.internal", "database": "dwh"},
                        "credentials": {
                            "resolver": "env_var",
                            "support": "development_only",
                            "fields": {"username": "MSSQL_USER", "password": "MSSQL_PASSWORD"},
                        },
                    },
                    "clickhouse_registry": {
                        "type": "clickhouse",
                        "connection": {"host": "clickhouse.internal", "database": "analytics"},
                        "credentials": {
                            "resolver": "env_var",
                            "support": "development_only",
                            "fields": {"username": "CH_USER", "password": "CH_PASSWORD"},
                        },
                    },
                },
            },
        ),
        "credential_runtime_path": _write(
            tmp_path / "credential-runtime.json",
            {
                "schema": "dpone.credential-runtime.v1",
                "environment": environment,
                "vault": {
                    "address": "https://vault.invalid",
                    "auth": {"method": "kubernetes", "role": "dpone-runtime"},
                },
            },
        ),
    }
    for name in (
        "route_attestation_path",
        "route_attestation_bundle_path",
        "route_certification_bundle_path",
        "route_attestation_policy_path",
    ):
        inputs[name] = tmp_path / f"{name}.json"
    return inputs


def _fingerprint(path: Path) -> str:
    return canonical_fingerprint(json.loads(path.read_text(encoding="utf-8")))


def test_live_input_loader_rejects_symlink_swap_at_consume_boundary(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    binding_path = inputs["binding_set_path"]
    outside = tmp_path / "outside-binding.json"
    outside.write_text(binding_path.read_text(encoding="utf-8"), encoding="utf-8")
    binding_path.unlink()
    binding_path.symlink_to(outside)

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        LiveSafeSampleRuntimeInputLoader().load(
            pipeline_source_path=inputs["pipeline_source_path"],
            binding_set_path=binding_path,
            connection_registry_path=inputs["connection_registry_path"],
            credential_runtime_path=inputs["credential_runtime_path"],
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"


def test_live_runtime_rejects_legacy_plan_without_source_snapshot_before_artifact_fetch(
    tmp_path: Path,
) -> None:
    inputs = _inputs(tmp_path)
    plan = replace(_plan(inputs), source_snapshot=None)

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=plan,
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **inputs,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"


def test_live_input_loader_rejects_oversized_environment_input_before_parsing(tmp_path: Path) -> None:
    inputs = _inputs(tmp_path)
    binding_path = inputs["binding_set_path"]
    binding_path.write_bytes(b"x" * (4 * 1024 * 1024 + 1))

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        LiveSafeSampleRuntimeInputLoader().load(
            pipeline_source_path=inputs["pipeline_source_path"],
            binding_set_path=binding_path,
            connection_registry_path=inputs["connection_registry_path"],
            credential_runtime_path=inputs["credential_runtime_path"],
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID"


def _pinned_pack(tmp_path: Path, source_path: Path) -> dict[str, object]:
    source_sha = hashlib.sha256(source_path.read_bytes()).hexdigest()
    payload = {
        "kind": "gitops.airflow_pack",
        "schema_version": "3",
        "workload": {"workload_id": "orders_daily", "manifest": "pipeline.yaml"},
        "workload_dependencies": [{"kind": "manifest", "path": "pipeline.yaml", "sha256": source_sha}],
    }
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    relative = "releases/sha256-" + "a" * 64 + "/packs/orders_daily.airflow-pack.json"
    pack_path = tmp_path / ".dpone-cache" / relative
    pack_path.parent.mkdir(parents=True, exist_ok=True)
    pack_path.write_bytes(raw)
    return {
        "id": "orders_daily",
        "artifact_ref": f"cache://{relative}",
        "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
    }


def _plan(
    paths: dict[str, Path],
    *,
    environment: str = "development",
    process_name: str = "orders_daily",
):
    source_sha256 = "sha256:" + hashlib.sha256(paths["pipeline_source_path"].read_bytes()).hexdigest()
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment=environment),
        SafeSamplePolicySet.default().for_environment(environment),
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            proof=f"route_certification:{_CERTIFICATION_ID}",
            mode="pushdown",
        ),
    )
    if environment == "production":
        policy_result = replace(policy_result, passed=True, errors=())
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment=environment,
        policy_result=policy_result,
        temporary_target_plan=TemporaryTargetPlan(
            mode="temporary",
            pipeline_id="orders_daily",
            process=process_name,
            sink_type="clickhouse",
            connection_ref="clickhouse_runtime",
            original_table={"schema": "analytics", "name": "orders"},
            temporary_table={"schema": "dpone_tmp_dev", "name": "orders_daily_sample"},
            ttl_seconds=3600,
            cleanup_required=True,
            pii_policy="masked",
        ),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            environment=environment,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery={
                "mode": "init_fetch",
                "artifact_registry_ref": "dpone-artifacts",
                "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
                "source": {"artifact_registry_ref": "dpone-artifacts"},
                "verify": {"checksums": "required", "attestations": "optional"},
            },
            workload_packs=(_pinned_pack(paths["pipeline_source_path"].parent, paths["pipeline_source_path"]),),
            index_path=(paths["pipeline_source_path"].parent / ".dpone-cache/current/airflow-index.json").as_posix(),
            deployment_path=(paths["pipeline_source_path"].parent / ".dpone-cache/current/deployment.json").as_posix(),
            binding_set_ref=_fingerprint(paths["binding_set_path"]),
            connection_registry_ref=_fingerprint(paths["connection_registry_path"]),
            credential_runtime_ref=_fingerprint(paths["credential_runtime_path"]),
            runtime_image_digest="sha256:" + "d" * 64,
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256=source_sha256,
        ),
    )


def test_live_runtime_assembly_builds_ports_without_resolving_credentials(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)

    assembly = build_live_safe_sample_runtime_assembly(
        plan=_plan(paths),
        cache_root=tmp_path / ".dpone-cache",
        source_root=tmp_path,
        **paths,
    )

    assert assembly.plan.runnable is True
    assert assembly.plan.policy_result.capabilities.proof == f"route_certification:{_CERTIFICATION_ID}"
    assert assembly.data_copier is not None
    assert assembly.temporary_target_executor is not None


@pytest.mark.parametrize("source_type", _MSSQL_ALIASES)
def test_live_runtime_route_attestation_and_registry_match_accept_source_aliases(
    tmp_path: Path,
    source_type: str,
) -> None:
    paths = _inputs(tmp_path)
    pipeline = json.loads(paths["pipeline_source_path"].read_text(encoding="utf-8"))
    pipeline["processes"][0]["source"]["type"] = source_type
    _write(paths["pipeline_source_path"], pipeline)

    assembly = build_live_safe_sample_runtime_assembly(
        plan=_plan(paths),
        cache_root=tmp_path / ".dpone-cache",
        source_root=tmp_path,
        **paths,
    )

    assert assembly.route_attestation_verification.route_id == _CERTIFICATION_ID
    assert assembly.plan.runnable is True


def test_live_runtime_assembly_selects_pack_by_pipeline_id_when_process_name_differs(
    tmp_path: Path,
) -> None:
    paths = _inputs(tmp_path)
    source = json.loads(paths["pipeline_source_path"].read_text(encoding="utf-8"))
    source["processes"][0]["name"] = "extract_orders"
    paths["pipeline_source_path"].write_text(json.dumps(source), encoding="utf-8")

    assembly = build_live_safe_sample_runtime_assembly(
        plan=_plan(paths, process_name="extract_orders"),
        cache_root=tmp_path / ".dpone-cache",
        source_root=tmp_path,
        **paths,
    )

    assert assembly.plan.temporary_target_plan is not None
    assert assembly.plan.temporary_target_plan.pipeline_id == "orders_daily"
    assert assembly.plan.temporary_target_plan.process == "extract_orders"


def test_live_runtime_assembly_injects_vault_backend_from_pinned_runtime(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = _inputs(tmp_path)
    injected_reader = object()
    observed: dict[str, object] = {}

    def build_reader(**kwargs: object) -> object:
        observed["credential_runtime"] = kwargs["credential_runtime"]
        observed["connection_registry"] = kwargs["connection_registry"]
        return injected_reader

    class CapturingResolver:
        def __init__(self, **kwargs: object) -> None:
            observed["vault_kv_reader"] = kwargs["vault_kv_reader"]

        def resolve(self, connection_ref: str) -> object:
            raise AssertionError(f"credential {connection_ref} must remain lazy during assembly")

    monkeypatch.setattr(
        safe_sample_live_runtime,
        "build_required_vault_kv_reader",
        build_reader,
    )
    monkeypatch.setattr(
        safe_sample_live_runtime,
        "BindingCredentialResolver",
        CapturingResolver,
    )

    assembly = build_live_safe_sample_runtime_assembly(
        plan=_plan(paths),
        cache_root=tmp_path / ".dpone-cache",
        source_root=tmp_path,
        **paths,
    )

    assert assembly.plan.runnable is True
    assert observed["vault_kv_reader"] is injected_reader
    assert observed["credential_runtime"] == json.loads(paths["credential_runtime_path"].read_text(encoding="utf-8"))
    assert observed["connection_registry"] == json.loads(paths["connection_registry_path"].read_text(encoding="utf-8"))


def test_live_runtime_assembly_accepts_scaffold_dev_alias_for_development_plan(tmp_path: Path) -> None:
    paths = _inputs(tmp_path, environment="dev")

    assembly = build_live_safe_sample_runtime_assembly(
        plan=_plan(paths, environment="development"),
        cache_root=tmp_path / ".dpone-cache",
        source_root=tmp_path,
        **paths,
    )

    assert assembly.plan.environment == "development"


@pytest.mark.parametrize(
    ("context_field", "input_name", "code"),
    [
        (
            "binding_set_ref",
            "binding_set_path",
            "DPONE_SAFE_SAMPLE_BINDING_SET_FINGERPRINT_MISMATCH",
        ),
        (
            "connection_registry_ref",
            "connection_registry_path",
            "DPONE_SAFE_SAMPLE_CONNECTION_REGISTRY_FINGERPRINT_MISMATCH",
        ),
        (
            "credential_runtime_ref",
            "credential_runtime_path",
            "DPONE_SAFE_SAMPLE_CREDENTIAL_RUNTIME_FINGERPRINT_MISMATCH",
        ),
    ],
)
def test_live_runtime_assembly_rejects_each_changed_pinned_input(
    tmp_path: Path,
    context_field: str,
    input_name: str,
    code: str,
) -> None:
    paths = _inputs(tmp_path)
    plan = _plan(paths)
    payload = json.loads(paths[input_name].read_text(encoding="utf-8"))
    payload["changed_after_promotion"] = True
    _write(paths[input_name], payload)

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=plan,
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **paths,
        )

    assert getattr(plan.deployment_context, context_field) is not None
    assert exc.value.code == code


def test_live_runtime_assembly_rejects_unverified_production_route_before_ports(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    paths = _inputs(tmp_path, environment="production")
    monkeypatch.setattr(
        safe_sample_live_runtime,
        "verify_route_attestation_files",
        lambda **_: _route_verification(
            decision="invalid",
            code="DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID",
        ),
    )
    monkeypatch.setattr(
        safe_sample_live_runtime,
        "BindingCredentialResolver",
        lambda **_: pytest.fail("credential resolver must not be created before route verification"),
    )

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=_plan(paths, environment="production"),
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **paths,
        )

    assert exc.value.code == "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"
    assert exc.value.verification is not None
    assert exc.value.verification.decision == "invalid"


def test_live_runtime_assembly_rejects_source_changed_after_release_before_ports(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    plan = _plan(paths)
    payload = json.loads(paths["pipeline_source_path"].read_text(encoding="utf-8"))
    payload["processes"][0]["source"]["table"]["name"] = "orders_changed_after_release"
    _write(paths["pipeline_source_path"], payload)

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=plan,
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **paths,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"


def test_live_runtime_assembly_rejects_corrupt_pinned_pack_before_ports(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    plan = _plan(paths)
    context = plan.deployment_context
    assert context is not None
    artifact_ref = str(context.workload_packs[0]["artifact_ref"])
    pack_path = tmp_path / ".dpone-cache" / artifact_ref.removeprefix("cache://")
    pack_path.write_bytes(pack_path.read_bytes() + b"corrupt")

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=plan,
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **paths,
        )

    assert exc.value.code == "DPONE_CACHE_CHECKSUM_MISMATCH"


def test_live_runtime_assembly_requires_manifest_dependency_in_pack(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)
    plan = _plan(paths)
    context = plan.deployment_context
    assert context is not None
    indexed_pack = dict(context.workload_packs[0])
    artifact_ref = str(indexed_pack["artifact_ref"])
    pack_path = tmp_path / ".dpone-cache" / artifact_ref.removeprefix("cache://")
    payload = json.loads(pack_path.read_text(encoding="utf-8"))
    payload["workload_dependencies"] = []
    raw = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
    pack_path.write_bytes(raw)
    indexed_pack["sha256"] = "sha256:" + hashlib.sha256(raw).hexdigest()
    indexed_pack["bytes"] = len(raw)
    plan = replace(plan, deployment_context=replace(context, workload_packs=(indexed_pack,)))

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=plan,
            cache_root=tmp_path / ".dpone-cache",
            source_root=tmp_path,
            **paths,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"


def test_live_runtime_assembly_requires_explicit_source_root_before_pack_read(tmp_path: Path) -> None:
    paths = _inputs(tmp_path)

    with pytest.raises(LiveSafeSampleRuntimeAssemblyError) as exc:
        build_live_safe_sample_runtime_assembly(
            plan=_plan(paths),
            cache_root=tmp_path / ".dpone-cache",
            **paths,
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_SOURCE_ROOT_REQUIRED"


def test_legacy_runtime_copier_helper_cannot_enable_live_io(tmp_path: Path) -> None:
    from dpone.readiness.safe_sample_runtime_handoff import build_safe_sample_runtime_data_copier

    with pytest.raises(ValueError, match="DPONE_SAFE_SAMPLE_LIVE_ASSEMBLY_REQUIRED"):
        build_safe_sample_runtime_data_copier(
            pipeline_source_path=tmp_path / "unused.yaml",
            enable_live_copy=True,
        )
