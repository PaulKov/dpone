from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.route_attestation import RouteAttestationVerification
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


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(
    monkeypatch: pytest.MonkeyPatch,
    *,
    project_dir: Path | None = None,
    route_decision: str = "verified",
    forged_route_ids: tuple[str, ...] = (),
) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(
            lambda logger: SimpleNamespace(
                logger=logger,
                settings=SimpleNamespace(project_dir=project_dir, repo_root=project_dir),
                verified_safe_sample_route_ids=forged_route_ids,
            )
        ),
    )
    from dpone.readiness import safe_sample_live_runtime

    code = (
        "DPONE_ROUTE_ATTESTATION_VERIFIED"
        if route_decision == "verified"
        else "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"
    )
    monkeypatch.setattr(
        safe_sample_live_runtime,
        "verify_route_attestation_files",
        lambda **_: _route_verification(decision=route_decision, code=code),
    )


def _route_verification(*, decision: str, code: str) -> RouteAttestationVerification:
    return RouteAttestationVerification(
        decision=decision,
        code=code,
        message="Route attestation verification test decision.",
        attestation_id="sha256:" + "9" * 64,
        attestation_sha256="sha256:" + "8" * 64,
        certification_bundle_sha256="sha256:" + "7" * 64,
        policy_fingerprint="sha256:" + "6" * 64,
        route_id="mssql_clickhouse_incremental_merge_airflow_kpo",
        release_id="sha256:" + "a" * 64,
        deployment_id="sha256:" + "b" * 64,
        environment="production",
        authorization_profile="safe_sample_production",
        signer={"certificate_identity": "ci://dpone/test", "verifier_version": "3.0.4"},
        validity={},
        verified_at="2026-07-15T12:00:00Z",
    )


def _route_arguments(tmp_path: Path) -> list[str]:
    return [
        "--route-attestation",
        str(tmp_path / "route-attestation.json"),
        "--route-attestation-bundle",
        str(tmp_path / "route-attestation.sigstore.json"),
        "--route-certification-bundle",
        str(tmp_path / "route-certification.json"),
        "--route-attestation-policy",
        str(tmp_path / "route-attestation-policy.json"),
    ]


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _pack(cache_root: Path, pipeline_source: Path | None = None) -> dict[str, object]:
    dependencies: list[dict[str, str]] = []
    if pipeline_source is not None:
        dependencies.append(
            {
                "kind": "manifest",
                "path": pipeline_source.relative_to(cache_root.parent).as_posix(),
                "sha256": hashlib.sha256(pipeline_source.read_bytes()).hexdigest(),
            }
        )
    payload = (
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders_daily", "manifest": "pipeline.yaml"},
                "workload_dependencies": dependencies,
            },
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    relative = "releases/sha256-" + "a" * 64 + "/packs/orders_daily.airflow-pack.json"
    path = cache_root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return {
        "id": "orders_daily",
        "artifact_ref": f"cache://{relative}",
        "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
        "bytes": len(payload),
    }


def _init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "local-test",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "local-test"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }


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


def _plan(
    cache_root: Path,
    *,
    pipeline_source: Path | None = None,
    include_source_snapshot: bool = True,
    proof: str = "unit",
    environment: str = "development",
    binding_set_ref: str | None = None,
    connection_registry_ref: str | None = None,
    credential_runtime_ref: str | None = None,
) -> dict[str, object]:
    if pipeline_source is None and include_source_snapshot:
        pipeline_source = _pipeline_source(cache_root.parent / "pipeline.yaml")
    policy = SafeSamplePolicySet.default().for_environment(environment)
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment=environment),
        policy,
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            proof=proof,
            _production_proof_verified=environment in {"prod", "production"},
        ),
    )
    execution_plan = SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment=environment,
        policy_result=policy_result,
        temporary_target_plan=_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="environment",
            runnable=True,
            environment=environment,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(_pack(cache_root, pipeline_source),),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
            binding_set_ref=binding_set_ref or "sha256:" + "d" * 64,
            connection_registry_ref=connection_registry_ref or "sha256:" + "e" * 64,
            credential_runtime_ref=credential_runtime_ref or "sha256:" + "f" * 64,
            runtime_image_digest="sha256:" + "1" * 64,
        ),
        source_snapshot=(
            SafeSampleSourceSnapshot(
                pipeline_id="orders_daily",
                path=pipeline_source.name,
                sha256="sha256:" + hashlib.sha256(pipeline_source.read_bytes()).hexdigest(),
            )
            if pipeline_source is not None
            else None
        ),
    )
    return execution_plan.to_dict()


def _successful_data_copy(
    plan: Any,
    target_plan: TemporaryTargetPlan,
    *,
    diagnostics: dict[str, object],
) -> dict[str, object]:
    from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder

    return {
        "schema": "dpone.safe-sample-data-copy.v1",
        "status": "copied",
        "source_request": SafeSampleSourceRequestBuilder().build(plan.policy_result, target_plan).to_dict(),
        "copy_request": {
            "schema": "dpone.safe-sample-certified-copy-request.v1",
            "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_dev",
                "table": {"schema": "dbo", "name": "orders"},
            },
            "sink": {
                "type": "clickhouse",
                "connection_ref": target_plan.connection_ref,
                "temporary_table": dict(target_plan.temporary_table),
            },
            "strategy": "incremental_merge",
            "sample_rows": plan.sample_rows,
            "max_bytes": plan.policy_result.policy.max_bytes,
            "timeout_seconds": plan.policy_result.policy.timeout_seconds,
            "source_read_only": True,
            "pii_policy": target_plan.pii_policy,
            "proof": plan.policy_result.capabilities.proof,
        },
        "rows_read": 1,
        "rows_written": 1,
        "bytes_read": 128,
        "pii_policy": "masked",
        "diagnostics": diagnostics,
        "errors": [],
    }


def _pipeline_source(path: Path) -> Path:
    path.write_text(
        "\n".join(
            [
                "schema: dpone.pipeline.v1",
                "metadata:",
                "  id: orders_daily",
                "processes:",
                "  - name: orders_daily",
                "    source:",
                "      type: mssql",
                "      connection_ref: mssql_dev",
                "      table:",
                "        schema: dbo",
                "        name: orders",
                "    sink:",
                "      type: clickhouse",
                "      connection_ref: clickhouse_dev",
                "      table:",
                "        schema: analytics",
                "        name: orders",
                "      strategy:",
                "        mode: incremental_merge",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _binding_set(path: Path) -> Path:
    return _write_json(
        path,
        {
            "schema": "dpone.binding-set.v1",
            "environment": "development",
            "bindings": {
                "mssql_dev": {"connection_ref": "mssql_dev"},
                "clickhouse_dev": {"connection_ref": "clickhouse_dev"},
            },
            "runtime": {
                "kubernetes_namespace": "dpone-development",
                "service_account": "dpone-runtime",
                "pool": "dpone_development",
            },
        },
    )


def _connection_registry(path: Path) -> Path:
    return _write_json(
        path,
        {
            "schema": "dpone.connection-registry.v1",
            "connections": {
                "mssql_dev": {
                    "type": "mssql",
                    "connection": {"host": "mssql.internal", "port": 1433, "database": "dwh"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"username": "MSSQL_USER", "password": "MSSQL_PASSWORD"},
                    },
                },
                "clickhouse_dev": {
                    "type": "clickhouse",
                    "connection": {"host": "clickhouse.internal", "port": 9000, "database": "analytics"},
                    "credentials": {
                        "resolver": "env_var",
                        "support": "development_only",
                        "fields": {"username": "CH_USER", "password": "CH_PASSWORD"},
                    },
                },
            },
        },
    )


def _credential_runtime(path: Path, *, secret_material: bool = False) -> Path:
    auth: dict[str, object] = {"method": "kubernetes", "role": "dpone-runtime-development"}
    if secret_material:
        auth["token"] = "must-not-leak"
    return _write_json(
        path,
        {
            "schema": "dpone.credential-runtime.v1",
            "environment": "development",
            "vault": {
                "address": "https://vault.example.internal",
                "namespace": "data-platform",
                "auth": auth,
            },
        },
    )


def _fingerprint(path: Path) -> str:
    return canonical_fingerprint(json.loads(path.read_text(encoding="utf-8")))


def test_ops_safe_sample_runtime_run_cli_executes_pinned_init_fetch_and_writes_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(tmp_path / "plan.json", _plan(cache_root))
    output_dir = tmp_path / "runtime"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.safe-sample-runtime-run.v1"
    assert payload["execution_status"] == "failed"
    assert payload["release_id"] == "sha256:" + "a" * 64
    assert payload["deployment_id"] == "sha256:" + "b" * 64
    assert payload["runtime_execution"]["init_fetch"]["passed"] is True
    assert payload["runtime_execution"]["init_fetch"]["artifacts"][0]["artifact_ref"].startswith("cache://")
    assert payload["runtime_execution"]["init_fetch"]["artifacts"][0]["path"].startswith("$RUNTIME_ARTIFACT_ROOT/")
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    evidence_path = output_dir / "safe-sample-runtime-execution.json"
    assert evidence_path.exists()
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["init_fetch"]["passed"] is True
    serialized = json.dumps(payload, sort_keys=True)
    assert str(tmp_path) not in serialized
    assert str(Path.home()) not in serialized


def test_ops_safe_sample_runtime_run_cli_rejects_missing_source_pin_without_runtime_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(cache_root, include_source_snapshot=False),
    )
    output_dir = tmp_path / "runtime"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["code"] == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"
    assert not output_dir.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("pipeline_id", "other_pipeline"),
        ("path", "other.yaml"),
        ("sha256", "sha256:" + "0" * 64),
    ],
)
def test_ops_safe_sample_runtime_run_cli_rejects_mismatched_source_pin_without_runtime_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    payload = _plan(cache_root)
    source_snapshot = payload["source_snapshot"]
    assert isinstance(source_snapshot, dict)
    source_snapshot[field] = value
    plan_path = _write_json(tmp_path / "plan.json", payload)
    output_dir = tmp_path / "runtime"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    error = json.loads(capsys.readouterr().out)
    assert error["code"] == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"
    assert not output_dir.exists()


def test_ops_safe_sample_runtime_run_cli_is_nonzero_when_copier_blocks_without_errors(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(tmp_path / "plan.json", _plan(cache_root))
    output_dir = tmp_path / "runtime"

    class EmptyErrorBlockedCopier:
        def copy(self, **kwargs: object) -> dict[str, object]:
            del kwargs
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "blocked",
                "rows_read": 0,
                "rows_written": 0,
                "bytes_read": 0,
                "pii_policy": "masked",
                "errors": [],
            }

    from dpone.services.ops import command_handlers_safe_sample

    monkeypatch.setattr(
        command_handlers_safe_sample,
        "_runtime_ports_or_error",
        lambda args, plan, *, ctx: (plan, EmptyErrorBlockedCopier(), None, None, None),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.safe-sample-runtime-run.v1"
    assert payload["execution_status"] == "failed"
    assert payload["runtime_execution"]["schema"] == "dpone.safe-sample-runtime-execution.v1"
    assert payload["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


def test_ops_safe_sample_runtime_run_cli_fails_closed_for_non_json_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(
            cache_root,
            proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        ),
    )
    output_dir = tmp_path / "runtime"

    class NonJsonDiagnosticsCopier:
        def copy(self, **kwargs: object) -> dict[str, object]:
            plan = kwargs["plan"]
            target_plan = kwargs["target_plan"]
            assert isinstance(target_plan, TemporaryTargetPlan)
            return _successful_data_copy(
                plan,
                target_plan,
                diagnostics={"unsupported": object()},
            )

    from dpone.services.ops import command_handlers_safe_sample

    monkeypatch.setattr(
        command_handlers_safe_sample,
        "_runtime_ports_or_error",
        lambda args, plan, *, ctx: (plan, NonJsonDiagnosticsCopier(), None, None, None),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(output_dir),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["execution_status"] == "failed"
    assert payload["runtime_execution"]["data_copy"]["diagnostics"] == {}
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


def test_ops_safe_sample_runtime_run_cli_rejects_invalid_execution_plan_before_runtime(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_payload = _plan(cache_root)
    deployment_context = plan_payload["deployment_context"]
    assert isinstance(deployment_context, dict)
    deployment_context["runtime_artifact_delivery"] = {"mode": "init_fetch"}
    plan_path = _write_json(tmp_path / "plan.json", plan_payload)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_EXECUTION_PLAN_INVALID"
    assert "deployment_context.runtime_artifact_delivery.identity" in payload["message"]
    assert not (tmp_path / "runtime").exists()


def test_ops_safe_sample_runtime_run_cli_uses_certified_copier_from_pipeline_source(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(cache_root, proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo"),
    )
    pipeline_source = _pipeline_source(tmp_path / "pipeline.yaml")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(pipeline_source),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    data_copy = payload["runtime_execution"]["data_copy"]
    assert data_copy["copy_request"]["certification_id"] == "mssql_clickhouse_incremental_merge_airflow_kpo"
    assert data_copy["copy_request"]["source"]["connection_ref"] == "mssql_dev"
    assert data_copy["copy_request"]["sink"]["connection_ref"] == "clickhouse_dev"
    assert data_copy["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"


def test_ops_safe_sample_runtime_run_cli_can_enable_credential_resolving_sql_copy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    monkeypatch.setenv("MSSQL_USER", "mssql_runtime")
    monkeypatch.setenv("MSSQL_PASSWORD", "must-not-leak-mssql")
    monkeypatch.setenv("CH_USER", "clickhouse_runtime")
    monkeypatch.setenv("CH_PASSWORD", "must-not-leak-clickhouse")

    seen_credentials: list[object] = []

    class FakeMssqlFactory:
        def create(self, credentials: object) -> object:
            seen_credentials.append(credentials)

            class Client:
                def fetch(self, plan: Any) -> dict[str, object]:
                    return {
                        "rows": [{"order_id": 1, "email": "alice@example.test"}],
                        "rows_read": 1,
                        "bytes_read": 128,
                        "diagnostics": {"query_id": "mssql-1"},
                    }

            return Client()

    class FakeClickHouseFactory:
        def create(self, credentials: object) -> object:
            seen_credentials.append(credentials)

            class Client:
                def insert(self, plan: Any) -> dict[str, object]:
                    return {
                        "rows_written": plan.row_count,
                        "bytes_written": 128,
                        "diagnostics": {"query_id": "ch-1"},
                    }

            return Client()

    from dpone.runtime import safe_sample_sql_clients

    monkeypatch.setattr(safe_sample_sql_clients, "MssqlCredentialsSafeSampleSqlClientFactory", FakeMssqlFactory)
    monkeypatch.setattr(
        safe_sample_sql_clients,
        "ClickHouseCredentialsSafeSampleSqlClientFactory",
        FakeClickHouseFactory,
    )

    target_statements: list[str] = []

    class FakeTargetConnectionProviderFactory:
        def create(self, credentials: object) -> object:
            seen_credentials.append(credentials)

            class Connection:
                def execute(self, statement: str) -> None:
                    target_statements.append(statement)

            connection = Connection()
            return lambda: connection

    monkeypatch.setattr(
        safe_sample_sql_clients,
        "ClickHouseCredentialsConnectionProviderFactory",
        FakeTargetConnectionProviderFactory,
    )

    cache_root = tmp_path / ".dpone-cache"
    pipeline_source = _pipeline_source(tmp_path / "pipeline.yaml")
    binding_set = _binding_set(tmp_path / "binding-set.json")
    connection_registry = _connection_registry(tmp_path / "connection-registry.json")
    credential_runtime = _credential_runtime(tmp_path / "credential-runtime.json")
    binding_fingerprint = _fingerprint(binding_set)
    registry_fingerprint = _fingerprint(connection_registry)
    runtime_fingerprint = _fingerprint(credential_runtime)
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(
            cache_root,
            pipeline_source=pipeline_source,
            proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
            binding_set_ref=binding_fingerprint,
            connection_registry_ref=registry_fingerprint,
            credential_runtime_ref=runtime_fingerprint,
        ),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(pipeline_source),
                "--enable-live-copy",
                "--binding-set",
                str(binding_set),
                "--connection-registry",
                str(connection_registry),
                "--credential-runtime",
                str(credential_runtime),
                *_route_arguments(tmp_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    data_copy = payload["runtime_execution"]["data_copy"]
    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "passed"
    assert data_copy["status"] == "copied"
    assert data_copy["rows_read"] == 1
    assert data_copy["rows_written"] == 1
    assert data_copy["diagnostics"]["source"]["client"] == {"query_id": "mssql-1"}
    assert data_copy["diagnostics"]["sink"]["client"] == {"query_id": "ch-1"}
    assert data_copy["diagnostics"]["credential_resolution"] == {
        "source": {
            "connection_ref": "mssql_dev",
            "resolver": "env_var",
            "resolved_version": None,
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "binding_set_fingerprint": binding_fingerprint,
            "connection_registry_fingerprint": registry_fingerprint,
            "credential_runtime_fingerprint": runtime_fingerprint,
            "credential_runtime_environment": "development",
            "credential_runtime_auth_method": "kubernetes",
            "runtime_image_digest": "sha256:" + "1" * 64,
        },
        "sink": {
            "connection_ref": "clickhouse_dev",
            "resolver": "env_var",
            "resolved_version": None,
            "release_id": "sha256:" + "a" * 64,
            "deployment_id": "sha256:" + "b" * 64,
            "binding_set_fingerprint": binding_fingerprint,
            "connection_registry_fingerprint": registry_fingerprint,
            "credential_runtime_fingerprint": runtime_fingerprint,
            "credential_runtime_environment": "development",
            "credential_runtime_auth_method": "kubernetes",
            "runtime_image_digest": "sha256:" + "1" * 64,
        },
    }
    prepare = payload["runtime_execution"]["temporary_target_prepare"]
    cleanup = payload["runtime_execution"]["temporary_target_cleanup"]
    assert prepare["adapter_metadata"]["applied"] is True
    assert prepare["adapter_metadata"]["server_side_expiry"] is True
    assert cleanup["adapter_metadata"]["applied"] is True
    assert target_statements[0] == "CREATE DATABASE IF NOT EXISTS `dpone_tmp_development`"
    assert target_statements[-1] == "DROP TABLE IF EXISTS `dpone_tmp_development`.`orders_daily_abc123`"
    assert len(seen_credentials) == 3
    assert "must-not-leak" not in repr(payload)
    assert "vault.example.internal" not in repr(payload)
    assert "dpone-runtime-development" not in repr(payload)
    assert "alice@example.test" not in repr(payload)


def test_ops_safe_sample_runtime_run_cli_rejects_runtime_input_fingerprint_mismatch_before_io(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    monkeypatch.setenv("MSSQL_USER", "mssql_runtime")
    monkeypatch.setenv("MSSQL_PASSWORD", "must-not-leak-mssql")
    monkeypatch.setenv("CH_USER", "clickhouse_runtime")
    monkeypatch.setenv("CH_PASSWORD", "must-not-leak-clickhouse")
    cache_root = tmp_path / ".dpone-cache"
    pipeline_source = _pipeline_source(tmp_path / "pipeline.yaml")
    binding_set = _binding_set(tmp_path / "binding-set.json")
    connection_registry = _connection_registry(tmp_path / "connection-registry.json")
    credential_runtime = _credential_runtime(tmp_path / "credential-runtime.json")
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(
            cache_root,
            pipeline_source=pipeline_source,
            proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
            binding_set_ref=_fingerprint(binding_set),
            connection_registry_ref=_fingerprint(connection_registry),
            credential_runtime_ref=_fingerprint(credential_runtime),
        ),
    )
    registry_payload = json.loads(connection_registry.read_text(encoding="utf-8"))
    registry_payload["connections"]["clickhouse_dev"]["connection"]["database"] = "changed_after_promotion"
    _write_json(connection_registry, registry_payload)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(pipeline_source),
                "--enable-live-copy",
                "--binding-set",
                str(binding_set),
                "--connection-registry",
                str(connection_registry),
                "--credential-runtime",
                str(credential_runtime),
                *_route_arguments(tmp_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_CONNECTION_REGISTRY_FINGERPRINT_MISMATCH"
    assert not (tmp_path / "runtime").exists()


def test_ops_safe_sample_runtime_run_cli_requires_credential_runtime_for_live_copy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(cache_root, proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo"),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(_pipeline_source(tmp_path / "pipeline.yaml")),
                "--enable-live-copy",
                "--binding-set",
                str(_binding_set(tmp_path / "binding-set.json")),
                "--connection-registry",
                str(_connection_registry(tmp_path / "connection-registry.json")),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_LIVE_COPY_REQUIRES_RUNTIME_BINDINGS"
    assert "--credential-runtime" in payload["message"]


def test_ops_safe_sample_runtime_run_cli_rejects_live_copy_without_pipeline_source(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(cache_root, proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo"),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--enable-live-copy",
                "--binding-set",
                str(_binding_set(tmp_path / "binding-set.json")),
                "--connection-registry",
                str(_connection_registry(tmp_path / "connection-registry.json")),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_LIVE_COPY_REQUIRES_PIPELINE_SOURCE"


def test_ops_safe_sample_runtime_run_cli_reports_invalid_live_copy_inputs(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(cache_root, proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo"),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(_pipeline_source(tmp_path / "pipeline.yaml")),
                "--enable-live-copy",
                "--binding-set",
                str(tmp_path / "missing-binding-set.json"),
                "--connection-registry",
                str(_connection_registry(tmp_path / "connection-registry.json")),
                "--credential-runtime",
                str(_credential_runtime(tmp_path / "credential-runtime.json")),
                *_route_arguments(tmp_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID"
    assert payload["stage"] == "safe_sample_runtime_run_cli"
    assert "missing-binding-set.json" in payload["message"]
    assert "must-not-leak" not in repr(payload)


def test_ops_safe_sample_runtime_run_cli_validates_credential_runtime_before_live_copy(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch, project_dir=tmp_path)
    cache_root = tmp_path / ".dpone-cache"
    pipeline_source = _pipeline_source(tmp_path / "pipeline.yaml")
    binding_set = _binding_set(tmp_path / "binding-set.json")
    connection_registry = _connection_registry(tmp_path / "connection-registry.json")
    credential_runtime = _credential_runtime(tmp_path / "credential-runtime.json", secret_material=True)
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(
            cache_root,
            pipeline_source=pipeline_source,
            proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
            binding_set_ref=_fingerprint(binding_set),
            connection_registry_ref=_fingerprint(connection_registry),
            credential_runtime_ref=_fingerprint(credential_runtime),
        ),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "safe-sample-runtime-run",
                "--plan-json",
                str(plan_path),
                "--pipeline-source",
                str(pipeline_source),
                "--enable-live-copy",
                "--binding-set",
                str(binding_set),
                "--connection-registry",
                str(connection_registry),
                "--credential-runtime",
                str(credential_runtime),
                *_route_arguments(tmp_path),
                "--cache-root",
                str(cache_root),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID"
    assert "DPONE_CREDENTIAL_RUNTIME_SECRET_MATERIAL_FORBIDDEN" in payload["message"]
    assert "must-not-leak" not in repr(payload)


def test_ops_safe_sample_runtime_run_cli_uses_only_trusted_route_ids_for_production(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    certification_id = "mssql_clickhouse_incremental_merge_airflow_kpo"
    cache_root = tmp_path / ".dpone-cache"
    pipeline_source = _pipeline_source(tmp_path / "pipeline.yaml")
    binding_set = _binding_set(tmp_path / "binding-set.json")
    connection_registry = _connection_registry(tmp_path / "connection-registry.json")
    credential_runtime = _credential_runtime(tmp_path / "credential-runtime.json")
    plan_path = _write_json(
        tmp_path / "plan.json",
        _plan(
            cache_root,
            pipeline_source=pipeline_source,
            proof=f"route_certification:{certification_id}",
            environment="production",
            binding_set_ref=_fingerprint(binding_set),
            connection_registry_ref=_fingerprint(connection_registry),
            credential_runtime_ref=_fingerprint(credential_runtime),
        ),
    )
    arguments = [
        "ops",
        "safe-sample-runtime-run",
        "--plan-json",
        str(plan_path),
        "--pipeline-source",
        str(pipeline_source),
        "--enable-live-copy",
        "--binding-set",
        str(binding_set),
        "--connection-registry",
        str(connection_registry),
        "--credential-runtime",
        str(credential_runtime),
        *_route_arguments(tmp_path),
        "--cache-root",
        str(cache_root),
        "--output-dir",
        str(tmp_path / "runtime"),
        "--format",
        "json",
    ]

    _patch_cli(
        monkeypatch,
        project_dir=tmp_path,
        route_decision="invalid",
        forged_route_ids=(certification_id,),
    )
    with pytest.raises(SystemExit) as unverified:
        cli_main.main(arguments)
    assert unverified.value.code == 4
    assert json.loads(capsys.readouterr().out)["code"] == "DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"

    _patch_cli(monkeypatch, project_dir=tmp_path)
    with pytest.raises(SystemExit) as verified:
        cli_main.main(arguments)
    assert verified.value.code == 2
    assert json.loads(capsys.readouterr().out)["code"] == "DPONE_BINDING_SET_ENVIRONMENT_MISMATCH"
