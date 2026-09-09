from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

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
    SourceSamplingCapabilityDetector,
    TemporaryTargetPlan,
)

_SOURCE_BYTES = b"schema: dpone.pipeline.v1\n"
_SOURCE_SHA256 = "sha256:" + hashlib.sha256(_SOURCE_BYTES).hexdigest()


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
        "artifact_registry_ref": "dpone-dev-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-dev-artifacts"},
        "verify": {"checksums": "required", "attestations": "optional"},
    }


def _execution_plan():
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
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(
                {
                    "id": "orders_daily",
                    "artifact_ref": "cache://releases/sha256-a/packs/orders_daily.airflow-pack.json",
                    "sha256": "sha256:" + "c" * 64,
                    "bytes": 128,
                },
            ),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
            binding_set_ref="sha256:" + "d" * 64,
            connection_registry_ref="sha256:" + "e" * 64,
            credential_runtime_ref="sha256:" + "f" * 64,
            runtime_image_digest="sha256:" + "1" * 64,
            airflow_bundle_ref="git:7ac31f2",
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256=_SOURCE_SHA256,
        ),
    )


def _execution_plan_with_route_certification():
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
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256=_SOURCE_SHA256,
        ),
    )


def _successful_data_copy(
    plan: Any,
    target_plan: TemporaryTargetPlan,
    *,
    rows: int,
    bytes_read: int,
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
        "rows_read": rows,
        "rows_written": rows,
        "bytes_read": bytes_read,
        "pii_policy": "masked",
        "diagnostics": {},
        "errors": [],
    }


def test_safe_sample_runtime_runner_executes_and_writes_evidence(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            calls.append("init_fetch")
            return {
                "schema": "dpone.init-fetch-result.v1",
                "passed": True,
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
            }

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("prepare_target")
            return {"backend": "fake-clickhouse", "token": "must-not-leak"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("cleanup_target")
            return {"backend": "fake-clickhouse", "dropped": True}

    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan(), output_dir=tmp_path / "evidence").to_dict()

    runtime_schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    run_schema = json.loads(Path("docs/schemas/gitops/safe-sample-runtime-run.schema.json").read_text(encoding="utf-8"))
    evidence_path = tmp_path / "evidence" / "safe-sample-runtime-execution.json"
    evidence_bytes = evidence_path.read_bytes()
    evidence_payload = json.loads(evidence_bytes)

    jsonschema.validate(report, run_schema)
    jsonschema.validate(report["runtime_execution"], runtime_schema)
    jsonschema.validate(evidence_payload, runtime_schema)
    assert calls == ["init_fetch", "prepare_target", "cleanup_target"]
    assert report["schema"] == "dpone.safe-sample-runtime-run.v1"
    assert report["execution_status"] == "failed"
    assert report["data_outcome"] == "unknown"
    assert report["release_id"] == "sha256:" + "a" * 64
    assert report["deployment_id"] == "sha256:" + "b" * 64
    assert report["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert report["runtime_execution"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    assert report["evidence_write"]["path"] == "$OUTPUT_ROOT/safe-sample-runtime-execution.json"
    assert report["evidence_write"]["sha256"] == "sha256:" + hashlib.sha256(evidence_bytes).hexdigest()
    assert report["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    assert "must-not-leak" not in evidence_path.read_text(encoding="utf-8")


def test_safe_sample_runtime_runner_uses_registry_backed_certified_copier(tmp_path: Path) -> None:
    from dpone.services.safe_sample_data_copier_registry import (
        RegistryBackedSafeSampleDataCopier,
        SafeSampleDataCopierRegistry,
    )
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            calls.append("init_fetch")
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("prepare_target")
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("cleanup_target")
            return {"backend": "fake-clickhouse", "dropped": True}

    class FakeCertifiedCopier:
        def copy(self, **kwargs: object) -> dict[str, object]:
            calls.append("copy_sample")
            plan = kwargs["plan"]
            target_plan = kwargs["target_plan"]
            assert isinstance(target_plan, TemporaryTargetPlan)
            return {
                **_successful_data_copy(plan, target_plan, rows=5, bytes_read=512),
                "access_token": "must-not-leak",
            }

    registry = SafeSampleDataCopierRegistry.with_copiers(
        {"mssql_clickhouse_incremental_merge_airflow_kpo": FakeCertifiedCopier()}
    )
    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=RegistryBackedSafeSampleDataCopier(registry),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan_with_route_certification(), output_dir=tmp_path / "evidence").to_dict()

    assert calls == ["init_fetch", "prepare_target", "copy_sample", "cleanup_target"]
    assert report["execution_status"] == "succeeded"
    assert report["data_outcome"] == "passed"
    assert report["runtime_execution"]["data_copy"]["status"] == "copied"
    assert report["runtime_execution"]["data_copy"]["rows_written"] == 5
    assert report["errors"] == []
    assert "must-not-leak" not in repr(report)
    assert "must-not-leak" not in (tmp_path / "evidence" / "safe-sample-runtime-execution.json").read_text(
        encoding="utf-8"
    )


def test_safe_sample_runtime_runner_redacts_evidence_writer_failures(tmp_path: Path) -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class FailingEvidenceWriter:
        def write(self, result: Any, output_dir: str | Path) -> Any:
            raise RuntimeError("token=must-not-leak vault password=also-must-not-leak")

    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        ),
        evidence_writer=FailingEvidenceWriter(),
    )

    report = runner.run(_execution_plan(), output_dir=tmp_path / "evidence").to_dict()

    assert report["execution_status"] == "failed"
    assert report["evidence_write"] is None
    assert report["errors"][-1]["code"] == "DPONE_SAFE_SAMPLE_EVIDENCE_WRITE_FAILED"
    assert "details redacted" in report["errors"][-1]["message"]
    assert "must-not-leak" not in repr(report)
    assert "also-must-not-leak" not in repr(report)


def test_safe_sample_runtime_runner_registry_copier_falls_back_closed_without_match(tmp_path: Path) -> None:
    from dpone.services.safe_sample_data_copier_registry import (
        RegistryBackedSafeSampleDataCopier,
        SafeSampleDataCopierRegistry,
    )
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=RegistryBackedSafeSampleDataCopier(SafeSampleDataCopierRegistry.empty()),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan_with_route_certification(), output_dir=tmp_path / "evidence").to_dict()

    assert report["execution_status"] == "failed"
    assert report["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert report["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"


def test_local_fail_closed_safe_sample_runtime_writes_evidence_without_external_io(tmp_path: Path) -> None:
    from dpone.services.safe_sample_cli_runtime import run_local_fail_closed_safe_sample_runtime

    report = run_local_fail_closed_safe_sample_runtime(
        _execution_plan(),
        output_dir=tmp_path / "evidence",
    ).to_dict()

    assert report["schema"] == "dpone.safe-sample-runtime-run.v1"
    assert report["execution_status"] == "failed"
    assert report["runtime_execution"]["init_fetch"]["status"] == "skipped"
    assert report["runtime_execution"]["init_fetch"]["network"] is False
    assert report["runtime_execution"]["temporary_target_prepare"]["adapter_metadata"]["applied"] is False
    assert report["runtime_execution"]["temporary_target_cleanup"]["adapter_metadata"]["applied"] is False
    assert report["runtime_execution"]["deployment_identity"] == {
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "binding_set_ref": "sha256:" + "d" * 64,
        "connection_registry_ref": "sha256:" + "e" * 64,
        "credential_runtime_ref": "sha256:" + "f" * 64,
        "runtime_image_digest": "sha256:" + "1" * 64,
        "airflow_bundle_ref": "git:7ac31f2",
        "workload_packs": [
            {
                "id": "orders_daily",
                "artifact_ref": "cache://releases/sha256-a/packs/orders_daily.airflow-pack.json",
                "sha256": "sha256:" + "c" * 64,
                "bytes": 128,
            }
        ],
        "runtime_artifact_delivery": _init_fetch_delivery(),
    }
    assert report["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert report["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    evidence_path = tmp_path / "evidence" / "safe-sample-runtime-execution.json"
    assert evidence_path.exists()
    evidence_payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence_payload["deployment_identity"]["connection_registry_ref"] == "sha256:" + "e" * 64
    assert "connections" not in repr(evidence_payload)


def test_local_safe_sample_runtime_handoff_executes_pinned_init_fetch_for_runnable_deployment(
    tmp_path: Path,
) -> None:
    from dpone.readiness.safe_sample_runtime_handoff import run_local_safe_sample_runtime_handoff

    pack_bytes = (
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders_daily", "manifest": "pipeline.yaml"},
                "workload_dependencies": [{"kind": "manifest", "path": "pipeline.yaml", "sha256": _SOURCE_SHA256}],
            },
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    pack_sha256 = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    release_dir = ("sha256:" + "a" * 64).replace(":", "-")
    artifact_ref = f"cache://releases/{release_dir}/packs/orders_daily.airflow-pack.json"
    registry_pack = tmp_path / ".dpone-cache" / "releases" / release_dir / "packs" / "orders_daily.airflow-pack.json"
    registry_pack.parent.mkdir(parents=True)
    registry_pack.write_bytes(pack_bytes)
    plan = _execution_plan_with_workload_pack(
        artifact_ref=artifact_ref,
        sha256=pack_sha256,
        artifact_bytes=len(pack_bytes),
    )

    report = run_local_safe_sample_runtime_handoff(
        plan,
        output_dir=tmp_path / "evidence",
        cache_root=tmp_path / ".dpone-cache",
    ).to_dict()

    init_fetch = report["runtime_execution"]["init_fetch"]
    assert init_fetch["schema"] == "dpone.init-fetch-result.v1"
    assert init_fetch["passed"] is True
    assert init_fetch["artifacts"][0]["artifact_ref"] == artifact_ref
    assert (tmp_path / "evidence" / "runtime-artifacts" / "init-fetch-manifest.json").exists()
    assert (
        tmp_path
        / "evidence"
        / "runtime-artifacts"
        / "releases"
        / release_dir
        / "packs"
        / "orders_daily.airflow-pack.json"
    ).exists()
    assert report["runtime_execution"]["data_copy"]["status"] == "blocked"
    assert report["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"


def test_local_safe_sample_runtime_handoff_does_not_accept_route_receipt_instead_of_source_pin(
    tmp_path: Path,
) -> None:
    from dpone.readiness.safe_sample_pinned_source import PinnedWorkloadSourceError
    from dpone.readiness.safe_sample_runtime_handoff import run_local_safe_sample_runtime_handoff

    pack_bytes = (
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders_daily", "manifest": "pipeline.yaml"},
                "workload_dependencies": [{"kind": "manifest", "path": "pipeline.yaml", "sha256": _SOURCE_SHA256}],
            },
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    release_dir = ("sha256:" + "a" * 64).replace(":", "-")
    artifact_ref = f"cache://releases/{release_dir}/packs/orders_daily.airflow-pack.json"
    registry_pack = tmp_path / ".dpone-cache" / "releases" / release_dir / "packs" / "orders_daily.airflow-pack.json"
    registry_pack.parent.mkdir(parents=True)
    registry_pack.write_bytes(pack_bytes)
    plan = _execution_plan_with_workload_pack(
        artifact_ref=artifact_ref,
        sha256="sha256:" + hashlib.sha256(pack_bytes).hexdigest(),
        artifact_bytes=len(pack_bytes),
    )
    plan = replace(
        plan,
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="other.yaml",
            sha256=_SOURCE_SHA256,
        ),
    )
    output_dir = tmp_path / "evidence"

    with pytest.raises(PinnedWorkloadSourceError) as exc:
        run_local_safe_sample_runtime_handoff(
            plan,
            output_dir=output_dir,
            cache_root=tmp_path / ".dpone-cache",
            route_attestation_verification={
                "schema": "dpone.route-attestation-verification.v1",
                "decision": "verified",
            },
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH"
    assert not output_dir.exists()


def _execution_plan_with_workload_pack(*, artifact_ref: str, sha256: str, artifact_bytes: int):
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
        temporary_target_plan=replace(_target_plan(), process="extract_orders"),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(
                {
                    "id": "orders_daily",
                    "artifact_ref": artifact_ref,
                    "sha256": sha256,
                    "bytes": artifact_bytes,
                },
            ),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256=_SOURCE_SHA256,
        ),
    )
