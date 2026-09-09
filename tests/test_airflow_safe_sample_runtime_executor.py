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
    TemporaryTargetPlan,
)

_INIT_FETCH_RELEASE_DIR = "sha256-" + "a" * 64
_CERTIFICATION_ID = "mssql_clickhouse_incremental_merge_airflow_kpo"
_CERTIFICATION_PROOF = f"route_certification:{_CERTIFICATION_ID}"
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
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            proof=_CERTIFICATION_PROOF,
        ),
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


def _execution_plan_with_pack(artifact_ref: str, sha256: str, declared_bytes: int):
    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            proof=_CERTIFICATION_PROOF,
        ),
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
                    "artifact_ref": artifact_ref,
                    "sha256": sha256,
                    "bytes": declared_bytes,
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


def _execution_plan_with_pack_and_delivery(
    artifact_ref: str,
    sha256: str,
    declared_bytes: int,
    delivery: dict[str, object],
):
    policy = SafeSamplePolicySet.default().for_environment("development")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development"),
        policy,
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            proof=_CERTIFICATION_PROOF,
        ),
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
            runtime_artifact_delivery=delivery,
            workload_packs=(
                {
                    "id": "orders_daily",
                    "artifact_ref": artifact_ref,
                    "sha256": sha256,
                    "bytes": declared_bytes,
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


def _runtime_pack_bytes() -> bytes:
    return (
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders_daily", "manifest": "pipeline.yaml"},
                "workload_dependencies": [
                    {
                        "kind": "manifest",
                        "path": "pipeline.yaml",
                        "sha256": _SOURCE_SHA256,
                    }
                ],
            },
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def _data_copy_result(**overrides: object) -> dict[str, object]:
    plan = _execution_plan()
    target_plan = plan.temporary_target_plan
    assert target_plan is not None
    from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder

    result: dict[str, object] = {
        "schema": "dpone.safe-sample-data-copy.v1",
        "status": "copied",
        "source_request": SafeSampleSourceRequestBuilder().build(plan.policy_result, target_plan).to_dict(),
        "copy_request": {
            "schema": "dpone.safe-sample-certified-copy-request.v1",
            "certification_id": _CERTIFICATION_ID,
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
            "pii_policy": "masked",
            "proof": plan.policy_result.capabilities.proof,
        },
        "rows_read": 10,
        "rows_written": 10,
        "bytes_read": 1024,
        "pii_policy": "masked",
        "diagnostics": {},
        "errors": [],
    }
    result.update(overrides)
    return result


def _execute_with_data_copy(data_copy: dict[str, object]) -> dict[str, Any]:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            del plan
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            return {"backend": "fake-clickhouse", "dropped": True}

    class StaticDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            del plan, target_plan, init_fetch
            return dict(data_copy)

    return (
        SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=StaticDataCopier(),
        )
        .execute(_execution_plan())
        .to_dict()
    )


def test_safe_sample_runtime_executor_fetches_prepares_and_cleans_without_source_io(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            calls.append("init_fetch")
            assert plan.to_dict()["artifact_pinning"]["release_id"] == "sha256:" + "a" * 64
            return {
                "schema": "dpone.init-fetch-result.v1",
                "passed": True,
                "release_id": "sha256:" + "a" * 64,
                "deployment_id": "sha256:" + "b" * 64,
                "artifacts": [],
            }

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("prepare_target")
            return {"backend": "fake-clickhouse", "password": "must-not-leak"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("cleanup_target")
            return {"backend": "fake-clickhouse", "dropped": True}

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
    ).execute(_execution_plan())

    payload = result.to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, schema)
    assert calls == ["init_fetch", "prepare_target", "cleanup_target"]
    assert payload["schema"] == "dpone.safe-sample-runtime-execution.v1"
    assert payload["execution_mode"] == "local_handoff"
    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["release_id"] == "sha256:" + "a" * 64
    assert payload["deployment_id"] == "sha256:" + "b" * 64
    assert payload["temporary_target_prepare"]["status"] == "prepared"
    assert payload["data_copy"]["status"] == "blocked"
    assert payload["data_copy"]["source_request"] == {
        "schema": "dpone.safe-sample-source-request.v1",
        "status": "planned",
        "mode": "pushdown",
        "sample_rows": 1000,
        "max_bytes": 10 * 1024**3,
        "timeout_seconds": 300,
        "source_read_only": True,
        "full_scan_allowed": True,
        "estimated_read_bytes": None,
        "proof": _CERTIFICATION_PROOF,
        "pii_policy": "masked",
        "target": {
            "mode": "temporary",
            "connection_ref": "clickhouse_dev",
            "temporary_table": {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"},
            "ttl_seconds": 86400,
        },
        "errors": [],
    }
    assert payload["data_copy"]["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    assert payload["temporary_target_cleanup"]["status"] == "cleaned"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"
    assert "must-not-leak" not in repr(payload)


@pytest.mark.parametrize(
    ("snapshot", "code"),
    [
        (None, "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING"),
        (
            SafeSampleSourceSnapshot(
                pipeline_id="other_pipeline",
                path="pipeline.yaml",
                sha256=_SOURCE_SHA256,
            ),
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
        ),
    ],
)
def test_safe_sample_runtime_executor_rejects_invalid_source_snapshot_before_side_effects(
    snapshot: SafeSampleSourceSnapshot | None,
    code: str,
) -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class UnexpectedArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            del plan
            calls.append("init_fetch")
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class UnexpectedTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            calls.append("prepare_target")
            return {}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            del plan
            calls.append("cleanup_target")
            return {}

    plan = replace(_execution_plan(), source_snapshot=snapshot)
    payload = (
        SafeSampleRuntimeExecutor(
            artifact_fetcher=UnexpectedArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=UnexpectedTargetAdapter()),
        )
        .execute(plan)
        .to_dict()
    )

    assert calls == []
    assert payload["execution_status"] == "failed"
    assert payload["init_fetch"] is None
    assert payload["temporary_target_prepare"] is None
    assert payload["temporary_target_cleanup"] is None
    assert payload["errors"][0]["code"] == code


def test_safe_sample_runtime_executor_accepts_only_verified_redacted_route_receipt() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    receipt = {
        "schema": "dpone.route-attestation-verification.v1",
        "decision": "verified",
        "code": "DPONE_ROUTE_ATTESTATION_VERIFIED",
        "message": "verified",
        "route_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
        "vault_token": "must-not-leak",
    }
    executor = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        route_attestation_verification=receipt,
    )

    payload = executor.execute(_execution_plan()).to_dict()

    assert payload["execution_mode"] == "live_copy"
    assert payload["route_attestation_verification"]["decision"] == "verified"
    assert "vault_token" not in payload["route_attestation_verification"]
    assert "must-not-leak" not in repr(payload)

    with pytest.raises(ValueError, match="DPONE_ROUTE_ATTESTATION_VERIFIED"):
        SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            route_attestation_verification={**receipt, "decision": "invalid"},
        )


def test_safe_sample_runtime_executor_uses_injected_data_copy_boundary(tmp_path: Path) -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
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

    class FakeDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            calls.append("copy_sample")
            assert target_plan.temporary_table == {"schema": "dpone_tmp_development", "name": "orders_daily_abc123"}
            assert init_fetch["passed"] is True
            return _data_copy_result(password="must-not-leak")

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=FakeDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert calls == ["init_fetch", "prepare_target", "copy_sample", "cleanup_target"]
    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "passed"
    assert payload["data_copy"]["status"] == "copied"
    assert payload["data_copy"]["rows_written"] == 10
    assert payload["errors"] == []
    assert "must-not-leak" not in repr(payload)


@pytest.mark.parametrize("status", ["failed", "blocked"])
def test_safe_sample_runtime_executor_fails_closed_when_terminal_copy_omits_errors(status: str) -> None:
    payload = _execute_with_data_copy(_data_copy_result(status=status, rows_read=0, rows_written=0))

    assert payload["schema"] == "dpone.safe-sample-runtime-execution.v1"
    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["data_copy"]["status"] == status
    assert payload["errors"] == payload["data_copy"]["errors"]
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"
    assert len(payload["errors"][0]["message"]) <= 500


@pytest.mark.parametrize(
    ("include_status", "status"),
    [
        pytest.param(False, None, id="missing"),
        pytest.param(True, "copied token=must-not-leak", id="malformed"),
        pytest.param(True, ["copied"], id="non-string"),
    ],
)
def test_safe_sample_runtime_executor_fails_closed_for_missing_or_malformed_copy_status(
    include_status: bool,
    status: object,
) -> None:
    data_copy = _data_copy_result()
    if include_status:
        data_copy["status"] = status
    else:
        data_copy.pop("status")

    payload = _execute_with_data_copy(data_copy)

    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["data_copy"]["status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"
    assert "must-not-leak" not in repr(payload)


@pytest.mark.parametrize(
    "missing_field",
    ("source_request", "copy_request", "diagnostics", "pii_policy"),
)
def test_safe_sample_runtime_executor_requires_complete_success_envelope(
    missing_field: str,
) -> None:
    data_copy = _data_copy_result()
    data_copy.pop(missing_field)

    payload = _execute_with_data_copy(data_copy)

    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["data_copy"]["status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


def test_safe_sample_runtime_executor_requires_exact_success_schema_identity() -> None:
    payload = _execute_with_data_copy(_data_copy_result(schema="dpone.safe-sample-data-copy.v2"))

    assert payload["execution_status"] == "failed"
    assert payload["data_copy"]["schema"] == "dpone.safe-sample-data-copy.v1"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


@pytest.mark.parametrize("request_field", ("source_request", "copy_request"))
def test_safe_sample_runtime_executor_reconciles_success_request_row_budgets(
    request_field: str,
) -> None:
    data_copy = _data_copy_result()
    request = dict(data_copy[request_field])
    request["sample_rows"] = 999
    data_copy[request_field] = request

    payload = _execute_with_data_copy(data_copy)

    assert payload["execution_status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


@pytest.mark.parametrize(
    "data_copy",
    [
        pytest.param(_data_copy_result(rows_read=1001, rows_written=1001), id="row-budget-exceeded"),
        pytest.param(_data_copy_result(bytes_read=10 * 1024**3 + 1), id="byte-budget-exceeded"),
        pytest.param(_data_copy_result(rows_read=10, rows_written=9), id="copy-count-mismatch"),
        pytest.param(
            _data_copy_result(diagnostics={"source": {"rows_returned": 11}}),
            id="source-count-mismatch",
        ),
        pytest.param(
            _data_copy_result(diagnostics={"sink": {"rows_sent": 9}}),
            id="target-count-mismatch",
        ),
        pytest.param(
            _data_copy_result(diagnostics={"source": {"client": {"rows_returned": 11}}}),
            id="production-source-client-count-mismatch",
        ),
        pytest.param(
            _data_copy_result(diagnostics={"sink": {"client": {"rows_sent": 9}}}),
            id="production-sink-client-count-mismatch",
        ),
    ],
)
def test_safe_sample_runtime_executor_fails_closed_for_budget_or_count_inconsistency(
    data_copy: dict[str, object],
) -> None:
    payload = _execute_with_data_copy(data_copy)

    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["data_copy"]["status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


def test_safe_sample_runtime_executor_accepts_matching_production_client_counters() -> None:
    payload = _execute_with_data_copy(
        _data_copy_result(
            diagnostics={
                "source": {"client": {"client": "dbapi_mssql", "rows_returned": 10}},
                "sink": {"client": {"client": "clickhouse_connection", "rows_sent": 10}},
            }
        )
    )

    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "passed"
    assert payload["errors"] == []


@pytest.mark.parametrize(
    "data_copy",
    (
        pytest.param(_data_copy_result(quarantined_rows=1), id="copied-nonzero-quarantine"),
        pytest.param(
            _data_copy_result(status="copied_with_quarantine", rows_written=10),
            id="quarantine-count-missing",
        ),
        pytest.param(
            _data_copy_result(status="copied_with_quarantine", quarantined_rows=0),
            id="quarantine-count-zero",
        ),
        pytest.param(
            _data_copy_result(status="copied_with_quarantine", quarantined_rows=True),
            id="quarantine-count-boolean",
        ),
        pytest.param(
            _data_copy_result(
                status="copied_with_quarantine",
                rows_written=8,
                quarantined_rows=1,
            ),
            id="quarantine-row-reconciliation-mismatch",
        ),
    ),
)
def test_safe_sample_runtime_executor_rejects_contradictory_quarantine_counts(
    data_copy: dict[str, object],
) -> None:
    payload = _execute_with_data_copy(data_copy)

    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


@pytest.mark.parametrize(
    "unsafe_diagnostic",
    (
        pytest.param(object(), id="unsupported-object"),
        pytest.param(float("nan"), id="nan"),
        pytest.param(float("inf"), id="positive-infinity"),
        pytest.param(float("-inf"), id="negative-infinity"),
    ),
)
def test_safe_sample_runtime_executor_rejects_non_json_diagnostics(
    unsafe_diagnostic: object,
) -> None:
    payload = _execute_with_data_copy(_data_copy_result(diagnostics={"unsafe": unsafe_diagnostic}))

    assert payload["execution_status"] == "failed"
    assert payload["data_copy"]["diagnostics"] == {}
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"
    json.dumps(payload, allow_nan=False)


def test_safe_sample_runtime_executor_rejects_plan_policy_row_budget_mismatch_before_io() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class ExplodingArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            calls.append("init_fetch")
            raise AssertionError("init-fetch must not run for a contradictory row budget")

    class ExplodingTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("prepare_target")
            raise AssertionError("target prepare must not run for a contradictory row budget")

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("cleanup_target")
            raise AssertionError("target cleanup must not run when prepare did not run")

    plan = _execution_plan()
    forged_policy_result = replace(
        plan.policy_result,
        request=replace(plan.policy_result.request, sample_rows=plan.sample_rows - 1),
    )
    forged_plan = replace(plan, policy_result=forged_policy_result)

    payload = (
        SafeSampleRuntimeExecutor(
            artifact_fetcher=ExplodingArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=ExplodingTargetAdapter()),
        )
        .execute(forged_plan)
        .to_dict()
    )

    assert calls == []
    assert payload["execution_status"] == "failed"
    assert payload["data_copy"] is None
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID"


def test_safe_sample_runtime_executor_maps_quarantine_copy_outcome() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class QuarantineDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return _data_copy_result(
                status="copied_with_quarantine",
                rows_written=9,
                quarantined_rows=1,
            )

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=QuarantineDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, schema)
    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "passed_with_quarantine"
    assert payload["data_copy"]["status"] == "copied_with_quarantine"
    assert payload["data_copy"]["quarantined_rows"] == 1
    assert payload["errors"] == []


def test_safe_sample_runtime_executor_maps_empty_copy_to_no_data() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class EmptyDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return _data_copy_result(rows_read=0, rows_written=0, bytes_read=0)

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=EmptyDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, schema)
    assert payload["execution_status"] == "succeeded"
    assert payload["data_outcome"] == "no_data"
    assert payload["data_copy"]["status"] == "copied"
    assert payload["data_copy"]["rows_read"] == 0
    assert payload["errors"] == []


def test_safe_sample_runtime_executor_maps_quality_gate_failure_outcome() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class QualityGateDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "failed",
                "rows_read": 10,
                "rows_written": 0,
                "bytes_read": 1024,
                "pii_policy": "masked",
                "errors": [
                    {
                        "schema": "dpone.error.v1",
                        "code": "DPONE_SAFE_SAMPLE_QUALITY_GATE_FAILED",
                        "stage": "certified_data_copier",
                        "severity": "error",
                        "message": "Safe sample quality gate failed.",
                        "fixes": [],
                    }
                ],
            }

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=QualityGateDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-runtime-execution.schema.json").read_text(encoding="utf-8")
    )
    jsonschema.validate(payload, schema)
    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "failed_quality_gate"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_QUALITY_GATE_FAILED"


def test_safe_sample_runtime_executor_reports_cleanup_failure_after_successful_copy() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class LeakingCleanupAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            raise RuntimeError("temporary cleanup token=must-not-leak")

    class SuccessfulDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return _data_copy_result(rows_read=3, rows_written=3, bytes_read=256)

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=LeakingCleanupAdapter()),
        data_copier=SuccessfulDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "passed"
    assert payload["data_copy"]["status"] == "copied"
    assert payload["temporary_target_cleanup"]["status"] == "cleanup_failed"
    assert payload["errors"][0]["code"] == "DPONE_RUNTIME_TEMPORARY_TARGET_DROP_FAILED"
    assert "details redacted" in payload["errors"][0]["message"]
    assert "must-not-leak" not in repr(payload)


def test_safe_sample_runtime_executor_cleans_target_after_copy_exception() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
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

    class FailingDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            calls.append("copy_sample")
            raise RuntimeError("copy failed password=must-not-leak")

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=FailingDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert calls == ["init_fetch", "prepare_target", "copy_sample", "cleanup_target"]
    assert payload["execution_status"] == "failed"
    assert payload["data_outcome"] == "unknown"
    assert payload["temporary_target_cleanup"]["status"] == "cleaned"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_RUNTIME_FAILED"
    assert "must-not-leak" not in repr(payload)


def test_safe_sample_runtime_executor_redacts_secret_like_exception_messages() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class ExplodingDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            raise RuntimeError("password=must-not-leak token=also-must-not-leak")

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=ExplodingDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert payload["execution_status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_RUNTIME_FAILED"
    assert "details redacted" in payload["errors"][0]["message"]
    assert "must-not-leak" not in repr(payload)
    assert "also-must-not-leak" not in repr(payload)


def test_safe_sample_runtime_executor_redacts_secret_like_returned_error_messages() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class ReturningLeakyErrorDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "failed",
                "rows_read": 0,
                "rows_written": 0,
                "bytes_read": 0,
                "pii_policy": "masked",
                "errors": [
                    {
                        "schema": "dpone.error.v1",
                        "code": "DPONE_SAFE_SAMPLE_SOURCE_READ_FAILED",
                        "stage": "certified_data_copier",
                        "severity": "error",
                        "message": "MSSQL read failed with password=must-not-leak token=also-must-not-leak",
                        "fixes": [],
                    }
                ],
            }

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=ReturningLeakyErrorDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert payload["execution_status"] == "failed"
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_SOURCE_READ_FAILED"
    assert "details redacted" in payload["errors"][0]["message"]
    assert "details redacted" in payload["data_copy"]["errors"][0]["message"]
    assert "must-not-leak" not in repr(payload)
    assert "also-must-not-leak" not in repr(payload)


def test_safe_sample_runtime_executor_redacts_secret_like_list_diagnostics() -> None:
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    class ReturningLeakyDiagnosticListDataCopier:
        def copy(
            self, *, plan: Any, target_plan: TemporaryTargetPlan, init_fetch: dict[str, object]
        ) -> dict[str, object]:
            return {
                "schema": "dpone.safe-sample-data-copy.v1",
                "status": "failed",
                "rows_read": 0,
                "rows_written": 0,
                "bytes_read": 0,
                "pii_policy": "masked",
                "diagnostics": {
                    "attempts": [
                        "resolver failed with token=must-not-leak",
                        {"detail": "retry used password=also-must-not-leak"},
                    ]
                },
                "errors": [],
            }

    result = SafeSampleRuntimeExecutor(
        artifact_fetcher=FakeArtifactFetcher(),
        temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
        data_copier=ReturningLeakyDiagnosticListDataCopier(),
    ).execute(_execution_plan())

    payload = result.to_dict()
    assert "details redacted" in repr(payload["data_copy"]["diagnostics"])
    assert "must-not-leak" not in repr(payload)
    assert "also-must-not-leak" not in repr(payload)


def test_safe_sample_init_fetch_adapter_executes_pinned_runtime_fetch(tmp_path: Path) -> None:
    from dpone.runtime.safe_sample_init_fetch import SafeSampleInitFetchArtifactFetcher

    pack_bytes = _runtime_pack_bytes()
    pack_sha256 = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    artifact_ref = f"cache://releases/{_INIT_FETCH_RELEASE_DIR}/packs/orders_daily.airflow-pack.json"
    registry_pack = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "orders_daily.airflow-pack.json"
    )
    registry_pack.parent.mkdir(parents=True)
    registry_pack.write_bytes(pack_bytes)

    result = SafeSampleInitFetchArtifactFetcher(
        registry_root=tmp_path / "registry",
        destination_root=tmp_path / "runtime-artifacts",
    ).fetch(_execution_plan_with_pack(artifact_ref, pack_sha256, len(pack_bytes)))

    assert result["schema"] == "dpone.init-fetch-result.v1"
    assert result["passed"] is True
    assert result["release_id"] == "sha256:" + "a" * 64
    assert result["deployment_id"] == "sha256:" + "b" * 64
    assert result["artifact_registry_ref"] == "dpone-dev-artifacts"
    assert result["artifacts"][0]["artifact_ref"] == artifact_ref
    assert result["artifacts"][0]["bytes"] == len(pack_bytes)
    assert (tmp_path / "runtime-artifacts" / "init-fetch-manifest.json").exists()
    assert (
        tmp_path
        / "runtime-artifacts"
        / "releases"
        / _INIT_FETCH_RELEASE_DIR
        / "packs"
        / "orders_daily.airflow-pack.json"
    ).exists()


@pytest.mark.parametrize(
    ("verify", "message"),
    (
        ({"checksums": "optional", "attestations": "optional"}, "checksums must be required"),
        ({"checksums": "required", "attestations": "disabled"}, "attestations must be optional or required_for_prod"),
    ),
)
def test_safe_sample_init_fetch_adapter_rejects_unsafe_verification_policy(
    tmp_path: Path,
    verify: dict[str, object],
    message: str,
) -> None:
    from dpone.runtime.safe_sample_init_fetch import SafeSampleInitFetchArtifactFetcher

    pack_bytes = _runtime_pack_bytes()
    pack_sha256 = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    artifact_ref = f"cache://releases/{_INIT_FETCH_RELEASE_DIR}/packs/orders_daily.airflow-pack.json"
    registry_pack = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "orders_daily.airflow-pack.json"
    )
    registry_pack.parent.mkdir(parents=True)
    registry_pack.write_bytes(pack_bytes)
    delivery = dict(_init_fetch_delivery())
    delivery["verify"] = verify

    with pytest.raises(ValueError, match=message):
        SafeSampleInitFetchArtifactFetcher(
            registry_root=tmp_path / "registry",
            destination_root=tmp_path / "runtime-artifacts",
        ).fetch(_execution_plan_with_pack_and_delivery(artifact_ref, pack_sha256, len(pack_bytes), delivery))

    assert not (tmp_path / "runtime-artifacts").exists()


def test_safe_sample_init_fetch_adapter_rejects_declared_size_mismatch(
    tmp_path: Path,
) -> None:
    from dpone.runtime.artifact_delivery import InitFetchError
    from dpone.runtime.safe_sample_init_fetch import SafeSampleInitFetchArtifactFetcher

    pack_bytes = _runtime_pack_bytes()
    pack_sha256 = "sha256:" + hashlib.sha256(pack_bytes).hexdigest()
    artifact_ref = f"cache://releases/{_INIT_FETCH_RELEASE_DIR}/packs/orders_daily.airflow-pack.json"
    registry_pack = (
        tmp_path / "registry" / "releases" / _INIT_FETCH_RELEASE_DIR / "packs" / "orders_daily.airflow-pack.json"
    )
    registry_pack.parent.mkdir(parents=True)
    registry_pack.write_bytes(pack_bytes)

    with pytest.raises(InitFetchError) as exc:
        SafeSampleInitFetchArtifactFetcher(
            registry_root=tmp_path / "registry",
            destination_root=tmp_path / "runtime-artifacts",
        ).fetch(_execution_plan_with_pack(artifact_ref, pack_sha256, len(pack_bytes) + 1))

    assert exc.value.code == "DPONE_CACHE_ARTIFACT_SIZE_MISMATCH"
    assert not (tmp_path / "runtime-artifacts").exists()
