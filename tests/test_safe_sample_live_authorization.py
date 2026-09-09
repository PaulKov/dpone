from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.services.safe_sample_execution_plan import AirflowDeploymentContext, SafeSampleExecutionPlanBuilder
from dpone.services.safe_sample_live_authorization import (
    SafeSampleLiveAuthorizationError,
    SafeSampleLiveExecutionAuthorizer,
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


def _pipeline() -> dict[str, object]:
    return {
        "processes": [
            {
                "name": "orders_daily",
                "source": {"type": "mssql", "connection_ref": "mssql_dev"},
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                    "strategy": {"mode": "incremental_merge"},
                },
            }
        ]
    }


def _plan(environment: str = "development"):
    policy = SafeSamplePolicySet.default().for_environment(environment)
    result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment=environment),
        policy,
        SampleSourceCapabilities(
            supports_pushdown_sampling=True,
            full_scan_required=False,
            estimated_read_bytes=1024,
            proof=f"route_certification:{_CERTIFICATION_ID}",
            mode="pushdown",
        ),
    )
    if environment == "production":
        result = replace(result, passed=True, errors=())
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment=environment,
        policy_result=result,
        temporary_target_plan=TemporaryTargetPlan(
            mode="temporary",
            pipeline_id="orders_daily",
            process="orders_daily",
            sink_type="clickhouse",
            connection_ref="clickhouse_dev",
            original_table={"schema": "analytics", "name": "orders"},
            temporary_table={"schema": "dpone_tmp_dev", "name": "orders_daily_sample"},
            ttl_seconds=3600,
            cleanup_required=True,
            pii_policy="masked",
        ),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            environment="prod" if environment == "production" else "dev",
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery={
                "mode": "init_fetch",
                "artifact_registry_ref": "dpone-artifacts",
                "identity": {
                    "method": "kubernetes_workload_identity",
                    "service_account": "dpone-runtime",
                },
                "source": {"artifact_registry_ref": "dpone-artifacts"},
                "verify": {"checksums": "required", "attestations": "optional"},
            },
            workload_packs=({"id": "orders_daily", "sha256": "sha256:" + "c" * 64},),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
    )


def test_live_authorizer_recomputes_development_policy_from_pipeline() -> None:
    authorized = SafeSampleLiveExecutionAuthorizer().authorize(_plan(), pipeline_source=_pipeline())

    assert authorized.runnable is True
    assert authorized.policy_result.passed is True
    assert authorized.policy_result.capabilities.proof == f"route_certification:{_CERTIFICATION_ID}"


def test_live_authorizer_rejects_forged_production_policy_result() -> None:
    with pytest.raises(SafeSampleLiveAuthorizationError) as exc:
        SafeSampleLiveExecutionAuthorizer().authorize(_plan("production"), pipeline_source=_pipeline())

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_POLICY_NOT_AUTHORIZED"


def test_live_authorizer_accepts_only_injected_production_verification() -> None:
    authorized = SafeSampleLiveExecutionAuthorizer().authorize(
        _plan("production"),
        pipeline_source=_pipeline(),
        verified_route_ids=(_CERTIFICATION_ID,),
    )

    assert authorized.runnable is True
    assert authorized.policy_result.capabilities.production_proof_verified is True


def test_live_authorizer_rejects_serialized_request_mismatch() -> None:
    plan = _plan()
    forged_result = replace(
        plan.policy_result,
        request=replace(plan.policy_result.request, sample_rows=999),
    )

    with pytest.raises(SafeSampleLiveAuthorizationError) as exc:
        SafeSampleLiveExecutionAuthorizer().authorize(
            replace(plan, policy_result=forged_result),
            pipeline_source=_pipeline(),
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID"


@pytest.mark.parametrize(
    ("field", "unsafe_value"),
    (
        ("mode", "persistent"),
        ("cleanup_required", False),
        ("pii_policy", "not_logged"),
    ),
)
def test_live_authorizer_rejects_unsafe_temporary_target_contract(
    field: str,
    unsafe_value: object,
) -> None:
    plan = _plan()
    assert plan.temporary_target_plan is not None
    forged_target = replace(plan.temporary_target_plan, **{field: unsafe_value})

    with pytest.raises(SafeSampleLiveAuthorizationError) as exc:
        SafeSampleLiveExecutionAuthorizer().authorize(
            replace(plan, temporary_target_plan=forged_target),
            pipeline_source=_pipeline(),
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID"


def test_live_authorizer_rejects_pipeline_target_mismatch() -> None:
    plan = _plan()
    assert plan.temporary_target_plan is not None
    forged_target = replace(plan.temporary_target_plan, connection_ref="other_clickhouse")

    with pytest.raises(SafeSampleLiveAuthorizationError) as exc:
        SafeSampleLiveExecutionAuthorizer().authorize(
            replace(plan, temporary_target_plan=forged_target),
            pipeline_source=_pipeline(),
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_TEMPORARY_TARGET_CONNECTION_MISMATCH"
