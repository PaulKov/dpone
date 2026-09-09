from __future__ import annotations

import pytest

from dpone.services.safe_sample_policy import (
    SafeSamplePlanError,
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleSourceCapabilities,
    SampleTarget,
    SourceSamplingCapabilityDetector,
    TemporaryTargetPlanner,
    certified_sampling_routes,
)

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


def test_production_sample_policy_requires_pushdown_and_budget() -> None:
    policy = SafeSamplePolicySet.default().for_environment("production")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production")
    capabilities = SampleSourceCapabilities(
        supports_pushdown_sampling=False,
        full_scan_required=True,
        estimated_read_bytes=2 * 1024**3,
    )

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert result.passed is False
    assert [error["code"] for error in result.errors] == [
        "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED",
        "DPONE_SECURITY_SAMPLE_FULL_SCAN_FORBIDDEN",
        "DPONE_SECURITY_SAMPLE_BUDGET_EXCEEDED",
    ]
    assert result.to_dict()["policy"]["max_bytes"] == 1024**3


def test_development_sample_policy_allows_bounded_full_scan() -> None:
    policy = SafeSamplePolicySet.default().for_environment("development")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development")
    capabilities = SampleSourceCapabilities(
        supports_pushdown_sampling=False,
        full_scan_required=True,
        estimated_read_bytes=9 * 1024**3,
    )

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert result.passed is True
    assert result.errors == ()
    assert result.to_dict()["policy"]["allow_full_scan"] is True


def test_development_sample_policy_rejects_unbounded_full_scan() -> None:
    policy = SafeSamplePolicySet.default().for_environment("development")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development")
    capabilities = SampleSourceCapabilities(
        supports_pushdown_sampling=False,
        full_scan_required=True,
        estimated_read_bytes=None,
    )

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert result.passed is False
    assert [error["code"] for error in result.errors] == ["DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED"]


def test_development_sample_policy_rejects_non_positive_full_scan_budget() -> None:
    policy = SafeSamplePolicySet.default().for_environment("development")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="development")
    capabilities = SampleSourceCapabilities(
        supports_pushdown_sampling=False,
        full_scan_required=True,
        estimated_read_bytes=0,
    )

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert result.passed is False
    assert [error["code"] for error in result.errors] == ["DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED"]


def test_sample_policy_rejects_non_positive_sample_size() -> None:
    policy = SafeSamplePolicySet.default().for_environment("development")
    request = SampleRunRequest(sample_rows=0, target=SampleTarget.TEMPORARY, environment="development")

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, SampleSourceCapabilities.unknown())

    assert result.passed is False
    assert [error["code"] for error in result.errors] == ["DPONE_RUNTIME_SAMPLE_SIZE_INVALID"]


def test_source_sampling_capability_detector_uses_explicit_pushdown_proof() -> None:
    pipeline = {
        "processes": [
            {
                "source": {
                    "type": "mssql",
                    "sampling": {
                        "mode": "pushdown",
                        "proof": "connector_capability",
                        "estimated_read_bytes": 1024 * 1024,
                    },
                }
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)

    assert capabilities == SampleSourceCapabilities(
        supports_pushdown_sampling=True,
        full_scan_required=False,
        estimated_read_bytes=1024 * 1024,
        proof="connector_capability",
        mode="pushdown",
    )


def test_source_sampling_capability_detector_rejects_limit_only_pushdown_proof() -> None:
    pipeline = {
        "processes": [
            {
                "source": {
                    "type": "postgres",
                    "sampling": {
                        "mode": "pushdown",
                        "proof": "SELECT * FROM orders LIMIT 1000",
                        "estimated_read_bytes": 1024,
                    },
                }
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)

    assert capabilities == SampleSourceCapabilities(
        supports_pushdown_sampling=False,
        full_scan_required=None,
        estimated_read_bytes=1024,
        proof="SELECT * FROM orders LIMIT 1000",
        mode="pushdown",
    )


@pytest.mark.parametrize("source_type", _MSSQL_ALIASES)
def test_source_sampling_capability_detector_uses_certified_route_proof(source_type: str) -> None:
    pipeline = {
        "processes": [
            {
                "source": {"type": source_type},
                "sink": {
                    "type": "clickhouse",
                    "strategy": {"mode": "incremental_merge"},
                },
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)

    assert capabilities == SampleSourceCapabilities(
        supports_pushdown_sampling=True,
        full_scan_required=False,
        estimated_read_bytes=1024 * 1024,
        proof="route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        mode="pushdown",
    )
    assert capabilities.production_proof_verified is False


def test_static_route_catalog_cannot_authorize_production_sample() -> None:
    pipeline = {
        "processes": [
            {
                "source": {"type": "mssql"},
                "sink": {
                    "type": "clickhouse",
                    "strategy": {"mode": "incremental_merge"},
                },
            }
        ]
    }
    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production")
    policy = SafeSamplePolicySet.default().for_environment("production")

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert result.passed is False
    assert [error["code"] for error in result.errors] == ["DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"]


def test_injected_verified_route_can_authorize_production_sample() -> None:
    certification_id = "mssql_clickhouse_incremental_merge_airflow_kpo"
    pipeline = {
        "processes": [
            {
                "source": {"type": "mssql"},
                "sink": {
                    "type": "clickhouse",
                    "strategy": {"mode": "incremental_merge"},
                },
            }
        ]
    }
    capabilities = SourceSamplingCapabilityDetector(verified_route_ids=(certification_id,)).detect(pipeline)
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production")
    policy = SafeSamplePolicySet.default().for_environment("production")

    result = SafeSamplePolicyEvaluator().evaluate(request, policy, capabilities)

    assert capabilities.production_proof_verified is True
    assert capabilities.to_dict() == {
        "supports_pushdown_sampling": True,
        "full_scan_required": False,
        "estimated_read_bytes": 1024 * 1024,
        "proof": f"route_certification:{certification_id}",
        "mode": "pushdown",
    }
    assert result.passed is True


def test_authoring_cannot_forge_internal_production_verification() -> None:
    pipeline = {
        "processes": [
            {
                "source": {
                    "type": "mssql",
                    "sampling": {
                        "mode": "pushdown",
                        "proof": "route_certification_verified:forged",
                        "estimated_read_bytes": 1024,
                    },
                }
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector(
        verified_route_ids=("mssql_clickhouse_incremental_merge_airflow_kpo",)
    ).detect(pipeline)

    assert capabilities.supports_pushdown_sampling is False
    assert capabilities.production_proof_verified is False


def test_sampling_route_catalog_exposes_unverified_golden_route_candidate() -> None:
    routes = certified_sampling_routes()

    assert routes == (
        {
            "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
            "status": "experimental",
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "incremental_merge",
            "transport": "native_bcp_to_clickhouse",
            "schema_evolution": "widening",
            "airflow_runtime_mode": "kpo",
            "sampling_mode": "pushdown",
            "estimated_read_bytes": 1024 * 1024,
            "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        },
    )


def test_source_sampling_capability_detector_does_not_certify_full_refresh_route() -> None:
    pipeline = {
        "processes": [
            {
                "source": {"type": "mssql"},
                "sink": {
                    "type": "clickhouse",
                    "strategy": {"mode": "full_refresh"},
                },
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)

    assert capabilities == SampleSourceCapabilities.unknown()


def test_source_sampling_capability_detector_treats_limit_only_as_not_proven() -> None:
    pipeline = {
        "processes": [
            {
                "source": {
                    "type": "postgres",
                    "sampling": {"mode": "limit_only", "estimated_read_bytes": 1024},
                }
            }
        ]
    }

    capabilities = SourceSamplingCapabilityDetector().detect(pipeline)

    assert capabilities.supports_pushdown_sampling is False
    assert capabilities.full_scan_required is None
    assert capabilities.mode == "limit_only"


def test_temporary_target_preserves_logical_pipeline_id_and_uses_collision_safe_physical_name() -> None:
    planner = TemporaryTargetPlanner()

    dashed = planner.plan(
        pipeline_source=_pipeline_source("orders-daily"),
        environment="development",
        run_id="run-1",
    )
    underscored = planner.plan(
        pipeline_source=_pipeline_source("orders_daily"),
        environment="development",
        run_id="run-1",
    )

    assert dashed.pipeline_id == "orders-daily"
    assert dashed.temporary_table["name"].startswith("orders_daily_")
    assert underscored.pipeline_id == "orders_daily"
    assert dashed.temporary_table["name"] != underscored.temporary_table["name"]


def test_temporary_target_rejects_noncanonical_pipeline_id_without_echoing_it() -> None:
    raw_pipeline_id = "Orders Secret"

    with pytest.raises(SafeSamplePlanError) as exc:
        TemporaryTargetPlanner().plan(
            pipeline_source=_pipeline_source(raw_pipeline_id),
            environment="development",
            run_id="run-1",
        )

    assert exc.value.code == "DPONE_PIPELINE_ID_INVALID"
    assert raw_pipeline_id not in str(exc.value)


def test_temporary_target_requires_explicit_canonical_pipeline_identity() -> None:
    pipeline_source = _pipeline_source("orders_daily")
    pipeline_source.pop("metadata")

    with pytest.raises(SafeSamplePlanError) as exc:
        TemporaryTargetPlanner().plan(
            pipeline_source=pipeline_source,
            environment="development",
            run_id="run-1",
        )

    assert exc.value.code == "DPONE_PIPELINE_ID_INVALID"


def _pipeline_source(pipeline_id: str) -> dict[str, object]:
    return {
        "metadata": {"id": pipeline_id},
        "processes": [
            {
                "name": "orders",
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                },
            }
        ],
    }


def test_temporary_target_planner_builds_secret_free_ephemeral_target() -> None:
    pipeline = {
        "metadata": {"id": "orders_daily"},
        "processes": [
            {
                "name": "orders_daily",
                "sink": {
                    "type": "clickhouse",
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                },
            }
        ],
    }

    plan = TemporaryTargetPlanner().plan(
        pipeline_source=pipeline,
        environment="development",
        run_id="local-sample",
    )

    assert plan.mode == "temporary"
    assert plan.connection_ref == "clickhouse_dev"
    assert plan.original_table == {"schema": "analytics", "name": "orders"}
    assert plan.temporary_table["schema"] == "dpone_tmp_development"
    assert plan.temporary_table["name"].startswith("orders_daily_")
    assert plan.ttl_seconds == 24 * 60 * 60
    assert plan.cleanup_required is True
    assert plan.pii_policy == "masked"
    assert "password" not in repr(plan.to_dict()).lower()
