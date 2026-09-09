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
    SourceSamplingCapabilityDetector,
    TemporaryTargetPlan,
)


def _target_plan() -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_prod",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_prod", "name": "orders_daily_abc123"},
        ttl_seconds=86400,
        cleanup_required=True,
        pii_policy="masked",
    )


def test_source_request_plans_proven_production_pushdown_without_credentials() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder

    policy = SafeSamplePolicySet.default().for_environment("production")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production")
    result = SafeSamplePolicyEvaluator().evaluate(
        request,
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

    payload = SafeSampleSourceRequestBuilder().build(result, _target_plan()).to_dict()

    schema = json.loads(Path("docs/schemas/gitops/safe-sample-source-request.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(payload, schema)
    assert payload == {
        "schema": "dpone.safe-sample-source-request.v1",
        "status": "planned",
        "mode": "pushdown",
        "sample_rows": 1000,
        "max_bytes": 1024**3,
        "timeout_seconds": 60,
        "source_read_only": True,
        "full_scan_allowed": False,
        "estimated_read_bytes": 1024 * 1024,
        "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        "pii_policy": "masked",
        "target": {
            "mode": "temporary",
            "connection_ref": "clickhouse_prod",
            "temporary_table": {"schema": "dpone_tmp_prod", "name": "orders_daily_abc123"},
            "ttl_seconds": 86400,
        },
        "errors": [],
    }
    assert "password" not in repr(payload).lower()
    assert "vault" not in repr(payload).lower()


def test_source_request_blocks_production_without_pushdown_proof() -> None:
    from dpone.services.safe_sample_source_request import SafeSampleSourceRequestBuilder

    policy = SafeSamplePolicySet.default().for_environment("production")
    request = SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production")
    result = SafeSamplePolicyEvaluator().evaluate(request, policy, SampleSourceCapabilities.unknown())

    payload = SafeSampleSourceRequestBuilder().build(result, _target_plan()).to_dict()

    assert payload["schema"] == "dpone.safe-sample-source-request.v1"
    assert payload["status"] == "blocked"
    assert payload["mode"] == "blocked"
    assert payload["sample_rows"] == 1000
    assert payload["source_read_only"] is True
    assert payload["target"]["mode"] == "temporary"
    assert [error["code"] for error in payload["errors"]] == ["DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"]


def test_source_request_schema_rejects_planned_full_scan_without_positive_byte_estimate() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/safe-sample-source-request.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.safe-sample-source-request.v1",
        "status": "planned",
        "mode": "full_scan",
        "sample_rows": 1000,
        "max_bytes": 10 * 1024**3,
        "timeout_seconds": 300,
        "source_read_only": True,
        "full_scan_allowed": True,
        "estimated_read_bytes": 0,
        "proof": "dev_fixture",
        "pii_policy": "masked",
        "target": {
            "mode": "temporary",
            "connection_ref": "clickhouse_dev",
            "temporary_table": {"schema": "dpone_tmp_dev", "name": "orders_daily_abc123"},
            "ttl_seconds": 86400,
        },
        "errors": [],
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_data_copy_schema_rejects_nested_full_scan_without_positive_byte_estimate() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/safe-sample-data-copy.schema.json").read_text(encoding="utf-8"))
    payload = {
        "schema": "dpone.safe-sample-data-copy.v1",
        "status": "blocked",
        "source_request": {
            "schema": "dpone.safe-sample-source-request.v1",
            "status": "planned",
            "mode": "full_scan",
            "sample_rows": 1000,
            "max_bytes": 10 * 1024**3,
            "timeout_seconds": 300,
            "source_read_only": True,
            "full_scan_allowed": True,
            "estimated_read_bytes": 0,
            "proof": "dev_fixture",
            "pii_policy": "masked",
            "target": {"connection_ref": "clickhouse_dev"},
            "errors": [],
        },
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
                "connection_ref": "clickhouse_dev",
                "temporary_table": {"schema": "dpone_tmp_dev", "name": "orders"},
            },
            "strategy": "incremental_merge",
            "sample_rows": 1000,
            "max_bytes": 10 * 1024**3,
            "timeout_seconds": 300,
            "source_read_only": True,
            "pii_policy": "masked",
            "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
        },
        "rows_read": 0,
        "rows_written": 0,
        "bytes_read": 0,
        "pii_policy": "masked",
        "diagnostics": {},
        "errors": [],
    }

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)
