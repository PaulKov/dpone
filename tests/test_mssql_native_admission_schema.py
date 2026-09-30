"""Closed offline admission evidence for bounded MSSQL native routes."""

from __future__ import annotations

import json
from pathlib import Path

import jsonschema

from dpone.contracts.mssql_native_admission import mssql_native_admission_v2_schema
from dpone.readiness.managed import ExecutionPlanService

SCHEMA_PATH = Path("src/dpone/schema/dpone.mssql-native-admission.v2.schema.json")
EXAMPLE = Path("examples/native/clickhouse-to-mssql-sqlclient.yaml")


def test_checked_in_admission_schema_matches_producer_and_plan_projection() -> None:
    schema = mssql_native_admission_v2_schema()
    assert json.loads(SCHEMA_PATH.read_text(encoding="utf-8")) == schema
    admission = ExecutionPlanService().plan_manifest(EXAMPLE)["mssql_native"]["admission"]
    assert admission == {
        "schema_version": 2,
        "kind": "dpone.mssql-native-admission.v2",
        "status": "blocked",
        "import_backend": "mssql_sqlclient",
        "verification_backend": "target_local",
        "identity_version": 2,
        "capability_id": "sqlclient-session-applock-v1",
        "blockers": ["mssql_native.live_preflight_required"],
        "warnings": ["mssql_native.live_preflight_not_run"],
    }
    assert not list(jsonschema.Draft7Validator(schema).iter_errors(admission))


def test_sqlclient_example_projects_the_generic_transaction_catalog() -> None:
    state = ExecutionPlanService().plan_manifest(EXAMPLE)["state"]

    assert state["catalog"] == "generic_mssql_transaction_v2"
    assert state["tables"] == {
        "target_identity_registry": "dpone_target_identity",
        "target_fence": "dpone_target_fence",
        "load_attempt": "dpone_load_attempt",
        "load_operation": "dpone_load_operation",
        "load_receipt": "dpone_load_receipt",
    }
    assert state["triggers"] == {
        "target_identity_registry": "trg_dpone_target_identity_immutable",
        "target_fence": "trg_dpone_target_fence_monotonic",
        "load_attempt": "trg_dpone_load_attempt_immutable",
        "load_operation": "trg_dpone_load_operation_fence",
        "load_receipt": "trg_dpone_load_receipt_immutable",
    }


def test_bcp_native_example_projects_same_generic_catalog_without_changing_backend() -> None:
    plan = ExecutionPlanService().plan_manifest(Path("examples/native/clickhouse-to-mssql-target-local.yaml"))

    assert plan["mssql_native"]["import_backend"] == "bcp"
    assert plan["state"]["catalog"] == "generic_mssql_transaction_v2"
    assert plan["state"]["tables"]["load_receipt"] == "dpone_load_receipt"


def test_legacy_route_keeps_legacy_state_projection() -> None:
    plan = ExecutionPlanService().plan_manifest(Path("examples/source-sink/clickhouse-to-mssql.yaml"))

    assert "mssql_native" not in plan
    assert "catalog" not in plan["state"]
