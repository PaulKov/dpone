from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_assertions import (
    DataProductAssertionEvaluator,
    DataProductAssertionGate,
    DataProductAssertionPlanner,
)
from dpone.readiness.data_product_slo import DataProductSloEvaluator, DataProductSloPlanner
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder
from dpone.services.data_product_assertions import DataProductAssertionFacade


def test_disabled_assertions_manifest_emits_noop_plan() -> None:
    plan = DataProductAssertionPlanner().plan(manifest=_manifest(enabled=False))

    assert plan["schema_version"] == "dpone.data_product_assertion_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["suites"] == []
    assert plan["blockers"] == []


def test_assertion_plan_normalizes_suites_and_stable_id() -> None:
    first = DataProductAssertionPlanner().plan(manifest=_manifest())
    second = DataProductAssertionPlanner().plan(manifest=_manifest())

    assert first["status"] == "ready"
    assert first["profile"] == "prod_strict"
    assert first["product"] == {
        "id": "analytics.orders",
        "owner": "data-platform",
        "tier": "gold",
        "criticality": "high",
    }
    assert first["suites"][0]["id"] == "orders_contract_quality"
    assert {item["id"] for item in first["suites"][0]["assertions"]} == {
        "order_id_not_null",
        "order_id_unique",
        "amount_non_negative",
        "freshness_15m",
        "row_count_positive",
        "status_accepted",
        "email_regex",
        "amount_range",
    }
    assert first["assertion_plan_id"] == second["assertion_plan_id"]


def test_plan_blocks_duplicate_ids_missing_regulated_owner_and_unsafe_sql() -> None:
    duplicate = DataProductAssertionPlanner().plan(
        manifest=_manifest(extra_assertions=(_assertion("order_id_unique"),))
    )
    unsafe = DataProductAssertionPlanner().plan(
        manifest=_manifest(
            extra_assertions=(
                {
                    "id": "unsafe",
                    "type": "sql",
                    "query": "DROP TABLE analytics.orders",
                    "expect": {"column": "ok", "equals": 1},
                },
            )
        )
    )
    regulated_missing_owner = DataProductAssertionPlanner().plan(
        manifest=_manifest(profile="regulated", suite_owner=None)
    )

    assert duplicate["status"] == "blocked"
    assert "data_product_assertions.duplicate_assertion_id:order_id_unique" in duplicate["blockers"]
    assert unsafe["status"] == "blocked"
    assert "data_product_assertions.unsafe_sql:unsafe" in unsafe["blockers"]
    assert regulated_missing_owner["status"] == "blocked"
    assert "data_product_assertions.owner_required:orders_contract_quality" in regulated_missing_owner["blockers"]


def test_evaluator_checks_runtime_target_and_imported_assertions() -> None:
    plan = DataProductAssertionPlanner().plan(manifest=_manifest())
    evaluation = DataProductAssertionEvaluator().evaluate(
        plan=plan,
        runtime_artifacts=(_runtime(freshness_lag_seconds=120, rows_loaded=10),),
        target_evidence={
            "row_count": 10,
            "null_key_failures": {"order_id": 0},
            "duplicate_key_failures": {"order_id": 0},
            "accepted_values_failures": {"status": 0},
            "regex_failures": {"email": 0},
            "range_failures": {"amount": 0},
            "sql_results": {"amount_non_negative": {"failures": 0}},
        },
        imported_evidence=(
            _dbt_results(status="pass"),
            _gx_results(success=True),
            _datahub_assertions(status="SUCCESS"),
            _openmetadata_tests(status="Success"),
        ),
    )

    assert evaluation["schema_version"] == "dpone.data_product_assertion_evaluation.v1"
    assert evaluation["status"] == "passed"
    assert {item["status"] for item in evaluation["assertions"]} == {"passed"}
    assert evaluation["summary"]["passed"] == len(evaluation["assertions"])
    assert evaluation["blockers"] == []
    assert (
        evaluation["assertion_evaluation_id"]
        == DataProductAssertionEvaluator().evaluate(
            plan=plan,
            runtime_artifacts=(_runtime(freshness_lag_seconds=120, rows_loaded=10),),
            target_evidence={
                "row_count": 10,
                "null_key_failures": {"order_id": 0},
                "duplicate_key_failures": {"order_id": 0},
                "accepted_values_failures": {"status": 0},
                "regex_failures": {"email": 0},
                "range_failures": {"amount": 0},
                "sql_results": {"amount_non_negative": {"failures": 0}},
            },
            imported_evidence=(
                _dbt_results(status="pass"),
                _gx_results(success=True),
                _datahub_assertions(status="SUCCESS"),
                _openmetadata_tests(status="Success"),
            ),
        )["assertion_evaluation_id"]
    )


def test_clickhouse_probe_evaluates_read_only_assertions(monkeypatch, tmp_path: Path) -> None:
    fake_client = _FakeClickHouseClient()
    monkeypatch.setitem(
        sys.modules,
        "clickhouse_connect",
        SimpleNamespace(get_client=lambda **_: fake_client),
    )
    manifest_path = _write_json(tmp_path / "orders.json", _manifest(unknown_source="allow"))
    runtime_path = _write_json(tmp_path / "latest-run.json", _runtime(freshness_lag_seconds=120))
    connection_path = _write_json(tmp_path / "clickhouse.json", {"type": "clickhouse"})
    facade = DataProductAssertionFacade()
    plan = facade.plan(manifest_path=str(manifest_path))
    plan_path = _write_json(tmp_path / "assertion-plan.json", plan)

    evaluation = facade.evaluate(
        plan_path=str(plan_path),
        runtime_artifact_path=str(runtime_path),
        target_connection_path=str(connection_path),
    )

    assert evaluation["status"] == "passed"
    assert {item["status"] for item in evaluation["assertions"]} == {"passed"}
    assert any("FROM `analytics`.`orders`" in query for query in fake_client.queries)
    assert any("IS NULL" in query for query in fake_client.queries)
    assert any("GROUP BY `order_id`" in query for query in fake_client.queries)
    assert any("match(toString(`email`)" in query for query in fake_client.queries)


def test_evaluator_blocks_failed_assertions_and_gate_profiles() -> None:
    plan = DataProductAssertionPlanner().plan(manifest=_manifest())
    evaluation = DataProductAssertionEvaluator().evaluate(
        plan=plan,
        runtime_artifacts=(_runtime(freshness_lag_seconds=1800, rows_loaded=0),),
        target_evidence={
            "row_count": 0,
            "null_key_failures": {"order_id": 2},
            "duplicate_key_failures": {"order_id": 1},
            "accepted_values_failures": {"status": 1},
            "regex_failures": {"email": 1},
            "range_failures": {"amount": 1},
            "sql_results": {"amount_non_negative": {"failures": 3}},
        },
    )
    strict_gate = DataProductAssertionGate().evaluate(evaluation=evaluation, profile="prod_strict")
    advisory_gate = DataProductAssertionGate().evaluate(evaluation=evaluation, profile="advisory")

    assert evaluation["status"] == "blocked"
    assert "data_product_assertions.assertion_failed:order_id_not_null" in evaluation["blockers"]
    assert "data_product_assertions.assertion_failed:amount_non_negative" in evaluation["blockers"]
    assert strict_gate["schema_version"] == "dpone.data_product_assertion_gate.v1"
    assert strict_gate["status"] == "blocked"
    assert advisory_gate["status"] == "warning"
    assert advisory_gate["blockers"] == []
    assert "data_product_assertions.assertion_failed:order_id_unique" in advisory_gate["warnings"]


def test_imported_evidence_policy_allow_warn_block() -> None:
    allow_plan = DataProductAssertionPlanner().plan(manifest=_manifest(unknown_source="allow"))
    warn_plan = DataProductAssertionPlanner().plan(manifest=_manifest(unknown_source="warn"))
    block_plan = DataProductAssertionPlanner().plan(manifest=_manifest(unknown_source="block"))

    allow = DataProductAssertionEvaluator().evaluate(plan=allow_plan, runtime_artifacts=(_runtime(),))
    warn = DataProductAssertionEvaluator().evaluate(plan=warn_plan, runtime_artifacts=(_runtime(),))
    block = DataProductAssertionEvaluator().evaluate(plan=block_plan, runtime_artifacts=(_runtime(),))

    assert "data_product_assertions.import_missing:dbt_run_results" not in allow["warnings"]
    assert "data_product_assertions.import_missing:dbt_run_results" in warn["warnings"]
    assert "data_product_assertions.import_missing:dbt_run_results" in block["blockers"]


def test_assertion_report_and_public_json_schemas_validate_artifacts() -> None:
    plan = DataProductAssertionPlanner().plan(manifest=_manifest())
    evaluation = DataProductAssertionEvaluator().evaluate(plan=plan, runtime_artifacts=(_runtime(),))
    gate = DataProductAssertionGate().evaluate(evaluation=evaluation, profile="prod_strict")
    report = DataProductAssertionGate().report(evaluation=evaluation)

    assert report["schema_version"] == "dpone.data_product_assertion_report.v1"
    assert "# Data Product Assertion Report" in report["markdown"]

    for name, payload in (
        ("data-product-assertion-plan.schema.json", plan),
        ("data-product-assertion-evaluation.schema.json", evaluation),
        ("data-product-assertion-gate.schema.json", gate),
        ("data-product-assertion-report.schema.json", report),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def test_bundle_policy_and_registry_accept_assertion_artifacts() -> None:
    pack = _pack()
    gate = {
        "schema_version": "dpone.data_product_assertion_gate.v1",
        "assertion_gate_id": "sha256:" + "1" * 64,
        "status": "allowed",
        "profile": "prod_strict",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    report = {
        "schema_version": "dpone.data_product_assertion_report.v1",
        "assertion_report_id": "sha256:" + "2" * 64,
        "status": "passed",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "markdown": "# ok",
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("data_product_assertion_gate", "assertion-gate.json", gate, required=False),
        _artifact("data_product_assertion_report", "assertion-report.json", report, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_assertion_gate"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_assertion_gate=gate,
        data_product_assertion_report=report,
        environment="prod",
        stage="assertion_gate_passed",
    )

    assert bundle["summary"]["data_product_assertion_gate_id"] == gate["assertion_gate_id"]
    assert bundle["summary"]["data_product_assertion_report_id"] == report["assertion_report_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {"data_product_assertion_gate", "data_product_assertion_report"} <= {
        item["kind"] for item in record["artifact_refs"]
    }


def test_slo_quality_objective_consumes_assertion_gate() -> None:
    assertion_gate = {
        "schema_version": "dpone.data_product_assertion_gate.v1",
        "assertion_gate_id": "sha256:" + "5" * 64,
        "status": "blocked",
        "product_id": "analytics.orders",
        "blockers": ["data_product_assertions.assertion_failed:order_id_not_null"],
        "warnings": [],
    }

    plan = DataProductSloPlanner().plan(manifest=_slo_manifest(), assertion_gate=assertion_gate)
    evaluation = DataProductSloEvaluator().evaluate(plan=plan, runtime_artifacts=(_runtime(),))

    assert plan["assertion_gate_id"] == assertion_gate["assertion_gate_id"]
    assert evaluation["status"] == "blocked"
    assert "data_product_slo.assertion_gate_blocked" in evaluation["blockers"]


def _manifest(
    *,
    enabled: bool = True,
    profile: str = "prod_strict",
    unknown_source: str = "warn",
    suite_owner: str | None = "data-platform",
    extra_assertions: tuple[dict[str, object], ...] = (),
) -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {"id": "analytics.orders", "version": "2.0.0"},
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "assertions": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": profile,
                        "unknown_assertion_source": unknown_source,
                        "stale_after_seconds": 86400,
                        "suites": [
                            {
                                "id": "orders_contract_quality",
                                "owner": suite_owner,
                                "severity": "critical",
                                "assertions": [
                                    _assertion("order_id_not_null", assertion_type="null_key"),
                                    _assertion("order_id_unique", assertion_type="duplicate_key"),
                                    {
                                        "id": "amount_non_negative",
                                        "type": "sql",
                                        "query": ("SELECT count() AS failures FROM analytics.orders WHERE amount < 0"),
                                        "expect": {"column": "failures", "equals": 0},
                                    },
                                    {"id": "freshness_15m", "type": "freshness", "max_lag_seconds": 900},
                                    {"id": "row_count_positive", "type": "row_count", "min_rows": 1},
                                    {
                                        "id": "status_accepted",
                                        "type": "accepted_values",
                                        "column": "status",
                                        "values": ["new", "paid"],
                                        "max_failures": 0,
                                    },
                                    {
                                        "id": "email_regex",
                                        "type": "regex",
                                        "column": "email",
                                        "pattern": ".+@.+",
                                        "max_failures": 0,
                                    },
                                    {
                                        "id": "amount_range",
                                        "type": "range",
                                        "column": "amount",
                                        "min": 0,
                                        "max": 1000000,
                                        "max_failures": 0,
                                    },
                                    *extra_assertions,
                                ],
                            }
                        ],
                        "imports": {
                            "dbt_run_results": "target/run_results.json",
                            "great_expectations": ".dpone/quality/gx-validation-results.json",
                            "datahub_assertions": ".dpone/catalog/datahub-assertions.json",
                            "openmetadata_tests": ".dpone/catalog/openmetadata-tests.json",
                        },
                    },
                },
            },
        }
    }


def _assertion(assertion_id: str, *, assertion_type: str = "duplicate_key") -> dict[str, object]:
    return {"id": assertion_id, "type": assertion_type, "columns": ["order_id"], "max_failures": 0}


def _runtime(
    *,
    freshness_lag_seconds: int = 100,
    rows_loaded: int = 10,
    status: str = "success",
) -> dict[str, object]:
    return {
        "schema_version": "dpone.run.latest.v1",
        "status": status,
        "freshness_lag_seconds": freshness_lag_seconds,
        "rows_loaded": rows_loaded,
    }


def _dbt_results(*, status: str) -> dict[str, object]:
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
        "results": [{"unique_id": "test.analytics.orders.not_null", "status": status}],
    }


def _gx_results(*, success: bool) -> dict[str, object]:
    return {
        "results": [
            {"expectation_config": {"expectation_type": "expect_column_values_to_not_be_null"}, "success": success}
        ]
    }


def _datahub_assertions(*, status: str) -> dict[str, object]:
    return {"assertions": [{"urn": "urn:li:assertion:orders", "status": status}]}


def _openmetadata_tests(*, status: str) -> dict[str, object]:
    return {"tests": [{"name": "orders_count", "status": status}]}


class _FakeClickHouseClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def query(self, query: str) -> SimpleNamespace:
        self.queries.append(query)
        if query == "SELECT count() FROM `analytics`.`orders`":
            return SimpleNamespace(result_rows=[(10,)])
        return SimpleNamespace(result_rows=[(0,)])


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "schema_contract_major", "path": "analytics.orders"},),
        strategy="expand_contract",
    )


def _slo_manifest() -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {"id": "analytics.orders", "version": "2.0.0"},
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "slo": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "objectives": {"quality": {"max_duplicate_keys": 0}},
                    },
                },
            },
        }
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, sort_keys=True).encode(),
        required=required,
    )
