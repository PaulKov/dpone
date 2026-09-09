from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_contract_consumer_test_kit import (
    SchemaConsumerCertificationEvaluator,
    SchemaConsumerTestKitBuilder,
)
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_test_kit_builds_required_cases_from_consumer_matrix() -> None:
    matrix = _matrix(
        consumers=[
            _consumer("finance.daily_margin", reads=("amount", "customer_id"), status="blocked"),
            _consumer("ops.orders_audit", reads=("order_id",), status="compatible"),
        ],
        blockers=["schema_contract.consumer_version_incompatible:finance.daily_margin"],
    )

    kit = SchemaConsumerTestKitBuilder().build(matrix=matrix)

    assert kit["schema_version"] == "dpone.schema_contract_consumer_test_kit.v1"
    assert kit["status"] == "ready"
    assert kit["summary"] == {
        "test_cases_count": 2,
        "required_cases_count": 1,
        "covered_by_compatibility_view": 0,
    }
    required = [case for case in kit["test_cases"] if case["required"]]
    assert required[0]["consumer_id"] == "finance.daily_margin"
    assert required[0]["assertions"] == [
        {"kind": "column_available", "column": "amount"},
        {"kind": "column_available", "column": "customer_id"},
        {"kind": "version_constraint_allows_head", "constraint": "1.x", "head_version": "2.0.0"},
    ]
    assert kit["test_kit_id"].startswith("sha256:")


def test_test_kit_marks_compatibility_view_coverage() -> None:
    matrix = _matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    view_plan = {
        "schema_version": "dpone.schema_contract_compatibility_view_plan.v1",
        "status": "planned",
        "compatibility_view_plan_id": "sha256:" + "1" * 64,
        "views": [
            {
                "view": "analytics.orders__contract_v1",
                "version_constraint": "1.x",
                "status": "planned",
                "projections": [{"column": "amount", "expression": "amount", "kind": "direct"}],
            }
        ],
    }

    kit = SchemaConsumerTestKitBuilder().build(matrix=matrix, compatibility_view_plan=view_plan)

    assert kit["status"] == "ready"
    assert kit["summary"]["covered_by_compatibility_view"] == 1
    assert kit["test_cases"][0]["compatibility_view"] == "analytics.orders__contract_v1"
    assert kit["test_cases"][0]["assertions"][-1] == {
        "kind": "compatibility_view_available",
        "view": "analytics.orders__contract_v1",
    }


def test_certification_blocks_failed_required_case() -> None:
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )

    certification = SchemaConsumerCertificationEvaluator().evaluate(
        test_kit=kit,
        result={
            "status": "failed",
            "failed_cases": [kit["test_cases"][0]["test_case_id"]],
            "passed_cases": [],
        },
    )

    assert certification["schema_version"] == "dpone.schema_contract_consumer_certification.v1"
    assert certification["status"] == "blocked"
    assert "schema_contract_consumer_test.required_case_failed:finance.daily_margin" in certification["blockers"]


def test_test_kit_blocks_stale_contract_binding() -> None:
    matrix = _matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])

    kit = SchemaConsumerTestKitBuilder().build(
        matrix=matrix,
        contract_version={
            "schema_version": "dpone.schema_contract_version.v1",
            "contract_id": "analytics.orders",
            "version": "1.9.0",
            "contract_version_id": "sha256:" + "9" * 64,
        },
    )

    assert kit["status"] == "blocked"
    assert "schema_contract_consumer_test.contract_version_mismatch" in kit["blockers"]
    assert "schema_contract_consumer_test.contract_version_id_mismatch" in kit["blockers"]


def test_certification_blocks_failed_result_without_case_details() -> None:
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )

    certification = SchemaConsumerCertificationEvaluator().evaluate(test_kit=kit, result={"status": "failed"})

    assert certification["status"] == "blocked"
    assert "schema_contract_consumer_test.result_failed" in certification["blockers"]


def test_certification_passes_when_all_required_cases_pass() -> None:
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )

    certification = SchemaConsumerCertificationEvaluator().evaluate(test_kit=kit, result={"status": "passed"})

    assert certification["status"] == "certified"
    assert certification["consumer_certification_id"].startswith("sha256:")
    assert certification["summary"] == {"required_cases": 1, "passed_cases": 1, "failed_cases": 0}


def test_ids_are_stable_for_same_inputs() -> None:
    matrix = _matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])

    first = SchemaConsumerTestKitBuilder().build(matrix=matrix)
    second = SchemaConsumerTestKitBuilder().build(matrix=matrix)
    cert_first = SchemaConsumerCertificationEvaluator().evaluate(test_kit=first, result={"status": "passed"})
    cert_second = SchemaConsumerCertificationEvaluator().evaluate(test_kit=second, result={"status": "passed"})

    assert first["test_kit_id"] == second["test_kit_id"]
    assert cert_first["consumer_certification_id"] == cert_second["consumer_certification_id"]


def test_bundle_policy_and_registry_accept_consumer_certification() -> None:
    pack = _pack()
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )
    certification = {
        **SchemaConsumerCertificationEvaluator().evaluate(test_kit=kit, result={"status": "passed"}),
        "pack_id": pack.pack_id,
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("consumer_certification", "consumer-certification.json", certification, required=False),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={
                "required_artifacts": ["migration_pack", "consumer_certification"],
                "fail_on_warnings": False,
            },
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        consumer_certification=certification,
        environment="prod",
        stage="consumer_certified",
    )

    assert bundle["summary"]["consumer_certification_id"] == certification["consumer_certification_id"]
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert "consumer_certification" in {item["kind"] for item in record["artifact_refs"]}


def test_bundle_blocks_uncertified_consumer_certification() -> None:
    pack = _pack()
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )
    certification = {
        **SchemaConsumerCertificationEvaluator().evaluate(test_kit=kit, result={"status": "failed"}),
        "pack_id": pack.pack_id,
    }

    bundle = MigrationBundleBuilder().build(
        artifacts=(
            _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
            _artifact("consumer_certification", "consumer-certification.json", certification, required=False),
        )
    )

    assert bundle["status"] == "blocked"
    assert "migration_bundle.consumer_certification_blocked" in bundle["blockers"]


def test_public_json_schemas_validate_test_kit_and_certification() -> None:
    kit = SchemaConsumerTestKitBuilder().build(
        matrix=_matrix(consumers=[_consumer("finance.daily_margin", reads=("amount",), status="blocked")])
    )
    certification = SchemaConsumerCertificationEvaluator().evaluate(test_kit=kit, result={"status": "passed"})
    kit_schema = json.loads(
        Path("docs/schemas/schema-migration/schema-contract-consumer-test-kit.schema.json").read_text(encoding="utf-8")
    )
    certification_schema = json.loads(
        Path("docs/schemas/schema-migration/schema-contract-consumer-certification.schema.json").read_text(
            encoding="utf-8"
        )
    )

    assert kit_schema["properties"]["schema_version"]["const"] == kit["schema_version"]
    assert certification_schema["properties"]["schema_version"]["const"] == certification["schema_version"]
    assert set(kit_schema["required"]) <= set(kit)
    assert set(certification_schema["required"]) <= set(certification)


def _matrix(
    *,
    consumers: list[dict[str, object]],
    blockers: list[str] | None = None,
    status: str = "blocked",
) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": status,
        "contract_id": "analytics.orders",
        "base_version": "1.5.0",
        "head_version": "2.0.0",
        "head_contract_version_id": "sha256:" + "2" * 64,
        "consumer_matrix_id": "sha256:" + "3" * 64,
        "required_bump": "major",
        "consumers": consumers,
        "summary": {"consumers_count": len(consumers), "blocked_consumers": len(blockers or [])},
        "blockers": blockers or [],
        "warnings": [],
        "reviewer_actions": ["Run generated consumer contract tests before promotion."],
    }


def _consumer(consumer_id: str, *, reads: tuple[str, ...], status: str) -> dict[str, object]:
    return {
        "id": consumer_id,
        "type": "dashboard",
        "owner": "finance-analytics",
        "version_constraint": "1.x",
        "reads": {"columns": list(reads)},
        "status": status,
        "blockers": [] if status == "compatible" else [f"schema_contract.consumer_version_incompatible:{consumer_id}"],
        "confidence": "explicit",
        "source": "manual",
    }


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "column_removed", "path": "amount"},),
        strategy="expand_contract",
    )


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
