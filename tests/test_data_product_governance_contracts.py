from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_governance import (
    GovernanceExportPlanner,
    GovernancePayloadRenderer,
    GovernancePublisher,
    GovernancePublishVerifier,
)
from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_evidence_registry import MigrationEvidenceRecorder


def test_disabled_governance_export_emits_noop_plan() -> None:
    plan = GovernanceExportPlanner().plan(manifest=_manifest(enabled=False), evidence={}, registry_records=())

    assert plan["schema_version"] == "dpone.data_product_governance_export_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["export_items"] == []
    assert plan["blockers"] == []


def test_governance_export_plan_binds_evidence_and_blocks_mismatched_product() -> None:
    evidence = {
        "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
        "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        "data_product_fleet_gate": _artifact("data_product_fleet_gate", "allowed"),
    }
    plan = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence=evidence,
        registry_records=(_record(stage="policy_gate_passed"),),
        targets=("datahub", "openlineage"),
    )
    blocked = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence={"data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed", product_id="x")},
        registry_records=(),
        targets=("datahub",),
    )

    assert plan["status"] == "ready"
    assert [item["provider"] for item in plan["export_items"]] == ["datahub", "openlineage"]
    assert {ref["kind"] for ref in plan["evidence_refs"]} >= {
        "data_product_assertion_gate",
        "data_product_policy_gate",
        "data_product_fleet_gate",
    }
    assert (
        plan["governance_export_plan_id"]
        == GovernanceExportPlanner().plan(
            manifest=_manifest(),
            evidence=evidence,
            registry_records=(_record(stage="policy_gate_passed"),),
            targets=("datahub", "openlineage"),
        )["governance_export_plan_id"]
    )
    assert blocked["status"] == "blocked"
    assert "data_product_governance.product_id_mismatch:data_product_assertion_gate" in blocked["blockers"]


def test_governance_export_plan_includes_authority_evidence() -> None:
    evidence = {
        "data_product_authority_gate": {
            "schema_version": "dpone.data_product_authority_gate.v1",
            "authority_gate_id": "sha256:" + "5" * 64,
            "status": "allowed",
            "product_id": "analytics.orders",
            "blockers": [],
            "warnings": [],
        },
        "data_product_approval_quorum": {
            "schema_version": "dpone.data_product_approval_quorum.v1",
            "approval_quorum_id": "sha256:" + "6" * 64,
            "status": "allowed",
            "product_id": "analytics.orders",
            "blockers": [],
            "warnings": [],
        },
        "data_product_evidence_signature": {
            "schema_version": "dpone.data_product_evidence_signature.v1",
            "evidence_signature_id": "sha256:" + "7" * 64,
            "status": "signed",
            "product_id": "analytics.orders",
            "blockers": [],
            "warnings": [],
        },
    }

    plan = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence=evidence,
        registry_records=(),
        targets=("datahub",),
    )

    assert plan["status"] == "ready"
    assert {ref["kind"] for ref in plan["evidence_refs"]} >= set(evidence)


def test_governance_payload_renderers_are_provider_specific_and_deterministic() -> None:
    plan = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence={
            "data_product_assertion_gate": _artifact("data_product_assertion_gate", "allowed"),
            "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        },
        registry_records=(),
        targets=("datahub", "openmetadata", "openlineage", "opa", "json"),
    )
    renderer = GovernancePayloadRenderer()

    datahub = renderer.render(plan=plan, provider="datahub")
    openmetadata = renderer.render(plan=plan, provider="openmetadata")
    openlineage = renderer.render(plan=plan, provider="openlineage")
    opa = renderer.render(plan=plan, provider="opa")
    native = renderer.render(plan=plan, provider="json")

    assert datahub["schema_version"] == "dpone.data_product_governance_export_payload.v1"
    assert datahub["status"] == "rendered"
    assert datahub["payload"]["entityType"] == "dataProduct"
    assert openmetadata["payload"]["entityType"] == "table"
    assert openlineage["payload"]["eventType"] == "COMPLETE"
    assert opa["payload"]["labels"]["product_id"] == "analytics.orders"
    assert native["payload"]["product"]["id"] == "analytics.orders"
    assert datahub["payload_id"] == renderer.render(plan=plan, provider="datahub")["payload_id"]


def test_governance_publish_and_verify_receipts_are_safe_and_deterministic() -> None:
    payload = GovernancePayloadRenderer().render(
        plan=GovernanceExportPlanner().plan(
            manifest=_manifest(),
            evidence={"data_product_policy_gate": _artifact("data_product_policy_gate", "allowed")},
            registry_records=(),
            targets=("datahub",),
        ),
        provider="datahub",
    )
    publisher = GovernancePublisher()
    dry_run = publisher.publish(payload=payload, provider="datahub", connection={}, execute=False)
    missing_connection = publisher.publish(payload=payload, provider="datahub", connection={}, execute=True)
    published = publisher.publish(
        payload=payload,
        provider="datahub",
        connection={"endpoint": "https://metadata.example/api", "token": "secret"},
        execute=True,
    )
    verified = GovernancePublishVerifier().verify(receipt=published)
    tampered = GovernancePublishVerifier().verify(receipt={**published, "payload_sha256": "sha256:" + "0" * 64})

    assert dry_run["schema_version"] == "dpone.data_product_governance_publish_receipt.v1"
    assert dry_run["status"] == "dry_run"
    assert dry_run["network_writes"] == []
    assert missing_connection["status"] == "blocked"
    assert "data_product_governance_publish.connection_required" in missing_connection["blockers"]
    assert published["status"] == "published"
    assert "secret" not in json.dumps(published)
    assert verified["schema_version"] == "dpone.data_product_governance_publish_verification.v1"
    assert verified["status"] == "verified"
    assert tampered["status"] == "blocked"
    assert "data_product_governance_verify.payload_hash_mismatch" in tampered["blockers"]


def test_governance_public_json_schemas_validate_artifacts() -> None:
    plan = GovernanceExportPlanner().plan(
        manifest=_manifest(),
        evidence={"data_product_policy_gate": _artifact("data_product_policy_gate", "allowed")},
        registry_records=(),
        targets=("json",),
    )
    payload = GovernancePayloadRenderer().render(plan=plan, provider="json")
    receipt = GovernancePublisher().publish(payload=payload, provider="json", connection={}, execute=False)
    verification = GovernancePublishVerifier().verify(receipt=receipt)

    for name, artifact in (
        ("data-product-governance-export-plan.schema.json", plan),
        ("data-product-governance-export-payload.schema.json", payload),
        ("data-product-governance-publish-receipt.schema.json", receipt),
        ("data-product-governance-publish-verification.schema.json", verification),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(artifact)


def test_bundle_policy_and_registry_accept_governance_artifacts() -> None:
    pack = _pack()
    payload = {
        "schema_version": "dpone.data_product_governance_export_payload.v1",
        "payload_id": "sha256:" + "1" * 64,
        "payload_sha256": "sha256:" + "2" * 64,
        "provider": "datahub",
        "status": "rendered",
        "product_id": "analytics.orders",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    receipt = {
        "schema_version": "dpone.data_product_governance_publish_receipt.v1",
        "publish_receipt_id": "sha256:" + "3" * 64,
        "payload_id": payload["payload_id"],
        "payload_sha256": payload["payload_sha256"],
        "provider": "datahub",
        "status": "published",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    verification = {
        "schema_version": "dpone.data_product_governance_publish_verification.v1",
        "publish_verification_id": "sha256:" + "4" * 64,
        "publish_receipt_id": receipt["publish_receipt_id"],
        "payload_id": payload["payload_id"],
        "payload_sha256": payload["payload_sha256"],
        "provider": "datahub",
        "status": "verified",
        "pack_id": pack.pack_id,
        "blockers": [],
        "warnings": [],
    }
    artifacts = (
        _bundle_artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _bundle_artifact("data_product_governance_export_payload", "governance-payload.json", payload, required=False),
        _bundle_artifact("data_product_governance_publish_receipt", "governance-receipt.json", receipt, required=False),
        _bundle_artifact(
            "data_product_governance_publish_verification",
            "governance-verification.json",
            verification,
            required=False,
        ),
    )

    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)
    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "data_product_governance_publish_verification"]},
        ),
        artifact_payloads={item.kind: item.payload for item in artifacts},
    )
    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=None,
        trust_verification=None,
        diff=None,
        data_product_governance_export_payload=payload,
        data_product_governance_publish_receipt=receipt,
        data_product_governance_publish_verification=verification,
        environment="prod",
        stage="governance_publish_verified",
    )

    assert (
        bundle["summary"]["data_product_governance_publish_verification_id"] == verification["publish_verification_id"]
    )
    assert decision["status"] == "allowed"
    assert record["status"] == "ready"
    assert {
        "data_product_governance_export_payload",
        "data_product_governance_publish_receipt",
        "data_product_governance_publish_verification",
    } <= {item["kind"] for item in record["artifact_refs"]}


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "schema_contract": {"id": "analytics.orders", "version": "2.0.0"},
                    "governance_export": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "stale_evidence_policy": "block",
                        "targets": [
                            {"provider": "datahub", "mode": "render"},
                            {"provider": "openlineage", "mode": "render"},
                        ],
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str, *, product_id: str = "analytics.orders") -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        "status": status,
        "product_id": product_id,
        "blockers": [] if status in {"allowed", "warning", "waived"} else [f"{kind}.blocked"],
        "warnings": ["warning"] if status == "warning" else [],
        f"{kind.removeprefix('data_product_')}_id": "sha256:" + kind.encode().hex()[:64].ljust(64, "0"),
    }


def _record(*, stage: str) -> dict:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_record.v1",
        "record_id": "sha256:" + "9" * 64,
        "recorded_at": "2026-06-28T12:00:00Z",
        "environment": "prod",
        "stage": stage,
        "status": "ready",
        "product_id": "analytics.orders",
        "artifact_refs": [{"kind": "data_product_policy_gate", "evidence_id": "sha256:" + "8" * 64}],
        "blockers": [],
        "warnings": [],
    }


def _pack() -> MigrationPack:
    return MigrationPack(
        pack_id="sha256:" + "a" * 64,
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired_fingerprint="sha256:" + "b" * 64,
        actual_fingerprint="sha256:" + "c" * 64,
        desired={"engine": "MergeTree", "order_by": ["order_id"]},
        actual={"engine": "MergeTree", "order_by": ["order_id"]},
        strategy="direct",
        changes=({"kind": "table_setting", "setting": "index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        warnings=(),
        blockers=(),
    )


def _bundle_artifact(kind: str, path: str, payload: dict, *, required: bool) -> MigrationEvidenceArtifact:
    return MigrationEvidenceArtifact.from_bytes(
        kind=kind,
        path=path,
        content=json.dumps(payload, sort_keys=True).encode("utf-8"),
        required=required,
    )
