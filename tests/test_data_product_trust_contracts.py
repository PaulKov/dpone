from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_trust_index import TrustEvidenceLakeIndexer, TrustQueryEngine
from dpone.readiness.data_product_trust_rendering import TrustExportRenderer, TrustReportRenderer
from dpone.readiness.data_product_trust_snapshot import TrustGate, TrustSnapshotBuilder


def test_disabled_trust_center_emits_noop_artifacts() -> None:
    index = TrustEvidenceLakeIndexer().index(manifests=[_manifest(enabled=False)], evidence_payloads=())
    snapshot = TrustSnapshotBuilder().snapshot(index=index, product_id="analytics.orders", profile="prod_strict")
    gate = TrustGate().evaluate(snapshot=snapshot, profile="prod_strict")
    report = TrustReportRenderer().report(snapshot=snapshot, gate=gate)

    assert index["schema_version"] == "dpone.data_product_evidence_lake_index.v1"
    assert index["status"] == "disabled"
    assert snapshot["status"] == "disabled"
    assert gate["status"] == "allowed"
    assert report["status"] == "allowed"


def test_index_query_snapshot_and_gate_block_missing_required_domains() -> None:
    index = TrustEvidenceLakeIndexer().index(
        manifests=[_manifest()],
        evidence_payloads=[
            _gate("dpone.schema_contract_gate.v1", "schema_contract_gate", "schema_contract_gate_id"),
            _gate("dpone.data_product_assertion_gate.v1", "data_product_assertion_gate", "assertion_gate_id"),
            _gate("dpone.data_product_cost_gate.v1", "data_product_cost_gate", "cost_gate_id"),
        ],
    )
    query = TrustQueryEngine().query(index=index, product_id="analytics.orders", status="allowed")
    snapshot = TrustSnapshotBuilder().snapshot(index=index, product_id="analytics.orders", profile="prod_strict")
    gate = TrustGate().evaluate(snapshot=snapshot, profile="prod_strict")

    assert index["status"] == "ready"
    assert index["summary"]["evidence_refs"] == 3
    assert query["summary"]["matched"] == 3
    assert snapshot["schema_version"] == "dpone.data_product_trust_snapshot.v1"
    assert snapshot["domains"]["contract"]["status"] == "healthy"
    assert snapshot["domains"]["quality"]["status"] == "healthy"
    assert snapshot["domains"]["cost"]["status"] == "healthy"
    assert snapshot["domains"]["governance"]["status"] == "missing"
    assert "data_product_trust.required_domain_missing:governance" in snapshot["blockers"]
    assert gate["status"] == "blocked"
    assert "data_product_trust.score_below_threshold" in gate["blockers"]


def test_trust_snapshot_passes_with_complete_fresh_evidence_and_exports() -> None:
    index = TrustEvidenceLakeIndexer().index(
        manifests=[_manifest()],
        evidence_payloads=[
            _gate("dpone.schema_contract_gate.v1", "schema_contract_gate", "schema_contract_gate_id"),
            _gate("dpone.data_product_assertion_gate.v1", "data_product_assertion_gate", "assertion_gate_id"),
            _gate("dpone.data_product_policy_gate.v1", "data_product_policy_gate", "policy_gate_id"),
            _gate("dpone.data_product_compliance_gate.v1", "data_product_compliance_gate", "compliance_gate_id"),
            _gate("dpone.data_product_access_gate.v1", "data_product_access_gate", "access_gate_id"),
            _gate(
                "dpone.data_product_connection_rotation_gate.v1",
                "data_product_connection_rotation_gate",
                "connection_rotation_gate_id",
            ),
            _gate("dpone.data_product_cost_gate.v1", "data_product_cost_gate", "cost_gate_id"),
            _gate("dpone.data_product_ring_gate.v1", "data_product_ring_gate", "ring_gate_id"),
        ],
    )

    snapshot = TrustSnapshotBuilder().snapshot(index=index, product_id="analytics.orders", profile="prod_strict")
    gate = TrustGate().evaluate(snapshot=snapshot, profile="prod_strict")
    report = TrustReportRenderer().report(snapshot=snapshot, gate=gate)
    export = TrustExportRenderer().export(snapshot=snapshot, target="json")

    assert snapshot["status"] == "allowed"
    assert snapshot["trust_score"] >= 0.85
    assert gate["status"] == "allowed"
    assert "# Data Product Trust Center" in report["markdown"]
    assert export["schema_version"] == "dpone.data_product_trust_export.v1"
    assert export["target"] == "json"

    for name, payload in (
        ("data-product-evidence-lake-index.schema.json", index),
        ("data-product-trust-query-result.schema.json", TrustQueryEngine().query(index=index)),
        ("data-product-trust-snapshot.schema.json", snapshot),
        ("data-product-trust-gate.schema.json", gate),
        ("data-product-trust-report.schema.json", report),
        ("data-product-trust-export.schema.json", export),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def test_advisory_trust_gate_converts_blockers_to_warnings() -> None:
    manifest = _manifest()
    manifest["sink"]["options"]["data_product"]["trust_center"]["profile"] = "advisory"
    snapshot = TrustSnapshotBuilder().snapshot(
        index=TrustEvidenceLakeIndexer().index(manifests=[manifest], evidence_payloads=()),
        product_id="analytics.orders",
        profile="advisory",
    )
    gate = TrustGate().evaluate(snapshot=snapshot, profile="advisory")

    assert gate["status"] == "warning"
    assert gate["blockers"] == []
    assert "data_product_trust.required_domain_missing:contract" in gate["warnings"]


def _manifest(*, enabled: bool = True) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "trust_center": {
                        "enabled": enabled,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "stale_evidence_policy": "block",
                        "stale_after_seconds": 86400,
                        "required_domains": [
                            "contract",
                            "quality",
                            "governance",
                            "compliance",
                            "access",
                            "connection_security",
                            "cost",
                            "rollout",
                        ],
                        "include_artifacts": [
                            "schema_contract_gate",
                            "data_product_assertion_gate",
                            "data_product_policy_gate",
                            "data_product_compliance_gate",
                            "data_product_access_gate",
                            "data_product_connection_rotation_gate",
                            "data_product_cost_gate",
                            "data_product_ring_gate",
                        ],
                        "trust_score": {
                            "enabled": True,
                            "minimum": {"prod_strict": 0.85, "regulated": 0.95},
                            "weights": {
                                "contract": 0.15,
                                "quality": 0.15,
                                "governance": 0.15,
                                "compliance": 0.15,
                                "access": 0.15,
                                "connection_security": 0.05,
                                "cost": 0.1,
                                "rollout": 0.1,
                            },
                        },
                    },
                }
            }
        }
    }


def _gate(schema_version: str, kind: str, id_key: str, *, status: str = "allowed") -> dict:
    return {
        "schema_version": schema_version,
        "status": status,
        "product_id": "analytics.orders",
        "product": {"id": "analytics.orders", "owner": "data-platform", "tier": "gold", "criticality": "high"},
        id_key: f"sha256:{kind}",
        "blockers": [],
        "warnings": [],
    }
