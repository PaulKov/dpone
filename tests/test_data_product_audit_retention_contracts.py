from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.data_product_audit_retention import (
    AuditArchiveVerifier,
    AuditEvidenceArchivePlanner,
    AuditEvidenceArchiver,
    AuditRetentionPlanner,
    LegalHoldService,
)
from dpone.readiness.data_product_audit_retention_store import LocalFsAuditArchiveStore


def test_disabled_audit_retention_emits_noop_plan() -> None:
    plan = AuditEvidenceArchivePlanner().plan(manifest=_manifest(enabled=False), evidence={})

    assert plan["schema_version"] == "dpone.data_product_audit_archive_plan.v1"
    assert plan["status"] == "disabled"
    assert plan["product"]["id"] == "analytics.orders"
    assert plan["artifact_refs"] == []
    assert plan["blockers"] == []


def test_archive_plan_hashes_evidence_and_blocks_missing_required_artifact() -> None:
    evidence = {
        "data_product_audit_package": _artifact("data_product_audit_package", "allowed"),
        "data_product_compliance_gate": _artifact("data_product_compliance_gate", "allowed"),
    }

    plan = AuditEvidenceArchivePlanner().plan(manifest=_manifest(), evidence=evidence)
    repeated = AuditEvidenceArchivePlanner().plan(manifest=_manifest(), evidence=evidence)

    assert plan["status"] == "blocked"
    assert plan["audit_archive_plan_id"] == repeated["audit_archive_plan_id"]
    assert plan["archive"]["store_backend"] == "local_fs"
    assert plan["hash_chain"]
    assert plan["merkle_root"].startswith("sha256:")
    assert {
        "data_product_audit_retention.required_artifact_missing:data_product_policy_gate",
        "data_product_audit_retention.required_artifact_missing:data_product_authority_gate",
    } <= set(plan["blockers"])


def test_archive_run_verify_retention_and_legal_hold(tmp_path: Path) -> None:
    manifest = _manifest(store_uri=str(tmp_path / "archive"))
    evidence = {
        "data_product_audit_package": _artifact("data_product_audit_package", "allowed"),
        "data_product_compliance_gate": _artifact("data_product_compliance_gate", "allowed"),
        "data_product_policy_gate": _artifact("data_product_policy_gate", "waived"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
    }
    plan = AuditEvidenceArchivePlanner().plan(manifest=manifest, evidence=evidence, observed_at="2026-06-28T12:00:00Z")

    dry_run = AuditEvidenceArchiver(LocalFsAuditArchiveStore()).run(plan=plan, execute=False)
    assert dry_run["status"] == "dry_run"
    assert not Path(dry_run["archive_uri"]).exists()
    run = AuditEvidenceArchiver(LocalFsAuditArchiveStore()).run(plan=plan, execute=True)
    verification = AuditArchiveVerifier(LocalFsAuditArchiveStore()).verify(archive_run=run)
    hold = LegalHoldService().apply(
        archive_run=run,
        reason="SOC2 evidence hold",
        authority_gate={"schema_version": "dpone.data_product_authority_gate.v1", "status": "allowed"},
    )
    retention = AuditRetentionPlanner().plan(
        manifest=manifest,
        archive_verification=verification,
        legal_hold=hold,
        observed_at="2037-01-01T00:00:00Z",
    )

    assert run["status"] == "archived"
    assert (Path(run["archive_uri"]) / "archive-manifest.json").exists()
    assert verification["schema_version"] == "dpone.data_product_audit_archive_verification.v1"
    assert verification["status"] == "verified"
    assert hold["status"] == "held"
    assert retention["status"] == "blocked"
    assert retention["decision"] == "held"
    assert "data_product_audit_retention.archive_under_legal_hold" in retention["blockers"]


def test_archive_verification_blocks_hash_mismatch(tmp_path: Path) -> None:
    manifest = _manifest(store_uri=str(tmp_path / "archive"))
    evidence = {
        "data_product_audit_package": _artifact("data_product_audit_package", "allowed"),
        "data_product_compliance_gate": _artifact("data_product_compliance_gate", "allowed"),
        "data_product_policy_gate": _artifact("data_product_policy_gate", "allowed"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
    }
    plan = AuditEvidenceArchivePlanner().plan(manifest=manifest, evidence=evidence)
    run = AuditEvidenceArchiver(LocalFsAuditArchiveStore()).run(plan=plan, execute=True)
    archived = Path(run["archive_uri"]) / "artifacts" / "data_product_policy_gate.json"
    archived.write_text('{"status": "tampered"}\n', encoding="utf-8")

    verification = AuditArchiveVerifier(LocalFsAuditArchiveStore()).verify(archive_run=run)

    assert verification["status"] == "blocked"
    assert "data_product_audit_retention.artifact_hash_mismatch:data_product_policy_gate" in verification["blockers"]


def test_legal_hold_requires_reason_and_authority_gate() -> None:
    blocked = LegalHoldService().apply(
        archive_run={"audit_archive_run_id": "sha256:" + "1" * 64, "status": "archived"},
        reason="",
        authority_gate={"schema_version": "dpone.data_product_authority_gate.v1", "status": "blocked"},
    )

    assert blocked["status"] == "blocked"
    assert "data_product_audit_retention.legal_hold_reason_required" in blocked["blockers"]
    assert "data_product_audit_retention.authority_gate_blocked" in blocked["blockers"]


def test_audit_retention_public_json_schemas_validate_artifacts(tmp_path: Path) -> None:
    manifest = _manifest(store_uri=str(tmp_path / "archive"))
    evidence = {
        "data_product_audit_package": _artifact("data_product_audit_package", "allowed"),
        "data_product_compliance_gate": _artifact("data_product_compliance_gate", "allowed"),
        "data_product_policy_gate": _artifact("data_product_policy_gate", "allowed"),
        "data_product_authority_gate": _artifact("data_product_authority_gate", "allowed"),
    }
    plan = AuditEvidenceArchivePlanner().plan(manifest=manifest, evidence=evidence)
    run = AuditEvidenceArchiver(LocalFsAuditArchiveStore()).run(plan=plan, execute=True)
    verification = AuditArchiveVerifier(LocalFsAuditArchiveStore()).verify(archive_run=run)
    retention = AuditRetentionPlanner().plan(manifest=manifest, archive_verification=verification)
    hold = LegalHoldService().apply(
        archive_run=run,
        reason="SOC2 evidence hold",
        authority_gate={"schema_version": "dpone.data_product_authority_gate.v1", "status": "allowed"},
    )

    for name, payload in (
        ("data-product-audit-archive-plan.schema.json", plan),
        ("data-product-audit-archive-run.schema.json", run),
        ("data-product-audit-archive-verification.schema.json", verification),
        ("data-product-audit-retention-plan.schema.json", retention),
        ("data-product-legal-hold.schema.json", hold),
    ):
        schema = json.loads((Path("docs/schemas/data-product") / name).read_text(encoding="utf-8"))
        Draft202012Validator(schema).validate(payload)


def _manifest(
    *,
    enabled: bool = True,
    mode: str = "gate",
    profile: str = "regulated",
    store_uri: str = ".dpone/audit-archive",
) -> dict:
    return {
        "sink": {
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "audit_retention": {
                        "enabled": enabled,
                        "mode": mode,
                        "profile": profile,
                        "stale_evidence_policy": "block",
                        "archive": {
                            "store_backend": "local_fs",
                            "store_uri": store_uri,
                            "layout": "{product_id}/{yyyy}/{mm}/{audit_archive_id}",
                            "include_artifacts": [
                                "data_product_audit_package",
                                "data_product_compliance_gate",
                                "data_product_policy_gate",
                                "data_product_authority_gate",
                            ],
                            "manifest_hash_chain": True,
                            "merkle_root": True,
                        },
                        "retention": {
                            "min_days": 2555,
                            "delete_after_days": 3650,
                            "expired_policy": "block",
                        },
                        "legal_hold": {
                            "enabled": True,
                            "policy": "block_delete",
                            "require_reason": True,
                            "require_authority_gate": True,
                        },
                    },
                }
            }
        }
    }


def _artifact(kind: str, status: str) -> dict:
    return {
        "schema_version": f"dpone.{kind}.v1",
        f"{kind.removeprefix('data_product_')}_id": "sha256:" + kind[:1] * 64,
        "status": status,
        "product_id": "analytics.orders",
        "recorded_at": "2026-06-28T12:00:00Z",
        "blockers": [f"{kind}.blocked"] if status == "blocked" else [],
        "warnings": [],
    }
