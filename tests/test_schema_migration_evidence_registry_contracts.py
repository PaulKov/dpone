from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_evidence_registry import (
    MigrationEvidenceAuditReporter,
    MigrationEvidenceQuery,
    MigrationEvidenceQueryService,
    MigrationEvidenceRecorder,
)
from dpone.readiness.schema_migration_evidence_registry_sqlite import SqliteEvidenceRegistryStore
from dpone.readiness.schema_migration_evidence_registry_store import LocalJsonEvidenceRegistryStore


def test_registry_records_bundle_gate_trust_and_diff_evidence() -> None:
    bundle = _bundle(_pack(), attest=True)
    gate = _gate(bundle, status="allowed")
    trust = _trust(bundle, status="trusted")
    diff = _diff(bundle, status="same")

    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=gate,
        trust_verification=trust,
        diff=diff,
        environment="prod",
        stage="approved",
        actor="ci",
    )

    assert record["schema_version"] == "dpone.schema_migration_evidence_registry_record.v1"
    assert record["status"] == "ready"
    assert record["target"] == {"sink_type": "clickhouse", "table": "analytics.orders"}
    assert record["environment"] == "prod"
    assert record["stage"] == "approved"
    assert record["pack_id"] == bundle["pack_id"]
    assert record["bundle_id"] == bundle["bundle_id"]
    assert record["gate_id"] == gate["gate_id"]
    assert record["trust_verification_id"] == trust["trust_verification_id"]
    assert record["diff_id"] == diff["diff_id"]
    assert record["record_id"].startswith("sha256:")
    assert record["scm"]["repository"] == "https://github.com/acme/data-platform"


def test_registry_blocks_mismatched_gate_and_trust_relationships() -> None:
    bundle = _bundle(_pack(), attest=True)

    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate={**_gate(bundle, status="allowed"), "pack_id": "sha256:" + "0" * 64},
        trust_verification={**_trust(bundle, status="trusted"), "bundle_id": "sha256:" + "1" * 64},
        diff=None,
        environment="prod",
        stage="approved",
        actor="ci",
    )

    assert record["status"] == "blocked"
    assert "evidence_registry.gate_pack_id_mismatch" in record["blockers"]
    assert "evidence_registry.trust_bundle_id_mismatch" in record["blockers"]


def test_local_json_store_is_idempotent_and_blocks_conflicting_logical_record(tmp_path: Path) -> None:
    store = LocalJsonEvidenceRegistryStore(tmp_path / "registry.json")
    record = _record(stage="approved")

    first = store.append(record)
    duplicate = store.append(record)
    conflict = store.append({**record, "record_id": "sha256:" + "9" * 64, "warnings": ["changed"]})

    assert first["status"] == "recorded"
    assert duplicate["status"] == "duplicate"
    assert conflict["status"] == "blocked"
    assert "evidence_registry.record_conflict" in conflict["blockers"]
    assert len(store.query(MigrationEvidenceQuery(target="clickhouse.analytics.orders"))) == 1


def test_history_latest_and_audit_report_filter_records(tmp_path: Path) -> None:
    store = LocalJsonEvidenceRegistryStore(tmp_path / "registry.json")
    store.append(_record(stage="planned", status="ready", recorded_at="2026-06-22T10:00:00Z"))
    store.append(
        _record(
            stage="approved",
            status="blocked",
            recorded_at="2026-06-22T11:00:00Z",
            bundle_id="sha256:" + "8" * 64,
        )
    )
    store.append(
        _record(
            stage="approved",
            status="ready",
            recorded_at="2026-06-22T12:00:00Z",
            bundle_id="sha256:" + "9" * 64,
        )
    )
    service = MigrationEvidenceQueryService(store)

    history = service.history(
        MigrationEvidenceQuery(target="clickhouse.analytics.orders", environment="prod", stage="approved")
    )
    latest = service.latest(
        MigrationEvidenceQuery(target="clickhouse.analytics.orders", environment="prod", stage="approved")
    )
    report = MigrationEvidenceAuditReporter().build(
        records=tuple(history["records"]),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    assert history["schema_version"] == "dpone.schema_migration_evidence_registry_query.v1"
    assert [item["status"] for item in history["records"]] == ["blocked", "ready"]
    assert latest["record"]["status"] == "ready"
    assert report["schema_version"] == "dpone.schema_migration_evidence_audit_report.v1"
    assert "missing_trust" not in report["warnings"]
    assert "approved" in report["markdown"]


def test_sqlite_store_matches_local_json_query_semantics(tmp_path: Path) -> None:
    local = LocalJsonEvidenceRegistryStore(tmp_path / "registry.json")
    sqlite = SqliteEvidenceRegistryStore(tmp_path / "registry.sqlite3")
    records = (
        _record(stage="planned", recorded_at="2026-06-22T10:00:00Z"),
        _record(stage="approved", recorded_at="2026-06-22T11:00:00Z"),
    )

    for record in records:
        local.append(record)
        sqlite.append(record)

    query = MigrationEvidenceQuery(target="clickhouse.analytics.orders", environment="prod")
    assert local.query(query) == sqlite.query(query)


def test_registry_records_certified_rehearsal_certificate_in_bundle_evidence() -> None:
    bundle = _bundle_with_rehearsal(_pack())

    record = MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=_gate(bundle, status="allowed"),
        trust_verification=_trust(bundle, status="trusted"),
        diff=None,
        environment="stage",
        stage="certified",
        actor="ci",
    )
    report = MigrationEvidenceAuditReporter().build(
        records=(record,),
        target="clickhouse.analytics.orders",
        date_from="2026-06-22",
        date_to="2026-06-22",
    )

    kinds = {item["kind"] for item in record["artifact_refs"]}
    assert record["status"] == "ready"
    assert "rehearsal_certificate" in kinds
    assert "certified" in report["markdown"]


def _record(
    *,
    stage: str,
    status: str = "ready",
    recorded_at: str = "2026-06-22T12:00:00Z",
    bundle_id: str | None = None,
) -> dict[str, object]:
    bundle = _bundle(_pack(), attest=True)
    if bundle_id:
        bundle["bundle_id"] = bundle_id
    return MigrationEvidenceRecorder().build_record(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        gate=_gate(bundle, status="allowed"),
        trust_verification=_trust(bundle, status="trusted"),
        diff=_diff(bundle, status="same"),
        environment="prod",
        stage=stage,
        actor="ci",
        recorded_at=recorded_at,
    ) | {"status": status}


def _bundle(pack: MigrationPack, *, attest: bool) -> dict[str, object]:
    artifact = _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True)
    bundle = MigrationBundleBuilder().build(artifacts=(artifact,), attest=attest)
    bundle["_artifact_bytes"] = {"pack.json": artifact.content}
    return bundle


def _bundle_with_rehearsal(pack: MigrationPack) -> dict[str, object]:
    certificate = {
        "schema_version": "dpone.schema_migration_rehearsal_certificate.v1",
        "certificate_id": "sha256:" + "6" * 64,
        "pack_id": pack.pack_id,
        "bundle_id": None,
        "environment": "stage",
        "target": pack.target.to_dict(),
        "status": "certified",
        "profile": "prod_strict",
        "checks": [],
        "blockers": [],
        "warnings": [],
        "metrics": {"duration_ms": 10, "rows_validated": 1, "operations_executed": 1},
    }
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("rehearsal_certificate", "rehearsal.json", certificate, required=False),
    )
    return MigrationBundleBuilder().build(artifacts=artifacts, attest=True)


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _gate(bundle: dict[str, object], *, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_bundle_gate.v1",
        "status": status,
        "gate_id": "sha256:" + "2" * 64,
        "bundle_id": bundle["bundle_id"],
        "pack_id": bundle["pack_id"],
        "target": bundle["target"],
        "blockers": [],
        "warnings": [],
    }


def _trust(bundle: dict[str, object], *, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_trust_verification.v1",
        "status": status,
        "trust_verification_id": "sha256:" + "3" * 64,
        "bundle_id": bundle["bundle_id"],
        "pack_id": bundle["pack_id"],
        "provenance_id": "sha256:" + "4" * 64,
        "scm": {
            "repository": "https://github.com/acme/data-platform",
            "commit_sha": "a" * 40,
            "ref": "refs/heads/main",
            "run_id": "123",
        },
        "blockers": [],
        "warnings": [],
    }


def _diff(bundle: dict[str, object], *, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_bundle_diff.v1",
        "status": status,
        "diff_id": "sha256:" + "5" * 64,
        "head_bundle_id": bundle["bundle_id"],
        "head_pack_id": bundle["pack_id"],
        "blockers": [],
        "warnings": [],
        "changes": [],
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)
