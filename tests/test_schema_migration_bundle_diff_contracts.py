from __future__ import annotations

import json

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_diff import (
    MigrationBundleDiffBuilder,
    MigrationBundleDiffInputs,
    MigrationBundleDiffOptions,
)


def test_identical_bundles_produce_same_status_and_stable_diff_id() -> None:
    base = _bundle(_pack(), attest=True)
    inputs = _inputs(base=base, head=base)
    builder = MigrationBundleDiffBuilder()

    first = builder.build(inputs=inputs, options=MigrationBundleDiffOptions(require_attestation=True))
    second = builder.build(inputs=inputs, options=MigrationBundleDiffOptions(require_attestation=True))

    assert first["schema_version"] == "dpone.schema_migration_bundle_diff.v1"
    assert first["status"] == "same"
    assert first["changes"] == []
    assert first["diff_id"] == second["diff_id"]


def test_pack_ddl_change_is_high_severity_review_delta() -> None:
    base_pack = _pack(ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",))
    head_pack = _pack(ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 16384",))
    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=_bundle(base_pack, attest=True), head=_bundle(head_pack, attest=True)),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "breaking"
    change = _change(diff, "migration_pack", "migration_pack.ddl")
    assert change["severity"] == "high"
    assert change["classification"] == "physical"
    assert "Review changed migration DDL before applying this bundle." == change["reviewer_action"]


def test_impact_required_approval_added_is_high_severity() -> None:
    pack = _pack()
    base = _bundle(pack, impact={"required_approvals": []}, attest=True)
    head = _bundle(pack, impact={"required_approvals": ["compatibility_breaking"]}, attest=True)

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=base, head=head),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "breaking"
    change = _change(diff, "impact_plan", "impact_plan.required_approvals")
    assert change["severity"] == "high"
    assert change["risk_tags"] == ["compatibility_breaking"]
    assert "Approve compatibility_breaking risk or update approval artifact." == change["reviewer_action"]


def test_destructive_risk_added_is_critical() -> None:
    pack = _pack()
    base = _bundle(pack, impact={"risk_tags": []}, attest=True)
    head = _bundle(pack, impact={"risk_tags": ["data_destructive"]}, attest=True)

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=base, head=head),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "breaking"
    change = _change(diff, "impact_plan", "impact_plan.risk_tags")
    assert change["severity"] == "critical"
    assert change["risk_tags"] == ["data_destructive"]


def test_approval_risk_removed_is_breaking() -> None:
    pack = _pack()
    base = _bundle(pack, approval={"approved_risks": ["compatibility_breaking"]}, attest=True)
    head = _bundle(pack, approval={"approved_risks": []}, attest=True)

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=base, head=head),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "breaking"
    change = _change(diff, "approval", "approval.approved_risks")
    assert change["severity"] == "high"
    assert change["classification"] == "approval"


def test_gate_allowed_to_blocked_is_critical() -> None:
    pack = _pack()
    base = _bundle(pack, attest=True)
    head = _bundle(pack, attest=True)
    base_gate = _gate(base, status="allowed")
    head_gate = _gate(head, status="blocked", blockers=["migration_bundle_gate.required_artifact_missing:approval"])

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=base, head=head, base_gate=base_gate, head_gate=head_gate),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "breaking"
    change = _change(diff, "gate", "gate.status")
    assert change["severity"] == "critical"
    assert change["classification"] == "policy"


def test_target_mismatch_blocks_by_default() -> None:
    base = _bundle(_pack(table="analytics.orders"), attest=True)
    head = _bundle(_pack(table="analytics.customers"), attest=True)

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(base=base, head=head),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "blocked"
    assert "migration_bundle_diff.target_mismatch" in diff["blockers"]


def test_failed_verification_blocks_diff() -> None:
    bundle = _bundle(_pack(), attest=False)

    diff = MigrationBundleDiffBuilder().build(
        inputs=_inputs(
            base=bundle,
            head=bundle,
            base_verification={
                "status": "blocked",
                "blockers": ["migration_bundle.attestation_missing"],
                "warnings": [],
            },
        ),
        options=MigrationBundleDiffOptions(require_attestation=True),
    )

    assert diff["status"] == "blocked"
    assert "migration_bundle_diff.base_verification_blocked" in diff["blockers"]
    assert "migration_bundle.attestation_missing" in diff["blockers"]


def _inputs(
    *,
    base: dict[str, object],
    head: dict[str, object],
    base_gate: dict[str, object] | None = None,
    head_gate: dict[str, object] | None = None,
    base_verification: dict[str, object] | None = None,
    head_verification: dict[str, object] | None = None,
) -> MigrationBundleDiffInputs:
    return MigrationBundleDiffInputs(
        base_bundle=base,
        head_bundle=head,
        base_artifacts=dict(base["_payloads"]),
        head_artifacts=dict(head["_payloads"]),
        base_verification=base_verification or {"status": "passed", "blockers": [], "warnings": []},
        head_verification=head_verification or {"status": "passed", "blockers": [], "warnings": []},
        base_gate=base_gate,
        head_gate=head_gate,
    )


def _bundle(
    pack: MigrationPack,
    *,
    impact: dict[str, object] | None = None,
    approval: dict[str, object] | None = None,
    attest: bool,
) -> dict[str, object]:
    pack_payload = pack.to_dict(command="plan")
    artifacts = [_artifact("migration_pack", "pack.json", pack_payload, required=True)]
    payloads: dict[str, dict[str, object]] = {"migration_pack": pack_payload}
    if impact is not None:
        impact_payload = {
            "schema_version": "dpone.schema_impact_plan.v1",
            "pack_id": pack.pack_id,
            "impact_plan_id": "sha256:" + "1" * 64,
            "required_approvals": impact.get("required_approvals", []),
            "risk_tags": impact.get("risk_tags", []),
            "blockers": [],
            "warnings": impact.get("warnings", []),
        }
        artifacts.append(_artifact("impact_plan", "impact.json", impact_payload, required=False))
        payloads["impact_plan"] = impact_payload
    if approval is not None:
        approval_payload = {
            "pack_id": pack.pack_id,
            "approved_by": "finance-data-owner",
            "approved_risks": approval.get("approved_risks", []),
        }
        artifacts.append(_artifact("approval", "approval.json", approval_payload, required=False))
        payloads["approval"] = approval_payload
    bundle = MigrationBundleBuilder().build(artifacts=tuple(artifacts), attest=attest)
    bundle["_payloads"] = payloads
    return bundle


def _pack(
    *,
    table: str = "analytics.orders",
    ddl: tuple[str, ...] = ("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
) -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=table),
        desired={"sink_type": "clickhouse", "table": table, "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": table, "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=ddl,
        strategy="online_safe",
    )


def _gate(bundle: dict[str, object], *, status: str, blockers: list[str] | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_bundle_gate.v1",
        "status": status,
        "profile": "prod_strict",
        "bundle_id": bundle["bundle_id"],
        "pack_id": bundle["pack_id"],
        "target": bundle["target"],
        "blockers": blockers or [],
        "warnings": [],
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _change(diff: dict[str, object], kind: str, path: str) -> dict[str, object]:
    for change in diff["changes"]:
        if change["kind"] == kind and change["path"] == path:
            return change
    raise AssertionError(f"change {kind}:{path} not found in {diff['changes']}")
