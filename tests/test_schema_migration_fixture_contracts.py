from __future__ import annotations

import json
from pathlib import Path

from jsonschema import Draft202012Validator

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.readiness.schema_migration_bundle import MigrationBundleBuilder, MigrationEvidenceArtifact
from dpone.readiness.schema_migration_bundle_policy import (
    MigrationBundlePolicyEvaluator,
    MigrationBundlePolicyOptions,
)
from dpone.readiness.schema_migration_data_profile import MigrationDataProfileAnalyzer
from dpone.readiness.schema_migration_fixture import (
    ArtifactSampleFixtureProvider,
    DataMaskingPolicy,
    MigrationFixturePlanner,
    SyntheticMigrationFixtureProvider,
)
from dpone.readiness.schema_migration_rehearsal import MigrationRehearsalCertifier


def test_fixture_plan_binds_pack_manifest_policy_and_hierarchy() -> None:
    pack = _pack()
    manifest = {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "rehearsal": {
                            "data_fixture": {
                                "mode": "synthetic",
                                "min_rows": 10_000,
                                "min_columns": 4,
                                "include_edge_cases": True,
                                "preserve_hierarchy": True,
                                "seed": "dpone-rehearsal-v1",
                            },
                            "quality_profile": {"row_count": True, "duplicate_key": True, "null_key": True},
                        }
                    }
                }
            }
        },
        "schema": {
            "columns": [
                {"name": "id", "type": "Int64", "key": True},
                {"name": "parent_id", "type": "Nullable(Int64)", "parent": "id"},
                {"name": "amount", "type": "Decimal(18, 2)"},
                {"name": "comment", "type": "Nullable(String)"},
            ]
        },
    }

    plan = MigrationFixturePlanner().plan(pack=pack.to_dict(command="plan"), manifest=manifest, policy={})

    assert plan["schema_version"] == "dpone.schema_migration_fixture_plan.v1"
    assert plan["status"] == "planned"
    assert plan["pack_id"] == pack.pack_id
    assert plan["mode"] == "synthetic"
    assert plan["requirements"]["min_rows"] == 10_000
    assert plan["requirements"]["min_columns"] == 4
    assert plan["columns"][:2] == [
        {"name": "id", "type": "Int64"},
        {"name": "parent_id", "type": "Nullable(Int64)"},
    ]
    assert plan["key_columns"] == ["id"]
    assert plan["hierarchy"] == {"parent_column": "parent_id", "child_column": "id"}
    assert plan["checks"]["row_count"] is True
    assert plan["checks"]["duplicate_key"] is True
    assert plan["checks"]["null_key"] is True
    assert plan["fixture_plan_id"].startswith("sha256:")


def test_synthetic_fixture_is_deterministic_wide_and_contains_edge_cases() -> None:
    plan = _fixture_plan(min_rows=24, min_columns=8)

    first = SyntheticMigrationFixtureProvider().rows(plan)
    second = SyntheticMigrationFixtureProvider().rows(plan)

    assert first == second
    assert len(first) == 24
    assert len(first[0]) >= 8
    assert any(row["comment"] is None for row in first)
    assert any(row["comment"] == "" for row in first)
    assert any(row["amount"] == "-1.00" for row in first)
    assert first[3]["parent_id"] == first[0]["id"]


def test_masked_sample_requires_salt_and_masks_sensitive_values(tmp_path: Path, monkeypatch) -> None:
    sample_path = tmp_path / "orders.jsonl"
    sample_path.write_text(
        json.dumps({"id": 1, "email": "person@example.com", "phone": "+100", "full_name": "Ada Lovelace"}) + "\n",
        encoding="utf-8",
    )
    plan = {
        **_fixture_plan(mode="masked_sample", artifact_path=str(sample_path)),
        "masking": {
            "salt_env": "DPONE_REHEARSAL_MASKING_SALT",
            "columns": {"email": "hash", "phone": "null", "full_name": "token"},
        },
    }

    rows_without_salt = ArtifactSampleFixtureProvider(masking=DataMaskingPolicy()).rows(plan)

    assert rows_without_salt == []
    monkeypatch.setenv("DPONE_REHEARSAL_MASKING_SALT", "salt")
    rows = ArtifactSampleFixtureProvider(masking=DataMaskingPolicy()).rows(plan)
    assert rows[0]["email"] != "person@example.com"
    assert rows[0]["email"].startswith("sha256:")
    assert rows[0]["phone"] is None
    assert rows[0]["full_name"].startswith("tok_")


def test_data_profile_blocks_duplicate_null_key_and_orphan_child() -> None:
    rows = [
        {"id": 1, "parent_id": None, "amount": "1.00"},
        {"id": 1, "parent_id": None, "amount": "1.00"},
        {"id": None, "parent_id": 99, "amount": "2.00"},
    ]

    profile = MigrationDataProfileAnalyzer().profile(
        rows=rows,
        pack_id="sha256:" + "1" * 64,
        fixture_build_id="sha256:" + "2" * 64,
        stage="after",
        key_columns=("id",),
        hierarchy={"parent_column": "parent_id", "child_column": "id"},
        checks={
            "row_count": True,
            "typed_hash": True,
            "duplicate_key": True,
            "null_key": True,
            "nested_parent_child": True,
        },
    )

    assert profile["schema_version"] == "dpone.schema_migration_data_profile.v1"
    assert profile["status"] == "blocked"
    assert "schema_migration_profile.duplicate_key:id" in profile["blockers"]
    assert "schema_migration_profile.null_key:id" in profile["blockers"]
    assert "schema_migration_profile.orphan_child:parent_id" in profile["blockers"]
    assert profile["metrics"]["row_count"] == 3
    assert profile["profile_id"].startswith("sha256:")


def test_certifier_includes_fixture_profile_evidence_and_blocks_row_loss() -> None:
    run = {
        "pack_id": "sha256:" + "1" * 64,
        "bundle_id": "sha256:" + "2" * 64,
        "environment": "stage",
        "target": {"sink_type": "clickhouse", "table": "analytics.orders"},
        "status": "passed",
        "executed": True,
        "metrics": {"duration_ms": 100},
        "rehearsal_run_id": "sha256:" + "3" * 64,
        "blockers": [],
        "warnings": [],
    }
    fixture_build = {
        "schema_version": "dpone.schema_migration_fixture_build.v1",
        "fixture_build_id": "sha256:" + "4" * 64,
        "pack_id": run["pack_id"],
        "status": "built",
        "blockers": [],
        "warnings": [],
        "metrics": {"row_count": 10},
    }
    before = _profile(run["pack_id"], fixture_build["fixture_build_id"], "before", 10)
    after = _profile(run["pack_id"], fixture_build["fixture_build_id"], "after", 9)

    certificate = MigrationRehearsalCertifier().certify(
        run=run,
        profile="prod_strict",
        fixture_build=fixture_build,
        before_profile=before,
        after_profile=after,
    )

    assert certificate["status"] == "blocked"
    assert certificate["fixture_build_id"] == fixture_build["fixture_build_id"]
    assert certificate["before_profile_id"] == before["profile_id"]
    assert certificate["after_profile_id"] == after["profile_id"]
    assert "schema_migration_rehearsal.row_count_drift" in certificate["blockers"]


def test_bundle_policy_can_require_fixture_build_and_quality_profile() -> None:
    pack = _pack()
    fixture_build = {
        "schema_version": "dpone.schema_migration_fixture_build.v1",
        "fixture_build_id": "sha256:" + "4" * 64,
        "pack_id": pack.pack_id,
        "status": "built",
        "blockers": [],
        "warnings": [],
    }
    quality_profile = _profile(pack.pack_id, fixture_build["fixture_build_id"], "after", 10)
    artifacts = (
        _artifact("migration_pack", "pack.json", pack.to_dict(command="plan"), required=True),
        _artifact("fixture_build", "fixture-build.json", fixture_build, required=False),
        _artifact("quality_profile", "profile-after.json", quality_profile, required=False),
    )
    bundle = MigrationBundleBuilder().build(artifacts=artifacts, attest=True)

    decision = MigrationBundlePolicyEvaluator().evaluate(
        bundle=bundle,
        verification={"status": "passed", "blockers": [], "warnings": []},
        policy=MigrationBundlePolicyOptions.resolve(
            profile="prod_strict",
            policy_payload={"required_artifacts": ["migration_pack", "fixture_build", "quality_profile"]},
        ),
        artifact_payloads={artifact.kind: artifact.payload for artifact in artifacts},
    )

    assert bundle["status"] == "ready"
    assert bundle["summary"]["fixture_build_id"] == fixture_build["fixture_build_id"]
    assert bundle["summary"]["quality_profile_id"] == quality_profile["profile_id"]
    assert decision["status"] == "allowed"


def test_fixture_public_json_schemas_validate_generated_artifacts() -> None:
    pack = _pack()
    plan = MigrationFixturePlanner().plan(
        pack=pack.to_dict(command="plan"), manifest={}, policy={"mode": "synthetic", "min_rows": 2}
    )
    rows = SyntheticMigrationFixtureProvider().rows(plan)
    fixture_build = {
        "schema_version": "dpone.schema_migration_fixture_build.v1",
        "fixture_build_id": "sha256:" + "4" * 64,
        "pack_id": pack.pack_id,
        "fixture_plan_id": plan["fixture_plan_id"],
        "target": plan["target"],
        "status": "built",
        "mode": "synthetic",
        "columns": [column["name"] for column in plan["columns"]],
        "key_columns": plan["key_columns"],
        "hierarchy": plan["hierarchy"],
        "checks": plan["checks"],
        "metrics": {"row_count": len(rows), "column_count": len(plan["columns"])},
        "blockers": [],
        "warnings": [],
    }
    profile = MigrationDataProfileAnalyzer().profile(
        rows=rows,
        pack_id=pack.pack_id,
        fixture_build_id=fixture_build["fixture_build_id"],
        stage="after",
        key_columns=plan["key_columns"],
        hierarchy=plan["hierarchy"],
        checks=plan["checks"],
    )

    _validate_schema("fixture-plan.schema.json", plan)
    _validate_schema("fixture-build.schema.json", fixture_build)
    _validate_schema("data-profile.schema.json", profile)


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={
            "sink_type": "clickhouse",
            "table": "analytics.orders",
            "columns": [
                {"name": "id", "type": "Int64"},
                {"name": "parent_id", "type": "Nullable(Int64)"},
                {"name": "amount", "type": "Decimal(18, 2)"},
                {"name": "comment", "type": "Nullable(String)"},
            ],
            "order_by": ["id"],
        },
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )


def _fixture_plan(
    *, mode: str = "synthetic", artifact_path: str | None = None, min_rows: int = 8, min_columns: int = 4
) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_fixture_plan.v1",
        "fixture_plan_id": "sha256:" + "5" * 64,
        "pack_id": "sha256:" + "1" * 64,
        "target": {"sink_type": "clickhouse", "table": "analytics.orders"},
        "status": "planned",
        "mode": mode,
        "artifact_path": artifact_path,
        "requirements": {
            "min_rows": min_rows,
            "max_rows": 100_000,
            "min_columns": min_columns,
            "include_edge_cases": True,
            "preserve_hierarchy": True,
            "seed": "dpone-rehearsal-v1",
        },
        "columns": [
            {"name": "id", "type": "Int64"},
            {"name": "parent_id", "type": "Nullable(Int64)"},
            {"name": "amount", "type": "Decimal(18, 2)"},
            {"name": "comment", "type": "Nullable(String)"},
            *[{"name": f"extra_{index}", "type": "String"} for index in range(max(0, min_columns - 4))],
        ],
        "key_columns": ["id"],
        "hierarchy": {"parent_column": "parent_id", "child_column": "id"},
        "checks": {"row_count": True, "typed_hash": True},
        "blockers": [],
        "warnings": [],
    }


def _profile(pack_id: object, fixture_build_id: object, stage: str, row_count: int) -> dict[str, object]:
    profile_id = "sha256:" + (stage[0] if stage else "9") * 64
    return {
        "schema_version": "dpone.schema_migration_data_profile.v1",
        "profile_id": profile_id,
        "pack_id": pack_id,
        "fixture_build_id": fixture_build_id,
        "stage": stage,
        "status": "profiled",
        "metrics": {"row_count": row_count},
        "blockers": [],
        "warnings": [],
    }


def _artifact(kind: str, path: str, payload: dict[str, object], *, required: bool) -> MigrationEvidenceArtifact:
    content = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return MigrationEvidenceArtifact.from_bytes(kind=kind, path=path, content=content, required=required)


def _validate_schema(schema_name: str, payload: dict[str, object]) -> None:
    schema_path = Path("docs/schemas/schema-migration") / schema_name
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator(schema).validate(payload)
