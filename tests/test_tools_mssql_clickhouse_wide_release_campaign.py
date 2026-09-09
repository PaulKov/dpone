from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_wide_release_campaign.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_wide_release_campaign", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _secret_context() -> tuple[str, ...]:
    return ("mssql-secret", "clickhouse-secret", "minio-access", "minio-secret")


def test_campaign_requires_complete_in_memory_secret_context(monkeypatch) -> None:
    module = _load_tool_module()
    for name in (
        "DPONE_IT_MSSQL_PASSWORD",
        "DPONE_IT_CH_PASSWORD",
        "DPONE_IT_S3_ACCESS_KEY",
        "DPONE_IT_S3_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError, match="secret_context_required"):
        module._campaign_secrets_from_environment()


def test_campaign_rejects_secret_bearing_authority_before_creating_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_tool_module()
    snapshot = module.capture_local_source_snapshot()
    snapshot = type(snapshot)(snapshot.git_head_sha, snapshot.source_snapshot_sha256, False)
    monkeypatch.setattr(module, "capture_local_source_snapshot", lambda: snapshot)
    generation = "sha256:" + "a" * 64
    dbt = tmp_path / "dbt.json"
    dbt.write_text(
        json.dumps(
            {
                "relation_generation_sha256": generation,
                "run_results_path": "run_results.json",
                "manifest_path": "manifest.json",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "run_results.json").write_text("{}\n", encoding="utf-8")
    (tmp_path / "manifest.json").write_text("{}\n", encoding="utf-8")
    authority = tmp_path / "authority.json"
    authority.write_text(json.dumps({"s3_bucket": "minio-secret"}), encoding="utf-8")
    receipts = {}
    for slot in module._SLOTS:
        receipt = tmp_path / f"{slot}.json"
        receipt.write_text("{}\n", encoding="utf-8")
        receipts[slot] = receipt
    monkeypatch.setattr(
        module,
        "load_authority",
        lambda *args, **kwargs: {
            "mssql_connection_sha256": "sha256:" + "1" * 64,
            "clickhouse_connection_sha256": "sha256:" + "2" * 64,
            "target_schema_sha256_by_slot": {slot: "sha256:" + "3" * 64 for slot in module._SLOTS},
            "s3_endpoint_sha256": "sha256:" + "4" * 64,
            "s3_bucket": "minio-secret",
            "s3_named_collection": "dpone_stage",
        },
    )
    monkeypatch.setattr(
        module,
        "require_exact_dbt_wide_evidence",
        lambda *args, **kwargs: {"relation_generation_sha256": generation},
    )
    monkeypatch.setattr(
        module,
        "verify_local_route_certification_receipt",
        lambda *args, **kwargs: {
            "receipt_sha256": "sha256:" + "5" * 64,
            "source_generation_sha256": generation,
        },
    )
    monkeypatch.setattr(module, "verify_campaign", lambda *args, **kwargs: {})
    output = tmp_path / "campaign"

    with pytest.raises(ValueError, match="secret_material_detected"):
        module.build_campaign(
            release_id="0.74.0",
            source_relation="dbt.wide",
            dbt_evidence=dbt,
            authority=authority,
            receipts=receipts,
            targets={slot: f"ch.{slot}" for slot in module._SLOTS},
            output_dir=output,
            mssql_connection_sha256="sha256:" + "1" * 64,
            clickhouse_connection_sha256="sha256:" + "2" * 64,
            target_schema_sha256_by_slot={slot: "sha256:" + "3" * 64 for slot in module._SLOTS},
            s3_endpoint_sha256="sha256:" + "4" * 64,
            s3_bucket="minio-secret",
            s3_named_collection="dpone_stage",
            forbidden_secret_values=("mssql-secret", "clickhouse-secret", "minio-access", "minio-secret"),
        )

    assert output.exists() is False


def test_wide_release_campaign_requires_three_unique_verified_slots(tmp_path: Path, monkeypatch) -> None:
    module = _load_tool_module()
    snapshot = module.capture_local_source_snapshot()
    snapshot = type(snapshot)(snapshot.git_head_sha, snapshot.source_snapshot_sha256, False)
    monkeypatch.setattr(module, "capture_local_source_snapshot", lambda: snapshot)
    generation = "sha256:" + "a" * 64
    dbt = tmp_path / "dbt.json"
    dbt.write_text(
        json.dumps(
            {
                "mssql_connection_sha256": "sha256:" + "b" * 64,
                "relation_generation_sha256": generation,
                "run_results_path": "dbt_run_results.json",
                "manifest_path": "dbt_manifest.json",
            }
        ),
        encoding="utf-8",
    )
    authority = tmp_path / "authority.json"
    authority.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(
        module,
        "require_exact_dbt_wide_evidence",
        lambda *args, **kwargs: {"relation_generation_sha256": generation},
    )

    def copy(source: Path, destination: Path) -> Path:
        destination.mkdir(parents=True, exist_ok=True)
        target = destination / source.name
        target.write_bytes(source.read_bytes())
        return target

    monkeypatch.setattr(module, "_copy_dbt_closure", copy)
    monkeypatch.setattr(module, "_copy_receipt_closure", copy)
    monkeypatch.setattr(module, "verify_campaign", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        module,
        "verify_local_route_certification_receipt",
        lambda *args, **kwargs: {
            "receipt_sha256": "sha256:" + "c" * 64,
            "source_generation_sha256": generation,
        },
    )
    receipts = {}
    for slot in module._SLOTS:
        path = tmp_path / f"{slot}.json"
        path.write_text("{}\n", encoding="utf-8")
        receipts[slot] = path
    targets = {slot: f"dpone_it.{slot}" for slot in module._SLOTS}
    target_schemas = {slot: "sha256:" + str(index + 1) * 64 for index, slot in enumerate(module._SLOTS)}
    monkeypatch.setattr(
        module,
        "load_authority",
        lambda *args, **kwargs: {
            "mssql_connection_sha256": "sha256:" + "b" * 64,
            "clickhouse_connection_sha256": "sha256:" + "d" * 64,
            "target_schema_sha256_by_slot": target_schemas,
            "s3_endpoint_sha256": "sha256:" + "e" * 64,
            "s3_bucket": "dpone-stage",
            "s3_named_collection": "dpone_stage",
        },
    )

    campaign = module.build_campaign(
        release_id="0.74.0",
        source_relation="wide_release_dbt.wide_dbt_result",
        dbt_evidence=dbt,
        authority=authority,
        receipts=receipts,
        targets=targets,
        output_dir=tmp_path / "campaign",
        mssql_connection_sha256="sha256:" + "b" * 64,
        clickhouse_connection_sha256="sha256:" + "d" * 64,
        target_schema_sha256_by_slot=target_schemas,
        s3_endpoint_sha256="sha256:" + "e" * 64,
        s3_bucket="dpone-stage",
        s3_named_collection="dpone_stage",
        forbidden_secret_values=_secret_context(),
    )

    payload = json.loads(campaign.read_text(encoding="utf-8"))
    assert [item["slot"] for item in payload["slots"]] == list(module._SLOTS)
    assert payload["production_certification"] == "UNVERIFIED"
    schema = json.loads(
        Path("docs/schemas/dpone.mssql-clickhouse-wide-release-campaign.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(payload)

    failed_output = tmp_path / "failed-campaign"
    monkeypatch.setattr(
        module,
        "verify_campaign",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("final_verification_failed")),
    )
    with pytest.raises(ValueError, match="final_verification_failed"):
        module.build_campaign(
            release_id="0.74.0",
            source_relation="wide_release_dbt.wide_dbt_result",
            dbt_evidence=dbt,
            authority=authority,
            receipts=receipts,
            targets=targets,
            output_dir=failed_output,
            mssql_connection_sha256="sha256:" + "b" * 64,
            clickhouse_connection_sha256="sha256:" + "d" * 64,
            target_schema_sha256_by_slot=target_schemas,
            s3_endpoint_sha256="sha256:" + "e" * 64,
            s3_bucket="dpone-stage",
            s3_named_collection="dpone_stage",
            forbidden_secret_values=_secret_context(),
        )
    assert failed_output.exists() is False
    assert list(tmp_path.glob(".failed-campaign.*")) == []

    with pytest.raises(ValueError, match="slot_closure_invalid"):
        module.build_campaign(
            release_id="0.74.0",
            source_relation="wide_release_dbt.wide_dbt_result",
            dbt_evidence=dbt,
            authority=authority,
            receipts={"bcp_native_required": receipts["bcp_native_required"]},
            targets=targets,
            output_dir=tmp_path / "invalid",
            mssql_connection_sha256="sha256:" + "b" * 64,
            clickhouse_connection_sha256="sha256:" + "d" * 64,
            target_schema_sha256_by_slot=target_schemas,
            s3_endpoint_sha256="sha256:" + "e" * 64,
            s3_bucket="dpone-stage",
            s3_named_collection="dpone_stage",
            forbidden_secret_values=_secret_context(),
        )


def test_wide_release_slot_subjects_cannot_swap_native_and_python_backends() -> None:
    module = _load_tool_module()
    required = module._expected_subject(
        slot="bcp_native_required",
        release_id="0.74.0",
        source_relation="dbt.wide",
        target_relation="ch.required",
        mssql_connection_sha256="sha256:" + "1" * 64,
        clickhouse_connection_sha256="sha256:" + "2" * 64,
        target_schema_sha256="sha256:" + "3" * 64,
        s3_endpoint_sha256="sha256:" + "4" * 64,
        s3_bucket="dpone-stage",
        s3_named_collection="dpone_stage",
    )
    off = module._expected_subject(
        slot="bcp_native_off",
        release_id="0.74.0",
        source_relation="dbt.wide",
        target_relation="ch.off",
        mssql_connection_sha256="sha256:" + "1" * 64,
        clickhouse_connection_sha256="sha256:" + "2" * 64,
        target_schema_sha256="sha256:" + "3" * 64,
        s3_endpoint_sha256="sha256:" + "4" * 64,
        s3_bucket="dpone-stage",
        s3_named_collection="dpone_stage",
    )

    assert required.requested_acceleration_mode == "required"
    assert off.requested_acceleration_mode == "off"
    assert required.target_relation != off.target_relation


def test_campaign_schema_rejects_duplicate_slot_closure() -> None:
    schema = json.loads(
        Path("docs/schemas/dpone.mssql-clickhouse-wide-release-campaign.v1.schema.json").read_text(encoding="utf-8")
    )
    slot = {
        "slot": "bcp_native_required",
        "receipt": {"path": "slot.json", "sha256": "sha256:" + "1" * 64},
        "receipt_sha256": "sha256:" + "2" * 64,
        "target_relation": "dst.one",
    }
    payload = {
        "schema_version": "dpone.mssql-clickhouse-wide-release-campaign.v1",
        "created_at": "2026-08-10T00:00:00Z",
        "release_id": "0.74.0",
        "git_head_sha": "a" * 40,
        "source_snapshot_sha256": "sha256:" + "3" * 64,
        "worktree_dirty": False,
        "environment_class": "LOCAL_DOCKER",
        "source_relation": "src.wide",
        "source_generation_sha256": "sha256:" + "4" * 64,
        "expected_rows": 10_000,
        "expected_source_columns": 201,
        "expected_target_columns": 202,
        "mssql_connection_sha256": "sha256:" + "5" * 64,
        "clickhouse_connection_sha256": "sha256:" + "6" * 64,
        "authority": {"path": "authority.json", "sha256": "sha256:" + "9" * 64},
        "dbt_evidence": {"path": "dbt.json", "sha256": "sha256:" + "7" * 64},
        "slots": [slot, slot, slot],
        "local_behavior_passed": True,
        "local_evidence_status": "PASS",
        "production_certification": "UNVERIFIED",
        "production_certification_reason": "local only",
        "campaign_sha256": "sha256:" + "8" * 64,
    }
    assert list(Draft202012Validator(schema).iter_errors(payload))


def test_campaign_artifact_verifier_rejects_symlink_escape(tmp_path: Path) -> None:
    module = _load_tool_module()
    root = tmp_path / "campaign"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    linked = root / "artifact.json"
    linked.symlink_to(outside)
    value = {
        "path": "artifact.json",
        "sha256": "sha256:" + hashlib.sha256(outside.read_bytes()).hexdigest(),
    }

    with pytest.raises(ValueError, match="artifact_path_invalid"):
        module._verify_campaign_artifact(root, value)


def test_campaign_verifier_rejects_symlinked_index(tmp_path: Path) -> None:
    module = _load_tool_module()
    outside = tmp_path / "outside.json"
    outside.write_text("{}\n", encoding="utf-8")
    linked = tmp_path / "campaign.json"
    linked.symlink_to(outside)

    with pytest.raises(ValueError, match="campaign_path_invalid"):
        module.verify_campaign(
            linked,
            release_id="0.74.0",
            source_relation="dbt.wide",
            targets={slot: f"ch.{slot}" for slot in module._SLOTS},
            mssql_connection_sha256="sha256:" + "1" * 64,
            clickhouse_connection_sha256="sha256:" + "2" * 64,
            target_schema_sha256_by_slot={slot: "sha256:" + "3" * 64 for slot in module._SLOTS},
            s3_endpoint_sha256="sha256:" + "4" * 64,
            s3_bucket="dpone-stage",
            s3_named_collection="dpone_stage",
            forbidden_secret_values=_secret_context(),
        )


def test_campaign_verifier_rejects_source_change_during_deep_verification(tmp_path: Path, monkeypatch) -> None:
    module = _load_tool_module()
    snapshot = module.capture_local_source_snapshot()
    snapshot = type(snapshot)(snapshot.git_head_sha, snapshot.source_snapshot_sha256, False)
    changed = type(snapshot)(snapshot.git_head_sha, "sha256:" + "f" * 64, False)
    targets = {slot: f"ch.{slot}" for slot in module._SLOTS}
    schemas = {slot: "sha256:" + str(index + 1) * 64 for index, slot in enumerate(module._SLOTS)}
    payload = {
        "schema_version": module._SCHEMA_VERSION,
        "created_at": "2026-08-10T00:00:00Z",
        "release_id": "0.74.0",
        "git_head_sha": snapshot.git_head_sha,
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "worktree_dirty": False,
        "environment_class": "LOCAL_DOCKER",
        "source_relation": "dbt.wide",
        "source_generation_sha256": "sha256:" + "a" * 64,
        "expected_rows": 10_000,
        "expected_source_columns": 201,
        "expected_target_columns": 202,
        "mssql_connection_sha256": "sha256:" + "b" * 64,
        "clickhouse_connection_sha256": "sha256:" + "c" * 64,
        "authority": {"path": "authority.json", "sha256": "sha256:" + "d" * 64},
        "dbt_evidence": {"path": "dbt.json", "sha256": "sha256:" + "e" * 64},
        "slots": [
            {
                "slot": slot,
                "receipt": {"path": f"{slot}.json", "sha256": "sha256:" + str(index + 1) * 64},
                "receipt_sha256": "sha256:" + "9" * 64,
                "target_relation": targets[slot],
            }
            for index, slot in enumerate(module._SLOTS)
        ],
        "local_behavior_passed": True,
        "local_evidence_status": "PASS",
        "production_certification": "UNVERIFIED",
        "production_certification_reason": "local Docker evidence is not production deployment certification",
    }
    payload["campaign_sha256"] = module.canonical_sha256(payload)
    campaign = tmp_path / "campaign.json"
    campaign.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(module, "capture_local_source_snapshot", iter((snapshot, changed)).__next__)
    monkeypatch.setattr(module, "_verify_campaign_artifact", lambda *args: tmp_path / "artifact.json")
    monkeypatch.setattr(
        module,
        "load_authority",
        lambda *args, **kwargs: {
            "mssql_connection_sha256": "sha256:" + "b" * 64,
            "clickhouse_connection_sha256": "sha256:" + "c" * 64,
            "target_schema_sha256_by_slot": schemas,
            "s3_endpoint_sha256": "sha256:" + "4" * 64,
            "s3_bucket": "dpone-stage",
            "s3_named_collection": "dpone_stage",
        },
    )
    monkeypatch.setattr(
        module,
        "require_exact_dbt_wide_evidence",
        lambda *args, **kwargs: {"relation_generation_sha256": "sha256:" + "a" * 64},
    )
    monkeypatch.setattr(
        module,
        "verify_local_route_certification_receipt",
        lambda *args, **kwargs: {
            "receipt_sha256": "sha256:" + "9" * 64,
            "source_generation_sha256": "sha256:" + "a" * 64,
        },
    )

    with pytest.raises(ValueError, match="source_snapshot_changed"):
        module.verify_campaign(
            campaign,
            release_id="0.74.0",
            source_relation="dbt.wide",
            targets=targets,
            mssql_connection_sha256="sha256:" + "b" * 64,
            clickhouse_connection_sha256="sha256:" + "c" * 64,
            target_schema_sha256_by_slot=schemas,
            s3_endpoint_sha256="sha256:" + "4" * 64,
            s3_bucket="dpone-stage",
            s3_named_collection="dpone_stage",
            forbidden_secret_values=_secret_context(),
        )
