from __future__ import annotations

import importlib
import importlib.util
import json
import sys
from dataclasses import replace
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from dpone.ops.certification_artifacts import artifact_payload_passed


def _load_tools_module(name: str):
    """Load an un-packaged certification helper without test-order coupling."""

    tools_path = str(Path("tools").resolve())
    if tools_path not in sys.path:
        sys.path.insert(0, tools_path)
    return importlib.import_module(name)


def _load_tool_module():
    path = Path("tools/local_route_certification_receipt.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_local_route_certification_receipt", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _snapshot(module, *, dirty: bool = False):
    return module.LocalSourceSnapshot(
        git_head_sha="a" * 40,
        source_snapshot_sha256="sha256:" + "b" * 64,
        worktree_dirty=dirty,
    )


def _bcp_result(path: Path, *, passed: bool = True) -> None:
    digest = "sha256:" + "1" * 64
    payload = {
        "rows": 10_000,
        "column_count": 202,
        "source_count": 10_000,
        "target_count": 10_000,
        "duplicate_count": 0,
        "typed_hash_passed": passed,
        "typed_hash_source": digest,
        "typed_hash_target": digest,
        "elapsed_seconds": 1.0,
        "prepare_seconds": 0.0,
        "export_seconds": 0.4,
        "load_seconds": 0.6,
        "artifact_bytes": 1,
        "passed": passed,
        "failed_phase": None,
        "error": None,
        "schema_version": "dpone.mssql_clickhouse.wide_type_certification.v1",
        "rows_per_second": 10_000.0,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _parquet_result(path: Path) -> None:
    digest = "sha256:" + "1" * 64
    payload = {
        "rows": 10_000,
        "column_count": 202,
        "source_count": 10_000,
        "target_count": 10_000,
        "duplicate_count": 0,
        "typed_hash_source": digest,
        "typed_hash_target": digest,
        "parquet_chunks": 2,
        "parquet_bytes": 1024,
        "object_prefix": "s3://dpone-stage/run/",
        "cleanup_verified": True,
        "elapsed_seconds": 1.0,
        "passed": True,
        "failed_phase": None,
        "error": None,
        "schema_version": "dpone.mssql_clickhouse.parquet_s3_type_certification.v1",
    }
    path.write_text(json.dumps(payload), encoding="utf-8")


def _upstream_evidence(tmp_path: Path) -> Path:
    evidence = _load_tools_module("mssql_dbt_wide_evidence")
    schema = _load_tools_module("mssql_dbt_wide_schema")
    for name in ("dbt_run_results.json", "dbt_manifest.json"):
        (tmp_path / name).write_text('{"metadata":{}}\n', encoding="utf-8")
    digest = "sha256:" + "1" * 64
    payload = {
        "created_at": "2026-08-10T00:00:00Z",
        "release_id": "0.74.0",
        "git_head_sha": "a" * 40,
        "source_snapshot_sha256": "sha256:" + "b" * 64,
        "worktree_dirty": False,
        "project_sha256": "sha256:" + "c" * 64,
        "mssql_connection_sha256": "sha256:" + "2" * 64,
        "source_relation": "src.orders",
        "output_relation": "dbt_calc.wide_dbt_result",
        "model_unique_id": "model.dpone_mssql_clickhouse_wide.wide_dbt_result",
        "materialization": "table",
        "source_count": 10_000,
        "target_count": 10_000,
        "distinct_key_count": 10_000,
        "source_column_count": 201,
        "target_column_count": 202,
        "schema_mismatch_count": 0,
        "canonical_source_schema_sha256": schema.canonical_wide_source_schema_sha256(201),
        "canonical_source_mismatch_count": 0,
        "source_schema_sha256": "sha256:" + "3" * 64,
        "passthrough_schema_sha256": "sha256:" + "3" * 64,
        "source_data_sha256": "sha256:" + "4" * 64,
        "passthrough_data_sha256": "sha256:" + "4" * 64,
        "output_data_sha256": digest,
        "output_data_rows": 10_000,
        "relation_generation_sha256": "",
        "calculated_column_type": "decimal(38,8)",
        "calculated_column_nullable": True,
        "calculated_mismatch_count": 0,
        "run_results_path": "dbt_run_results.json",
        "run_results_sha256": "sha256:"
        + __import__("hashlib").sha256((tmp_path / "dbt_run_results.json").read_bytes()).hexdigest(),
        "manifest_path": "dbt_manifest.json",
        "manifest_sha256": "sha256:"
        + __import__("hashlib").sha256((tmp_path / "dbt_manifest.json").read_bytes()).hexdigest(),
        "passed": True,
        "error": None,
        "schema_version": "dpone.dbt_sqlserver.wide_materialization.v1",
        "environment_class": "LOCAL_DOCKER",
        "production_certification": "UNVERIFIED",
        "production_certification_reason": "local Docker evidence is not production deployment certification",
        "evidence_status": "PASS",
        "blockers": [],
    }
    payload["relation_generation_sha256"] = evidence.relation_generation_sha256(payload)
    payload["evidence_sha256"] = evidence.canonical_sha256(payload)
    path = tmp_path / "mssql_dbt_wide_materialization.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _expected(module):
    return module.LocalRouteExpectedSubject(
        release_id="0.74.0",
        route=module.LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
        source_relation="dbt_calc.wide_dbt_result",
        target_relation="dpone_it.wide_native",
        transport="typed_binary_bcp_native",
        binary_format="native",
        requested_acceleration_mode="required",
        expected_rows=10_000,
        expected_column_count=202,
        mssql_connection_sha256="sha256:" + "2" * 64,
        clickhouse_connection_sha256="sha256:" + "8" * 64,
        target_schema_sha256="sha256:" + "7" * 64,
    )


def _decision() -> dict[str, object]:
    return {
        "schema_version": "dpone.runtime.decision_audit.v1",
        "decision_id": "native_transfer.acceleration",
        "phase": "load",
        "component": "native_wire_transcoder",
        "category": "backend_selection",
        "requested": "required",
        "selected": "native_accelerated",
        "fallback_allowed": False,
        "fallback_reason": None,
        "release_gate": "green",
        "warnings": [],
        "blockers": [],
        "route_id": None,
        "provider": "dpone-native-accel",
        "provider_version": "0.74.0",
        "details": {
            "available": True,
            "backend_id": "mssql_bcp_native_to_clickhouse_native",
            "certified": True,
            "schema_hash": "sha256:" + "5" * 64,
            "source_format": "mssql-bcp-native",
            "source_type_count": 202,
            "target_format": "Native",
            "type_layout_hash": "sha256:" + "6" * 64,
        },
    }


def _loaded_slices(*rows: int) -> list[dict[str, int]]:
    return [
        {
            "partition_index": index,
            "slice_index": 0,
            "rows_exported": row_count,
            "rows_loaded": row_count,
        }
        for index, row_count in enumerate(rows)
    ]


def _bcp_details(
    *,
    target_rows_per_partition: int = 10_000,
    loaded_slices: list[dict[str, int]] | None = None,
) -> dict[str, object]:
    return {
        "hashed_rows": 10_000,
        "target_rows_per_partition": target_rows_per_partition,
        "export_workers": 1,
        "load_workers": 1,
        "bcp_file_format": "native",
        "loaded_slices": _loaded_slices(10_000) if loaded_slices is None else loaded_slices,
    }


def _parquet_details() -> dict[str, object]:
    return {
        "hashed_rows": 10_000,
        "s3_endpoint_sha256": "sha256:" + "9" * 64,
        "bucket": "dpone-stage",
        "object_prefix": "s3://dpone-stage/run/",
        "named_collection": "dpone_stage",
        "parquet_chunks": 2,
        "parquet_bytes": 1024,
    }


def test_local_route_receipt_binds_closed_upstream_result_and_trusted_subject(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    upstream = _upstream_evidence(tmp_path)
    snapshot = _snapshot(module)

    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=snapshot,
        final_source_snapshot=snapshot,
        result_path=result,
        release_id="0.74.0",
        route=_expected(module).route,
        source_relation="dbt_calc.wide_dbt_result",
        target_relation="dpone_it.wide_native",
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(_decision(),),
        mssql_connection_sha256="sha256:" + "2" * 64,
        clickhouse_connection_sha256="sha256:" + "8" * 64,
        target_schema_sha256="sha256:" + "7" * 64,
        upstream_evidence_path=upstream,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
        created_at="2026-08-09T12:00:00Z",
    )

    payload = module.verify_local_route_certification_receipt(
        receipt.path,
        source_snapshot=snapshot,
        expected=_expected(module),
    )
    schema = json.loads(
        Path("docs/schemas/dpone.local-route-certification-receipt.v1.schema.json").read_text(encoding="utf-8")
    )
    Draft202012Validator(schema).validate(payload)
    assert payload["source_generation_sha256"].startswith("sha256:")
    assert payload["artifact_cleanup"] == {"verified": True, "remaining_artifacts": []}


def test_local_route_receipt_never_marks_dirty_or_changed_source_as_exact_pass(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _parquet_result(result)
    start = _snapshot(module, dirty=True)
    end = module.LocalSourceSnapshot(start.git_head_sha, "sha256:" + "c" * 64, True)

    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path,
        source_snapshot=start,
        final_source_snapshot=end,
        result_path=result,
        release_id="0.74.0",
        route=module.LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
        source_relation="src.orders",
        target_relation="dst.orders",
        transport="parquet_s3_pull",
        transport_details=_parquet_details(),
        binary_format="parquet",
        requested_acceleration_mode=None,
        runtime_decisions=(),
        mssql_connection_sha256="sha256:" + "2" * 64,
        clickhouse_connection_sha256="sha256:" + "8" * 64,
        target_schema_sha256="sha256:" + "7" * 64,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
        created_at="2026-08-09T12:00:00Z",
    )

    payload = json.loads(receipt.path.read_text(encoding="utf-8"))
    assert payload["evidence_status"] == "UNVERIFIED"
    assert payload["blockers"] == ["source_snapshot.worktree_dirty", "source_snapshot.changed_during_run"]


def test_upstream_evidence_rejects_partial_or_tampered_contract(tmp_path: Path) -> None:
    module = _load_tool_module()
    path = tmp_path / "partial.json"
    payload = {
        "schema_version": "dpone.dbt_sqlserver.wide_materialization.v1",
        "git_head_sha": "a" * 40,
        "source_snapshot_sha256": "sha256:" + "b" * 64,
        "output_relation": "dbt_calc.wide_dbt_result",
        "passed": True,
        "evidence_status": "PASS",
    }
    payload["evidence_sha256"] = module.canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="upstream_evidence_shape_invalid"):
        module.require_exact_upstream_evidence(
            path,
            source_snapshot=_snapshot(module),
            output_relation="dbt_calc.wide_dbt_result",
            connection_sha256="sha256:" + "2" * 64,
            release_id="0.74.0",
            expected_rows=10_000,
            expected_source_columns=201,
            expected_target_columns=202,
        )
    path.write_text('{"schema_version":"x","schema_version":"y"}', encoding="utf-8")
    with pytest.raises(ValueError, match="certification_json_duplicate_key"):
        module.require_exact_upstream_evidence(
            path,
            source_snapshot=_snapshot(module),
            output_relation="dbt_calc.wide_dbt_result",
            connection_sha256="sha256:" + "2" * 64,
            release_id="0.74.0",
            expected_rows=10_000,
            expected_source_columns=201,
            expected_target_columns=202,
        )


def test_upstream_evidence_rejects_secret_bearing_retained_dbt_artifact(tmp_path: Path) -> None:
    module = _load_tool_module()
    path = _upstream_evidence(tmp_path)
    manifest = tmp_path / "dbt_manifest.json"
    manifest.write_text('{"password":"forbidden"}\n', encoding="utf-8")
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["manifest_sha256"] = "sha256:" + __import__("hashlib").sha256(manifest.read_bytes()).hexdigest()
    payload.pop("evidence_sha256")
    payload["evidence_sha256"] = module.canonical_sha256(payload)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="secret_material_detected"):
        module.require_exact_upstream_evidence(
            path,
            source_snapshot=_snapshot(module),
            output_relation="dbt_calc.wide_dbt_result",
            connection_sha256="sha256:" + "2" * 64,
            release_id="0.74.0",
            expected_rows=10_000,
            expected_source_columns=201,
            expected_target_columns=202,
        )


@pytest.mark.parametrize(
    ("payload", "forbidden"),
    (
        ({"message": "PWD=plain-secret"}, ()),
        ({"message": "plain-secret"}, ("plain-secret",)),
        ({"url": "mssql://user:plain-secret@server/database"}, ()),
    ),
)
def test_upstream_evidence_rejects_secret_bearing_neutral_values(
    tmp_path: Path,
    payload: dict[str, str],
    forbidden: tuple[str, ...],
) -> None:
    evidence_module = _load_tools_module("mssql_dbt_wide_evidence")
    path = _upstream_evidence(tmp_path)
    manifest = tmp_path / "dbt_manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    evidence = json.loads(path.read_text(encoding="utf-8"))
    evidence["manifest_sha256"] = "sha256:" + __import__("hashlib").sha256(manifest.read_bytes()).hexdigest()
    evidence.pop("evidence_sha256")
    evidence["evidence_sha256"] = evidence_module.canonical_sha256(evidence)
    path.write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(ValueError, match="secret_material_detected"):
        evidence_module.require_exact_dbt_wide_evidence(
            path,
            release_id="0.74.0",
            git_head_sha="a" * 40,
            source_snapshot_sha256="sha256:" + "b" * 64,
            output_relation="dbt_calc.wide_dbt_result",
            connection_sha256="sha256:" + "2" * 64,
            expected_rows=10_000,
            expected_source_columns=201,
            expected_target_columns=202,
            forbidden_secret_values=forbidden,
        )


def test_exact_receipt_rejects_result_mutation_and_subject_relabel(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    snapshot = _snapshot(module)
    expected = _expected(module)
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=snapshot,
        final_source_snapshot=snapshot,
        result_path=result,
        release_id="0.74.0",
        route=expected.route,
        source_relation=expected.source_relation,
        target_relation=expected.target_relation,
        transport=expected.transport,
        transport_details=_bcp_details(),
        binary_format=expected.binary_format,
        requested_acceleration_mode=expected.requested_acceleration_mode,
        runtime_decisions=(_decision(),),
        mssql_connection_sha256=expected.mssql_connection_sha256,
        clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
        target_schema_sha256=expected.target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
        created_at="2026-08-10T00:00:00Z",
    )
    module.verify_local_route_certification_receipt(receipt.path, source_snapshot=snapshot, expected=expected)

    wrong = (
        module.LocalRouteExpectedSubject(**{**expected.__dict__, "target_relation": "dst.forged"})
        if hasattr(expected, "__dict__")
        else module.LocalRouteExpectedSubject(
            release_id=expected.release_id,
            route=expected.route,
            source_relation=expected.source_relation,
            target_relation="dst.forged",
            transport=expected.transport,
            binary_format=expected.binary_format,
            requested_acceleration_mode=expected.requested_acceleration_mode,
            expected_rows=expected.expected_rows,
            expected_column_count=expected.expected_column_count,
            mssql_connection_sha256=expected.mssql_connection_sha256,
            clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
            target_schema_sha256=expected.target_schema_sha256,
        )
    )
    with pytest.raises(ValueError, match="target_relation_mismatch"):
        module.verify_local_route_certification_receipt(receipt.path, source_snapshot=snapshot, expected=wrong)
    with pytest.raises(ValueError, match="clickhouse_connection_sha256_mismatch"):
        module.verify_local_route_certification_receipt(
            receipt.path,
            source_snapshot=snapshot,
            expected=replace(expected, clickhouse_connection_sha256="sha256:" + "9" * 64),
        )
    result_copy = receipt.path.parent / "result.json"
    result_copy.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="artifact_digest_mismatch"):
        module.verify_local_route_certification_receipt(receipt.path, source_snapshot=snapshot, expected=expected)


def test_local_unverified_receipt_cannot_satisfy_generic_certification_slot() -> None:
    payload = {
        "passed": True,
        "evidence_status": "PASS",
        "blockers": [],
        "production_certification": "UNVERIFIED",
    }
    assert artifact_payload_passed(payload, name="route_live_evidence_bundle") is False


def test_schema_bound_receipt_cannot_shed_local_markers_and_become_production_evidence() -> None:
    payload = {
        "passed": True,
        "evidence_status": "PASS",
        "blockers": [],
        "receipt_sha256": "sha256:" + "1" * 64,
        "source_snapshot_sha256": "sha256:" + "2" * 64,
    }

    assert artifact_payload_passed(payload, name="route_live_evidence_bundle") is False


def test_exact_receipt_rejects_contradictory_native_backend_details(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    decision = _decision()
    decision["details"] = {
        **decision["details"],
        "available": False,
        "certified": False,
        "backend_id": None,
    }
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=_snapshot(module),
        final_source_snapshot=_snapshot(module),
        result_path=result,
        release_id="0.74.0",
        route=_expected(module).route,
        source_relation=_expected(module).source_relation,
        target_relation=_expected(module).target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(decision,),
        mssql_connection_sha256=_expected(module).mssql_connection_sha256,
        clickhouse_connection_sha256=_expected(module).clickhouse_connection_sha256,
        target_schema_sha256=_expected(module).target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
    )

    with pytest.raises(ValueError, match="native_required_not_observed"):
        module.verify_local_route_certification_receipt(
            receipt.path,
            source_snapshot=_snapshot(module),
            expected=_expected(module),
        )


def test_exact_receipt_accepts_one_consistent_decision_per_loaded_partition(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    expected = _expected(module)
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=_snapshot(module),
        final_source_snapshot=_snapshot(module),
        result_path=result,
        release_id="0.74.0",
        route=expected.route,
        source_relation=expected.source_relation,
        target_relation=expected.target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(
            target_rows_per_partition=2_500,
            loaded_slices=_loaded_slices(2_500, 2_500, 2_500, 2_500),
        ),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(_decision(), _decision(), _decision(), _decision()),
        mssql_connection_sha256=expected.mssql_connection_sha256,
        clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
        target_schema_sha256=expected.target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
    )

    verified = module.verify_local_route_certification_receipt(
        receipt.path,
        source_snapshot=_snapshot(module),
        expected=expected,
    )

    assert len(verified["runtime_decisions"]) == 4


def test_exact_receipt_uses_observed_slice_closure_not_planned_target_size(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    expected = _expected(module)
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=_snapshot(module),
        final_source_snapshot=_snapshot(module),
        result_path=result,
        release_id="0.74.0",
        route=expected.route,
        source_relation=expected.source_relation,
        target_relation=expected.target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(
            target_rows_per_partition=2_500,
            loaded_slices=_loaded_slices(10_000),
        ),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(_decision(),),
        mssql_connection_sha256=expected.mssql_connection_sha256,
        clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
        target_schema_sha256=expected.target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
    )

    verified = module.verify_local_route_certification_receipt(
        receipt.path,
        source_snapshot=_snapshot(module),
        expected=expected,
    )

    assert verified["transport_details"]["loaded_slices"] == _loaded_slices(10_000)


def test_exact_receipt_rejects_incomplete_loaded_slice_row_closure(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    expected = _expected(module)
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=_snapshot(module),
        final_source_snapshot=_snapshot(module),
        result_path=result,
        release_id="0.74.0",
        route=expected.route,
        source_relation=expected.source_relation,
        target_relation=expected.target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(
            target_rows_per_partition=2_500,
            loaded_slices=_loaded_slices(2_500),
        ),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(_decision(),),
        mssql_connection_sha256=expected.mssql_connection_sha256,
        clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
        target_schema_sha256=expected.target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
    )

    with pytest.raises(ValueError, match="bcp_slice_closure_mismatch"):
        module.verify_local_route_certification_receipt(
            receipt.path,
            source_snapshot=_snapshot(module),
            expected=expected,
        )


def test_failed_receipt_schema_allows_no_completed_loaded_slices(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result, passed=False)
    snapshot = _snapshot(module)
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=snapshot,
        final_source_snapshot=snapshot,
        result_path=result,
        release_id="0.74.0",
        route=_expected(module).route,
        source_relation=_expected(module).source_relation,
        target_relation=_expected(module).target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(loaded_slices=[]),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(),
        mssql_connection_sha256=_expected(module).mssql_connection_sha256,
        clickhouse_connection_sha256=_expected(module).clickhouse_connection_sha256,
        target_schema_sha256=None,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=False,
    )
    schema = json.loads(
        Path("docs/schemas/dpone.local-route-certification-receipt.v1.schema.json").read_text(encoding="utf-8")
    )
    payload = json.loads(receipt.path.read_text(encoding="utf-8"))

    assert tuple(Draft202012Validator(schema).iter_errors(payload)) == ()
    assert payload["evidence_status"] == "UNVERIFIED"
    assert payload["transport_details"]["loaded_slices"] == []


def test_exact_receipt_rejects_incomplete_or_mixed_partition_decisions(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    expected = _expected(module)

    for name, decisions, code in (
        ("incomplete", (_decision(),), "acceleration_missing"),
        (
            "mixed",
            (
                _decision(),
                _decision(),
                {**_decision(), "details": {**_decision()["details"], "schema_hash": "sha256:" + "9" * 64}},
                _decision(),
            ),
            "acceleration_partition_mismatch",
        ),
    ):
        receipt = module.write_local_route_certification_receipt(
            output_dir=tmp_path / name,
            source_snapshot=_snapshot(module),
            final_source_snapshot=_snapshot(module),
            result_path=result,
            release_id="0.74.0",
            route=expected.route,
            source_relation=expected.source_relation,
            target_relation=expected.target_relation,
            transport="typed_binary_bcp_native",
            transport_details=_bcp_details(
                target_rows_per_partition=2_500,
                loaded_slices=_loaded_slices(2_500, 2_500, 2_500, 2_500),
            ),
            binary_format="native",
            requested_acceleration_mode="required",
            runtime_decisions=decisions,
            mssql_connection_sha256=expected.mssql_connection_sha256,
            clickhouse_connection_sha256=expected.clickhouse_connection_sha256,
            target_schema_sha256=expected.target_schema_sha256,
            upstream_evidence_path=None,
            cleanup_verified=True,
            remaining_artifacts=(),
            passed=True,
        )
        with pytest.raises(ValueError, match=code):
            module.verify_local_route_certification_receipt(
                receipt.path,
                source_snapshot=_snapshot(module),
                expected=expected,
            )


def test_exact_receipt_rejects_required_native_with_fallback_allowed(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    decision = {**_decision(), "fallback_allowed": True}
    receipt = module.write_local_route_certification_receipt(
        output_dir=tmp_path / "receipt",
        source_snapshot=_snapshot(module),
        final_source_snapshot=_snapshot(module),
        result_path=result,
        release_id="0.74.0",
        route=_expected(module).route,
        source_relation=_expected(module).source_relation,
        target_relation=_expected(module).target_relation,
        transport="typed_binary_bcp_native",
        transport_details=_bcp_details(),
        binary_format="native",
        requested_acceleration_mode="required",
        runtime_decisions=(decision,),
        mssql_connection_sha256=_expected(module).mssql_connection_sha256,
        clickhouse_connection_sha256=_expected(module).clickhouse_connection_sha256,
        target_schema_sha256=_expected(module).target_schema_sha256,
        upstream_evidence_path=None,
        cleanup_verified=True,
        remaining_artifacts=(),
        passed=True,
    )

    with pytest.raises(ValueError, match="native_required_not_observed"):
        module.verify_local_route_certification_receipt(
            receipt.path,
            source_snapshot=_snapshot(module),
            expected=_expected(module),
        )


def test_exact_result_rejects_missing_full_row_hash(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    payload = json.loads(result.read_text(encoding="utf-8"))
    payload["typed_hash_source"] = None
    payload["typed_hash_target"] = None
    result.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="result_not_exact"):
        module.verify_result_payload(result, transport="typed_binary_bcp_native")


def test_receipt_rejects_secret_bearing_runtime_decision(tmp_path: Path) -> None:
    module = _load_tool_module()
    result = tmp_path / "result.json"
    _bcp_result(result)
    decision = _decision()
    decision["details"] = {**decision["details"], "access_key": "forbidden"}

    with pytest.raises(ValueError, match="secret_material_detected"):
        module.write_local_route_certification_receipt(
            output_dir=tmp_path / "receipt",
            source_snapshot=_snapshot(module),
            final_source_snapshot=_snapshot(module),
            result_path=result,
            release_id="0.74.0",
            route=_expected(module).route,
            source_relation="src.orders",
            target_relation="dst.orders",
            transport="typed_binary_bcp_native",
            transport_details=_bcp_details(),
            binary_format="native",
            requested_acceleration_mode="required",
            runtime_decisions=(decision,),
            mssql_connection_sha256="sha256:" + "2" * 64,
            clickhouse_connection_sha256="sha256:" + "8" * 64,
            target_schema_sha256="sha256:" + "7" * 64,
            upstream_evidence_path=None,
            cleanup_verified=True,
            remaining_artifacts=(),
            passed=True,
        )


@pytest.mark.parametrize(
    "payload",
    (
        {
            "schema_version": "dpone.local_route_certification_receipt.v1",
            "passed": True,
            "evidence_status": "PASS",
        },
        {
            "environment_class": "LOCAL_DOCKER",
            "passed": True,
            "evidence_status": "PASS",
        },
        {
            "production_certification": False,
            "passed": True,
            "evidence_status": "PASS",
        },
    ),
)
def test_local_or_noncanonical_production_marker_never_satisfies_generic_slot(
    payload: dict[str, object],
) -> None:
    assert artifact_payload_passed(payload, name="route_live_evidence_bundle") is False
