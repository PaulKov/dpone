from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.compare import CdcCompareRepairService, CdcRowHasher, InMemoryCdcCompareReader
from dpone.runtime.cdc.compare_models import CdcCompareRow
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


def _stream() -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )


def _row(order_id: int, **payload: object) -> CdcCompareRow:
    return CdcCompareRow.from_payload(unique_key=("order_id",), payload={"order_id": order_id, **payload})


def test_cdc_compare_detects_missing_extra_and_mismatch_and_builds_repair_plan(tmp_path: Path) -> None:
    source = InMemoryCdcCompareReader(
        (
            _row(1, status="paid", amount="10.50"),
            _row(2, status="open", amount="20.00"),
        )
    )
    target = InMemoryCdcCompareReader(
        (
            _row(1, status="stale", amount="10.50"),
            _row(3, status="orphan", amount="30.00"),
        )
    )

    report = CdcCompareRepairService().compare(
        output_dir=tmp_path,
        stream=_stream(),
        source_reader=source,
        target_reader=target,
        max_diffs=10,
    )

    payload = json.loads((tmp_path / "cdc_compare_repair.json").read_text(encoding="utf-8"))
    assert report.passed is False
    assert report.rows_source == 2
    assert report.rows_target == 2
    assert report.matched_rows == 0
    assert report.diff_count == 3
    assert [diff.kind for diff in report.diffs] == (["value_mismatch", "missing_in_target", "extra_in_target"])
    assert [action.operation for action in report.repair_plan.actions] == ["update", "insert", "delete"]
    assert report.repair_plan.actions[0].payload == {"amount": "10.50", "order_id": 1, "status": "paid"}
    assert report.repair_plan.actions[2].payload == {"order_id": 3}
    assert payload["schema_version"] == "dpone.cdc_compare_repair.v1"
    assert payload["repair_plan"]["schema_version"] == "dpone.cdc_repair_plan.v1"
    assert payload["repair_plan"]["action_count"] == 3
    assert (tmp_path / "cdc_compare_repair.md").exists()


def test_cdc_compare_passes_for_identical_current_state(tmp_path: Path) -> None:
    rows = (_row(1, status="paid"), _row(2, status="open"))

    report = CdcCompareRepairService().compare(
        output_dir=tmp_path,
        stream=_stream(),
        source_reader=InMemoryCdcCompareReader(rows),
        target_reader=InMemoryCdcCompareReader(rows),
    )

    assert report.passed is True
    assert report.diff_count == 0
    assert report.matched_rows == 2
    assert report.repair_plan.actions == tuple()


def test_cdc_compare_honors_max_diffs_with_warning(tmp_path: Path) -> None:
    source = InMemoryCdcCompareReader(tuple(_row(index, status="open") for index in range(1, 5)))
    target = InMemoryCdcCompareReader(tuple())

    report = CdcCompareRepairService().compare(
        output_dir=tmp_path,
        stream=_stream(),
        source_reader=source,
        target_reader=target,
        max_diffs=2,
    )

    assert report.passed is False
    assert report.diff_count == 2
    assert report.metrics["total_diff_count"] == 4
    assert "cdc_compare.max_diffs_reached" in report.warnings


def test_cdc_row_hasher_is_deterministic_and_order_insensitive() -> None:
    hasher = CdcRowHasher()

    first = hasher.payload_hash({"order_id": 1, "status": "paid", "amount": "10.50"})
    second = hasher.payload_hash({"amount": "10.50", "status": "paid", "order_id": 1})

    assert first == second
    assert len(first) == 64
