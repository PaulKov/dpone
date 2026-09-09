from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_reconciliation_repair import RouteReconciliationRepairService


def test_route_reconciliation_repair_passes_when_rows_match(tmp_path: Path) -> None:
    report = RouteReconciliationRepairService().evaluate(
        output_dir=tmp_path / "route-repair",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        source_rows=({"id": 1, "status": "paid", "amount": 10},),
        target_rows=({"id": 1, "status": "paid", "amount": 10},),
        key_columns=("id",),
        compare_columns=("status", "amount"),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert payload["schema_version"] == "dpone.route_reconciliation_repair.v1"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert payload["passed"] is True
    assert payload["repair_plan"]["actions"] == []
    assert payload["blockers"] == []


def test_route_reconciliation_repair_builds_actionable_repair_plan(tmp_path: Path) -> None:
    report = RouteReconciliationRepairService().evaluate(
        output_dir=tmp_path / "route-repair",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        source_rows=(
            {"id": 1, "status": "paid", "amount": 10},
            {"id": 2, "status": "new", "amount": 20},
            {"id": 4, "status": "deleted", "amount": 40, "deleted_at": "2026-06-14T10:00:00Z"},
        ),
        target_rows=(
            {"id": 1, "status": "pending", "amount": 10},
            {"id": 3, "status": "orphan", "amount": 30},
            {"id": 4, "status": "deleted", "amount": 40},
        ),
        key_columns=("id",),
        compare_columns=("status", "amount"),
        delete_column="deleted_at",
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    actions = payload["repair_plan"]["actions"]
    action_pairs = {(item["action"], item["key"]["id"]) for item in actions}
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert report.passed is False
    assert "route_reconciliation_repair.unrepaired_differences" in payload["blockers"]
    assert ("replay_source_row", 1) in action_pairs
    assert ("replay_source_row", 2) in action_pairs
    assert ("delete_target_row", 3) in action_pairs
    assert ("delete_target_row", 4) in action_pairs
    assert payload["repair_plan"]["source_boundary"] == "full-snapshot"
    assert "Route reconciliation repair" in markdown
