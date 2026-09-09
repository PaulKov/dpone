from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_schema_evolution import RouteSchemaEvolutionService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _route_payload(
    source: str = "mssql", sink: str = "clickhouse", strategy: str = "incremental_merge"
) -> dict[str, str]:
    return {
        "source": source,
        "sink": sink,
        "strategy": strategy,
        "case_id": f"{source}_to_{sink}__{strategy}",
        "colon_id": f"{source}:{sink}:{strategy}",
    }


def _schema_evolution_payload(*, passed: bool = True, route: dict[str, str] | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_schema_evolution_evidence.v1",
        "route": route or _route_payload(),
        "passed": passed,
        "blockers": [] if passed else ["cdc_schema_compatibility.incompatible"],
        "warnings": [],
        "change": {
            "change_id": "orders-add-status-reason",
            "kind": "add_column",
            "captured_at": "2026-06-14T10:00:00Z",
            "source_table": "dbo.orders",
            "source_column": "status_reason",
            "target_table": "analytics.orders",
            "target_column": "status_reason",
            "old_type": "",
            "new_type": "nvarchar(100)",
            "old_nullable": True,
            "new_nullable": True,
            "source_offset": "0x15",
            "breaking": False,
        },
        "plan": {
            "sink_impact": "add nullable String column",
            "compatibility_level": "compatible" if passed else "incompatible",
            "target_ddl_preview": "ALTER TABLE analytics.orders ADD COLUMN status_reason Nullable(String)",
            "ddl_dry_run_passed": passed,
            "backfill_required": False,
            "backfill_plan": "",
            "type_widening_safe": passed,
            "offset_schema_ordering_safe": passed,
            "approved_by": [],
        },
    }


def test_route_schema_evolution_allows_safe_schema_change(tmp_path: Path) -> None:
    schema_evolution = _write_json(tmp_path / "cdc_schema.json", _schema_evolution_payload())

    report = RouteSchemaEvolutionService().evaluate(
        output_dir=tmp_path / "route-schema",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        schema_evolution_json=schema_evolution,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_schema_evolution.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert payload["apply_decision"]["mode"] == "auto_apply"
    assert payload["apply_decision"]["safe_to_apply"] is True
    assert payload["change"]["kind"] == "add_column"
    assert payload["blockers"] == []
    assert "Route schema evolution" in markdown


def test_route_schema_evolution_blocks_unsafe_or_mismatched_artifacts(tmp_path: Path) -> None:
    unsafe = _write_json(
        tmp_path / "unsafe.json",
        _schema_evolution_payload(passed=False, route=_route_payload(source="postgres", sink="mssql")),
    )

    report = RouteSchemaEvolutionService().evaluate(
        output_dir=tmp_path / "route-schema",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        schema_evolution_json=unsafe,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is False
    assert payload["apply_decision"]["mode"] == "blocked"
    assert "cdc_schema_compatibility.incompatible" in payload["blockers"]
    assert "route_schema_evolution.route_mismatch" in payload["blockers"]
