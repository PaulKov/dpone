from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.schema_history import SchemaHistoryRegistry
from dpone.readiness.schema_notifications import SchemaNotificationService


def test_schema_history_registry_records_versions_and_diffs(tmp_path: Path) -> None:
    registry = SchemaHistoryRegistry(tmp_path)

    first = registry.record(table="landing.orders", schema=[("id", "bigint")], run_id="run01")
    second = registry.record(table="landing.orders", schema=[("id", "bigint"), ("name", "text")], run_id="run02")

    assert first.version == 1
    assert second.version == 2
    assert second.diff["added"] == ["name"]
    assert registry.current("landing.orders")["version"] == 2
    assert Path(second.artifact_path).exists()


def test_schema_notification_service_writes_markdown_and_json(tmp_path: Path) -> None:
    ledger = tmp_path / "ledger.json"
    ledger.write_text(
        json.dumps(
            {
                "status": "deferred",
                "blockers": ["schema_evolution.blocking:type_widen:id"],
                "actions": [{"column": "id", "risk_level": "blocking", "decision": "defer"}],
            }
        ),
        encoding="utf-8",
    )

    report = SchemaNotificationService(tmp_path / "notifications").write(ledger_path=ledger, channel="artifact")

    assert report["channel"] == "artifact"
    assert report["status"] == "deferred"
    assert Path(report["json_path"]).exists()
    assert "schema_evolution.blocking:type_widen:id" in Path(report["markdown_path"]).read_text(encoding="utf-8")
