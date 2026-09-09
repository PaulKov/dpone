from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _stream_payload() -> dict[str, object]:
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "source_dataset": "dbo.orders",
        "target_dataset": "analytics.orders",
        "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
        "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
    }


def _route_payload() -> dict[str, object]:
    return {
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "case_id": "mssql_to_clickhouse__cdc",
        "colon_id": "mssql:clickhouse:cdc",
    }


def _upstream_payload(schema_version: str) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": True,
        "evidence_status": "PASS",
        "blockers": [],
    }


def _schema_change_payload(**plan_overrides: object) -> dict[str, object]:
    plan: dict[str, object] = {
        "sink_impact": "add nullable String column",
        "compatibility_level": "compatible",
        "target_ddl_preview": "ALTER TABLE analytics.orders ADD COLUMN status_reason Nullable(String)",
        "ddl_dry_run_passed": True,
        "backfill_required": False,
        "backfill_plan": "",
        "type_widening_safe": True,
        "offset_schema_ordering_safe": True,
        "approved_by": ["data-architect"],
    }
    plan.update(plan_overrides)
    return {
        "change": {
            "change_id": "orders-add-status-reason",
            "kind": "add_column",
            "captured_at": "2026-06-12T12:20:00Z",
            "source_table": "dbo.orders",
            "source_column": "status_reason",
            "target_table": "analytics.orders",
            "target_column": "status_reason",
            "new_type": "nvarchar(100)",
            "new_nullable": True,
            "source_offset": "0x15",
            "breaking": False,
        },
        "plan": plan,
    }


def test_ops_cdc_schema_evolution_evidence_cli_outputs_json_and_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema_change = _write_json(tmp_path / "schema_change.json", _schema_change_payload())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-schema-evolution-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--observability-json",
                str(observability),
                "--recovery-json",
                str(recovery),
                "--schema-change-json",
                str(schema_change),
                "--output-dir",
                str(tmp_path / "schema"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert Path(payload["evidence_artifacts"]["cdc_offset_schema_ordering"]).exists()


def test_ops_cdc_schema_evolution_evidence_cli_returns_nonzero_for_incompatible_change(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema_change = _write_json(
        tmp_path / "schema_change.json",
        _schema_change_payload(compatibility_level="incompatible", offset_schema_ordering_safe=False),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-schema-evolution-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--observability-json",
                str(observability),
                "--recovery-json",
                str(recovery),
                "--schema-change-json",
                str(schema_change),
                "--output-dir",
                str(tmp_path / "schema"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "cdc_schema_compatibility.incompatible" in payload["blockers"]
    assert "cdc_offset_schema_ordering.unsafe" in payload["blockers"]
