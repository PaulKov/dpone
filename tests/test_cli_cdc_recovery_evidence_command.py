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
        "metrics": {"duplicate_events": 0, "retention_remaining_seconds": 7200},
    }


def _scenario_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "scenario_id": "orders-recovery-smoke",
        "kind": "consumer_restart",
        "injected_at": "2026-06-12T12:05:00Z",
        "failed_after_offset": "0x12",
        "recovered_offset": "0x14",
        "resume_completed": True,
        "sink_commit_state": "committed",
        "offset_commit_state": "committed",
        "offset_committed_after_sink_commit": True,
        "partial_sink_commit_detected": False,
        "partial_sink_commit_repaired": True,
        "repair_actions": [],
        "replayed_events": 1,
        "duplicate_events": 0,
        "idempotent_replay_passed": True,
        "poison_events": 0,
        "quarantined_events": 0,
        "retention_remaining_seconds": 3600,
        "recovery_margin_seconds": 2400,
    }
    payload.update(overrides)
    return payload


def test_ops_cdc_recovery_evidence_cli_outputs_json_and_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    scenario = _write_json(tmp_path / "scenario.json", _scenario_payload())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-recovery-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--observability-json",
                str(observability),
                "--scenario-json",
                str(scenario),
                "--output-dir",
                str(tmp_path / "recovery"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert Path(payload["evidence_artifacts"]["cdc_offset_commit_ordering"]).exists()


def test_ops_cdc_recovery_evidence_cli_returns_nonzero_for_unsafe_recovery(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    scenario = _write_json(
        tmp_path / "scenario.json",
        _scenario_payload(offset_committed_after_sink_commit=False, recovery_margin_seconds=30),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-recovery-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--observability-json",
                str(observability),
                "--scenario-json",
                str(scenario),
                "--output-dir",
                str(tmp_path / "recovery"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "cdc_offset_commit_ordering.unsafe" in payload["blockers"]
    assert "cdc_retention_recovery_margin.too_low" in payload["blockers"]
