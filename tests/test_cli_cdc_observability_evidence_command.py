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


def _handoff_payload() -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_handoff.v1",
        "stream": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "source_dataset": "dbo.orders",
            "target_dataset": "analytics.orders",
            "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
            "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
        },
        "route": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "case_id": "mssql_to_clickhouse__cdc",
            "colon_id": "mssql:clickhouse:cdc",
        },
        "passed": True,
        "blockers": [],
    }


def _apply_payload() -> dict[str, object]:
    return {
        "schema_version": "dpone.cdc_apply_certification.v1",
        "stream": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "source_dataset": "dbo.orders",
            "target_dataset": "analytics.orders",
            "stream_id": "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders",
            "colon_id": "mssql:clickhouse:cdc:dbo.orders:analytics.orders",
        },
        "route": {
            "source": "mssql",
            "sink": "clickhouse",
            "strategy": "cdc",
            "case_id": "mssql_to_clickhouse__cdc",
            "colon_id": "mssql:clickhouse:cdc",
        },
        "passed": True,
        "evidence_status": "PASS",
        "blockers": [],
        "metrics": {"duplicate_events": 0},
    }


def _metrics_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "captured_at": "2026-06-12T12:00:00Z",
        "lag_seconds": 10,
        "freshness_seconds": 20,
        "retention_remaining_seconds": 3600,
        "events_per_second": 500,
        "duplicate_events": 0,
        "replayed_events": 0,
        "offset_commit_status": "committed",
        "last_committed_offset": "0x13",
    }
    payload.update(overrides)
    return payload


def test_ops_cdc_observability_evidence_cli_outputs_json_and_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    metrics = _write_json(tmp_path / "metrics.json", _metrics_payload())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-observability-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--metrics-json",
                str(metrics),
                "--output-dir",
                str(tmp_path / "obs"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert Path(payload["evidence_artifacts"]["cdc_offset_commit_health"]).exists()


def test_ops_cdc_observability_evidence_cli_returns_nonzero_for_slo_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    handoff = _write_json(tmp_path / "handoff.json", _handoff_payload())
    apply = _write_json(tmp_path / "apply.json", _apply_payload())
    metrics = _write_json(tmp_path / "metrics.json", _metrics_payload(lag_seconds=900))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-observability-evidence",
                "--handoff-json",
                str(handoff),
                "--apply-certification-json",
                str(apply),
                "--metrics-json",
                str(metrics),
                "--output-dir",
                str(tmp_path / "obs"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "cdc_lag_slo.exceeded" in payload["blockers"]
