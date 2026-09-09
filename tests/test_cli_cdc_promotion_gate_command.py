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


def _upstream_payload(schema_version: str, *, passed: bool = True) -> dict[str, object]:
    return {
        "schema_version": schema_version,
        "stream": _stream_payload(),
        "route": _route_payload(),
        "passed": passed,
        "evidence_status": "PASS" if passed else "FAIL",
        "blockers": [] if passed else ["upstream.failed"],
        "warnings": [],
    }


def test_ops_cdc_promotion_gate_cli_outputs_json_and_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1"))
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-promotion-gate",
                "--apply-certification-json",
                str(apply),
                "--handoff-json",
                str(handoff),
                "--observability-json",
                str(observability),
                "--recovery-json",
                str(recovery),
                "--schema-evolution-json",
                str(schema),
                "--output-dir",
                str(tmp_path / "promotion"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["production_ready"] is True
    assert payload["promote_offsets"] is True
    assert Path(payload["evidence_artifacts"]["cdc_stream_identity_consistency"]).exists()


def test_ops_cdc_promotion_gate_cli_returns_nonzero_when_upstream_failed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    apply = _write_json(tmp_path / "apply.json", _upstream_payload("dpone.cdc_apply_certification.v1"))
    handoff = _write_json(tmp_path / "handoff.json", _upstream_payload("dpone.cdc_handoff.v1"))
    observability = _write_json(
        tmp_path / "observability.json", _upstream_payload("dpone.cdc_observability.v1", passed=False)
    )
    recovery = _write_json(tmp_path / "recovery.json", _upstream_payload("dpone.cdc_recovery_evidence.v1"))
    schema = _write_json(tmp_path / "schema.json", _upstream_payload("dpone.cdc_schema_evolution_evidence.v1"))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-promotion-gate",
                "--apply-certification-json",
                str(apply),
                "--handoff-json",
                str(handoff),
                "--observability-json",
                str(observability),
                "--recovery-json",
                str(recovery),
                "--schema-evolution-json",
                str(schema),
                "--output-dir",
                str(tmp_path / "promotion"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["production_ready"] is False
    assert payload["promote_offsets"] is False
    assert "cdc_observability_gate.not_passed" in payload["blockers"]
