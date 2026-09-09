from __future__ import annotations

import json
from pathlib import Path

import pytest
from dpone_airflow_pack import cli_sync


def test_sync_cli_returns_nonzero_for_structured_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(
        cli_sync,
        "sync_airflow_pack_cache",
        lambda _options: {
            "kind": "dpone.airflow_pack_cache_status",
            "schema_version": "1",
            "status": "blocked",
            "warnings": [],
            "blockers": [{"code": "airflow_pack_cache_budget_exceeded"}],
        },
    )

    exit_code = cli_sync.main(_arguments(tmp_path))

    output = capsys.readouterr()
    assert exit_code == 1
    assert json.loads(output.out)["status"] == "blocked"
    assert output.err == ""


def test_sync_cli_redacts_failure_and_writes_it_to_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def fail(_options: object) -> None:
        raise ValueError("password=private-value")

    monkeypatch.setattr(cli_sync, "sync_airflow_pack_cache", fail)

    with pytest.warns(RuntimeWarning, match="EXTERNAL_WARNING_UNPUBLISHED"):
        exit_code = cli_sync.main(_arguments(tmp_path))

    output = capsys.readouterr()
    assert exit_code == 1
    assert output.out == ""
    assert "private-value" not in output.err
    assert "password=[REDACTED]" in output.err


def _arguments(tmp_path: Path) -> list[str]:
    return [
        "--once",
        "--index-uri",
        "s3://bucket/latest/pack-index.json",
        "--cache-dir",
        str(tmp_path / "cache"),
    ]
