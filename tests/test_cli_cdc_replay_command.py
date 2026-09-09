from __future__ import annotations

import json
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


def test_cdc_replay_plan_outputs_blockers_for_unsafe_rewind(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "cdc",
                "replay-plan",
                "--backend",
                "postgres_logical",
                "--pipeline-name",
                "orders-cdc",
                "--schema",
                "public",
                "--table",
                "orders",
                "--stored-offset",
                "0/20",
                "--replay-from",
                "0/10",
                "--retention-min",
                "0/05",
                "--high-watermark",
                "0/30",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["safe_to_execute"] is False
    assert "replay.requires_allow_rewind" in payload["blockers"]
    assert "replay.artifact_required_for_consumed_postgres_slot" in payload["blockers"]


def test_cdc_replay_plan_allows_artifact_backed_replay(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "cdc",
                "replay-plan",
                "--backend",
                "mssql_cdc",
                "--pipeline-name",
                "orders-cdc",
                "--schema",
                "dbo",
                "--table",
                "orders",
                "--stored-offset",
                "0x00000000000000000020",
                "--replay-from",
                "0x00000000000000000010",
                "--replay-to",
                "0x00000000000000000020",
                "--retention-min",
                "0x00000000000000000005",
                "--artifact-uri",
                "file:///tmp/dpone/orders-cdc.jsonl",
                "--allow-rewind",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["safe_to_execute"] is True
    assert payload["artifact_uri"] == "file:///tmp/dpone/orders-cdc.jsonl"
    assert [step["name"] for step in payload["steps"]][-1] == "commit_offset_after_sink_commit"
