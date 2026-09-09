from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.ops.managed_credentials import ManagedCredentialReadinessService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_managed_credentials_readiness_redacts_values_and_writes_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DPONE_VENDOR_TOKEN", "super-secret-token")
    report = ManagedCredentialReadinessService().check(
        output_dir=tmp_path,
        profile="vendor_live",
        required_env=("DPONE_VENDOR_TOKEN", "DPONE_MISSING_TOKEN"),
    )

    assert report.passed is False
    assert report.blockers == ("missing_credentials",)
    assert report.missing == ("DPONE_MISSING_TOKEN",)
    assert report.present == ("DPONE_VENDOR_TOKEN",)
    assert "super-secret-token" not in report.to_json()
    assert "DPONE_VENDOR_TOKEN" in report.to_markdown()
    assert (tmp_path / "managed_credentials_readiness.json").exists()
    assert (tmp_path / "managed_credentials_readiness.md").exists()


def test_managed_credentials_readiness_cli_returns_nonzero_for_missing_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_cli(monkeypatch)
    ops_group = next(command for command in get_commands() if command.name == "ops")
    assert "managed-credentials-readiness" in {command.name for command in ops_group.subcommands}

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "managed-credentials-readiness",
                "--output-dir",
                str(tmp_path),
                "--profile",
                "vendor_live",
                "--required-env",
                "DPONE_MISSING_TOKEN",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.managed_credentials.readiness.v1"
    assert payload["passed"] is False
    assert payload["missing"] == ["DPONE_MISSING_TOKEN"]
