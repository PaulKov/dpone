from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_supply_chain_command_group_is_registered() -> None:
    assert "supply-chain" in {command.name for command in get_commands()}


def test_supply_chain_attest_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "demo"
version = "1.2.3"
dependencies = ["pyyaml>=6"]
""".strip(),
        encoding="utf-8",
    )
    subject = tmp_path / "dist" / "demo.whl"
    subject.parent.mkdir()
    subject.write_bytes(b"wheel")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "supply-chain",
                "attest",
                "--project-root",
                str(tmp_path),
                "--output-dir",
                str(tmp_path / "supply-chain"),
                "--release",
                "v1.2.3",
                "--subject",
                str(subject),
                "--repository",
                "https://github.com/example/demo",
                "--commit-sha",
                "abc123",
                "--builder-id",
                "local-ci",
                "--signing-key",
                "local-secret",
                "--signing-key-id",
                "test-key",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert Path(payload["bundle_path"]).is_file()
    assert Path(payload["signature_path"]).is_file()


def test_supply_chain_attest_fails_without_signing_key(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "demo"
version = "1.2.3"
dependencies = ["pyyaml>=6"]
""".strip(),
        encoding="utf-8",
    )
    subject = tmp_path / "dist" / "demo.whl"
    subject.parent.mkdir()
    subject.write_bytes(b"wheel")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "supply-chain",
                "attest",
                "--project-root",
                str(tmp_path),
                "--output-dir",
                str(tmp_path / "supply-chain"),
                "--release",
                "v1.2.3",
                "--subject",
                str(subject),
                "--repository",
                "https://github.com/example/demo",
                "--commit-sha",
                "abc123",
                "--builder-id",
                "local-ci",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["blockers"] == ["signature.missing_key"]
    assert payload["signature_path"] == ""
