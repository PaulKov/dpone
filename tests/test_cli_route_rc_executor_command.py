from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.services.ops import command_handlers_routes


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_ops_route_rc_execute_cli_defaults_to_dry_run_and_delegates_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    orchestration_json = tmp_path / "route_rc_orchestration.json"
    orchestration_json.write_text('{"schema_version": "dpone.route_rc_orchestrator.v1"}\n', encoding="utf-8")
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.route_rc_executor.v1", "passed": True, "executed": False}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Executor:
        def execute(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Release:
        def route_rc_executor(self) -> _Executor:
            return _Executor()

    monkeypatch.setattr(
        command_handlers_routes.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-rc-execute",
                "--orchestration-json",
                str(orchestration_json),
                "--output-dir",
                str(tmp_path / "execution"),
                "--timeout-seconds",
                "17",
                "--max-attempts",
                "3",
                "--redact",
                "super-secret",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["executed"] is False
    assert captured["orchestration_json"] == str(orchestration_json)
    assert captured["output_dir"] == str(tmp_path / "execution")
    assert captured["execute"] is False
    assert captured["timeout_seconds"] == 17
    assert captured["max_attempts"] == 3
    assert captured["extra_redactions"] == ("super-secret",)


def test_ops_route_rc_execute_cli_returns_nonzero_for_blocked_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    orchestration_json = tmp_path / "route_rc_orchestration.json"
    orchestration_json.write_text('{"schema_version": "dpone.route_rc_orchestrator.v1"}\n', encoding="utf-8")
    captured: dict[str, object] = {}

    class _Report:
        passed = False

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.route_rc_executor.v1", "passed": False, "executed": True}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Executor:
        def execute(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Release:
        def route_rc_executor(self) -> _Executor:
            return _Executor()

    monkeypatch.setattr(
        command_handlers_routes.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-rc-execute",
                "--orchestration-json",
                str(orchestration_json),
                "--output-dir",
                str(tmp_path / "execution"),
                "--execute",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 1
    assert payload["passed"] is False
    assert captured["execute"] is True
