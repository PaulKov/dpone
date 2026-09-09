from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.readiness.doctor import DoctorService
from dpone.readiness.python_import_health import PythonImportHealth


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_readiness_commands_are_registered() -> None:
    assert {"doctor", "certify"}.issubset({command.name for command in get_commands()})


def test_doctor_outputs_json_without_heavy_runtime(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["doctor", "--format", "json"])

    assert exc.value.code in {0, 1}
    payload = json.loads(capsys.readouterr().out)
    assert "checks" in payload
    assert any(check["name"] == "python" for check in payload["checks"])


def test_doctor_does_not_trust_loaded_optional_module_without_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("psycopg")
    module.__spec__ = None
    monkeypatch.setitem(sys.modules, "psycopg", module)

    probed: list[str] = []

    def import_probe(module_name: str) -> PythonImportHealth:
        probed.append(module_name)
        if module_name == "psycopg":
            return PythonImportHealth(False, "python_import_not_installed", "module is not installed")
        return PythonImportHealth(True, None, "module import succeeded")

    payload = DoctorService(import_probe=import_probe).run()
    psycopg = next(check for check in payload["checks"] if check["name"] == "psycopg")

    assert psycopg == {
        "name": "psycopg",
        "status": "warn",
        "message": "module is not installed",
        "required": False,
    }
    assert "psycopg" in probed


def test_certify_outputs_default_matrix_json(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["certify", "--format", "json"])

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert "missing_required" in payload
    assert "postgres.full_refresh" in payload["missing_required"]
