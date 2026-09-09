from __future__ import annotations

import argparse
import warnings
from argparse import Namespace
from importlib.metadata import version
from pathlib import Path

import pytest

from dpone.cli import legacy as legacy_cli
from dpone.cli import main as cli_main
from dpone.contracts.errors import ETLConfigurationError


class _UnexpectedCall(RuntimeError):
    pass


@pytest.fixture
def no_context(monkeypatch: pytest.MonkeyPatch):
    def _boom(*args, **kwargs):
        raise _UnexpectedCall("AppContext.from_env must not be called for --help")

    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(_boom))


def test_main_help_does_not_build_context(no_context) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(["--help"])
    assert exc.value.code == 0


@pytest.mark.parametrize("flag", ["--version", "-v"])
def test_main_version_does_not_build_context(no_context, capsys: pytest.CaptureFixture[str], flag: str) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main([flag])
    assert exc.value.code == 0
    assert capsys.readouterr().out.strip() == f"dpone {version('dpone')}"


def test_subcommand_help_does_not_build_context(no_context) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(["manifest", "list", "--help"])
    assert exc.value.code == 0


def test_runtime_init_fetch_rejects_invalid_plan_without_building_context(
    no_context,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("DPONE_INIT_FETCH_PLAN_B64", raising=False)
    monkeypatch.delenv("DPONE_INIT_FETCH_PLAN_SHA256", raising=False)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["airflow", "runtime-init-fetch"])

    assert exc.value.code == 2


@pytest.mark.parametrize(
    ("command", "expected"),
    (
        ("runtime-init-fetch", "DPONE_INIT_FETCH_PLAN_B64"),
        ("runtime-pack-exec", "runtime-fetch-ready.json"),
    ),
)
def test_runtime_delivery_help_names_its_internal_wire_contract(
    no_context,
    capsys: pytest.CaptureFixture[str],
    command: str,
    expected: str,
) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(["airflow", command, "--help"])

    assert exc.value.code == 0
    assert expected in capsys.readouterr().out


def test_all_registered_command_help_surfaces_do_not_build_context(no_context) -> None:
    paths = _registered_command_paths(cli_main.build_parser())

    assert ("run",) in paths
    assert ("ops", "route-readiness") in paths
    for path in paths:
        with pytest.raises(SystemExit) as exc:
            cli_main.main([*path, "--help"])
        assert exc.value.code == 0, path


def _registered_command_paths(parser: argparse.ArgumentParser) -> list[tuple[str, ...]]:
    paths: list[tuple[str, ...]] = []
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for name, subparser in action.choices.items():
                path = (name,)
                paths.append(path)
                paths.extend((name, *child) for child in _registered_command_paths(subparser))
    return paths


def test_legacy_build_parser_and_main_emit_deprecation_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        parser = legacy_cli.build_parser()
        assert parser.prog == "dpone"
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        with pytest.raises(SystemExit) as exc:
            legacy_cli.main(["--help"])
        assert exc.value.code == 0
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)


def test_legacy_removed_symbol_has_helpful_error() -> None:
    with pytest.raises(AttributeError) as exc:
        _ = legacy_cli._cmd_dag_report  # type: ignore[attr-defined]
    assert "DAG commands now live in `dpone.commands.dag.*`" in str(exc.value)


def test_command_packages_do_not_import_legacy_shim() -> None:
    root = Path("src/dpone")
    violations: list[str] = []
    for package in ("commands", "services", "cli_render"):
        for path in sorted((root / package).rglob("*.py")):
            text = path.read_text(encoding="utf-8")
            if "dpone.cli.legacy" in text or "commands.legacy_adapter" in text or "run_legacy" in text:
                violations.append(str(path))
    assert violations == []


class _ParserStub:
    def __init__(self, command):
        self.command = command
        self.help_printed = False

    def parse_args(self, argv):
        return Namespace(_command=self.command, argv=argv)

    def print_help(self) -> None:
        self.help_printed = True


class _LoggerStub:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)


def test_main_returns_command_exit_code(monkeypatch: pytest.MonkeyPatch) -> None:
    class Command:
        def run(self, args, ctx):
            assert args.argv == ["docs", "check-docs"]
            assert ctx == "ctx"
            return "0"

    logger = _LoggerStub()
    monkeypatch.setattr(cli_main, "build_parser", lambda: _ParserStub(Command()))
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: "ctx"))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["docs", "check-docs"])

    assert exc.value.code == 0
    assert logger.errors == []


def test_main_maps_configuration_errors_to_exit_code_2(monkeypatch: pytest.MonkeyPatch) -> None:
    class Command:
        def run(self, args, ctx):
            raise ETLConfigurationError("bad manifest")

    logger = _LoggerStub()
    monkeypatch.setattr(cli_main, "build_parser", lambda: _ParserStub(Command()))
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: "ctx"))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["manifest", "validate"])

    assert exc.value.code == 2
    assert logger.errors == ["bad manifest"]


def test_main_maps_keyboard_interrupt_to_exit_code_130(monkeypatch: pytest.MonkeyPatch) -> None:
    class Command:
        def run(self, args, ctx):
            raise KeyboardInterrupt

    logger = _LoggerStub()
    monkeypatch.setattr(cli_main, "build_parser", lambda: _ParserStub(Command()))
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: "ctx"))

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["manifest", "validate"])

    assert exc.value.code == 130
    assert logger.errors == ["Interrupted"]
