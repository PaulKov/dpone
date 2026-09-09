"""Contracts for exact-environment CLI use in production-hydrated live proofs."""

from __future__ import annotations

import os
import subprocess
import sysconfig
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.runtime.state.mssql_generic_transaction_names import (
    GENERIC_TRANSACTION_CATALOG_VERSION,
)
from tests.integration.postgres import postgres_mssql_production_hydration_cli as cli
from tests.integration.postgres import postgres_mssql_production_hydration_live_support as live_support

_EXPECTED_HEADER = f"-- dpone generic MSSQL transaction catalog v{GENERIC_TRANSACTION_CATALOG_VERSION}\n"


def _entry_point(value: str = "dpone.cli.main:main") -> metadata.EntryPoint:
    return metadata.EntryPoint(name="dpone", value=value, group="console_scripts")


def _fake_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    mode: int = 0o755,
    entry_points: tuple[metadata.EntryPoint, ...] | None = None,
) -> Path:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    executable = scripts / cli._console_script_name(os.name)
    executable.touch(mode=mode)
    executable.chmod(mode)
    monkeypatch.setattr(cli.sysconfig, "get_path", lambda name: str(scripts))
    installed = (_entry_point(),) if entry_points is None else entry_points
    monkeypatch.setattr(
        cli.metadata,
        "distribution",
        lambda name: SimpleNamespace(entry_points=installed),
    )
    return executable


def test_state_ddl_uses_current_install_scheme_without_ambient_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _fake_environment(tmp_path, monkeypatch)
    monkeypatch.setenv("PATH", str(tmp_path / "unrelated"))
    observed: dict[str, object] = {}

    def completed(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed["command"] = command
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout=f"{_EXPECTED_HEADER}SELECT 1;\n", stderr="")

    monkeypatch.setattr(cli.subprocess, "run", completed)

    ddl = cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")

    assert ddl == f"{_EXPECTED_HEADER}SELECT 1;\n"
    assert observed == {
        "command": [
            str(executable),
            "state",
            "render-mssql-transaction-ddl",
            "--database",
            "state_db",
            "--schema",
            "governance",
        ],
        "kwargs": {
            "check": False,
            "capture_output": True,
            "text": True,
            "timeout": cli.DPONE_CLI_TIMEOUT_SECONDS,
        },
    }


def test_installed_state_cli_renders_when_its_scripts_directory_is_not_on_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scripts_dir = Path(sysconfig.get_path("scripts"))
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    monkeypatch.setenv("PATH", str(unrelated))

    ddl = cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")

    assert cli.resolve_environment_dpone_cli().parent == scripts_dir
    assert ddl.startswith(_EXPECTED_HEADER)


def test_state_ddl_rejects_missing_current_environment_console_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _fake_environment(tmp_path, monkeypatch)
    executable.unlink()

    with pytest.raises(RuntimeError, match="exact-environment dpone CLI"):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


@pytest.mark.skipif(os.name == "nt", reason="POSIX executable-bit contract")
def test_state_ddl_rejects_non_executable_posix_console_script(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_environment(tmp_path, monkeypatch, mode=0o644)

    with pytest.raises(RuntimeError, match="exact-environment dpone CLI"):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


@pytest.mark.parametrize(
    ("entry_points", "expected"),
    [
        ((), "metadata does not match"),
        ((_entry_point(), _entry_point()), "metadata does not match"),
        ((_entry_point("unexpected.module:main"),), "metadata does not match"),
    ],
)
def test_state_ddl_rejects_console_script_metadata_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    entry_points: tuple[metadata.EntryPoint, ...],
    expected: str,
) -> None:
    _fake_environment(tmp_path, monkeypatch, entry_points=entry_points)

    with pytest.raises(RuntimeError, match=expected):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


def test_state_ddl_rejects_missing_distribution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_environment(tmp_path, monkeypatch)

    def missing(name: str) -> Any:
        raise metadata.PackageNotFoundError(name)

    monkeypatch.setattr(cli.metadata, "distribution", missing)

    with pytest.raises(RuntimeError, match="installed dpone distribution"):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


@pytest.mark.parametrize(
    ("completed", "expected"),
    [
        (subprocess.CompletedProcess([], 7, stdout="", stderr="failure"), "exit code 7"),
        (subprocess.CompletedProcess([], 0, stdout="SELECT 1;\n", stderr=""), "unexpected state catalog"),
    ],
)
def test_state_ddl_rejects_failed_or_malformed_render(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    completed: subprocess.CompletedProcess[str],
    expected: str,
) -> None:
    _fake_environment(tmp_path, monkeypatch)
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: completed)

    with pytest.raises(RuntimeError, match=expected):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


def test_state_ddl_rejects_render_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    executable = _fake_environment(tmp_path, monkeypatch)

    def timed_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired(str(executable), cli.DPONE_CLI_TIMEOUT_SECONDS)

    monkeypatch.setattr(cli.subprocess, "run", timed_out)

    with pytest.raises(RuntimeError, match="bounded render timeout"):
        cli.render_state_ddl_with_environment_cli(database="state_db", schema="governance")


@pytest.mark.parametrize(
    ("os_name", "expected"),
    [("nt", "dpone.exe"), ("posix", "dpone")],
)
def test_console_script_name_is_platform_specific(os_name: str, expected: str) -> None:
    assert cli._console_script_name(os_name) == expected


def test_live_fixture_preflights_cli_before_vendor_connections(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_render(*, database: str, schema: str) -> str:
        raise RuntimeError(f"preflight:{database}:{schema}")

    def unexpected_connection(*args: object, **kwargs: object) -> Any:
        raise AssertionError("vendor connection opened before CLI preflight")

    monkeypatch.setattr(live_support, "render_state_ddl_with_environment_cli", reject_render)
    monkeypatch.setattr(live_support, "mssql_connector", unexpected_connection)
    monkeypatch.setattr(live_support, "postgres_connector", unexpected_connection)

    with pytest.raises(RuntimeError, match="preflight:dpone_ph_state_.*:governance"):
        with live_support.production_hydration_live_fixture(tmp_path):
            raise AssertionError("fixture must not yield after a failed CLI preflight")
