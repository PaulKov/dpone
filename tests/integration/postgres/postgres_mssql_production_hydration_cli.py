"""Exact-environment CLI boundary for production-hydration live proofs."""

from __future__ import annotations

import os
import subprocess
import sysconfig
from importlib import metadata
from pathlib import Path

from dpone.runtime.state.mssql_generic_transaction_names import (
    GENERIC_TRANSACTION_CATALOG_VERSION,
)

DPONE_CLI_TIMEOUT_SECONDS = 180
_DPONE_DISTRIBUTION = "dpone"
_DPONE_ENTRY_POINT = "dpone"
_DPONE_ENTRY_POINT_VALUE = "dpone.cli.main:main"


def render_state_ddl_with_environment_cli(*, database: str, schema: str) -> str:
    """Render generic state DDL with the CLI installed for this interpreter."""

    executable = resolve_environment_dpone_cli()
    command = [
        str(executable),
        "state",
        "render-mssql-transaction-ddl",
        "--database",
        database,
        "--schema",
        schema,
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=DPONE_CLI_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("exact-environment dpone CLI exceeded its bounded render timeout") from exc
    if completed.returncode != 0:
        raise RuntimeError(f"exact-environment dpone CLI failed with exit code {completed.returncode}")

    ddl = completed.stdout
    expected_header = f"-- dpone generic MSSQL transaction catalog v{GENERIC_TRANSACTION_CATALOG_VERSION}\n"
    if not ddl.startswith(expected_header):
        raise RuntimeError("exact-environment dpone CLI returned an unexpected state catalog")
    return ddl


def resolve_environment_dpone_cli() -> Path:
    """Resolve and validate the console script owned by this Python install."""

    scripts_dir = Path(sysconfig.get_path("scripts"))
    executable = scripts_dir / _console_script_name(os.name)
    is_executable = os.name == "nt" or os.access(executable, os.X_OK)
    if not scripts_dir.is_absolute() or not executable.is_file() or not is_executable:
        raise RuntimeError("production hydration live proof requires the exact-environment dpone CLI")

    try:
        distribution = metadata.distribution(_DPONE_DISTRIBUTION)
    except metadata.PackageNotFoundError as exc:
        raise RuntimeError("production hydration live proof requires the installed dpone distribution") from exc
    entry_points = [
        entry_point
        for entry_point in distribution.entry_points
        if entry_point.group == "console_scripts" and entry_point.name == _DPONE_ENTRY_POINT
    ]
    if len(entry_points) != 1 or entry_points[0].value != _DPONE_ENTRY_POINT_VALUE:
        raise RuntimeError("installed dpone console-script metadata does not match the certified entry point")
    return executable


def _console_script_name(os_name: str) -> str:
    return "dpone.exe" if os_name == "nt" else "dpone"


__all__ = [
    "DPONE_CLI_TIMEOUT_SECONDS",
    "render_state_ddl_with_environment_cli",
    "resolve_environment_dpone_cli",
]
