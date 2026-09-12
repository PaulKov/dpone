"""Production pack-exec composer for authenticated v3 composition.

These tests exercise the public KPO path: the composer used by
``cmd_airflow_runtime_pack_exec``, not a test-only
``CompositionNativeDbtDispatcher`` constructor. Generic child startup explodes
so a missing dispatcher cannot silently fall back to native-v2 or shell.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
from dataclasses import replace

import pytest

from dpone.commands import airflow_runtime_delivery_cmd
from dpone.runtime.composition_verified_dispatch import COMPOSITION_SUPERVISOR_B64_ENV
from dpone.runtime.verified_pack_execution import execute_verified_pack_command
from dpone.runtime.verified_pack_launcher import (
    RUNTIME_RELEASE_ADMISSION_ENV,
    VerifiedPackCommand,
)
from tests.test_verified_pack_execution import (
    NATIVE_DBT_ARGV,
    ORDINARY_ARGV,
    admitted,
    rejection_reason,
)


@pytest.fixture
def run_volume(tmp_path):
    return {
        "run_output_dir": tmp_path / "run",
        "xcom_return_path": tmp_path / "xcom" / "return.json",
    }


@pytest.fixture
def command(tmp_path):
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    return VerifiedPackCommand(argv=NATIVE_DBT_ARGV, env={}, working_directory=worktree)


@pytest.fixture
def denied_child(monkeypatch):
    def explode(*args, **kwargs):
        pytest.fail("generic child startup was attempted for a composition command")

    monkeypatch.setattr(subprocess, "Popen", explode)
    monkeypatch.setattr(subprocess, "run", explode)


@pytest.fixture
def clean_ambient(monkeypatch):
    monkeypatch.delenv(RUNTIME_RELEASE_ADMISSION_ENV, raising=False)
    monkeypatch.delenv(COMPOSITION_SUPERVISOR_B64_ENV, raising=False)


def test_composer_returns_dispatcher_for_authenticated_composition(command, clean_ambient):
    from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher

    dispatcher = compose_verified_pack_dispatcher(admitted(command))

    assert dispatcher is not None
    assert callable(getattr(dispatcher, "run", None))


@pytest.mark.parametrize("env", [{}, {RUNTIME_RELEASE_ADMISSION_ENV: ""}])
def test_composer_returns_none_for_legacy_or_empty_admission(command, clean_ambient, env):
    from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher

    assert compose_verified_pack_dispatcher(replace(command, env=env)) is None


def test_pack_exec_cli_passes_composer_dispatcher_for_admitted_command(command, clean_ambient, monkeypatch):
    admitted_command = admitted(command)
    captured: dict[str, object] = {}

    class PreparedService:
        def prepare_pack_exec(self) -> VerifiedPackCommand:
            return admitted_command

    def record_execute(prepared, **kwargs: object) -> int:
        captured["command"] = prepared
        captured["kwargs"] = kwargs
        return 5

    monkeypatch.setattr(airflow_runtime_delivery_cmd, "AirflowRuntimeInitFetchService", PreparedService)
    monkeypatch.setattr(airflow_runtime_delivery_cmd, "execute_verified_pack_command", record_execute)

    code = airflow_runtime_delivery_cmd.cmd_airflow_runtime_pack_exec(
        argparse.Namespace(),
        ctx=object(),
        logger=logging.getLogger("test.runtime-pack-exec.dispatcher"),
    )

    assert code == 5
    assert captured["command"] is admitted_command
    kwargs = captured["kwargs"]
    assert isinstance(kwargs, dict)
    assert kwargs.get("composition_dispatcher") is not None


def test_production_composer_never_starts_generic_popen(command, run_volume, clean_ambient, denied_child):
    from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher

    admitted_command = admitted(command)
    dispatcher = compose_verified_pack_dispatcher(admitted_command)

    status = execute_verified_pack_command(
        admitted_command,
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert rejection_reason(run_volume) == "composition_native_worker_unavailable"


def test_missing_parent_context_rejects_as_native_worker_unavailable(command, run_volume, clean_ambient, denied_child):
    from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher

    admitted_command = admitted(command)
    status = execute_verified_pack_command(
        admitted_command,
        composition_dispatcher=compose_verified_pack_dispatcher(admitted_command),
        **run_volume,
    )

    assert status == 5
    assert rejection_reason(run_volume) == "composition_native_worker_unavailable"


def test_ordinary_transfer_stays_ordinary_worker_unavailable(command, run_volume, clean_ambient, denied_child):
    from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher

    admitted_command = admitted(replace(command, argv=ORDINARY_ARGV))
    status = execute_verified_pack_command(
        admitted_command,
        composition_dispatcher=compose_verified_pack_dispatcher(admitted_command),
        **run_volume,
    )

    assert status == 5
    assert rejection_reason(run_volume) == "composition_ordinary_worker_unavailable"


def test_library_omitting_dispatcher_still_reports_unavailable(command, run_volume, clean_ambient, denied_child):
    assert execute_verified_pack_command(admitted(command), **run_volume) == 5
    assert rejection_reason(run_volume) == "composition_dispatcher_unavailable"
