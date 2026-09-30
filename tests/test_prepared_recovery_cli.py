"""Operator recovery CLI keeps secrets and mutation behind explicit confirmation."""

from __future__ import annotations

import argparse
import json
import logging
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.commands import prepared_recovery_cmd
from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError


def _args(*, action: str = "plan", confirmation: str | None = None, plan_file: str = "plan.json") -> argparse.Namespace:
    return argparse.Namespace(
        prepared_recovery_action=action,
        binding_set="binding.yaml",
        connection_registry="registry.yaml",
        connection_ref="sink",
        cluster="cluster",
        database="analytics",
        target="target",
        operation_id="original-operation",
        authority_version=0,
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC).isoformat(),
        confirmation_digest=confirmation,
        format="json",
        plan_file=plan_file,
    )


class _Runtime:
    def __init__(self, *, error: ClusterPublicationError | None = None) -> None:
        self.error = error
        self.executions = 0

    def plan(self, args: argparse.Namespace):
        if self.error:
            raise self.error
        return SimpleNamespace(
            plan_digest="safe-digest",
            token="safe-correlation",
            replica_count=2,
            to_public_dict=lambda: {"status": "ready", "plan_digest": "safe-digest"},
        )

    def execute(self, plan: object, *, confirmation_digest: str):
        self.executions += 1
        return SimpleNamespace(authority=SimpleNamespace(operation_id="original-operation"))

    def save_plan(self, plan: object, path: str) -> None:
        return None

    def load_plan(self, args: argparse.Namespace, path: str):
        return self.plan(args)


def test_plan_json_is_redacted_and_read_only(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runtime = _Runtime()
    monkeypatch.setattr(prepared_recovery_cmd, "build_prepared_recovery_runtime", lambda args: runtime)
    code = prepared_recovery_cmd.cmd_prepared_recovery(_args(), ctx=None, logger=logging.getLogger(__name__))
    output = capsys.readouterr().out
    assert code == 0
    assert json.loads(output) == {"status": "ready", "plan_digest": "safe-digest"}
    assert "original-operation" not in output
    assert runtime.executions == 0


def test_execute_requires_matching_digest(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runtime = _Runtime()
    monkeypatch.setattr(prepared_recovery_cmd, "build_prepared_recovery_runtime", lambda args: runtime)
    code = prepared_recovery_cmd.cmd_prepared_recovery(
        _args(action="execute", confirmation="wrong"), ctx=None, logger=logging.getLogger(__name__)
    )
    assert code == 2
    assert runtime.executions == 0
    assert "original-operation" not in capsys.readouterr().out


def test_execute_reports_replica_summary_without_operation_id(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runtime = _Runtime()
    monkeypatch.setattr(prepared_recovery_cmd, "build_prepared_recovery_runtime", lambda args: runtime)
    assert (
        prepared_recovery_cmd.cmd_prepared_recovery(
            _args(action="execute", confirmation="safe-digest"), ctx=None, logger=logging.getLogger(__name__)
        )
        == 0
    )
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["replica_summary"] == {"expected": 2, "published": 2}
    assert payload["correlation_id"] == "safe-correlation"
    assert "original-operation" not in output
    assert runtime.executions == 1


def test_safety_block_exits_two(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runtime = _Runtime(error=ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_UNSAFE", "legacy authority"))
    monkeypatch.setattr(prepared_recovery_cmd, "build_prepared_recovery_runtime", lambda args: runtime)
    assert prepared_recovery_cmd.cmd_prepared_recovery(_args(), ctx=None, logger=logging.getLogger(__name__)) == 2
    assert "AUTHORITY_UNSAFE" in capsys.readouterr().out


def test_unknown_outcome_exits_one(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    runtime = _Runtime(error=ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_DDL_UNKNOWN", "unknown"))
    monkeypatch.setattr(prepared_recovery_cmd, "build_prepared_recovery_runtime", lambda args: runtime)
    assert prepared_recovery_cmd.cmd_prepared_recovery(_args(), ctx=None, logger=logging.getLogger(__name__)) == 1
    assert "DDL_UNKNOWN" in capsys.readouterr().out


def test_other_ops_commands_unchanged() -> None:
    from dpone.commands.registry_ops import operations_group

    names = {command.name for command in operations_group().subcommands}
    assert {"artifact-index", "recovery-plan", "reconcile", "clickhouse-prepared-recovery"} <= names
