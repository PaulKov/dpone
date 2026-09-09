from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.strategy_intelligence.replay_adapters import ReplayBackendResult


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_live_replay_support_requires_connection_id() -> None:
    from dpone.commands import replay_cmd_support
    from dpone.contracts.configuration_errors import ETLConfigurationError

    args = SimpleNamespace(
        live_backend=True,
        connection_id=None,
        target_table="orders",
        target_schema="dbo",
        staging_schema="staging",
        connection_type="env",
        mount_point=None,
        connection_path=None,
        sink_type="mssql",
        artifact_dir=".dpone/replay",
    )

    with pytest.raises(ETLConfigurationError, match="--connection-id"):
        replay_cmd_support.build_replay_execution_service(args)


def test_live_replay_support_requires_target_table() -> None:
    from dpone.commands import replay_cmd_support
    from dpone.contracts.configuration_errors import ETLConfigurationError

    args = SimpleNamespace(
        live_backend=True,
        connection_id="mssql_dwh",
        target_table=None,
        target_schema="dbo",
        staging_schema="staging",
        connection_type="env",
        mount_point=None,
        connection_path=None,
        sink_type="mssql",
        artifact_dir=".dpone/replay",
    )

    with pytest.raises(ETLConfigurationError, match="--target-table"):
        replay_cmd_support.build_replay_execution_service(args)


def test_resync_cli_executes_with_live_runtime_backend_factory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    from dpone.commands import replay_cmd_support

    factory = _RecordingRuntimeReplayBackendFactory()
    monkeypatch.setattr(replay_cmd_support, "RuntimeReplayBackendFactory", lambda: factory)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "resync",
                "--run-id",
                "01JREPLAY000000000000000101",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--partition",
                "2026-06-05",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-id",
                "mssql_dwh",
                "--connection-type",
                "env",
                "--target-schema",
                "dbo",
                "--target-table",
                "orders",
                "--staging-schema",
                "staging",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["operations"] == [
        "validate_staging:incremental_merge",
        "execute_finalizer:mssql:incremental_merge",
        "reconcile:mssql:incremental_merge",
        "commit_state:01JREPLAY000000000000000101",
    ]
    assert factory.connections == [
        {
            "sink_type": "mssql",
            "connection_id": "mssql_dwh",
            "credentials_source": "env",
            "target_schema": "dbo",
            "target_table": "orders",
            "staging_schema": "staging",
            "mount_point": None,
            "path": None,
        }
    ]


def test_resync_cli_uses_current_replay_support_module_after_reload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    import dpone.commands as commands_pkg
    from dpone.commands import resync_cmd

    del resync_cmd
    sys.modules.pop("dpone.commands.replay_cmd_support", None)
    if hasattr(commands_pkg, "replay_cmd_support"):
        delattr(commands_pkg, "replay_cmd_support")
    from dpone.commands import replay_cmd_support

    factory = _RecordingRuntimeReplayBackendFactory()
    monkeypatch.setattr(replay_cmd_support, "RuntimeReplayBackendFactory", lambda: factory)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "resync",
                "--run-id",
                "01JREPLAY000000000000000103",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-id",
                "mssql_dwh",
                "--connection-type",
                "env",
                "--target-schema",
                "dbo",
                "--target-table",
                "orders",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["operations"][-1] == "commit_state:01JREPLAY000000000000000103"
    assert factory.connections[0]["connection_id"] == "mssql_dwh"


def test_resume_cli_executes_with_live_runtime_backend_factory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    from dpone.commands import replay_cmd_support

    factory = _RecordingRuntimeReplayBackendFactory()
    monkeypatch.setattr(replay_cmd_support, "RuntimeReplayBackendFactory", lambda: factory)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "resume",
                "01JREPLAY000000000000000102",
                "--from-stage",
                "finalize",
                "--source-type",
                "postgres",
                "--sink-type",
                "kafka",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--live-backend",
                "--connection-id",
                "kafka_cluster",
                "--connection-type",
                "params",
                "--target-table",
                "dwh.orders",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["executed"] is True
    assert payload["operations"] == [
        "validate_staging:incremental_merge",
        "produce_replay_events:kafka:incremental_merge",
        "reconcile:kafka:incremental_merge",
        "commit_state:01JREPLAY000000000000000102",
    ]
    assert factory.connections[0]["sink_type"] == "kafka"
    assert factory.connections[0]["credentials_source"] == "params"


class _RecordingRuntimeReplayBackendFactory:
    def __init__(self) -> None:
        self.connections: list[dict[str, object]] = []

    def build(self, connection) -> _RecordingReplayBackend:
        self.connections.append(
            {
                "sink_type": connection.sink_type,
                "connection_id": connection.connection_id,
                "credentials_source": connection.credentials_source,
                "target_schema": connection.target_schema,
                "target_table": connection.target_table,
                "staging_schema": connection.staging_schema,
                "mount_point": connection.mount_point,
                "path": connection.path,
            }
        )
        return _RecordingReplayBackend()


class _RecordingReplayBackend:
    def validate_staging(self, request) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"validated {request.run_id}")

    def execute_finalizer(self, request) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"finalized {request.sink_type}")

    def produce_replay_events(self, request) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"produced {request.sink_type}")

    def reconcile(self, request) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"reconciled {request.run_id}")

    def commit_state(self, request) -> ReplayBackendResult:
        return ReplayBackendResult(True, f"committed {request.run_id}")
