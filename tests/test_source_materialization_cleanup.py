from __future__ import annotations

import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime import source_materialization_audit
from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.native_wire_artifacts import SourceNativeArtifact
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.native_wire_transcoder import NativeWireTranscoder
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializationPolicy,
    SourceMaterializedSnapshot,
)
from dpone.runtime.source_materialization_cleanup import (
    SourceMaterializationCleanupPolicy,
    SourceMaterializationCleanupResult,
)
from dpone.runtime.source_materialization_preparation import guard_source_materialization_preparation
from dpone.runtime.sources.strategies.mssql.mssql_source_materialization import (
    MssqlWorkTableMaterializationProvider,
)


def test_prepared_source_artifact_publishes_cleanup_decision(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    file_path = tmp_path / "data.tsv"
    file_path.write_text("1\n", encoding="utf-8")
    events: list[tuple[tuple[object, ...], dict[str, object]]] = []
    monkeypatch.setattr(
        source_materialization_audit,
        "publish_runtime_decision",
        lambda *args, **kwargs: events.append((args, kwargs)),
    )
    artifact = PreparedSourceArtifact(
        FileExportArtifact(str(file_path), ["id"], format="mssql-delimited"),
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_a]",
            cleanup=lambda: SourceMaterializationCleanupResult(status="deleted"),
            evidence={"work_table": "[dbo].[__dpone_snapshot_a]"},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green", provider="mssql_work_table"),
        cleanup_policy="eager",
    )

    artifact.load_with(lambda _inner: 1)
    receipt = artifact.terminate(ArtifactTerminalOutcome.SUCCESS)

    assert receipt.cleanup_succeeded is True
    assert events
    assert events[0][1]["decision_id"] == "source_materialization.cleanup"
    assert events[0][1]["phase"] == "cleanup"
    assert events[0][1]["details"]["cleanup_result"]["status"] == "deleted"


def test_prepared_source_artifact_records_cleanup_result_after_success(tmp_path: Path) -> None:
    file_path = tmp_path / "data.tsv"
    file_path.write_text("1\n", encoding="utf-8")
    artifact = PreparedSourceArtifact(
        FileExportArtifact(str(file_path), ["id"], format="mssql-delimited"),
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_a]",
            cleanup=lambda: SourceMaterializationCleanupResult(status="deferred", reason="mssql_cleanup_lock_timeout"),
            evidence={"work_table": "[dbo].[__dpone_snapshot_a]"},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green"),
        cleanup_policy="eager",
    )

    rows = artifact.load_with(lambda inner: 1 if Path(inner.file_path).exists() else 0)
    receipt = artifact.terminate(ArtifactTerminalOutcome.SUCCESS)

    assert rows == 1
    assert receipt.cleanup_succeeded is True
    assert artifact.source_materialization["cleanup_result"]["status"] == "deferred"
    assert artifact.source_materialization["snapshot"]["cleanup_result"]["reason"] == "mssql_cleanup_lock_timeout"


def test_mssql_work_table_cleanup_retries_transient_lock_timeout() -> None:
    connector = _FakeMssqlConnector()
    connector.drop_failures_remaining = 1
    provider = MssqlWorkTableMaterializationProvider(
        connector, run_id_factory=lambda: "retry_lock_timeout", clock=lambda: 100.0
    )
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_schema="dpone_work",
        cleanup=SourceMaterializationCleanupPolicy(retry_attempts=2, retry_backoff_ms=0),
    )

    snapshot = provider.prepare(
        _load_config(),
        query="SELECT [id] FROM [dbo].[orders]",
        schema=[("id", "int")],
        policy=policy,
    )
    cleanup = snapshot.cleanup()

    assert cleanup.status == "deleted"
    assert cleanup.details["attempts"] == 2
    assert connector.executed[-4:] == [
        "SET LOCK_TIMEOUT 5000",
        "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_retry_lock_timeout]",
        "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_retry_lock_timeout]",
        "SET LOCK_TIMEOUT -1",
    ]


@pytest.mark.parametrize("target_format", ["Native", "RowBinary"])
@pytest.mark.parametrize(
    ("outcome", "cleanup_policy", "expected_cleanup"),
    [
        (ArtifactTerminalOutcome.SUCCESS, "eager", True),
        (ArtifactTerminalOutcome.ABORT, "eager", True),
        (ArtifactTerminalOutcome.SUCCESS, "on_success", True),
        (ArtifactTerminalOutcome.ABORT, "on_success", False),
        (ArtifactTerminalOutcome.ABORT, "keep_on_failure", False),
        (ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN, "eager", False),
    ],
)
def test_snapshot_cleanup_survives_native_transport_closing_inner_file(
    tmp_path: Path,
    target_format: str,
    outcome: ArtifactTerminalOutcome,
    cleanup_policy: str,
    expected_cleanup: bool,
) -> None:
    """Native stream EOF releases its file before the processor decides the snapshot outcome."""

    schema = [("id", "int")]
    path = tmp_path / "snapshot.bcp"
    path.write_bytes(struct.pack("<i", 42))
    inner = SourceNativeArtifact(
        path,
        columns=["id"],
        estimated_rows=1,
        native_wire_contract=build_mssql_bcp_native_contract(
            schema=schema,
            query="SELECT [id] FROM [dbo].[snapshot]",
            bcp_version="18",
            target_format=target_format,
        ),
    )
    cleaned: list[str] = []
    artifact = PreparedSourceArtifact(
        inner,
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_test]",
            cleanup=lambda: cleaned.append("snapshot"),
            evidence={},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green"),
        cleanup_policy=cleanup_policy,
    )
    owner = OwnedPayloadScope.from_extract_result(SimpleNamespace(artifact=artifact))
    stream = NativeWireTranscoder().to_clickhouse_binary(inner, schema)

    assert b"".join(stream.iter_bytes()).endswith(struct.pack("<i", 42))
    assert cleaned == []
    receipt = owner.terminate(outcome)
    owner.terminate(outcome)

    assert receipt.outcome is outcome
    assert receipt.cleanup_succeeded is True
    assert cleaned == (["snapshot"] if expected_cleanup else [])


def test_sweep_discovery_error_does_not_claim_empty_success(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _FakeMssqlConnector()

    def denied(*args, **kwargs):
        raise PermissionError("catalog discovery denied")

    monkeypatch.setattr(connector, "get_records", denied)

    with pytest.raises(PermissionError, match="catalog discovery denied"):
        MssqlWorkTableMaterializationProvider(connector).prepare(
            _load_config(),
            query="SELECT [id] FROM [dbo].[orders]",
            schema=[("id", "int")],
            policy=SourceMaterializationPolicy(work_schema="dpone_work"),
        )

    assert connector.executed == []


def test_sweep_uses_server_clock_for_configured_ttl(monkeypatch: pytest.MonkeyPatch) -> None:
    connector = _FakeMssqlConnector()
    queries: list[str] = []

    def discover(query: str, **kwargs):
        queries.append(query)
        return []

    monkeypatch.setattr(connector, "get_records", discover)

    result = MssqlWorkTableMaterializationProvider(connector).sweep_stale(
        _load_config(),
        policy=SourceMaterializationPolicy(work_schema="dpone_work", ttl_hours=24),
    )

    assert result.status == "green"
    assert result.scanned == 0
    assert "t.create_date < DATEADD(hour, -24, SYSDATETIME())" in queries[0]
    assert connector.executed == []


@pytest.mark.parametrize("status", ["deleted", "deferred"])
def test_preparation_audit_failure_does_not_reclassify_physical_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    status: str,
) -> None:
    attempted: list[str] = []
    primary = RuntimeError("preparation failed")

    def unavailable(*args, **kwargs):
        attempted.append(kwargs["details"]["cleanup_result"]["status"])
        if len(attempted) == 1:
            raise OSError("audit unavailable")

    monkeypatch.setattr(source_materialization_audit, "publish_runtime_decision", unavailable)

    with pytest.raises(RuntimeError) as caught:
        with guard_source_materialization_preparation(
            cleanup=lambda: SourceMaterializationCleanupResult(status=status),
            provider="mssql_work_table",
            cleanup_policy="eager",
        ):
            raise primary

    assert caught.value is primary
    assert attempted == [status]
    assert primary.__notes__ == ["source cleanup audit failed: OSError"]


def test_mssql_work_table_provider_sweeps_expired_dpone_snapshots_before_prepare() -> None:
    connector = _FakeMssqlConnector()
    connector.stale_tables = ("__dpone_snapshot_old_a", "__dpone_snapshot_old_b")
    provider = MssqlWorkTableMaterializationProvider(connector, run_id_factory=lambda: "fresh", clock=lambda: 100.0)

    snapshot = provider.prepare(
        _load_config(),
        query="SELECT [id] FROM [dbo].[orders]",
        schema=[("id", "int")],
        policy=SourceMaterializationPolicy(mode="required", allow_source_writes=True, work_schema="dpone_work"),
    )

    assert snapshot.evidence["pre_materialization_sweep"]["deleted"] == 2
    assert "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_old_a]" in connector.executed
    assert "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_old_b]" in connector.executed
    assert any(
        query.startswith("SELECT [id] INTO [dpone_work].[__dpone_snapshot_fresh]") for query in connector.executed
    )


def test_mssql_work_table_sweep_blocks_unsafe_prefix_without_drop() -> None:
    connector = _FakeMssqlConnector()
    connector.stale_tables = ("tmp_orders",)
    provider = MssqlWorkTableMaterializationProvider(connector, run_id_factory=lambda: "fresh", clock=lambda: 100.0)
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_schema="dpone_work",
        table_prefix="tmp_",
    )

    sweep = provider.sweep_stale(_load_config(), policy=policy)

    assert sweep.status == "blocked"
    assert sweep.blockers == ("source_materialization_unsafe_table_prefix",)
    assert not any("DROP TABLE" in query for query in connector.executed)


def _load_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="DWH_Raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=1000,
        options={"partition_tmp_dir": "/tmp"},
    )


class _FakeMssqlConnector:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.drop_failures_remaining = 0
        self.stale_tables: tuple[str, ...] = ()

    def quote_identifier(self, name: str) -> str:
        return f"[{name}]"

    def execute_query(self, query: str, params=None) -> int:
        del params
        self.executed.append(query)
        if "DROP TABLE IF EXISTS" in query and self.drop_failures_remaining:
            self.drop_failures_remaining -= 1
            raise RuntimeError("Lock request time out period exceeded. Error 1222")
        return 1

    def get_records(self, query: str, params=None, as_dict: bool = False):
        del params
        if "FROM sys.tables" in query:
            if as_dict:
                return [{"table_name": table} for table in self.stale_tables]
            return [(table,) for table in self.stale_tables]
        if "COUNT_BIG" in query:
            return [{"row_count": 10}] if as_dict else [(10,)]
        raise AssertionError(f"unexpected query: {query}")
