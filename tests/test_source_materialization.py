from __future__ import annotations

from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.runtime_connection import (
    ResolvedBindingConnection,
    ResolvedConnectionDescriptor,
    RuntimeConnectionAuthorityError,
)
from dpone.runtime.credentials.authority import RuntimeResolvedConnections
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.source_materialization import (
    PreparedSourceArtifact,
    SourceMaterializationDecision,
    SourceMaterializationIndexPolicy,
    SourceMaterializationPlanner,
    SourceMaterializationPolicy,
    SourceMaterializedSnapshot,
)
from dpone.runtime.source_materialization_cleanup import (
    SourceMaterializationCleanupPolicy,
)
from dpone.runtime.source_materialization_location import (
    bind_source_materialization_location,
    effective_source_materialization_policy,
)
from dpone.runtime.source_scan import SourceShape
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.runtime.sources.strategies.mssql.mssql_source_materialization import (
    MssqlWorkTableMaterializationProvider,
)


class CapturingLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.events.append((event, payload))


def _resolved_mssql(
    *,
    host: str = "mssql.example",
    database: str,
    schema: str,
) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        credentials=CredentialsConfig(
            host=host,
            port=1433,
            database=database,
            schema=schema,
        ),
        safe_metadata={"resolver": "kubernetes_secret_volume"},
        descriptor=ResolvedConnectionDescriptor(
            connection_type="mssql",
            properties={"host": host, "port": 1433, "database": database, "schema": schema},
        ),
    )


def test_materialization_policy_parses_public_snapshot_options() -> None:
    policy = SourceMaterializationPolicy.from_source_options(
        {
            "native_transfer": {
                "snapshot": {
                    "materialization": {
                        "mode": "required",
                        "allow_source_writes": True,
                        "work_database": "Example_System",
                        "work_schema": "dpone_work",
                        "table_prefix": "__dpone_snapshot_",
                        "ttl_hours": 12,
                        "cleanup_policy": "keep_on_failure",
                        "reuse_policy": "same_run_verified",
                        "min_speedup_pct": 30,
                        "max_materialization_seconds": 120,
                        "max_work_table_bytes": "2GiB",
                        "isolation": "snapshot",
                        "index": {"mode": "required", "boundary_columns": ["id", "doc_date"]},
                        "update_statistics": "required",
                    }
                }
            }
        }
    )

    assert policy.mode == "required"
    assert policy.provider == "auto"
    assert policy.allow_source_writes is True
    assert policy.work_database == "Example_System"
    assert policy.work_schema == "dpone_work"
    assert policy.ttl_hours == 12
    assert policy.cleanup_policy == "keep_on_failure"
    assert policy.reuse_policy == "same_run_verified"
    assert policy.min_speedup_pct == 30.0
    assert policy.max_materialization_seconds == 120
    assert policy.max_work_table_bytes == 2 * 1024 * 1024 * 1024
    assert policy.isolation == "snapshot"
    assert policy.index.mode == "required"
    assert policy.index.boundary_columns == ("id", "doc_date")
    assert policy.update_statistics == "required"
    assert policy.cleanup.retry_attempts == 2
    assert policy.cleanup.retry_backoff_ms == 250


def test_materialization_policy_parses_logical_work_connection_ref() -> None:
    policy = SourceMaterializationPolicy.from_source_options(
        {
            "native_transfer": {
                "snapshot": {
                    "materialization": {
                        "work_connection_ref": "mssql_dpone_snapshot_work",
                    }
                }
            }
        }
    )

    assert policy.work_connection_ref == "mssql_dpone_snapshot_work"
    assert policy.work_database is None
    assert policy.work_schema is None


def test_materialization_policy_rejects_mixed_logical_and_physical_work_location() -> None:
    with pytest.raises(ValueError, match="work_connection_ref cannot be combined"):
        SourceMaterializationPolicy.from_source_options(
            {
                "native_transfer": {
                    "snapshot": {
                        "materialization": {
                            "work_connection_ref": "mssql_dpone_snapshot_work",
                            "work_database": "Example_System",
                        }
                    }
                }
            }
        )


def test_logical_work_connection_binds_environment_location_atomically() -> None:
    load_config = _load_config()
    load_config.options = {
        "native_transfer": {
            "snapshot": {
                "materialization": {
                    "work_connection_ref": "mssql_dpone_snapshot_work",
                }
            }
        }
    }
    connections = RuntimeResolvedConnections(
        strict=True,
        source=_resolved_mssql(database="analytics_staging", schema="clickhouse"),
        source_materialization=_resolved_mssql(database="DWH_Dev", schema="system"),
    )

    bind_source_materialization_location(load_config=load_config, connections=connections)
    effective = effective_source_materialization_policy(
        SourceMaterializationPolicy.from_source_options(load_config.options),
        load_config=load_config,
    )

    assert effective.work_connection_ref == "mssql_dpone_snapshot_work"
    assert effective.work_database == "DWH_Dev"
    assert effective.work_schema == "system"


def test_logical_work_connection_rejects_cross_server_location() -> None:
    load_config = _load_config()
    load_config.options = {
        "native_transfer": {
            "snapshot": {
                "materialization": {
                    "work_connection_ref": "mssql_dpone_snapshot_work",
                }
            }
        }
    }
    connections = RuntimeResolvedConnections(
        strict=True,
        source=_resolved_mssql(database="analytics_staging", schema="clickhouse"),
        source_materialization=_resolved_mssql(
            host="other-sql.example",
            database="DWH_Dev",
            schema="system",
        ),
    )

    with pytest.raises(RuntimeConnectionAuthorityError) as raised:
        bind_source_materialization_location(load_config=load_config, connections=connections)

    assert raised.value.code == "DPONE_MSSQL_SOURCE_MATERIALIZATION_AUTHORITY_MISMATCH"


def test_auto_blocks_source_writes_until_explicitly_allowed() -> None:
    decision = SourceMaterializationPlanner().plan(
        SourceMaterializationPolicy(mode="auto", allow_source_writes=False),
        source_shape=SourceShape(table_kind="heap", has_seekable_boundary=False, stats_confidence="low"),
        provider_available=True,
        permissions_ok=True,
    )

    assert decision.selected is False
    assert decision.release_gate == "warning"
    assert "source_materialization_requires_allow_source_writes" in decision.warnings


def test_required_blocks_before_export_when_permissions_are_missing() -> None:
    decision = SourceMaterializationPlanner().plan(
        SourceMaterializationPolicy(mode="required", allow_source_writes=True),
        source_shape=SourceShape(table_kind="view", has_seekable_boundary=False, stats_confidence="low"),
        provider_available=True,
        permissions_ok=False,
    )

    assert decision.selected is False
    assert decision.release_gate == "blocked"
    assert "source_materialization_permission_denied" in decision.blockers


def test_materialization_decision_reports_explicit_work_database() -> None:
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_database="Example_System",
        work_schema="dbo",
    )

    decision = SourceMaterializationPlanner().plan(
        policy,
        source_shape=SourceShape(table_kind="view", has_seekable_boundary=False, stats_confidence="low"),
        provider_available=True,
        permissions_ok=True,
    )

    assert decision.to_evidence()["work_database"] == "Example_System"
    assert decision.to_evidence()["work_schema"] == "dbo"


def test_materialization_planner_selects_only_when_speedup_meets_threshold() -> None:
    planner = SourceMaterializationPlanner()
    policy = SourceMaterializationPolicy(mode="auto", allow_source_writes=True, min_speedup_pct=25)
    source_shape = SourceShape(table_kind="view", has_seekable_boundary=False, stats_confidence="low")

    slow = planner.plan(
        policy,
        source_shape=source_shape,
        provider_available=True,
        permissions_ok=True,
        current_rows_per_second=100_000,
        materialized_rows_per_second=120_000,
    )
    fast = planner.plan(
        policy,
        source_shape=source_shape,
        provider_available=True,
        permissions_ok=True,
        current_rows_per_second=100_000,
        materialized_rows_per_second=140_000,
    )

    assert slow.selected is False
    assert slow.measured_speedup_pct == 20.0
    assert "source_materialization_speedup_below_threshold" in slow.reasons
    assert fast.selected is True
    assert fast.measured_speedup_pct == 40.0
    assert "source_materialization_selected" in fast.reasons


def test_prepared_source_artifact_cleans_snapshot_after_terminal_success(tmp_path: Path) -> None:
    file_path = tmp_path / "data.tsv"
    file_path.write_text("1\n", encoding="utf-8")
    cleanup_calls: list[str] = []
    artifact = PreparedSourceArtifact(
        FileExportArtifact(str(file_path), ["id"], format="mssql-delimited"),
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_a]",
            cleanup=lambda: cleanup_calls.append("cleanup"),
            evidence={"work_table": "[dbo].[__dpone_snapshot_a]"},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green"),
        cleanup_policy="eager",
    )

    rows = artifact.load_with(lambda inner: 1 if Path(inner.file_path).exists() else 0)
    receipt = artifact.terminate(ArtifactTerminalOutcome.SUCCESS)

    assert rows == 1
    assert receipt.cleanup_succeeded is True
    assert cleanup_calls == ["cleanup"]


def test_prepared_source_artifact_retains_snapshot_on_failure_when_policy_requires(tmp_path: Path) -> None:
    file_path = tmp_path / "data.tsv"
    file_path.write_text("1\n", encoding="utf-8")
    cleanup_calls: list[str] = []
    artifact = PreparedSourceArtifact(
        FileExportArtifact(str(file_path), ["id"], format="mssql-delimited"),
        snapshot=SourceMaterializedSnapshot(
            qualified_name="[dbo].[__dpone_snapshot_a]",
            cleanup=lambda: cleanup_calls.append("cleanup"),
            evidence={"work_table": "[dbo].[__dpone_snapshot_a]"},
        ),
        decision=SourceMaterializationDecision(selected=True, release_gate="green"),
        cleanup_policy="keep_on_failure",
    )

    with pytest.raises(RuntimeError, match="load failed"):
        artifact.load_with(lambda _inner: (_ for _ in ()).throw(RuntimeError("load failed")))
    receipt = artifact.terminate(ArtifactTerminalOutcome.ABORT)

    assert receipt.cleanup_succeeded is True
    assert cleanup_calls == []


def test_mssql_work_table_provider_creates_projected_snapshot_and_rewrites_query() -> None:
    connector = _FakeMssqlConnector()
    provider = MssqlWorkTableMaterializationProvider(
        connector,
        run_id_factory=lambda: "abc123",
        clock=lambda: 100.0,
    )
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_schema="dpone_work",
        table_prefix="__dpone_snapshot_",
    )

    snapshot = provider.prepare(
        _load_config(),
        query="SELECT [id], [name] FROM [dbo].[orders]",
        schema=[("id", "int"), ("name", "nvarchar(50)")],
        policy=policy,
    )

    assert connector.executed[0].startswith("SELECT [id], [name] INTO [dpone_work].[__dpone_snapshot_abc123]")
    assert "FROM (SELECT [id], [name] FROM [dbo].[orders]) AS dpone_src" in connector.executed[0]
    assert snapshot.rewrite_query(["id", "name"]) == "SELECT [id], [name] FROM [dpone_work].[__dpone_snapshot_abc123]"
    cleanup = snapshot.cleanup()
    assert cleanup is not None
    assert cleanup.to_dict()["status"] == "deleted"
    assert connector.executed[-3:] == [
        "SET LOCK_TIMEOUT 5000",
        "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_abc123]",
        "SET LOCK_TIMEOUT -1",
    ]


def test_mssql_work_table_provider_uses_explicit_work_database_without_moving_source() -> None:
    connector = _FakeMssqlConnector()
    connector.stale_tables = ("__dpone_snapshot_old",)
    provider = MssqlWorkTableMaterializationProvider(
        connector,
        run_id_factory=lambda: "cross_database",
        clock=lambda: 100.0,
    )
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_database="Example_System",
        work_schema="dbo",
        table_prefix="__dpone_snapshot_",
    )
    source_query = "SELECT [id] FROM [analytics_staging].[clickhouse].[v_example_contact_dimension]"

    assert provider.permissions_ok(_load_config(), policy) is True
    snapshot = provider.prepare(
        _load_config(),
        query=source_query,
        schema=[("id", "int")],
        policy=policy,
    )

    assert "EXEC [Example_System].sys.sp_executesql" in connector.queried[0]
    assert any("FROM [Example_System].sys.tables" in query for query in connector.queried)
    create = next(query for query in connector.executed if query.startswith("SELECT [id] INTO"))
    assert create.startswith("SELECT [id] INTO [Example_System].[dbo].[__dpone_snapshot_cross_database]")
    assert f"FROM ({source_query}) AS dpone_src" in create
    assert snapshot.rewrite_query(["id"]) == (
        "SELECT [id] FROM [Example_System].[dbo].[__dpone_snapshot_cross_database]"
    )
    assert snapshot.evidence["work_database"] == "Example_System"
    assert snapshot.evidence["work_schema"] == "dbo"
    assert "DROP TABLE IF EXISTS [Example_System].[dbo].[__dpone_snapshot_old]" in connector.executed

    cleanup = snapshot.cleanup()

    assert cleanup.status == "deleted"
    assert connector.executed[-2] == ("DROP TABLE IF EXISTS [Example_System].[dbo].[__dpone_snapshot_cross_database]")


def test_mssql_work_table_provider_rejects_ambiguous_explicit_work_database() -> None:
    connector = _FakeMssqlConnector()
    provider = MssqlWorkTableMaterializationProvider(connector)
    policy = SourceMaterializationPolicy(
        work_database="Example_System.dbo",
        work_schema="dbo",
    )

    with pytest.raises(ValueError, match="database must not contain dots"):
        provider.sweep_stale(_load_config(), policy=policy)

    assert connector.queried == []
    assert connector.executed == []


def test_mssql_work_table_cleanup_defers_lock_timeout_instead_of_blocking_forever() -> None:
    connector = _FakeMssqlConnector()
    connector.failures["DROP TABLE IF EXISTS"] = RuntimeError("Lock request time out period exceeded. Error 1222")
    provider = MssqlWorkTableMaterializationProvider(
        connector, run_id_factory=lambda: "lock_timeout", clock=lambda: 100.0
    )
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_schema="dpone_work",
        cleanup=SourceMaterializationCleanupPolicy(retry_attempts=0),
    )

    snapshot = provider.prepare(
        _load_config(),
        query="SELECT [id] FROM [dbo].[orders]",
        schema=[("id", "int")],
        policy=policy,
    )
    cleanup = snapshot.cleanup()

    assert cleanup is not None
    assert cleanup.status == "deferred"
    assert cleanup.reason == "mssql_cleanup_lock_timeout"
    assert cleanup.details["lock_timeout_ms"] == 5000
    assert cleanup.details["attempts"] == 1
    assert connector.executed[-3:] == [
        "SET LOCK_TIMEOUT 5000",
        "DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_lock_timeout]",
        "SET LOCK_TIMEOUT -1",
    ]


def test_mssql_work_table_cleanup_still_raises_unexpected_drop_errors() -> None:
    connector = _FakeMssqlConnector()
    connector.failures["DROP TABLE IF EXISTS"] = RuntimeError("permission denied")
    provider = MssqlWorkTableMaterializationProvider(connector, run_id_factory=lambda: "bad_drop", clock=lambda: 100.0)
    snapshot = provider.prepare(
        _load_config(),
        query="SELECT [id] FROM [dbo].[orders]",
        schema=[("id", "int")],
        policy=SourceMaterializationPolicy(mode="required", allow_source_writes=True, work_schema="dpone_work"),
    )

    with pytest.raises(RuntimeError, match="permission denied"):
        snapshot.cleanup()

    assert connector.executed[-1] == "SET LOCK_TIMEOUT -1"


def test_mssql_work_table_auto_index_and_statistics_failures_do_not_block() -> None:
    connector = _FakeMssqlConnector()
    connector.fail_on = {"CREATE INDEX", "UPDATE STATISTICS"}
    policy = SourceMaterializationPolicy(
        mode="required",
        allow_source_writes=True,
        work_schema="dpone_work",
        index=SourceMaterializationIndexPolicy(mode="auto", boundary_columns=("id",)),
        update_statistics="auto",
    )

    snapshot = MssqlWorkTableMaterializationProvider(
        connector,
        run_id_factory=lambda: "auto_fail",
        clock=lambda: 100.0,
    ).prepare(
        _load_config(),
        query="SELECT [id] FROM [dbo].[orders]",
        schema=[("id", "int")],
        policy=policy,
    )

    assert snapshot.evidence["index_created"] is False
    assert snapshot.evidence["statistics_updated"] is False
    assert snapshot.evidence["warnings"] == [
        "source_materialization_index_auto_failed",
        "source_materialization_update_statistics_auto_failed",
    ]


def test_mssql_queryout_materializes_before_export_and_exports_from_work_table(tmp_path: Path) -> None:
    connector = _FakeMssqlConnector()
    config = _load_config(
        tmp_path=tmp_path,
        options={
            "partition_tmp_dir": str(tmp_path),
            "native_transfer": {
                "snapshot": {
                    "materialization": {
                        "mode": "required",
                        "provider": "mssql_work_table",
                        "allow_source_writes": True,
                        "work_schema": "dpone_work",
                        "table_prefix": "__dpone_snapshot_",
                    },
                    "physical_chunking": {"mode": "off"},
                }
            },
        },
    )

    artifact = MSSQLQueryoutArtifactFactory(connector, CapturingLogger(), sink_connector=object()).artifact_for_query(
        config,
        "SELECT [id], [name] FROM [dbo].[orders]",
        [("id", "int"), ("name", "nvarchar(50)")],
    )

    assert isinstance(artifact, PreparedSourceArtifact)
    assert artifact.source_materialization["provider"] == "mssql_work_table"
    assert "SELECT [id], [name] FROM [dpone_work].[__dpone_snapshot_" in connector.bcp_queries[0]
    assert "REPLACE(" not in connector.bcp_queries[0]


@pytest.mark.parametrize("stage", ["SELECT [id] INTO", "CREATE INDEX", "UPDATE STATISTICS"])
@pytest.mark.parametrize("cleanup_policy", ["eager", "keep_on_failure"])
def test_failed_snapshot_preparation_obeys_cleanup_policy(stage: str, cleanup_policy: str) -> None:
    connector = _FakeMssqlConnector()
    primary = RuntimeError("preparation failed")
    connector.failures[stage] = primary
    policy = SourceMaterializationPolicy(
        work_schema="dpone_work",
        cleanup_policy=cleanup_policy,
        index=SourceMaterializationIndexPolicy(mode="required", boundary_columns=("id",)),
        update_statistics="required",
    )

    with pytest.raises(RuntimeError) as caught:
        MssqlWorkTableMaterializationProvider(connector, run_id_factory=lambda: "owned").prepare(
            _load_config(),
            query="SELECT [id] FROM [dbo].[orders]",
            schema=[("id", "int")],
            policy=policy,
        )

    assert caught.value is primary
    drops = [sql for sql in connector.executed if sql.startswith("DROP TABLE")]
    assert drops == (
        ["DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_owned]"] if cleanup_policy == "eager" else []
    )


@pytest.mark.parametrize("stage", ["encoding", "bcp"])
def test_queryout_failure_before_owner_cleans_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    stage: str,
) -> None:
    from dpone.runtime.sources.strategies.mssql import mssql_queryout_bcp

    connector = _FakeMssqlConnector()
    primary = RuntimeError("export assembly failed")

    def fail(*args, **kwargs):
        raise primary

    if stage == "encoding":
        monkeypatch.setattr(mssql_queryout_bcp, "_encoded_query", fail)
    else:
        monkeypatch.setattr(connector, "bcp_queryout", fail)
    config = _load_config(
        tmp_path=tmp_path,
        options={
            "partition_tmp_dir": str(tmp_path),
            "native_transfer": {
                "snapshot": {
                    "materialization": {
                        "mode": "required",
                        "provider": "mssql_work_table",
                        "allow_source_writes": True,
                        "work_schema": "dpone_work",
                        "cleanup_policy": "eager",
                    },
                    "physical_chunking": {"mode": "off"},
                }
            },
        },
    )

    with pytest.raises(RuntimeError) as caught:
        MSSQLQueryoutArtifactFactory(connector, CapturingLogger(), sink_connector=object()).artifact_for_query(
            config,
            "SELECT [id] FROM [dbo].[orders]",
            [("id", "int")],
        )

    assert caught.value is primary
    drops = [sql for sql in connector.executed if sql.startswith("DROP TABLE")]
    assert len(drops) == 1
    assert drops[0].startswith("DROP TABLE IF EXISTS [dpone_work].[__dpone_snapshot_")


def test_preparation_cleanup_failure_preserves_primary_error() -> None:
    connector = _FakeMssqlConnector()
    primary = RuntimeError("preparation failed")
    connector.failures = {
        "CREATE INDEX": primary,
        "DROP TABLE": PermissionError("do not expose raw connector messages"),
    }

    with pytest.raises(RuntimeError) as caught:
        MssqlWorkTableMaterializationProvider(connector, run_id_factory=lambda: "owned").prepare(
            _load_config(),
            query="SELECT [id] FROM [dbo].[orders]",
            schema=[("id", "int")],
            policy=SourceMaterializationPolicy(
                work_schema="dpone_work",
                cleanup_policy="eager",
                index=SourceMaterializationIndexPolicy(mode="required", boundary_columns=("id",)),
            ),
        )

    assert caught.value is primary
    assert primary.__notes__ == ["source materialization cleanup failed: PermissionError"]


def _load_config(
    *,
    tmp_path: Path | None = None,
    options: dict[str, object] | None = None,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="orders",
        target_schema="DWH_Raw",
        target_table="orders",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=1000,
        options=options or {"partition_tmp_dir": str(tmp_path or Path("/tmp"))},
    )


class _FakeMssqlConnector:
    bcp_path = "bcp"
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.executed: list[str] = []
        self.queried: list[str] = []
        self.bcp_queries: list[str] = []
        self.fail_on: set[str] = set()
        self.failures: dict[str, Exception] = {}
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
        for marker, exc in self.failures.items():
            if marker in query:
                raise exc
        if any(marker in query for marker in self.fail_on):
            raise RuntimeError(query)
        return 1

    def get_records(self, query: str, params=None, as_dict: bool = False):
        del params
        self.queried.append(query)
        if "dpone_source_materialization_permissions" in query:
            return [{"can_create_table": 1, "can_alter_schema": 1}] if as_dict else [(1, 1)]
        if "COUNT_BIG" in query:
            return [{"row_count": 10}] if as_dict else [(10,)]
        if "dpone_source_shape" in query:
            return [{"table_kind": "heap", "has_seekable_boundary": 0, "stats_confidence": "low"}]
        if "sys.tables" in query:
            if as_dict:
                return [{"table_name": table} for table in self.stale_tables]
            return [(table,) for table in self.stale_tables]
        raise AssertionError(f"unexpected query: {query}")

    def bcp_queryout(self, query: str, output_path: str, *, options=None) -> int:
        del options
        self.bcp_queries.append(query)
        Path(output_path).write_text("1\tone\n", encoding="utf-8")
        return 1
