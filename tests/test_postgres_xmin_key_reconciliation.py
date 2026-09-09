from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import dpone.runtime.sources.strategies.postgres.postgres_snapshot_extract as snapshot_extract_module
from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.incremental_snapshot import (
    DELTA_HASH_COLUMN,
    IncrementalSnapshotEnvelope,
    KeySnapshotReconciliationPolicy,
    snapshot_token_digest,
)
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_repair import (
    admit_snapshot_repair,
    consume_snapshot_repair,
)
from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import PostgresFetchedSchema
from dpone.runtime.sources.strategies.postgres.postgres_delta_snapshot import (
    PostgresDeltaSnapshotFileArtifact,
)
from dpone.runtime.sources.strategies.postgres.postgres_key_snapshot import (
    KEY_HASH_COLUMN,
    PostgresKeySnapshotFileArtifact,
)
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_extract import (
    PostgresXMinExtractStrategy,
)
from dpone.runtime.state.xmin_storage import XMinState
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.runtime.support.mssql_snapshot_projection import require_clustered_key_width
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


class _Logger:
    def log_xmin_state_info(self, *_args, **_kwargs) -> None:
        return None

    def log_etl_progress(self, *_args, **_kwargs) -> None:
        return None

    def info(self, *_args, **_kwargs) -> None:
        return None

    def warning(self, *_args, **_kwargs) -> None:
        return None


class _Connector:
    database = "sample-metrics"

    def __init__(self) -> None:
        self.connection = object()
        self.in_transaction = False
        self.operations: list[str] = []

    def begin(self) -> None:
        self.operations.append("begin")
        self.in_transaction = True

    def execute_query(self, query, params=None) -> int:
        del params
        self.operations.append(str(query))
        return 0

    def get_records(self, query, params=None, as_dict=False):
        del params, as_dict
        rendered = str(query)
        self.operations.append(rendered)
        if "txid_current_snapshot" in rendered:
            return [{"snapshot_token": "100:102:", "extraction_horizon": 102}]
        if "relfrozenxid" in rendered:
            return [{"checkpoint_frozen": False, "relation_frozen_xid": 10, "database_frozen_xid": 5}]
        return []

    def commit_transaction(self) -> None:
        self.operations.append("commit")
        self.in_transaction = False

    def rollback(self) -> None:
        self.operations.append("rollback")
        self.in_transaction = False


class _TargetConnector:
    database = "DWH_Dev"

    def __init__(self, *, exists: bool, has_rows: bool) -> None:
        self.exists = exists
        self.has_rows = has_rows

    def table_exists(self, schema, table, *, database=None) -> bool:
        del schema, table, database
        return self.exists

    @staticmethod
    def qualified_name(schema, table, *, database=None) -> str:
        return f"[{database}].[{schema}].[{table}]"

    def get_records(self, query):
        del query
        return [(1,)] if self.has_rows else []


class _StateStorage:
    def load_state_by_key(self, key):
        del key
        return None


class _SourceAuthority:
    """Explicit unit-test authority for the exact branded snapshot session."""

    @staticmethod
    def verify_snapshot(*, connector, snapshot_lease, load_config):
        del load_config
        snapshot_lease.require_for(connector)
        return SimpleNamespace(verified=True)


class _XminManager:
    def __init__(self, connector: _Connector) -> None:
        self.connector = connector

    def get_snapshot_xmin_anchor(self) -> int:
        assert self.connector.in_transaction
        self.connector.operations.append("anchor")
        return 100

    def calculate_safe_xmin(self, anchor: int, previous: XMinState | None) -> XMinState:
        return XMinState(
            xmin_value=anchor,
            timestamp=datetime.now(UTC),
            is_initial=previous is None,
        )

    def build_incremental_query(self, schema, table, previous, anchor, **_kwargs) -> str:
        return (
            f'SELECT *, t.xmin AS __dpone__xmin FROM "{schema}"."{table}" AS t '
            f"WHERE t.xmin::text::bigint >= {previous.xmin_value} "
            f"AND t.xmin::text::bigint < {anchor}"
        )

    @staticmethod
    def should_perform_full_refresh(state: XMinState) -> bool:
        return state.is_initial


def _config(tmp_path: Path) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_sample_metrics_source",
        target_conn_id="mssql_sample_metrics_target",
        source_schema="public",
        source_table="metrics_value",
        source_database="sample-metrics",
        target_schema="sample_metrics",
        target_table="metrics_value",
        target_database="DWH_Dev",
        staging_schema="sample_metrics",
        staging_database="DWH_Dev",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["guid"],
        export_format="csv",
        compress_export=False,
        options={
            "batch_commit_mode": "whole",
            "sink_type": "mssql",
            "work_dir": str(tmp_path),
            "reconciliation": {
                "enabled": True,
                "mode": "key_snapshot",
                "consistency": "same_source_snapshot",
                "delete_policy": "soft_delete",
            },
            "state_identity": {
                "environment": "dev",
                "process": "platform.sample_metrics.metrics_value",
            },
            "schema_contract": {
                "columns": {
                    "guid": {"nullable": False},
                    "metric_value": {"nullable": False},
                }
            },
        },
    )


def _strategy(
    tmp_path: Path,
    *,
    target_exists: bool,
    target_has_rows: bool = False,
) -> tuple[PostgresXMinExtractStrategy, _Connector, list[str]]:
    connector = _Connector()
    strategy = PostgresXMinExtractStrategy(
        connector=connector,
        sink_connector=_TargetConnector(exists=target_exists, has_rows=target_has_rows),
        state_storage=_StateStorage(),
        logger=_Logger(),
    )
    strategy.bind_postgres_source_authority(_SourceAuthority())
    strategy.xmin_manager = _XminManager(connector)
    # Route-topology behavior has dedicated SQL Server session tests; these
    # fixtures isolate the PostgreSQL snapshot protocol.
    strategy._physical_target_binding = ("DWH_Dev", "sample_metrics", "metrics_value")
    strategy._physical_target_identity = b"t" * 32
    strategy.fetch_schema_projection = lambda _cfg: _projection(  # type: ignore[method-assign]
        (("guid", "uuid", "uniqueidentifier"), ("metric_value", "integer", "int"))
    )
    exported_queries: list[str] = []

    def export(
        query,
        schema,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
        relation_schema=None,
        after_copy=None,
    ):
        del load_config
        assert snapshot_lease is not None
        snapshot_lease.require_for(connector)
        if relation_schema is not None:
            assert [name for name, _dtype in relation_schema] == [name for name, _dtype in schema]
        rendered = str(query)
        exported_queries.append(rendered)
        path = tmp_path / f"artifact_{len(exported_queries)}.bcp"
        columns = [column for column, _dtype in schema]
        if len(schema) == 1:
            path.write_text("00000000-0000-0000-0000-000000000001\n", encoding="utf-8")
        else:
            values = {
                "guid": "00000000-0000-0000-0000-000000000001",
                "metric_value": "7",
                "__dpone__xmin": "101",
            }
            path.write_text("\t".join(values[column] for column in columns) + "\n", encoding="utf-8")
        if after_copy is not None:
            after_copy()
        artifact = FileExportArtifact(
            str(path),
            [column for column, _dtype in schema],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=BulkTextCodec(),
        )
        artifact.bind_extraction_lifecycle(snapshot_lease.lifecycle)
        return artifact

    def export_keys(
        query,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
    ):
        return export(
            query,
            [(str(load_config.unique_key[0]), "uniqueidentifier")],
            load_config,
            snapshot_lease=snapshot_lease,
        )

    strategy._export_to_file_whole = export  # type: ignore[method-assign]
    strategy._export_key_snapshot_file = export_keys  # type: ignore[method-assign]
    return strategy, connector, exported_queries


def _projection(columns: tuple[tuple[str, str, str], ...]) -> PostgresFetchedSchema:
    metadata = tuple(
        SourceColumnProvenance(name=name, declared_type=source_type, nullable=False)
        for name, source_type, _target_type in columns
    )
    return PostgresFetchedSchema(
        relation_schema=tuple((name, source_type) for name, source_type, _target_type in columns),
        projected_schema=tuple((name, target_type) for name, _source_type, target_type in columns),
        relation_metadata=metadata,
    )


def test_xmin_baseline_emits_delta_and_complete_keys_from_one_snapshot_scan(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=False)

    result = strategy.extract(_config(tmp_path), None)

    assert isinstance(result.artifact, IncrementalSnapshotEnvelope)
    assert result.snapshot_envelope is result.artifact
    assert result.force_full_refresh is False
    assert result.artifact.baseline is True
    assert result.state == result.artifact.candidate_checkpoint
    assert result.artifact.snapshot_token == snapshot_token_digest("100:102:")
    assert result.artifact.delta_receipt.complete is True
    assert result.artifact.delta_receipt.row_count == 1
    assert result.artifact.delta_receipt.columns[-1] == DELTA_HASH_COLUMN
    assert result.artifact.key_receipt.complete is True
    assert result.artifact.key_receipt.row_count == 1
    assert len(exported_queries) == 1
    assert "COUNT(" not in "\n".join(connector.operations).upper()
    assert connector.operations.index("begin") < connector.operations.index("anchor")
    assert connector.in_transaction is False


def test_xmin_baseline_releases_snapshot_before_local_key_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=False)
    lifecycle = ExtractionLifecycleAuthority()
    strategy._new_extraction_lifecycle = lambda: lifecycle  # type: ignore[method-assign]
    build = snapshot_extract_module.build_baseline_snapshot_files
    events: list[str] = []

    def observed_build(*args, **kwargs):
        assert connector.in_transaction is False
        assert lifecycle.require_completed().complete is True
        events.append("local-snapshot-projection")
        return build(*args, **kwargs)

    commit = connector.commit_transaction

    def observed_commit() -> None:
        events.append("commit")
        commit()

    connector.commit_transaction = observed_commit  # type: ignore[method-assign]
    monkeypatch.setattr(snapshot_extract_module, "build_baseline_snapshot_files", observed_build)

    result = strategy.extract(_config(tmp_path), None)

    assert events == ["commit", "local-snapshot-projection"]
    assert len(exported_queries) == 1
    assert result.snapshot_envelope is not None
    assert result.snapshot_envelope.baseline is True
    assert connector.operations.count("commit") == 1


def test_xmin_baseline_local_key_projection_failure_cleans_export_without_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy, connector, _exported_queries = _strategy(tmp_path, target_exists=False)
    raw_paths: list[Path] = []
    rollbacks_at_projection: list[int] = []
    export = strategy._export_to_file_whole

    def observed_export(*args, **kwargs):
        artifact = export(*args, **kwargs)
        raw_paths.append(Path(artifact.file_path))
        return artifact

    def failed_projection(*_args, **_kwargs):
        assert connector.in_transaction is False
        rollbacks_at_projection.append(connector.operations.count("rollback"))
        raise RuntimeError("local key projection failed")

    strategy._export_to_file_whole = observed_export  # type: ignore[method-assign]
    monkeypatch.setattr(snapshot_extract_module, "build_baseline_snapshot_files", failed_projection)

    with pytest.raises(RuntimeError, match="local key projection failed"):
        strategy.extract(_config(tmp_path), None)

    assert connector.operations.count("commit") == 1
    assert connector.operations.count("rollback") == rollbacks_at_projection[0]
    assert len(raw_paths) == 1
    assert not raw_paths[0].exists()
    assert list(tmp_path.iterdir()) == []


def test_xmin_snapshot_lease_evidence_precedes_schema_catalog_anchor_and_exports(tmp_path: Path) -> None:
    strategy, connector, _exported_queries = _strategy(tmp_path, target_exists=True)
    events: list[str] = []

    begin = connector.begin
    execute_query = connector.execute_query
    get_records = connector.get_records
    projection = strategy.fetch_schema_projection
    anchor = strategy.xmin_manager.get_snapshot_xmin_anchor
    export_full = strategy._export_to_file_whole
    export_keys = strategy._export_key_snapshot_file

    def observed_begin() -> None:
        events.append("begin")
        begin()

    def observed_execute(query, params=None) -> int:
        if "REPEATABLE READ" in str(query):
            events.append("set-repeatable-read")
        return execute_query(query, params)

    def observed_records(query, params=None, as_dict=False):
        rendered = str(query)
        if "txid_current_snapshot" in rendered:
            events.append("snapshot-token")
        elif "relfrozenxid" in rendered:
            events.append("freeze-catalog")
        return get_records(query, params, as_dict)

    class _ObservedLifecycle(ExtractionLifecycleAuthority):
        def acquire_snapshot(self, **kwargs):
            events.append("snapshot-evidence")
            return super().acquire_snapshot(**kwargs)

        def complete(self, **kwargs):
            events.append("complete")
            return super().complete(**kwargs)

    def observed_projection(load_config):
        events.append("source-schema")
        return projection(load_config)

    def observed_anchor() -> int:
        events.append("xmin-anchor")
        return anchor()

    def observed_full(query, schema, load_config, *, snapshot_lease, relation_schema, after_copy=None):
        events.append("delta-export")
        return export_full(
            query,
            schema,
            load_config,
            snapshot_lease=snapshot_lease,
            relation_schema=relation_schema,
            after_copy=after_copy,
        )

    def observed_keys(query, load_config, *, snapshot_lease):
        events.append("key-export")
        return export_keys(query, load_config, snapshot_lease=snapshot_lease)

    commit = connector.commit_transaction

    def observed_commit() -> None:
        events.append("commit")
        commit()

    connector.begin = observed_begin  # type: ignore[method-assign]
    connector.execute_query = observed_execute  # type: ignore[method-assign]
    connector.get_records = observed_records  # type: ignore[method-assign]
    connector.commit_transaction = observed_commit  # type: ignore[method-assign]
    strategy.fetch_schema_projection = observed_projection  # type: ignore[method-assign]
    strategy.xmin_manager.get_snapshot_xmin_anchor = observed_anchor  # type: ignore[method-assign]
    strategy._export_to_file_whole = observed_full  # type: ignore[method-assign]
    strategy._export_key_snapshot_file = observed_keys  # type: ignore[method-assign]
    strategy._new_extraction_lifecycle = _ObservedLifecycle  # type: ignore[method-assign]

    strategy.extract(_config(tmp_path), XMinState(90, datetime.now(UTC)))

    assert events == [
        "begin",
        "set-repeatable-read",
        "snapshot-token",
        "snapshot-evidence",
        "source-schema",
        "xmin-anchor",
        "freeze-catalog",
        "delta-export",
        "key-export",
        "complete",
        "commit",
    ]


def test_atomic_mssql_route_preflight_runs_before_postgres_schema_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy, _connector, _queries = _strategy(tmp_path, target_exists=True)
    strategy._physical_target_binding = None
    strategy._physical_target_identity = None
    events: list[str] = []

    monkeypatch.setattr(
        "dpone.runtime.sources.strategies.postgres.postgres_xmin_extract.resolve_atomic_mssql_target",
        lambda *_args, **_kwargs: (
            events.extend(("mssql-preflight", "target-identity"))
            or SimpleNamespace(
                database_name="DWH_Dev",
                schema_name="sample_metrics",
                table_name="metrics_value",
                digest=b"t" * 32,
            )
        ),
    )
    strategy.fetch_schema_projection = lambda _cfg: (  # type: ignore[method-assign]
        events.append("postgres-schema")
        or _projection((("guid", "uuid", "uniqueidentifier"), ("metric_value", "integer", "int")))
    )

    assert strategy.get_state(_config(tmp_path)) is None
    assert events[:3] == ["mssql-preflight", "target-identity", "postgres-schema"]


def test_missing_target_registry_binding_fails_before_postgres_schema_io(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy, _connector, _queries = _strategy(tmp_path, target_exists=True)
    strategy._physical_target_binding = None
    strategy._physical_target_identity = None
    events: list[str] = []

    def reject_binding(*_args, **_kwargs):
        events.append("mssql-preflight")
        events.append("target-identity-failed")
        raise RuntimeError("mssql_physical_target_registry_binding_missing_or_ambiguous")

    monkeypatch.setattr(
        "dpone.runtime.sources.strategies.postgres.postgres_xmin_extract.resolve_atomic_mssql_target",
        reject_binding,
    )
    strategy.fetch_schema_projection = lambda _cfg: (  # type: ignore[method-assign]
        events.append("postgres-schema") or _projection((("id", "integer", "int"),))
    )

    with pytest.raises(RuntimeError, match="registry_binding_missing_or_ambiguous"):
        strategy.get_state(_config(tmp_path))

    assert events == ["mssql-preflight", "target-identity-failed"]


def test_xmin_incremental_uses_bounded_delta_and_full_ordered_keys_in_two_scans(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    previous = XMinState(90, datetime.now(UTC))

    result = strategy.extract(_config(tmp_path), previous)

    envelope = result.snapshot_envelope
    assert envelope is not None
    assert len(exported_queries) == 2
    assert ">= 90" in exported_queries[0]
    assert "< 102" in exported_queries[0]
    assert "xmin" not in exported_queries[1].lower()
    assert "order by" in exported_queries[1].lower()
    assert envelope.previous_checkpoint == previous
    assert envelope.candidate_checkpoint.xmin_value == 100
    assert envelope.safe_checkpoint.xmin_value == 100
    assert envelope.visible_horizon == 102
    assert envelope.replay_amplification_xids == 2
    assert connector.operations.count("anchor") == 1


def test_xmin_snapshot_completes_after_both_receipts_then_commits_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    events: list[str] = []

    class _ObservedLifecycle(ExtractionLifecycleAuthority):
        def complete(self, *, completed_at: datetime | None = None):
            events.append("complete")
            return super().complete(completed_at=completed_at)

    lifecycle = _ObservedLifecycle()
    strategy._new_extraction_lifecycle = lambda: lifecycle  # type: ignore[method-assign]
    export_full = strategy._export_to_file_whole
    export_keys = strategy._export_key_snapshot_file

    def observed_full_export(
        query,
        schema,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease | None = None,
        relation_schema=None,
        after_copy=None,
    ):
        assert snapshot_lease is not None
        assert snapshot_lease.lifecycle is lifecycle
        assert lifecycle.require_in_progress().complete is False
        events.append(f"export:{len(exported_queries) + 1}")
        return export_full(
            query,
            schema,
            load_config,
            snapshot_lease=snapshot_lease,
            relation_schema=relation_schema,
            after_copy=after_copy,
        )

    def observed_key_export(
        query,
        load_config,
        *,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
    ):
        assert snapshot_lease.lifecycle is lifecycle
        assert lifecycle.require_in_progress().complete is False
        events.append(f"export:{len(exported_queries) + 1}")
        return export_keys(
            query,
            load_config,
            snapshot_lease=snapshot_lease,
        )

    strategy._export_to_file_whole = observed_full_export  # type: ignore[method-assign]
    strategy._export_key_snapshot_file = observed_key_export  # type: ignore[method-assign]

    def observed_envelope(**kwargs):
        assert kwargs["delta_artifact"].receipt.complete is True
        assert kwargs["key_artifact"].receipt.complete is True
        events.append("envelope-evidence")
        return IncrementalSnapshotEnvelope(**kwargs)

    monkeypatch.setattr(snapshot_extract_module, "IncrementalSnapshotEnvelope", observed_envelope)
    commit = connector.commit_transaction

    def observed_commit() -> None:
        events.append("commit")
        commit()

    connector.commit_transaction = observed_commit  # type: ignore[method-assign]

    config = _config(tmp_path)
    result = strategy.extract(config, XMinState(90, datetime.now(UTC)))

    assert events == ["export:1", "export:2", "envelope-evidence", "complete", "commit"]
    assert result.extraction_lifecycle is lifecycle
    assert lifecycle.require_completed().complete is True
    assert connector.operations.count("begin") == 1
    assert connector.operations.count("commit") == 1
    assert connector.operations.count("rollback") == 0


def test_xmin_second_export_failure_rolls_back_and_removes_both_files(tmp_path: Path) -> None:
    strategy, connector, _exported_queries = _strategy(tmp_path, target_exists=True)
    delattr(strategy, "_export_to_file_whole")
    delattr(strategy, "_export_key_snapshot_file")
    strategy._render_query = lambda _connector, query: str(query)  # type: ignore[method-assign]
    config = _config(tmp_path)
    exported_paths: list[Path] = []
    primary = RuntimeError("second snapshot export failed; credential=primary-secret")

    def copy_to_file(**kwargs):
        path = Path(str(kwargs["output_path"]))
        exported_paths.append(path)
        if len(exported_paths) == 1:
            path.write_text("00000000-0000-0000-0000-000000000001\t7\t101\n", encoding="utf-8")
            return {"total_bytes": path.stat().st_size, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}
        path.write_bytes(b"partial-key")
        raise primary

    connector.copy_to_file = copy_to_file  # type: ignore[attr-defined]
    lifecycles: list[ExtractionLifecycleAuthority] = []

    def new_lifecycle() -> ExtractionLifecycleAuthority:
        lifecycle = ExtractionLifecycleAuthority()
        lifecycles.append(lifecycle)
        return lifecycle

    strategy._new_extraction_lifecycle = new_lifecycle  # type: ignore[method-assign]

    def failed_rollback() -> None:
        connector.operations.append("rollback")
        connector.in_transaction = False
        raise OSError("credential=rollback-secret")

    connector.rollback = failed_rollback  # type: ignore[method-assign]

    with pytest.raises(RuntimeError) as raised:
        strategy.extract(config, XMinState(90, datetime.now(UTC)))

    assert raised.value is primary
    notes = getattr(primary, "__notes__", [])
    assert notes == ["postgres_snapshot.rollback_failed:builtins.OSError"]
    assert all("primary-secret" not in note and "rollback-secret" not in note for note in notes)
    assert len(lifecycles) == 1
    assert lifecycles[0].require_in_progress().complete is False
    assert connector.operations.count("begin") == 1
    assert connector.operations.count("commit") == 0
    assert connector.operations.count("rollback") == 1
    assert len(exported_paths) == 2
    assert all(not path.exists() for path in exported_paths)
    assert list(tmp_path.iterdir()) == []


def test_xmin_real_incremental_export_projects_full_contract_to_exact_key_schema(tmp_path: Path) -> None:
    strategy, connector, _exported_queries = _strategy(tmp_path, target_exists=True)
    delattr(strategy, "_export_to_file_whole")
    delattr(strategy, "_export_key_snapshot_file")
    strategy._render_query = lambda _connector, query: str(query)  # type: ignore[method-assign]
    exported_paths: list[Path] = []

    def copy_to_file(**kwargs):
        path = Path(str(kwargs["output_path"]))
        exported_paths.append(path)
        row = (
            "00000000-0000-0000-0000-000000000001\t7\t101\n"
            if len(exported_paths) == 1
            else "00000000-0000-0000-0000-000000000001\n"
        )
        path.write_text(row, encoding="utf-8")
        return {
            "total_bytes": path.stat().st_size,
            "chunk_count": 1,
            "elapsed": 0.1,
            "throughput": 1.0,
        }

    connector.copy_to_file = copy_to_file  # type: ignore[attr-defined]
    config = _config(tmp_path)
    result = strategy.extract(config, XMinState(90, datetime.now(UTC)))

    assert result.snapshot_envelope is not None
    assert result.snapshot_envelope.delta_receipt.complete is True
    assert result.snapshot_envelope.key_receipt.complete is True
    assert tuple(name for name, _dtype in result.snapshot_envelope.delta_schema) == (
        "guid",
        "metric_value",
        "__dpone__xmin",
        DELTA_HASH_COLUMN,
    )
    assert tuple(name for name, _dtype in result.snapshot_envelope.key_schema) == ("guid", KEY_HASH_COLUMN)
    assert tuple(config.options["schema_contract"]["columns"]) == ("guid", "metric_value")
    assert connector.operations.count("begin") == 1
    assert connector.operations.count("commit") == 1
    assert connector.operations.count("rollback") == 0
    assert all(not path.exists() for path in exported_paths)

    result.snapshot_envelope.cleanup()
    assert list(tmp_path.iterdir()) == []


def test_existing_checkpoint_full_repair_flows_from_authority_preview_to_finalizer(
    tmp_path: Path,
) -> None:
    strategy, _connector, exported_queries = _strategy(tmp_path, target_exists=True, target_has_rows=True)
    previous = XMinState(90, datetime.now(UTC), revision=7)
    config = _config(tmp_path)
    config.repair_authority_ref = "repair-work-item-001"
    preview_calls: list[dict[str, object]] = []

    class RepairState:
        def preview_repair_authority(self, **kwargs):
            preview_calls.append(kwargs)
            return SimpleNamespace(
                authority_id="repair-work-item-001",
                authority_digest="sha256:" + "a" * 64,
                allow=SimpleNamespace(full_baseline=True),
            )

    strategy.state_storage = RepairState()
    result = strategy.extract(config, previous)
    envelope = result.snapshot_envelope
    assert envelope is not None
    assert envelope.baseline is True
    assert envelope.previous_checkpoint == previous
    assert len(exported_queries) == 1
    assert preview_calls[0]["expected_checkpoint"] == previous
    assert preview_calls[0]["scope_hash"] == envelope.scope_hash

    class FinalizerState:
        def __init__(self) -> None:
            self.admitted: list[dict[str, object]] = []
            self.consumed: list[dict[str, object]] = []

        def admit_repair_authority(self, **kwargs):
            self.admitted.append(kwargs)
            return SimpleNamespace(
                authority_id="repair-work-item-001",
                authority_digest="sha256:" + "a" * 64,
            )

        def consume_repair_authority(self, **kwargs):
            self.consumed.append(kwargs)

    state = FinalizerState()
    admission = admit_snapshot_repair(
        state=state,
        executor=object(),
        load_config=config,
        envelope=envelope,
        policy=KeySnapshotReconciliationPolicy(enabled=True, mode="key_snapshot"),
        target_rows_before=10,
        active_rows_before=10,
        missing_rows=0,
    )
    consume_snapshot_repair(
        state=state,
        executor=object(),
        admission=admission,
        envelope=envelope,
        load_id="load-1",
        receipt_id="receipt-1",
    )

    assert admission.used_full_baseline is True
    assert state.admitted[0]["expected_checkpoint"] == previous
    assert state.consumed[0]["receipt_id"] == "receipt-1"


@pytest.mark.parametrize("unsafe_window", ["wraparound", "frozen"])
def test_authorized_full_repair_replaces_unsafe_xmin_window_and_consumes_receipt(
    tmp_path: Path,
    unsafe_window: str,
) -> None:
    strategy, connector, exported_queries = _strategy(
        tmp_path,
        target_exists=True,
        target_has_rows=True,
    )
    previous = XMinState(90, datetime.now(UTC), revision=7)
    config = _config(tmp_path)
    config.repair_authority_ref = "repair-work-item-unsafe-window"
    preview_calls: list[dict[str, object]] = []

    class RepairPreviewState:
        @staticmethod
        def preview_repair_authority(**kwargs):
            preview_calls.append(kwargs)
            return SimpleNamespace(allow=SimpleNamespace(full_baseline=True))

    strategy.state_storage = RepairPreviewState()
    safety_inputs: list[XMinState | None] = []
    if unsafe_window == "wraparound":

        def calculate_safe_xmin(upper: int, candidate_previous: XMinState | None) -> XMinState:
            safety_inputs.append(candidate_previous)
            return XMinState(
                upper,
                datetime.now(UTC),
                is_initial=candidate_previous is None,
                wraparound_detected=candidate_previous is not None,
            )

        strategy.xmin_manager.calculate_safe_xmin = calculate_safe_xmin  # type: ignore[method-assign]
    else:
        original_safety = strategy.xmin_manager.calculate_safe_xmin

        def calculate_safe_xmin(upper: int, candidate_previous: XMinState | None) -> XMinState:
            safety_inputs.append(candidate_previous)
            return original_safety(upper, candidate_previous)

        strategy.xmin_manager.calculate_safe_xmin = calculate_safe_xmin  # type: ignore[method-assign]
        original_records = connector.get_records

        def records(query, params=None, as_dict=False):
            if "relfrozenxid" in str(query):
                return [{"checkpoint_frozen": True, "relation_frozen_xid": 95, "database_frozen_xid": 50}]
            return original_records(query, params, as_dict)

        connector.get_records = records

    result = strategy.extract(config, previous)
    envelope = result.snapshot_envelope
    assert envelope is not None
    assert envelope.baseline is True
    assert envelope.previous_checkpoint == previous
    assert envelope.candidate_checkpoint.xmin_value == 100
    assert envelope.candidate_checkpoint.is_initial is False
    assert envelope.candidate_checkpoint.wraparound_detected is False
    assert envelope.safe_checkpoint == envelope.candidate_checkpoint
    assert envelope.visible_horizon == 102
    assert envelope.replay_amplification_xids == 2
    assert len(exported_queries) == 1
    assert not any("relfrozenxid" in operation for operation in connector.operations)
    assert safety_inputs == [None]
    assert preview_calls[0]["expected_checkpoint"] == previous
    assert preview_calls[0]["key"] == envelope.state_key
    assert preview_calls[0]["scope_hash"] == envelope.scope_hash

    class FinalizerState:
        def __init__(self) -> None:
            self.consumed: list[dict[str, object]] = []

        @staticmethod
        def admit_repair_authority(**_kwargs):
            return SimpleNamespace(
                authority_id="repair-work-item-unsafe-window",
                authority_digest="sha256:" + "a" * 64,
            )

        def consume_repair_authority(self, **kwargs):
            self.consumed.append(kwargs)

    state = FinalizerState()
    admission = admit_snapshot_repair(
        state=state,
        executor=object(),
        load_config=config,
        envelope=envelope,
        policy=KeySnapshotReconciliationPolicy(enabled=True, mode="key_snapshot"),
        target_rows_before=10,
        active_rows_before=10,
        missing_rows=0,
    )
    consume_snapshot_repair(
        state=state,
        executor=object(),
        admission=admission,
        envelope=envelope,
        load_id="load-unsafe-window",
        receipt_id="receipt-unsafe-window",
    )
    assert admission.used_full_baseline is True
    assert state.consumed[0]["key"] == envelope.state_key
    assert state.consumed[0]["load_id"] == "load-unsafe-window"
    assert state.consumed[0]["receipt_id"] == "receipt-unsafe-window"


@pytest.mark.parametrize("unsafe_window", ["wraparound", "frozen"])
def test_missing_target_without_repair_authority_does_not_bypass_unsafe_xmin_window(
    tmp_path: Path,
    unsafe_window: str,
) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=False)
    previous = XMinState(90, datetime.now(UTC), revision=7)
    if unsafe_window == "wraparound":
        strategy.xmin_manager.calculate_safe_xmin = lambda upper, _previous: XMinState(  # type: ignore[method-assign]
            upper,
            datetime.now(UTC),
            wraparound_detected=True,
        )
        expected = "wraparound_full_baseline_required"
    else:
        original_records = connector.get_records

        def records(query, params=None, as_dict=False):
            if "relfrozenxid" in str(query):
                return [{"checkpoint_frozen": True, "relation_frozen_xid": 95, "database_frozen_xid": 50}]
            return original_records(query, params, as_dict)

        connector.get_records = records
        expected = "frozen_checkpoint_full_baseline_required"

    with pytest.raises(ValueError, match=expected):
        strategy.extract(_config(tmp_path), previous)

    assert exported_queries == []
    assert "rollback" in connector.operations


def test_delete_only_authority_does_not_bypass_frozen_checkpoint_guard(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True, target_has_rows=True)
    previous = XMinState(90, datetime.now(UTC), revision=7)
    config = _config(tmp_path)
    config.repair_authority_ref = "repair-work-item-delete-only"

    class DeleteOnlyPreviewState:
        @staticmethod
        def preview_repair_authority(**_kwargs):
            return SimpleNamespace(allow=SimpleNamespace(full_baseline=False))

    strategy.state_storage = DeleteOnlyPreviewState()
    original_records = connector.get_records

    def records(query, params=None, as_dict=False):
        if "relfrozenxid" in str(query):
            return [{"checkpoint_frozen": True, "relation_frozen_xid": 95, "database_frozen_xid": 50}]
        return original_records(query, params, as_dict)

    connector.get_records = records
    with pytest.raises(ValueError, match="frozen_checkpoint_full_baseline_required"):
        strategy.extract(config, previous)

    assert exported_queries == []
    assert "rollback" in connector.operations


def test_xmin_uses_visible_horizon_for_delta_but_persists_safe_lower_boundary(tmp_path: Path) -> None:
    strategy, _connector, exported_queries = _strategy(tmp_path, target_exists=True)
    previous = XMinState(90, datetime.now(UTC))

    result = strategy.extract(_config(tmp_path), previous)

    assert ">= 90" in exported_queries[0]
    assert "< 102" in exported_queries[0]
    assert result.state.xmin_value == 100
    assert result.snapshot_envelope.candidate_checkpoint.xmin_value == 100


def test_cluster_wide_horizon_gap_is_evidence_without_an_implicit_blocker(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    previous = XMinState(90, datetime.now(UTC))
    original = connector.get_records

    def records(query, params=None, as_dict=False):
        if "txid_current_snapshot" in str(query):
            return [{"snapshot_token": "100:100000100:", "extraction_horizon": 100_000_100}]
        return original(query, params, as_dict)

    connector.get_records = records
    result = strategy.extract(_config(tmp_path), previous)

    assert result.snapshot_envelope.replay_amplification_xids == 100_000_000
    assert "< 100000100" in exported_queries[0]


def test_explicit_replay_amplification_limit_fails_before_export(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    config = _config(tmp_path)
    config.options["xmin_max_replay_amplification_xids"] = 1

    with pytest.raises(ValueError, match="replay_amplification_limit_exceeded"):
        strategy.extract(config, XMinState(90, datetime.now(UTC)))

    assert exported_queries == []
    assert "rollback" in connector.operations


def test_existing_target_without_collision_safe_state_requires_explicit_baseline(tmp_path: Path) -> None:
    strategy, _connector, _exported_queries = _strategy(
        tmp_path,
        target_exists=True,
        target_has_rows=True,
    )

    with pytest.raises(ValueError, match="state_missing_for_existing_target"):
        strategy.extract(_config(tmp_path), None)


def test_precreated_empty_target_allows_first_baseline(tmp_path: Path) -> None:
    strategy, _connector, exported_queries = _strategy(
        tmp_path,
        target_exists=True,
        target_has_rows=False,
    )

    result = strategy.extract(_config(tmp_path), None)

    assert result.snapshot_envelope is not None
    assert result.snapshot_envelope.baseline is True
    assert len(exported_queries) == 1


def test_key_receipt_preserves_uuid_and_bulk_encoded_special_text(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    uuid_value = "00000000-0000-0000-0000-000000000001"
    text_value = codec.encode("metric\twith\ncontrols\x1d")
    path = tmp_path / "keys.bcp"
    path.write_text(f"{uuid_value}\t{text_value}\n", encoding="utf-8")
    raw = FileExportArtifact(
        str(path),
        ["guid", "metric_code"],
        format="mssql-delimited",
        compressed=False,
        bulk_text_codec=codec,
    )

    artifact = PostgresKeySnapshotFileArtifact(
        raw,
        snapshot_token=snapshot_token_digest("100:100:"),
        scope_hash="sha256:scope",
        key_columns=("guid", "metric_code"),
    )

    assert tuple(artifact.columns) == ("guid", "metric_code", KEY_HASH_COLUMN)
    assert artifact.receipt.row_count == 1
    hashed_row = Path(artifact.file_path).read_text(encoding="utf-8").rstrip("\n").split("\t")
    assert hashed_row[:2] == [uuid_value, text_value]
    assert len(hashed_row[-1]) == 64
    artifact.cleanup()


def test_text_key_profile_preserves_binary_case_and_accent_identity(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    path = tmp_path / "text-keys.bcp"
    path.write_text("A\na\né\ne\n", encoding="utf-8")

    artifact = PostgresKeySnapshotFileArtifact(
        FileExportArtifact(
            str(path),
            ["metric_code"],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=codec,
        ),
        snapshot_token=snapshot_token_digest("100:102:"),
        scope_hash="sha256:scope",
        key_columns=("metric_code",),
        text_key_columns=("metric_code",),
    )

    assert artifact.receipt.row_count == 4
    artifact.cleanup()


@pytest.mark.parametrize("payload", ["metric-a\nmetric-a\n", "metric-a\nmetric-a \n"])
def test_key_artifact_defers_relational_key_validation_to_mssql_staging(
    tmp_path: Path,
    payload: str,
) -> None:
    path = tmp_path / "invalid-text-keys.bcp"
    path.write_text(payload, encoding="utf-8")

    artifact = PostgresKeySnapshotFileArtifact(
        FileExportArtifact(
            str(path),
            ["metric_code"],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=BulkTextCodec(),
        ),
        snapshot_token=snapshot_token_digest("100:102:"),
        scope_hash="sha256:scope",
        key_columns=("metric_code",),
        text_key_columns=("metric_code",),
    )

    assert artifact.receipt.row_count == 2
    assert list(tmp_path.glob("*.sqlite")) == []
    artifact.cleanup()


def test_key_receipt_rehashes_file_immediately_before_bcp(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    path = tmp_path / "keys.bcp"
    path.write_text("metric-a\n", encoding="utf-8")
    artifact = PostgresKeySnapshotFileArtifact(
        FileExportArtifact(
            str(path),
            ["metric_code"],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=codec,
        ),
        snapshot_token=snapshot_token_digest("100:100:"),
        scope_hash="sha256:scope",
        key_columns=("metric_code",),
    )
    Path(artifact.file_path).write_text("metric-b\t" + "0" * 64 + "\n", encoding="utf-8")

    with pytest.raises(ArtifactIntegrityError):
        artifact.materialize(object(), object(), (("metric_code", "text"), (KEY_HASH_COLUMN, "varchar(64)")))
    artifact.cleanup()


def test_delta_receipt_detects_file_tamper_and_staged_count_mismatch(tmp_path: Path) -> None:
    codec = BulkTextCodec()
    path = tmp_path / "delta.bcp"
    encoded = codec.encode("metric\twith\ncontrols\x1d")
    path.write_text(f"{encoded}\t1.23456789012345\n", encoding="utf-8")
    artifact = PostgresDeltaSnapshotFileArtifact(
        FileExportArtifact(
            str(path),
            ["metric_code", "metric_value"],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=codec,
        ),
        snapshot_token=snapshot_token_digest("100:102:"),
        scope_hash="sha256:scope",
        columns=("metric_code", "metric_value"),
    )

    assert artifact.receipt.complete is True
    assert artifact.receipt.columns == ("metric_code", "metric_value", DELTA_HASH_COLUMN)
    assert artifact.receipt.row_count == 1
    Path(artifact.file_path).write_text(f"{codec.encode('changed')}\t1\t{'0' * 64}\n", encoding="utf-8")
    with pytest.raises(ArtifactIntegrityError):
        artifact.materialize(object(), object(), ())
    artifact.cleanup()

    path = tmp_path / "delta_count.bcp"
    path.write_text(f"{codec.encode('metric')}\t1\n", encoding="utf-8")
    artifact = PostgresDeltaSnapshotFileArtifact(
        FileExportArtifact(
            str(path),
            ["metric_code", "metric_value"],
            format="mssql-delimited",
            compressed=False,
            bulk_text_codec=codec,
        ),
        snapshot_token=snapshot_token_digest("100:102:"),
        scope_hash="sha256:scope",
        columns=("metric_code", "metric_value"),
    )
    staged = SimpleNamespace(row_count=0, cleanup=lambda: None)
    manager = SimpleNamespace(create=lambda *_args: staged, load_from_file=lambda *_args: 0)
    with pytest.raises(ValueError, match="staged row count"):
        artifact.materialize(manager, object(), ())
    artifact.cleanup()


def test_relation_freeze_horizon_blocks_stale_checkpoint_before_export(tmp_path: Path) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    previous = XMinState(90, datetime.now(UTC))
    original = connector.get_records

    def records(query, params=None, as_dict=False):
        if "relfrozenxid" in str(query):
            return [{"checkpoint_frozen": True, "relation_frozen_xid": 95, "database_frozen_xid": 50}]
        return original(query, params, as_dict)

    connector.get_records = records
    with pytest.raises(ValueError, match="frozen_checkpoint_full_baseline_required"):
        strategy.extract(_config(tmp_path), previous)

    assert exported_queries == []
    assert "rollback" in connector.operations


def test_state_identity_includes_target_physical_projection(tmp_path: Path) -> None:
    strategy, _connector, _queries = _strategy(tmp_path, target_exists=True)
    config = _config(tmp_path)
    config.unique_key = ["metric_code"]
    schema = [("metric_code", "text")]
    config.options["schema_contract"] = {"columns": {"metric_code": {"nullable": False}}}
    config.options["physical_design"] = {"columns": {"metric_code": {"target_type": {"mssql": "nvarchar(450)"}}}}
    first = strategy._snapshot_extractor.state_key(config, schema)
    config.options["physical_design"]["columns"]["metric_code"]["target_type"]["mssql"] = "nvarchar(400)"
    second = strategy._snapshot_extractor.state_key(config, schema)

    assert first.digest != second.digest


def test_do_metric_code_exactly_fits_clustered_key_gate(tmp_path: Path) -> None:
    _strategy_instance, _connector, _queries = _strategy(tmp_path, target_exists=True)
    config = _config(tmp_path)
    config.unique_key = ["metric_code"]
    config.options["physical_design"] = {"columns": {"metric_code": {"target_type": {"mssql": "nvarchar(450)"}}}}

    assert require_clustered_key_width({"metric_code": "nvarchar(450)"}) == 900


def test_composite_key_over_900_bytes_fails_before_export(tmp_path: Path) -> None:
    strategy, _connector, exported_queries = _strategy(tmp_path, target_exists=True)
    config = _config(tmp_path)
    config.unique_key = ["left_code", "right_code"]
    config.options["schema_contract"] = {
        "columns": {
            "left_code": {"nullable": False},
            "right_code": {"nullable": False},
        }
    }
    config.options["physical_design"] = {
        "columns": {
            "left_code": {"target_type": {"mssql": "nvarchar(300)"}},
            "right_code": {"target_type": {"mssql": "nvarchar(300)"}},
        }
    }
    strategy.fetch_schema_projection = lambda _cfg: _projection(  # type: ignore[method-assign]
        (("left_code", "text", "nvarchar(max)"), ("right_code", "text", "nvarchar(max)"))
    )

    with pytest.raises(SnapshotReconciliationError, match="exceeds_900_bytes"):
        strategy.extract(config, XMinState(90, datetime.now(UTC)))

    assert exported_queries == []


@pytest.mark.parametrize(
    "unsafe_state",
    [
        XMinState(100, datetime.now(UTC), wraparound_detected=True),
        XMinState(100, datetime.now(UTC), is_initial=True),
    ],
)
def test_xmin_ambiguity_blocks_without_export_or_checkpoint(
    tmp_path: Path,
    unsafe_state: XMinState,
) -> None:
    strategy, connector, exported_queries = _strategy(tmp_path, target_exists=True)
    previous = XMinState(90, datetime.now(UTC))
    strategy.xmin_manager.calculate_safe_xmin = lambda _upper, _previous: unsafe_state  # type: ignore[method-assign]

    with pytest.raises(ValueError, match="wraparound_full_baseline_required"):
        strategy.extract(_config(tmp_path), previous)

    assert exported_queries == []
    assert "rollback" in connector.operations


@pytest.mark.parametrize(
    ("export_format", "unsafe", "expected"),
    [
        ("binary", False, "export_format=csv"),
        ("csv", True, "forbids unsafe"),
    ],
)
def test_key_snapshot_rejects_unsafe_public_wire_options(
    tmp_path: Path,
    export_format: str,
    unsafe: bool,
    expected: str,
) -> None:
    strategy, _connector, _queries = _strategy(tmp_path, target_exists=False)
    config = _config(tmp_path)
    config.export_format = export_format
    config.options["allow_unsafe_raw_mssql_bulk_files"] = unsafe

    with pytest.raises(ValueError, match=expected):
        strategy.extract(config, None)
