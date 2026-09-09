from __future__ import annotations

from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.dag.config import LoadConfigBuilder
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.artifacts import FileExportArtifact, InternalQueryArtifact
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.internal_query_capability import (
    INTERNAL_QUERY_CROSS_DIALECT,
    InternalQueryCapabilityDecision,
    InternalQueryCapabilityIssuer,
)
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_extract import PostgresXMinExtractStrategy
from dpone.runtime.state import XMinState


class _RenderedQuery:
    def as_string(self, connection: object) -> str:
        del connection
        return "SELECT id FROM public.orders"


class _PostgresConnector:
    connection = object()

    def begin(self) -> None:
        pass

    def execute_query(self, query: object) -> None:
        del query

    def get_records(self, query: object, params=None, as_dict: bool = False):
        del params
        if "txid_current_snapshot" in str(query):
            row = {"snapshot_token": "40:43:", "extraction_horizon": 43}
            return [row] if as_dict else [("40:43:", 43)]
        return []

    def rollback(self) -> None:
        pass

    def commit_transaction(self) -> None:
        pass


class _StateStorage:
    def load_state(self, source_schema: str, source_table: str) -> None:
        del source_schema, source_table
        return None


class _Logger:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, object]]] = []

    def log_etl_progress(self, event: str, payload: dict[str, object]) -> None:
        self.events.append((event, payload))

    def log_xmin_state_info(self, message: str, payload: dict[str, object]) -> None:
        del message, payload


def _load_config(
    *,
    source_identity: dict[str, object] | None = None,
    sink_identity: dict[str, object] | None = None,
    sink_type: str = "postgres",
) -> LoadConfig:
    return LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                **(source_identity or {}),
                "table": {"schema": "public", "name": "orders"},
            },
            "sink": {
                "type": sink_type,
                **(sink_identity or {}),
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {"mode": "full_refresh"},
            },
        }
    )


def _prepare_strategy(strategy: Any, monkeypatch: pytest.MonkeyPatch) -> FileExportArtifact:
    exported = FileExportArtifact("/tmp/dpone-ss69-export.csv", ["id"])
    projection = SimpleNamespace(
        projected_schema=(("id", "integer"),),
        relation_schema=(("id", "integer"),),
        relation_metadata=(),
        target_projection=None,
    )
    monkeypatch.setattr(strategy, "fetch_schema_projection", lambda _load_config: projection)
    monkeypatch.setattr(strategy, "format_select_query", lambda *_args, **_kwargs: _RenderedQuery())

    def export(*_args, snapshot_lease=None, **_kwargs):
        if snapshot_lease is not None:
            snapshot_lease.require_for(strategy.connector)
            exported.bind_extraction_lifecycle(snapshot_lease.lifecycle)
        return exported

    monkeypatch.setattr(strategy, "_export_to_file", export)
    return exported


def _extract_full(
    load_config: LoadConfig,
    monkeypatch: pytest.MonkeyPatch,
    grant_internal_query: bool = False,
) -> tuple[object, FileExportArtifact]:
    connector = _PostgresConnector()
    capability = _runtime_capability(load_config, connector) if grant_internal_query else None
    strategy = PostgresFullExtractStrategy(
        connector,
        _Logger(),
        internal_query_capability=capability,
    )
    exported = _prepare_strategy(strategy, monkeypatch)

    result = strategy.extract(load_config, None)

    return result.artifact, exported


def _extract_xmin(
    load_config: LoadConfig,
    monkeypatch: pytest.MonkeyPatch,
    grant_internal_query: bool = False,
) -> tuple[object, FileExportArtifact]:
    connector = _PostgresConnector()
    capability = _runtime_capability(load_config, connector) if grant_internal_query else None
    strategy = PostgresXMinExtractStrategy(
        connector,
        _StateStorage(),  # type: ignore[arg-type]
        _Logger(),
        sink_connector=connector,
        internal_query_capability=capability,
    )
    exported = _prepare_strategy(strategy, monkeypatch)
    safe_state = XMinState(
        xmin_value=42,
        timestamp=datetime.now(tz=UTC),
        is_initial=True,
    )
    monkeypatch.setattr(strategy, "_target_exists", lambda _load_config: False)
    monkeypatch.setattr(strategy.xmin_manager, "get_snapshot_xmin_anchor", lambda: 42)
    monkeypatch.setattr(strategy.xmin_manager, "calculate_safe_xmin", lambda *_args: safe_state)

    result = strategy.extract(load_config, None)

    return result.artifact, exported


_EXTRACTORS = (_extract_full, _extract_xmin)


def _runtime_capability(load_config: LoadConfig, connector: _PostgresConnector) -> InternalQueryCapabilityDecision:
    binding = ResolvedBindingConnection(
        credentials=CredentialsConfig(database="warehouse"),
        safe_metadata={"connection_ref": "postgres-shared"},
        descriptor=ResolvedConnectionDescriptor(connection_type="postgres", properties={}),
    )
    target_connector = SimpleNamespace(get_records=lambda _query: [])
    return InternalQueryCapabilityIssuer().issue(
        resolved_connections=SimpleNamespace(strict=True, source=binding, sink=binding),
        source_config={"type": "postgres"},
        sink_config={"type": "postgres"},
        load_config=load_config,
        source_connector=connector,
        target_connector=target_connector,
    )


def test_load_config_uses_canonical_connection_refs_as_logical_identity() -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-source"},
        sink_identity={"connection_ref": "postgres-target"},
    )

    assert load_config.source_conn_id == "postgres-source"
    assert load_config.target_conn_id == "postgres-target"


def test_load_config_preserves_explicit_legacy_connection_ids() -> None:
    load_config = _load_config(
        source_identity={"connection_id": "legacy-postgres-source"},
        sink_identity={"connection_id": "legacy-postgres-target"},
    )

    assert load_config.source_conn_id == "legacy-postgres-source"
    assert load_config.target_conn_id == "legacy-postgres-target"


def test_load_config_does_not_copy_physical_connection_data() -> None:
    physical_values = (
        "postgresql://user:password@source.internal/orders",
        "registry-source-body",
        "source-password",
        "https://sink.internal",
        "registry-sink-body",
        "sink-token",
    )
    load_config = _load_config(
        source_identity={
            "connection_ref": "postgres-source",
            "endpoint": physical_values[0],
            "registry": {"body": physical_values[1]},
            "credentials": {"password": physical_values[2]},
        },
        sink_identity={
            "connection_ref": "postgres-target",
            "endpoint": physical_values[3],
            "registry": {"body": physical_values[4]},
            "credentials": {"token": physical_values[5]},
        },
    )

    serialized_load_config = repr(asdict(load_config))

    assert load_config.source_conn_id == "postgres-source"
    assert load_config.target_conn_id == "postgres-target"
    assert all(value not in serialized_load_config for value in physical_values)


@pytest.mark.parametrize("extract", _EXTRACTORS, ids=("full", "xmin"))
def test_different_canonical_refs_use_exported_cross_database_path(
    extract: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-source"},
        sink_identity={"connection_ref": "postgres-target"},
    )

    artifact, exported = extract(load_config, monkeypatch)

    assert artifact is exported


@pytest.mark.parametrize("extract", _EXTRACTORS, ids=("full", "xmin"))
def test_identical_canonical_ref_without_runtime_capability_uses_exported_path(
    extract: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-shared"},
        sink_identity={"connection_ref": "postgres-shared"},
    )

    artifact, exported = extract(load_config, monkeypatch)

    assert artifact is exported


def test_runtime_issued_same_postgres_capability_retains_full_internal_query_fast_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-shared"},
        sink_identity={"connection_ref": "postgres-shared"},
    )

    artifact, exported = _extract_full(load_config, monkeypatch, True)

    assert artifact is not exported
    assert isinstance(artifact, InternalQueryArtifact)


def test_xmin_runtime_capability_still_uses_repeatable_read_file_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-shared"},
        sink_identity={"connection_ref": "postgres-shared"},
    )

    artifact, exported = _extract_xmin(load_config, monkeypatch, True)

    assert artifact is exported
    assert isinstance(artifact, FileExportArtifact)
    assert artifact.extraction_lifecycle is not None
    assert artifact.extraction_lifecycle.require_completed().complete is True


def test_xmin_insert_between_extract_and_materialize_uses_captured_rr_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "postgres-shared"},
        sink_identity={"connection_ref": "postgres-shared"},
    )
    connector = _PostgresConnector()
    strategy = PostgresXMinExtractStrategy(
        connector,
        _StateStorage(),  # type: ignore[arg-type]
        _Logger(),
        sink_connector=connector,
        internal_query_capability=_runtime_capability(load_config, connector),
    )
    source_rows = [1]
    captured_leases: list[PostgresRepeatableReadSnapshotLease] = []
    projection = SimpleNamespace(
        projected_schema=(("id", "integer"),),
        relation_schema=(("id", "integer"),),
        relation_metadata=(),
        target_projection=None,
    )
    path = tmp_path / "xmin-rr.csv"

    def export(*_args, snapshot_lease=None, **_kwargs):
        assert snapshot_lease is not None
        snapshot_lease.require_for(connector)
        captured_leases.append(snapshot_lease)
        path.write_text("".join(f"{value}\n" for value in source_rows), encoding="utf-8")
        artifact = FileExportArtifact(str(path), ["id"], rows_exported=len(source_rows))
        artifact.bind_extraction_lifecycle(snapshot_lease.lifecycle)
        return artifact

    monkeypatch.setattr(strategy, "fetch_schema_projection", lambda _config: projection)
    monkeypatch.setattr(strategy, "format_select_query", lambda *_args, **_kwargs: _RenderedQuery())
    monkeypatch.setattr(strategy, "_export_to_file", export)
    monkeypatch.setattr(strategy, "_target_exists", lambda _config: False)
    safe_state = XMinState(42, datetime.now(tz=UTC), is_initial=True)
    monkeypatch.setattr(strategy.xmin_manager, "get_snapshot_xmin_anchor", lambda: 42)
    monkeypatch.setattr(strategy.xmin_manager, "calculate_safe_xmin", lambda *_args: safe_state)

    result = strategy.extract(load_config, None)
    source_rows.append(2)

    class _Staging:
        def __init__(self) -> None:
            self.rows: list[int] = []
            self.dropped = False

        def create(self, _config, _schema) -> StagingTableArtifact:
            return StagingTableArtifact("stage", "xmin", ("id",), self)  # type: ignore[arg-type]

        def load_from_file(self, _handle, artifact: FileExportArtifact) -> int:
            self.rows.extend(int(value) for value in Path(artifact.file_path).read_text().splitlines())
            return len(self.rows)

        def drop(self, _handle) -> None:
            self.dropped = True

    staging = _Staging()
    handle = result.artifact.materialize(staging, load_config, result.schema)

    assert isinstance(result.artifact, FileExportArtifact)
    assert len(captured_leases) == 1
    assert result.artifact.extraction_lifecycle is captured_leases[0].lifecycle
    assert result.artifact.extraction_lifecycle.require_completed().snapshot_authority == "postgresql.repeatable_read"
    assert source_rows == [1, 2]
    assert staging.rows == [1]
    assert handle.row_count == 1
    handle.cleanup()
    result.artifact.cleanup()


@pytest.mark.parametrize("extract", _EXTRACTORS, ids=("full", "xmin"))
def test_identical_legacy_id_never_grants_internal_query_fast_path(
    extract: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_id": "legacy-postgres-shared"},
        sink_identity={"connection_id": "legacy-postgres-shared"},
    )

    artifact, exported = extract(load_config, monkeypatch)

    assert artifact is exported


def test_equal_cross_dialect_authoring_alias_is_only_metadata_and_uses_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config(
        source_identity={"connection_ref": "shared-name"},
        sink_identity={"connection_ref": "shared-name"},
        sink_type="mssql",
    )
    logger = _Logger()
    connector = _PostgresConnector()
    strategy = PostgresFullExtractStrategy(connector, logger)

    class _SourceAuthority:
        def verify_snapshot(self, *, connector, snapshot_lease, load_config):
            del load_config
            assert connector is strategy.connector
            snapshot_lease.require_for(connector)
            return SimpleNamespace(relation_oid=1)

    strategy.bind_postgres_source_authority(_SourceAuthority())
    exported = _prepare_strategy(strategy, monkeypatch)

    result = strategy.extract(load_config, None)

    assert load_config.source_conn_id == load_config.target_conn_id == "shared-name"
    assert result.artifact is exported
    decision = next(payload for event, payload in logger.events if event == "INTERNAL_QUERY_CAPABILITY_DECISION")
    assert decision["Code"] == INTERNAL_QUERY_CROSS_DIALECT


@pytest.mark.parametrize("extract", _EXTRACTORS, ids=("full", "xmin"))
def test_empty_connection_identities_never_authorize_internal_query(
    extract: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    load_config = _load_config()

    assert load_config.source_conn_id == ""
    assert load_config.target_conn_id == ""

    artifact, exported = extract(load_config, monkeypatch)

    assert artifact is exported
