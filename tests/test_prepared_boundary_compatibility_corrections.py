"""Private regression contracts for deliberate compatibility and cleanup fixes."""

import asyncio
from types import SimpleNamespace

import pytest

from tests.test_postgres_mssql_prepared_source_boundary import _prepared, _sealed_artifact
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules


def test_legacy_constructor_defers_connector_validation():
    from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
        PreparedPostgresSourceBoundary,
    )

    connector = object()
    lease = SimpleNamespace(require_for=lambda candidate: None)
    boundary = PreparedPostgresSourceBoundary(connector, lease, object(), object())
    boundary.require_active(connector)
    with pytest.raises(RuntimeError, match="connector_mismatch"):
        boundary.require_active(object())
    with pytest.raises(Exception) as caught:
        boundary.require_active_for_copy(connector)
    assert caught.value.reason == "exact_type_violation"


@pytest.mark.parametrize(
    "failure_type", [RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit]
)
def test_failed_close_reports_truth_and_never_retries(failure_type):
    boundary, connector, _ = _prepared(_feature_modules())
    primary = failure_type("private test failure")
    connector.rollback_error = primary
    with pytest.raises(BaseException) as caught:
        boundary.close_if_active()
    if failure_type is RuntimeError:
        assert caught.value.reason == "snapshot_cleanup_failed"
    else:
        assert caught.value is primary
    receipt = boundary.terminal_receipt
    assert receipt is not None and not receipt.cleanup_succeeded
    assert receipt.connection_quarantined
    attempts = sum(event[0] == "rollback" for event in connector.events)
    assert attempts == 1
    assert boundary.close_if_active() is None
    assert boundary.terminal_receipt is receipt
    assert sum(event[0] == "rollback" for event in connector.events) == attempts


@pytest.mark.parametrize("artifact_kind", ["missing", "wrong_type", "tampered"])
def test_invalid_artifact_aborts_before_commit(artifact_kind, tmp_path):
    boundary, connector, _ = _prepared(_feature_modules())
    artifact = None if artifact_kind == "missing" else object()
    if artifact_kind == "tampered":
        artifact = _sealed_artifact(tmp_path)
        (tmp_path / "sealed-v9.csv").write_bytes(b"2\n")
    with pytest.raises(Exception):
        boundary.complete(artifact)
    receipt = boundary.terminal_receipt
    assert receipt is not None and receipt.outcome == "aborted"
    assert sum(event[0] == "rollback" for event in connector.events) == 1
    assert not any(event[0] == "commit" for event in connector.events)


@pytest.mark.parametrize("use_service", [True, False])
def test_full_extract_requires_service_terminal_receipt(use_service, monkeypatch, tmp_path):
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
    from dpone.runtime.sources.strategies.postgres.postgres_whole_file_export_service import (
        PostgresWholeFileExportService,
    )
    from tests.test_postgres_mssql_prepared_source_boundary import _CopyConnector, _Logger, _WholeFileStrategy

    modules = _feature_modules()
    connector = _CopyConnector()
    boundary, _, _ = _prepared(modules, connector)
    strategy = PostgresFullExtractStrategy(connector, _Logger())
    service = PostgresWholeFileExportService(_WholeFileStrategy(connector))
    calls = []
    complete = type(boundary).complete

    def record_complete(self, artifact=None):
        calls.append(artifact)
        return complete(self, artifact)

    def export(query, schema, config, **kwargs):
        assert 'FROM ONLY "sales"."orders"' in query.as_string(None)
        assert kwargs["prepared_boundary"] is boundary
        assert kwargs["snapshot_lease"] is None
        if not use_service:
            return _sealed_artifact(tmp_path)
        kwargs.pop("batch_size")
        return service.export_full(query, schema, config, **kwargs)

    monkeypatch.setattr(type(boundary), "complete", record_complete)
    monkeypatch.setattr(strategy, "_export_to_file", export)
    monkeypatch.setattr(strategy, "_internal_query_authorized", lambda config: False)
    config = LoadConfig(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        source_schema="sales",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        options={
            "sink_type": "mssql",
            "runtime_storage": {"work_dir": str(tmp_path)},
            modules["boundary"].MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION: boundary,
        },
    )
    if use_service:
        result = strategy.extract(config, None)
        assert calls == [result.artifact]
        assert boundary.terminal_receipt.outcome == "completed"
        assert boundary.terminal_receipt.cleanup_succeeded
    else:
        with pytest.raises(Exception) as caught:
            strategy.extract(config, None)
        assert caught.value.reason == "snapshot_cleanup_failed"
        assert boundary.terminal_receipt.outcome == "aborted"
        assert calls == []


@pytest.mark.parametrize(
    "failure_type", [RuntimeError, asyncio.CancelledError, KeyboardInterrupt, SystemExit, GeneratorExit]
)
def test_initial_full_extract_validation_aborts_before_copy(failure_type, monkeypatch):
    from dpone.config import LoadConfig
    from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
    from tests.test_postgres_mssql_prepared_source_boundary import _Logger

    modules = _feature_modules()
    boundary, connector, _ = _prepared(modules)
    primary = failure_type("validation failed")

    def fail_validation(self, candidate):
        raise primary

    monkeypatch.setattr(type(boundary), "require_active", fail_validation)
    config = LoadConfig(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        source_schema="sales",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        options={modules["boundary"].MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION: boundary},
    )
    with pytest.raises(BaseException) as caught:
        PostgresFullExtractStrategy(connector, _Logger()).extract(config, None)
    assert caught.value is primary
    assert boundary.terminal_receipt.outcome == "aborted"
    assert sum(event[0] == "rollback" for event in connector.events) == 1
    assert not any(event[0] == "copy" for event in connector.events)


def test_aborted_boundary_cannot_be_reused_for_valid_artifact(tmp_path):
    boundary, connector, _ = _prepared(_feature_modules())
    with pytest.raises(Exception) as invalid:
        boundary.complete(object())
    assert invalid.value.reason == "exact_type_violation"
    receipt = boundary.terminal_receipt
    assert receipt.outcome == "aborted" and receipt.cleanup_succeeded
    with pytest.raises(Exception) as replay:
        boundary.complete(_sealed_artifact(tmp_path))
    assert replay.value.reason == "snapshot_lease_mismatch"
    assert boundary.terminal_receipt is receipt
    assert sum(event[0] == "rollback" for event in connector.events) == 1
    assert not any(event[0] == "commit" for event in connector.events)
