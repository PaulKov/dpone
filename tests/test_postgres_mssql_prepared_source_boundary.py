"""Prepared-boundary and actual COPY handoff behavior."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.config import LoadConfig
from dpone.runtime.artifact_integrity import CompletedFileWrite
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import PostgresBaseStrategy
from dpone.runtime.sources.strategies.postgres.postgres_file_export_mixin import PostgresFileExportMixin
from dpone.runtime.sources.strategies.postgres.postgres_whole_file_export_service import PostgresWholeFileExportService
from tests.test_postgres_mssql_r1_source_schema_runtime import (
    FakeCatalogConnector,
    _load_config,
    _policy,
    _source_authority,
)
from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import bind_behavior_scenario


def _raises(call):
    try:
        call()
    except Exception as exc:
        assert type(exc) is not AssertionError
        return exc
    raise AssertionError("typed rejection required")


def _prepared(
    modules: dict[str, Any],
    connector: FakeCatalogConnector | None = None,
    lifecycle: ExtractionLifecycleAuthority | None = None,
):
    connector = connector or FakeCatalogConnector()
    verifier = PostgresSourceAuthorityVerifier(_source_authority())
    snapshot = modules["snapshot"].PostgresVerifiedRelationSnapshotIssuerV1()
    issuer = modules["issuer"].PostgresMssqlRelationSchemaAuthorityIssuerV1(type_policy_authority=_policy())
    projection = modules["projection"].PostgresMssqlSourceSchemaProjectionAdapterV1(
        fetched_schema_factory=modules["boundary"].build_r1_postgres_fetched_schema
    )
    runtime = modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1(
        verifier=verifier,
        snapshot_scope_issuer=snapshot,
        schema_authority_issuer=issuer,
        projection_adapter=projection,
    )
    boundary = runtime.prepare_boundary(
        connector=connector,
        lifecycle=lifecycle or ExtractionLifecycleAuthority(),
        load_config=_load_config(),
    )
    return boundary, connector, runtime


def _from_only(modules: dict[str, Any]) -> dict[str, bool]:
    query = PostgresBaseStrategy.format_select_query("odd schema", 'order"items', ["id"], only_relation=True)
    exact = query.as_string(None)
    legacy = PostgresBaseStrategy.format_select_query("odd schema", 'order"items', ["id"]).as_string(None)
    return {
        "SELECT_FROM_ONLY": " FROM ONLY " in exact,
        "quoted_schema_identifier": '"odd schema"' in exact,
        "quoted_relation_identifier": '"order""items"' in exact,
        "legacy_default_unchanged": " FROM ONLY " not in legacy and " FROM " in legacy,
    }


def _whole_only(modules: dict[str, Any]) -> dict[str, bool]:
    boundary, connector, _runtime = _prepared(modules)
    before = len(connector.events)
    errors = []
    for options in (
        {"batch_commit_mode": "separate"},
        {"batch_commit_mode": "whole", "partitioning": {"column": "id", "num_partitions": 2}},
    ):
        config = _load_config()
        config.options = options
        strategy = _WholeFileStrategy(connector)
        errors.append(
            _raises(
                lambda config=config, strategy=strategy: strategy._export_to_file(
                    "SELECT 1",
                    [("column_1", "bigint")],
                    config,
                    100,
                    prepared_boundary=boundary,
                    relation_schema=(("column_1", "bigint"),),
                )
            )
        )
    boundary.close_if_active()
    return {
        "batch_commit_mode.whole": getattr(errors[0], "code", "")
        == "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_EXPORT_PROFILE_UNSUPPORTED",
        "nonpartitioned": getattr(errors[1], "code", "")
        == "DPONE_POSTGRES_MSSQL_SOURCE_SCHEMA_AUTHORITY_EXPORT_PROFILE_UNSUPPORTED",
        "reject_before_row_io": len(connector.events) == before + 1,
        "no_fallback": all(type(error).__name__ == "RuntimeConfigurationError" for error in errors),
    }


def _bare_blocked(modules: dict[str, Any]) -> dict[str, bool]:
    import tempfile

    boundary_type = modules["boundary"].PreparedPostgresSourceBoundary
    fake = SimpleNamespace(require_active=lambda _connector: None)
    values = [object(), fake, object.__new__(boundary_type)]
    connector = _CopyConnector()
    service = PostgresWholeFileExportService(_WholeFileStrategy(connector))

    def attempt(value: object) -> object:
        config = SimpleNamespace(options={"runtime_storage": {"work_dir": tempfile.mkdtemp()}})
        return service.export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            config,
            prepared_boundary=value,
            relation_schema=(("column_1", "bigint"),),
        )

    errors = [_raises(lambda value=value: attempt(value)) for value in values]

    Subclass = type("Subclass", (boundary_type,), {})
    subclass: object = object.__new__(Subclass)
    subclass_error = _raises(lambda: attempt(subclass))
    legacy_lease = object()
    return {
        "bare_scope": errors[0] is not None,
        "bare_lease": _raises(lambda: attempt(legacy_lease)) is not None,
        "fake_require_active": errors[1] is not None,
        "same_shaped_foreign": type(fake) is not boundary_type,
        "subclass": subclass_error is not None,
    }


def _exact_admission(modules: dict[str, Any]) -> dict[str, bool]:
    boundary, connector, runtime = _prepared(modules)
    authority = boundary.source_schema_authority
    projection = boundary.schema_projection
    forged = object.__new__(type(boundary))
    forged_error = _raises(lambda: forged.require_active_for_copy(connector))
    immutable = False
    try:
        boundary.schema_projection = object()
    except (FrozenInstanceError, AttributeError):
        immutable = True
    boundary.close_if_active()
    return {
        "exact_runtime": type(runtime) is modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1,
        "single_procedural_issue_project_bind": authority is not None and projection is not None,
        "module_private_brand": getattr(forged_error, "reason", "") == "exact_type_violation",
        "direct_object_new_rejected": forged_error is not None,
        "immutable_scoped_fields": immutable,
    }


class _Logger:
    def log_etl_progress(self, event: str, *_args: object, **_kwargs: object) -> None:
        return None


class _RecordingLogger(_Logger):
    def __init__(self, connector: FakeCatalogConnector, *, invalidate_on_start: bool = False) -> None:
        self._connector = connector
        self._invalidate_on_start = invalidate_on_start

    def log_etl_progress(self, event: str, *_args: object, **_kwargs: object) -> None:
        self._connector.events.append(("logger", event, ()))
        if event == "POSTGRES_COPY_START" and self._invalidate_on_start:
            self._connector.connection.info.transaction_status = 0


class _CopyConnector(FakeCatalogConnector):
    def get_records(
        self,
        query: Any,
        params: tuple[object, ...] | None = (),
        as_dict: bool = False,
    ) -> list[Any]:
        text = query.as_string(None) if hasattr(query, "as_string") else str(query)
        if "extraction_horizon" in text:
            self.events.append(("records", text, tuple(params or ())))
            assert as_dict is True
            return [{"snapshot_token": self.snapshot_token, "extraction_horizon": 20}]
        return super().get_records(query, tuple(params or ()), as_dict=as_dict)

    def copy_to_file(self, *, output_path: str, **_kwargs: Any) -> dict[str, Any]:
        self.copy_calls += 1
        query = _kwargs.get("query_sql", "")
        rendered = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.events.append(("copy", rendered, ()))
        payload = b"1\n"
        Path(output_path).write_bytes(payload)
        return {
            "rows_exported": 1,
            "sha256": hashlib.sha256(payload).hexdigest(),
            "total_bytes": len(payload),
            "copy_read_count": 1,
            "chunk_count": 1,
            "elapsed": 0.01,
            "throughput": 0.01,
        }

    def commit_transaction(self) -> None:
        self.events.append(("commit", "", ()))
        self.connection.info.transaction_status = 0
        self.connection.autocommit = True


class _DeferredReceiptCopyConnector(_CopyConnector):
    """Force artifact receipt construction after the first post-COPY check."""

    def copy_to_file(self, *, output_path: str, **kwargs: Any) -> dict[str, Any]:
        stats = super().copy_to_file(output_path=output_path, **kwargs)
        stats["sha256"] = ""
        stats["rows_exported"] = None
        return stats


class _WholeFileStrategy(PostgresFileExportMixin):
    def __init__(self, connector: _CopyConnector, before_copy=None, after_attach=None) -> None:
        self.connector = connector
        self.logger = _Logger()
        self.before_copy = before_copy
        self.after_attach = after_attach

    def _effective_postgres_file_wire(self, _config):
        return "CSV", False

    def _targets_mssql(self, _config) -> bool:
        return False

    def _new_extraction_lifecycle(self):
        return ExtractionLifecycleAuthority()

    def _prepare_copy_select_sql(self, query, *_args, **_kwargs):
        if self.before_copy is not None:
            self.before_copy()
        self.connector.events.append(("callback", "", ()))
        return query

    def _attach_rows_exported(self, artifact, *_args, **_kwargs):
        artifact.rows_exported = 1
        if self.after_attach is not None:
            self.after_attach()

    def _validate_file_contract(self, *_args, **_kwargs):
        return None


def _copy_scope(modules: dict[str, Any]) -> dict[str, bool]:
    import tempfile

    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)
    config = SimpleNamespace(options={"runtime_storage": {"work_dir": tempfile.mkdtemp()}})
    strategy = _WholeFileStrategy(connector)
    strategy.logger = _RecordingLogger(connector)
    artifact = PostgresWholeFileExportService(strategy).export_full(
        "SELECT 1",
        [("column_1", "bigint")],
        config,
        prepared_boundary=boundary,
        relation_schema=(("column_1", "bigint"),),
    )
    callback_index = next(index for index, event in enumerate(connector.events) if event[0] == "callback")
    logger_index = next(
        index
        for index, event in enumerate(connector.events)
        if event[0] == "logger" and event[1] == "POSTGRES_COPY_START"
    )
    copy_index = next(index for index, event in enumerate(connector.events) if event[0] == "copy")
    validation_indices = [
        index
        for index, event in enumerate(connector.events)
        if event[0] == "records" and "txid_current_snapshot" in event[1]
    ]
    validation_index = max(index for index in validation_indices if index < copy_index)
    return {
        "revalidate_after_render": callback_index < validation_index,
        "revalidate_immediately_before_copy": callback_index < logger_index < validation_index
        and copy_index == validation_index + 1,
        "positive_copy_execution": connector.copy_calls == 1 and artifact.rows_exported == 1,
        "no_raw_scope_downstream": not hasattr(boundary, "scope"),
    }


def _artifact_receipt(modules: dict[str, Any], tmp_path: Path | None = None) -> dict[str, bool]:
    import tempfile

    boundary, _connector, _runtime = _prepared(modules)
    directory = Path(tempfile.mkdtemp()) if tmp_path is None else tmp_path
    path = directory / "chunk.csv"
    payload = b"1\n"
    path.write_bytes(payload)
    valid = FileExportArtifact(
        str(path),
        ["column_1"],
        compressed=False,
        format="csv",
        _completed_write=CompletedFileWrite(hashlib.sha256(payload).hexdigest(), len(payload), 1),
    )
    missing_rows = FileExportArtifact(
        str(path),
        ["column_1"],
        compressed=False,
        format="csv",
        _completed_write=CompletedFileWrite(hashlib.sha256(payload).hexdigest(), len(payload), None),
    )
    wrong = _raises(lambda: boundary.complete(object()))
    assert wrong.reason == "exact_type_violation"
    assert boundary.terminal_receipt.outcome == "aborted"
    missing_boundary, _connector, _runtime = _prepared(modules)
    missing = _raises(lambda: missing_boundary.complete(missing_rows))
    assert missing.reason == "internal_invariant_violation"
    assert missing_boundary.terminal_receipt.outcome == "aborted"
    boundary, _connector, _runtime = _prepared(modules)
    valid.require_integrity_receipt()
    boundary.complete(valid)
    return {
        "exact_FileExportArtifact": wrong is not None,
        "row_count_present": missing is not None,
        "integrity_receipt_valid": valid.require_integrity_receipt().rows_exported == 1,
        "seal_before_scope_complete": boundary.terminal_receipt.outcome == "completed",
    }


def _cleanup(modules: dict[str, Any]) -> dict[str, bool]:
    boundary, connector, _runtime = _prepared(modules)
    primary = RuntimeError("primary")
    caught_primary = None
    try:
        boundary.abort_preserving(primary)
    except BaseException as error:
        caught_primary = error
    if caught_primary is not None:
        assert caught_primary is primary
    first = boundary.terminal_receipt
    boundary.close_if_active()
    second, connector2, _runtime2 = _prepared(modules)
    seal_error = RuntimeError("seal")
    caught_seal = None
    try:
        second.abort_preserving(seal_error)
    except BaseException as error:
        caught_seal = error
    if caught_seal is not None:
        assert caught_seal is seal_error
    second.close_if_active()
    return {
        "extraction_failure_cleanup": sum(e[0] == "rollback" for e in connector.events) == 1,
        "seal_failure_cleanup": sum(e[0] == "rollback" for e in connector2.events) == 1,
        "primary_preserved": primary.args == ("primary",) and seal_error.args == ("seal",),
        "scope_abort": first.outcome == "aborted",
        "finally_close": second.terminal_receipt is not None,
    }


def _legacy(modules: dict[str, Any]) -> dict[str, bool]:
    boundary_type = modules["boundary"].PreparedPostgresSourceBoundary
    commits: list[str] = []
    lifecycle = ExtractionLifecycleAuthority()
    connector = SimpleNamespace(
        connection=object(),
        commit_transaction=lambda: commits.append("commit"),
        rollback=lambda: None,
    )
    from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import issue_repeatable_read_snapshot_lease

    lease = issue_repeatable_read_snapshot_lease(
        connector=connector,
        lifecycle=lifecycle,
        raw_snapshot_token="10:20:",
    )
    legacy = boundary_type(
        connector=connector,
        snapshot_lease=lease,
        source_identity=object(),
        schema_projection=object(),
    )
    legacy.complete()
    query = PostgresBaseStrategy.format_select_query("sales", "orders", ["id"]).as_string(None)
    return {
        "legacy_constructor": legacy is not None,
        "legacy_no_arg_complete": True,
        "legacy_commit": commits == ["commit"],
        "legacy_only_relation_default_false": "FROM ONLY" not in query,
    }


bind_behavior_scenario("prepared.from-only", _from_only)
bind_behavior_scenario("prepared.whole-file-only", _whole_only)
bind_behavior_scenario("prepared.bare-lease-blocked", _bare_blocked)
bind_behavior_scenario("prepared.exact-boundary-admission", _exact_admission)
bind_behavior_scenario("prepared.scope-before-copy", _copy_scope)
bind_behavior_scenario("prepared.artifact-receipt", _artifact_receipt)
bind_behavior_scenario("prepared.artifact-cleanup", _cleanup)
bind_behavior_scenario("prepared.legacy-boundary-compatible", _legacy)


def test_whole_file_revalidates_after_callback_before_actual_copy(tmp_path: Path) -> None:
    modules = {}
    for key, name in {
        "models": "dpone.contracts.postgres_mssql_source_schema_models",
        "authority": "dpone.contracts.postgres_mssql_source_schema_authority",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "projection": "dpone.runtime.sources.postgres_mssql_source_schema_projection",
        "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
        "runtime": "dpone.runtime.postgres_mssql_source_schema_runtime",
        "boundary": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
    }.items():
        try:
            modules[key] = __import__(name, fromlist=["*"])
        except ModuleNotFoundError:
            pytest.fail(f"approved implementation missing: {name}", pytrace=False)
    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)

    callback_called = False

    def invalidate() -> None:
        nonlocal callback_called
        callback_called = True
        connector.connection.info.transaction_status = 0

    strategy = _WholeFileStrategy(connector, before_copy=invalidate)
    config = SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}})
    with pytest.raises(Exception):
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            config,
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
        )
    assert callback_called is True
    assert connector.copy_calls == 0


def test_whole_file_revalidates_after_injected_logger_before_actual_copy(tmp_path: Path) -> None:
    modules = {}
    for key, name in {
        "models": "dpone.contracts.postgres_mssql_source_schema_models",
        "authority": "dpone.contracts.postgres_mssql_source_schema_authority",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "projection": "dpone.runtime.sources.postgres_mssql_source_schema_projection",
        "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
        "runtime": "dpone.runtime.postgres_mssql_source_schema_runtime",
        "boundary": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
    }.items():
        try:
            modules[key] = __import__(name, fromlist=["*"])
        except ModuleNotFoundError:
            pytest.fail(f"approved implementation missing: {name}", pytrace=False)
    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)
    strategy = _WholeFileStrategy(connector)
    strategy.logger = _RecordingLogger(connector, invalidate_on_start=True)
    config = SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}})

    with pytest.raises(Exception) as raised:
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            config,
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
        )

    assert connector.copy_calls == 0
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"
    logger_index = next(index for index, event in enumerate(connector.events) if event[0] == "logger")
    validation_indices = [
        index
        for index, event in enumerate(connector.events)
        if event[0] == "records" and "txid_current_snapshot" in event[1]
    ]
    assert validation_indices[-1] > logger_index


def test_lifecycle_complete_failure_cleans_scope_and_allows_same_issuer_reopen(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    modules = {}
    for key, name in {
        "models": "dpone.contracts.postgres_mssql_source_schema_models",
        "authority": "dpone.contracts.postgres_mssql_source_schema_authority",
        "observation": "dpone.runtime.sources.postgres_mssql_source_schema_observation",
        "issuer": "dpone.runtime.sources.postgres_mssql_source_schema_issuer",
        "projection": "dpone.runtime.sources.postgres_mssql_source_schema_projection",
        "snapshot": "dpone.runtime.sources.postgres_verified_relation_snapshot",
        "runtime": "dpone.runtime.postgres_mssql_source_schema_runtime",
        "boundary": "dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary",
    }.items():
        try:
            modules[key] = __import__(name, fromlist=["*"])
        except ModuleNotFoundError:
            pytest.fail(f"approved implementation missing: {name}", pytrace=False)

    lifecycle = ExtractionLifecycleAuthority()
    connector = FakeCatalogConnector()
    boundary, _connector, runtime = _prepared(modules, connector, lifecycle)
    payload = b"1\n"
    path = tmp_path / "sealed.csv"
    path.write_bytes(payload)
    artifact = FileExportArtifact(
        str(path),
        ["column_1"],
        compressed=False,
        format="csv",
        _completed_write=CompletedFileWrite(hashlib.sha256(payload).hexdigest(), len(payload), 1),
    )

    def fail_complete(**_kwargs: object) -> None:
        raise RuntimeError("secret lifecycle failure")

    monkeypatch.setattr(lifecycle, "complete", fail_complete)
    error = None
    try:
        boundary.complete(artifact)
    except Exception as exc:
        error = exc
    assert getattr(error, "reason", "") == "snapshot_cleanup_failed"
    assert error is not None
    assert error.__cause__ is error.__context__ is None
    assert sum(event[0] == "rollback" for event in connector.events) == 1

    connector.incarnation = "9/42"
    reopened = runtime.prepare_boundary(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        load_config=_load_config(),
    )
    reopened.close_if_active()
    assert reopened.terminal_receipt is not None


def _sealed_artifact(tmp_path: Path) -> FileExportArtifact:
    payload = b"1\n"
    path = tmp_path / "sealed-v9.csv"
    path.write_bytes(payload)
    return FileExportArtifact(
        str(path),
        ["column_1"],
        compressed=False,
        format="csv",
        _completed_write=CompletedFileWrite(hashlib.sha256(payload).hexdigest(), len(payload), 1),
    )


def test_lifecycle_complete_caller_cancellation_propagates_unchanged_after_truthful_abort(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    lifecycle = ExtractionLifecycleAuthority()
    boundary, connector, runtime = _prepared(modules, lifecycle=lifecycle)
    issued_lifecycle_receipt = lifecycle.receipt
    assert issued_lifecycle_receipt is not None
    cancellation = asyncio.CancelledError("caller cancellation")

    def cancel_complete(_self: ExtractionLifecycleAuthority) -> None:
        raise cancellation

    monkeypatch.setattr(ExtractionLifecycleAuthority, "complete", cancel_complete)
    caught = None
    try:
        boundary.complete(_sealed_artifact(tmp_path))
    except BaseException as exc:
        caught = exc

    assert caught is cancellation
    assert caught.__cause__ is caught.__context__ is None
    receipt = boundary.terminal_receipt
    assert receipt is not None
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    assert rollback_count == 1
    assert (
        receipt.outcome,
        receipt.cleanup_attempted,
        receipt.cleanup_succeeded,
        receipt.cleanup_error_reason,
        receipt.connection_quarantined,
        lifecycle.receipt is issued_lifecycle_receipt,
        issued_lifecycle_receipt.complete,
        boundary.close_if_active(),
        boundary.terminal_receipt is receipt,
        sum(event[0] == "rollback" for event in connector.events),
        runtime.snapshot_scope_issuer.active,
    ) == ("aborted", True, True, None, False, True, False, None, True, rollback_count, False)

    reopened = runtime.prepare_boundary(
        connector=FakeCatalogConnector(),
        lifecycle=ExtractionLifecycleAuthority(),
        load_config=_load_config(),
    )
    assert reopened is not boundary
    assert reopened.terminal_receipt is None
    assert reopened.close_if_active() is None
    assert reopened.terminal_receipt is not None
    assert reopened.terminal_receipt.outcome == "aborted"


def test_prepared_boundary_translates_ordinary_complete_failure_after_truthful_abort(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    lifecycle = ExtractionLifecycleAuthority()
    boundary, connector, _runtime = _prepared(modules, lifecycle=lifecycle)

    def fail_complete(_self: ExtractionLifecycleAuthority) -> None:
        raise RuntimeError("secret cleanup-only failure")

    monkeypatch.setattr(ExtractionLifecycleAuthority, "complete", fail_complete)
    with pytest.raises(Exception) as raised:
        boundary.complete(_sealed_artifact(tmp_path))

    assert type(raised.value) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    assert getattr(raised.value, "reason", "") == "snapshot_cleanup_failed"
    assert raised.value.__cause__ is raised.value.__context__ is None
    receipt = boundary.terminal_receipt
    assert receipt is not None
    assert receipt.outcome == "aborted"
    assert receipt.cleanup_attempted is True
    assert receipt.cleanup_succeeded is True
    assert sum(event[0] == "rollback" for event in connector.events) == 1


def test_actual_postgres_source_and_full_strategy_keep_no_runtime_legacy_preparation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.sources.postgres import PostgresSource
    from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
        issue_repeatable_read_snapshot_lease,
    )

    commits: list[str] = []
    connector = SimpleNamespace(
        connection=object(),
        commit_transaction=lambda: commits.append("commit"),
        rollback=lambda: None,
    )
    source = PostgresSource(connector, state_storage=None, logger=_Logger())
    strategy = source._full_extract
    projection = SimpleNamespace(projected_schema=(("column_1", "bigint"),))

    monkeypatch.setattr(strategy, "_targets_mssql", lambda _config: True)
    monkeypatch.setattr(
        strategy,
        "_begin_repeatable_read_snapshot",
        lambda lifecycle: issue_repeatable_read_snapshot_lease(
            connector=connector,
            lifecycle=lifecycle,
            raw_snapshot_token="10:20:",
        ),
    )
    monkeypatch.setattr(strategy, "_verify_postgres_source_authority", lambda _lease, _config: object())
    monkeypatch.setattr(strategy, "fetch_schema_projection", lambda _config: projection)

    load_config = LoadConfig(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        source_schema="sales",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        options={"sink_type": "mssql"},
    )
    boundary = source.prepare_mssql_source_boundary(load_config)
    assert type(boundary).__name__ == "PreparedPostgresSourceBoundary"
    assert not hasattr(source, "_postgres_mssql_source_schema_runtime") or (
        source._postgres_mssql_source_schema_runtime is None
    )
    boundary.complete()
    assert commits == ["commit"]


def test_runtime_boundary_exposes_exact_lifecycle_for_copy_and_service_binds_it(
    tmp_path: Path,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    lifecycle = ExtractionLifecycleAuthority()
    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector, lifecycle)
    try:
        exposed = boundary.lifecycle_for_copy()
    except AttributeError:
        pytest.fail("restricted lifecycle_for_copy capability is missing", pytrace=False)
    assert exposed is lifecycle

    config = SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}})
    artifact = PostgresWholeFileExportService(_WholeFileStrategy(connector)).export_full(
        "SELECT 1",
        [("column_1", "bigint")],
        config,
        prepared_boundary=boundary,
        relation_schema=(("column_1", "bigint"),),
    )
    assert artifact.extraction_lifecycle is lifecycle
    assert lifecycle.receipt is not None and lifecycle.receipt.complete is True
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "completed"


def test_copy_admission_rejects_replaced_physical_connection() -> None:
    from psycopg.pq import TransactionStatus

    from tests.test_postgres_mssql_r1_source_schema_runtime import FakeConnection
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    connector._connection = FakeConnection()
    connector._connection.autocommit = False
    connector._connection.info.transaction_status = TransactionStatus.INTRANS

    with pytest.raises(Exception) as raised:
        boundary.require_active_for_copy(connector)
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"


@pytest.mark.parametrize("mutation", ("transaction", "transaction_incarnation", "physical_session"))
def test_after_copy_mutation_is_revalidated_before_artifact_seal(
    mutation: str,
    tmp_path: Path,
) -> None:
    from psycopg.pq import TransactionStatus

    from tests.test_postgres_mssql_r1_source_schema_runtime import FakeConnection
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)

    def mutate_after_copy() -> None:
        if mutation == "transaction":
            connector.connection.info.transaction_status = TransactionStatus.IDLE
        elif mutation == "transaction_incarnation":
            connector.incarnation = "9/42"
            assert connector.connection.info.transaction_status is TransactionStatus.INTRANS
        else:
            connector._connection = FakeConnection()
            connector._connection.autocommit = False
            connector._connection.info.transaction_status = TransactionStatus.INTRANS

    config = SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}})
    with pytest.raises(Exception) as raised:
        PostgresWholeFileExportService(_WholeFileStrategy(connector)).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            config,
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
            after_copy=mutate_after_copy,
        )
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "aborted"
    assert list(tmp_path.iterdir()) == []


def test_transaction_incarnation_is_revalidated_at_terminal_artifact_seal(
    tmp_path: Path,
) -> None:
    """A callback after the first post-COPY check cannot switch transactions."""

    from psycopg.pq import TransactionStatus

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = _DeferredReceiptCopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)

    def replace_transaction() -> None:
        connector.incarnation = "9/42"
        assert connector.connection.info.transaction_status is TransactionStatus.INTRANS

    strategy = _WholeFileStrategy(connector, after_attach=replace_transaction)
    config = SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}})
    with pytest.raises(Exception) as raised:
        PostgresWholeFileExportService(strategy).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            config,
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
        )

    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"
    assert raised.value.__cause__ is raised.value.__context__ is None
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "aborted"
    assert list(tmp_path.iterdir()) == []


def test_transaction_incarnation_change_during_lifecycle_completion_cannot_succeed(
    tmp_path: Path,
) -> None:
    """The terminal transition must recheck pins after lifecycle callbacks."""

    from psycopg.pq import TransactionStatus

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = _CopyConnector()
    ticks = 0
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)  # noqa: UP017 - Python 3.10 mypy stubs.

    def clock() -> datetime:
        nonlocal ticks
        ticks += 1
        if ticks == 2:
            connector.incarnation = "9/42"
            assert connector.connection.info.transaction_status is TransactionStatus.INTRANS
        return started + timedelta(seconds=ticks)

    boundary, _connector, _runtime = _prepared(
        modules,
        connector,
        ExtractionLifecycleAuthority(clock=clock),
    )
    with pytest.raises(Exception) as raised:
        boundary.complete(_sealed_artifact(tmp_path))

    assert ticks >= 2
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"
    assert raised.value.__cause__ is raised.value.__context__ is None
    assert not any(event[0] == "commit" for event in connector.events)
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "aborted"


def test_active_scope_revalidation_propagates_caller_cancellation_unchanged(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    issued_lifecycle_receipt = boundary.lifecycle.receipt
    assert issued_lifecycle_receipt is not None
    scope = boundary._scope
    cancellation = asyncio.CancelledError("caller cancelled during active-scope revalidation")
    original_require_active = type(scope).require_active
    calls = 0

    def cancel_second_revalidation(self: object, candidate: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise cancellation
        return original_require_active(self, candidate)

    monkeypatch.setattr(type(scope), "require_active", cancel_second_revalidation)
    caught = None
    try:
        PostgresWholeFileExportService(_WholeFileStrategy(connector)).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}}),
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
        )
    except BaseException as error:
        caught = error

    assert caught is cancellation
    assert caught.__cause__ is caught.__context__ is None
    assert calls == 2
    assert connector.copy_calls == 0
    receipt = boundary.terminal_receipt
    assert receipt is not None
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    assert rollback_count == 1
    assert (
        receipt.outcome,
        receipt.cleanup_attempted,
        receipt.cleanup_succeeded,
        receipt.cleanup_error_reason,
        receipt.connection_quarantined,
        boundary.lifecycle.receipt is issued_lifecycle_receipt,
        issued_lifecycle_receipt.complete,
        boundary.close_if_active(),
        boundary.terminal_receipt is receipt,
        sum(event[0] == "rollback" for event in connector.events),
    ) == ("aborted", True, True, None, False, True, False, None, True, rollback_count)


def test_quarantined_connector_admission_is_route_owned_without_exception_links() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    connector.quarantine()

    with pytest.raises(Exception) as raised:
        boundary.require_active_for_copy(connector)

    assert type(raised.value) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
    assert getattr(raised.value, "reason", "") == "snapshot_lease_mismatch"
    assert raised.value.__cause__ is raised.value.__context__ is None
    caught = None
    try:
        boundary.abort_preserving(raised.value)
    except BaseException as error:
        caught = error
    if caught is not None:
        assert caught is raised.value
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "aborted"


def test_lifecycle_for_copy_acquisition_is_inside_service_cleanup_guard(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    primary = RuntimeError("secret lifecycle acquisition failure")
    events: list[tuple[str, BaseException | None]] = []
    boundary_type = type(boundary)
    original_abort = boundary_type.abort_preserving

    def fail_lifecycle(_self: object) -> None:
        events.append(("acquire", None))
        raise primary

    def record_abort(self: object, error: BaseException) -> None:
        events.append(("abort", error))
        original_abort(self, error)

    monkeypatch.setattr(boundary_type, "lifecycle_for_copy", fail_lifecycle)
    monkeypatch.setattr(boundary_type, "abort_preserving", record_abort)
    caught = None
    try:
        PostgresWholeFileExportService(_WholeFileStrategy(connector)).export_full(
            "SELECT 1",
            [("column_1", "bigint")],
            SimpleNamespace(options={"runtime_storage": {"work_dir": str(tmp_path)}}),
            prepared_boundary=boundary,
            relation_schema=(("column_1", "bigint"),),
        )
    except BaseException as error:
        caught = error

    assert caught is primary
    assert events == [("acquire", None), ("abort", primary)]
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "aborted"


def test_legacy_no_runtime_full_extract_keeps_ordinary_from_and_owns_transaction(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Compatibility extraction needs neither exact scope nor ``FROM ONLY``."""

    from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
    from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
        issue_repeatable_read_snapshot_lease,
    )

    connector = _CopyConnector()
    strategy = PostgresFullExtractStrategy(connector, _Logger())
    projection = SimpleNamespace(
        projected_schema=(("column_1", "bigint"),),
        relation_schema=(("column_1", "bigint"),),
        relation_metadata=(),
        target_projection=None,
    )
    captured: dict[str, Any] = {}
    lifecycle_box: list[ExtractionLifecycleAuthority] = []

    def begin(lifecycle: ExtractionLifecycleAuthority) -> object:
        connector.begin()
        lifecycle_box.append(lifecycle)
        return issue_repeatable_read_snapshot_lease(
            connector=connector,
            lifecycle=lifecycle,
            raw_snapshot_token="10:20:",
        )

    def export(query: Any, *_args: Any, **kwargs: Any) -> FileExportArtifact:
        captured["query"] = query.as_string(None)
        captured["kwargs"] = kwargs
        return _sealed_artifact(tmp_path)

    monkeypatch.setattr(strategy, "fetch_schema_projection", lambda _config: projection)
    monkeypatch.setattr(strategy, "_targets_mssql", lambda _config: True)
    monkeypatch.setattr(strategy, "_internal_query_authorized", lambda _config: False)
    monkeypatch.setattr(strategy, "_verify_postgres_source_authority", lambda *_args: object())
    monkeypatch.setattr(strategy, "_begin_repeatable_read_snapshot", begin)
    monkeypatch.setattr(strategy, "_export_to_file", export)
    config = LoadConfig(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        source_schema="sales",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        options={"sink_type": "mssql"},
    )

    result = strategy.extract(config, None)

    copy_sql = captured["query"]
    assert ' FROM "sales"."orders"' in copy_sql
    assert " FROM ONLY " not in copy_sql
    assert result.artifact.require_integrity_receipt().rows_exported == 1
    assert captured["kwargs"]["snapshot_lease"] is not None
    assert captured["kwargs"].get("prepared_boundary") is None
    assert lifecycle_box[0].receipt is not None and lifecycle_box[0].receipt.complete is True
    assert sum(event[0] == "begin" for event in connector.events) == 1
    assert sum(event[0] == "commit" for event in connector.events) == 1


def test_cleanup_failure_after_artifact_seal_cannot_return_success(tmp_path: Path) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    lifecycle = ExtractionLifecycleAuthority()
    boundary, connector, _runtime = _prepared(modules, lifecycle=lifecycle)
    connector.rollback_error = RuntimeError("secret rollback failure")

    raised_error = None
    try:
        boundary.complete(_sealed_artifact(tmp_path))
    except BaseException as error:
        raised_error = error
    receipt = boundary.terminal_receipt
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    quarantine_count = sum(event[0] == "quarantine" for event in connector.events)
    replay_close_error = None
    try:
        replay_close_result = boundary.close_if_active()
    except BaseException as error:
        replay_close_result = object()
        replay_close_error = error
    cleanup_checks: list[tuple[str, bool]] = [
        (
            "cleanup failure translated",
            type(raised_error) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1,
        ),
        ("cleanup failure reason closed", getattr(raised_error, "reason", "") == "snapshot_cleanup_failed"),
        (
            "cleanup failure links closed",
            raised_error is not None and raised_error.__cause__ is raised_error.__context__ is None,
        ),
        (
            "cleanup failure receipt truthful",
            receipt is not None
            and (
                receipt.outcome,
                receipt.cleanup_attempted,
                receipt.cleanup_succeeded,
                receipt.cleanup_error_reason,
                receipt.connection_quarantined,
            )
            == ("completed", True, False, "snapshot_cleanup_failed", True),
        ),
        ("lifecycle receipt complete", lifecycle.receipt is not None and lifecycle.receipt.complete is True),
        ("cleanup rollback once", rollback_count == 1),
        ("cleanup quarantine once", quarantine_count == 1),
        ("cleanup connector quarantined", connector.quarantined is True),
        ("cleanup replay close suppressed", replay_close_error is None and replay_close_result is None),
        ("cleanup replay receipt stable", boundary.terminal_receipt is receipt),
        ("cleanup replay rollback stable", sum(event[0] == "rollback" for event in connector.events) == rollback_count),
        (
            "cleanup replay quarantine stable",
            sum(event[0] == "quarantine" for event in connector.events) == quarantine_count,
        ),
    ]

    ordered, ordered_connector, ordered_runtime = _prepared(modules)
    ordered_scope = ordered._scope
    release_observations: list[tuple[object, bool]] = []
    ordered_error = None
    if ordered_scope is not None:
        original_release = ordered_scope._release

        def observe_release(owner: object) -> None:
            release_observations.append((ordered_scope.terminal_receipt, ordered_runtime.snapshot_scope_issuer.active))
            original_release(owner)
            release_observations.append((ordered_scope.terminal_receipt, ordered_runtime.snapshot_scope_issuer.active))

        ordered_scope._release = observe_release
        try:
            ordered.complete(_sealed_artifact(tmp_path))
        except BaseException as error:
            ordered_error = error
    ordered_receipt = ordered.terminal_receipt
    cleanup_checks.extend(
        (
            ("ordered scope exists", ordered_scope is not None),
            ("ordered completion succeeds", ordered_error is None),
            ("ordered receipt frozen", ordered_receipt is not None),
            (
                "receipt precedes release",
                ordered_receipt is not None
                and release_observations == [(ordered_receipt, True), (ordered_receipt, False)],
            ),
            (
                "ordered receipt truthful",
                ordered_receipt is not None
                and (ordered_receipt.outcome, ordered_receipt.cleanup_succeeded) == ("completed", True),
            ),
            ("ordered rollback once", sum(event[0] == "rollback" for event in ordered_connector.events) == 1),
        )
    )
    assert [name for name, passed in cleanup_checks if not passed] == []


def test_terminal_receipt_exposes_only_closed_cleanup_error_reason() -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    connector.rollback_error = RuntimeError("secret dependency identity")
    primary = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed")
    abort_result = None
    caught = None
    try:
        abort_result = boundary.abort_preserving(primary)
    except BaseException as error:
        caught = error

    receipt = boundary.terminal_receipt
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    quarantine_count = sum(event[0] == "quarantine" for event in connector.events)
    replay_close_error = None
    try:
        replay_close_result = boundary.close_if_active()
    except BaseException as error:
        replay_close_result = object()
        replay_close_error = error
    terminal_checks: list[tuple[str, bool]] = [
        ("failure receipt frozen", receipt is not None),
        ("failure abort primary identity", caught is primary),
        ("failure abort result", abort_result is None),
        (
            "failure abort links closed",
            caught is not None and caught.__cause__ is caught.__context__ is None,
        ),
        ("failure rollback once", rollback_count == 1),
        ("failure quarantine once", quarantine_count == 1),
        ("failure connector quarantined", connector.quarantined is True),
        (
            "failure receipt truthful",
            receipt is not None
            and (
                receipt.outcome,
                receipt.cleanup_attempted,
                receipt.cleanup_succeeded,
                receipt.cleanup_error_reason,
                receipt.connection_quarantined,
            )
            == ("aborted", True, False, "snapshot_cleanup_failed", True),
        ),
        ("failure replay close suppressed", replay_close_error is None and replay_close_result is None),
        ("failure replay receipt stable", boundary.terminal_receipt is receipt),
        ("failure replay rollback stable", sum(event[0] == "rollback" for event in connector.events) == rollback_count),
        (
            "failure replay quarantine stable",
            sum(event[0] == "quarantine" for event in connector.events) == quarantine_count,
        ),
        ("failure receipt surface closed", receipt is not None and not hasattr(receipt, "cleanup_error_code")),
        ("failure receipt redacted", receipt is not None and "secret" not in repr(receipt)),
        ("failure exception redacted", caught is not None and "secret" not in str(caught)),
    ]

    clean, clean_connector, _clean_runtime = _prepared(modules)
    clean_primary = RuntimeError("clean primary secret")
    clean_caught = None
    try:
        clean.abort_preserving(clean_primary)
    except BaseException as error:
        clean_caught = error
    clean_receipt = clean.terminal_receipt
    terminal_checks.extend(
        [
            ("clean receipt frozen", clean_receipt is not None),
            ("clean abort primary identity", clean_caught is clean_primary),
            (
                "clean abort links closed",
                clean_caught is not None and clean_caught.__cause__ is clean_caught.__context__ is None,
            ),
            (
                "clean abort receipt truthful",
                clean_receipt is not None
                and (clean_receipt.outcome, clean_receipt.cleanup_succeeded, clean_receipt.connection_quarantined)
                == ("aborted", True, False),
            ),
            ("clean abort rollback once", sum(event[0] == "rollback" for event in clean_connector.events) == 1),
        ]
    )

    import asyncio

    release_cases = (
        (
            RuntimeError("ordinary issuer release secret"),
            modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed"),
        ),
        (
            asyncio.CancelledError("issuer release cancellation secret"),
            modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed"),
        ),
    )
    for release_failure, release_primary in release_cases:
        releasing, releasing_connector, releasing_runtime = _prepared(modules)
        releasing_scope = releasing._scope
        assert releasing_scope is not None
        release_attempts = 0

        def fail_before_release(_owner: object, failure: BaseException = release_failure) -> None:
            nonlocal release_attempts
            release_attempts += 1
            raise failure

        releasing_scope._release = fail_before_release
        release_caught = None
        try:
            releasing.abort_preserving(release_primary)
        except BaseException as error:
            release_caught = error
        release_receipt = releasing.terminal_receipt
        repeat_primary = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed")
        repeat_caught = None
        try:
            releasing.abort_preserving(repeat_primary)
        except BaseException as error:
            repeat_caught = error
        terminal_checks.extend(
            (
                ("pre-release receipt frozen", release_receipt is not None),
                ("pre-release primary identity", release_caught is release_primary),
                (
                    "pre-release primary links closed",
                    release_caught is not None and release_caught.__cause__ is release_caught.__context__ is None,
                ),
                ("failed release keeps issuer fenced", releasing_runtime.snapshot_scope_issuer.active is True),
                ("pre-release attempted once", release_attempts == 1),
                ("repeat abort preserves its primary", repeat_caught is repeat_primary),
                (
                    "repeat abort links closed",
                    repeat_caught is not None and repeat_caught.__cause__ is repeat_caught.__context__ is None,
                ),
                ("repeat abort keeps receipt identity", releasing.terminal_receipt is release_receipt),
                (
                    "pre-release rollback once",
                    sum(event[0] == "rollback" for event in releasing_connector.events) == 1,
                ),
                (
                    "pre-release receipt truthful",
                    release_receipt is not None
                    and (
                        release_receipt.outcome,
                        release_receipt.cleanup_attempted,
                        release_receipt.cleanup_succeeded,
                        release_receipt.cleanup_error_reason,
                        release_receipt.connection_quarantined,
                    )
                    == ("aborted", True, True, None, False),
                ),
                (
                    "secondary release detail redacted",
                    release_caught is not None and "secret" not in str(release_caught),
                ),
            )
        )

    cancellation_only, cancellation_connector, cancellation_runtime = _prepared(modules)
    cancellation_scope = cancellation_only._scope
    assert cancellation_scope is not None
    release_cancellation = asyncio.CancelledError("release-only cancellation secret")
    cancellation_release_attempts = 0

    def cancel_before_release(_owner: object) -> None:
        nonlocal cancellation_release_attempts
        cancellation_release_attempts += 1
        raise release_cancellation

    cancellation_scope._release = cancel_before_release
    cancellation_caught = None
    try:
        cancellation_only.close_if_active()
    except BaseException as error:
        cancellation_caught = error
    cancellation_receipt = cancellation_only.terminal_receipt
    cancellation_repeat_caught = None
    try:
        cancellation_repeat_result = cancellation_only.close_if_active()
    except BaseException as error:
        cancellation_repeat_result = object()
        cancellation_repeat_caught = error
    terminal_checks.extend(
        (
            ("release-only receipt frozen", cancellation_receipt is not None),
            ("release-only cancellation identity", cancellation_caught is release_cancellation),
            (
                "release-only cancellation links closed",
                cancellation_caught is not None
                and cancellation_caught.__cause__ is cancellation_caught.__context__ is None,
            ),
            ("release-only issuer remains fenced", cancellation_runtime.snapshot_scope_issuer.active is True),
            ("release-only attempted once", cancellation_release_attempts == 1),
            ("release-only repeat close suppressed", cancellation_repeat_caught is None),
            ("release-only repeat close returns none", cancellation_repeat_result is None),
            ("release-only repeat keeps receipt identity", cancellation_only.terminal_receipt is cancellation_receipt),
            (
                "release-only rollback once",
                sum(event[0] == "rollback" for event in cancellation_connector.events) == 1,
            ),
            (
                "release-only receipt truthful",
                cancellation_receipt is not None
                and (
                    cancellation_receipt.outcome,
                    cancellation_receipt.cleanup_attempted,
                    cancellation_receipt.cleanup_succeeded,
                    cancellation_receipt.cleanup_error_reason,
                    cancellation_receipt.connection_quarantined,
                )
                == ("aborted", True, True, None, False),
            ),
        )
    )

    from psycopg.pq import TransactionStatus

    class StalePinnedPhysical(_V12PinnedPhysicalConnection):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        def rollback(self) -> None:
            self.rollback_attempts += 1
            raise RuntimeError("stale pinned rollback secret")

        def close(self) -> None:
            super().close()
            self.closed = True
            stale_stage_observations.append(
                (
                    "stale_fenced",
                    self.closed,
                    stale_connector.quarantined,
                    stale_runtime.snapshot_scope_issuer.active,
                    stale_boundary.terminal_receipt,
                )
            )

    class ReplacedPhysicalConnector(FakeCatalogConnector):
        def __init__(self, pinned: StalePinnedPhysical) -> None:
            super().__init__()
            self._connection = pinned  # type: ignore[assignment]
            self.quarantine_if_current_calls: list[object] = []

        def quarantine_if_current(self, physical_connection: object) -> bool:
            self.quarantine_if_current_calls.append(physical_connection)
            stale_stage_observations.append(
                (
                    "exact_quarantine_rejected",
                    stale_physical.closed,
                    self.quarantined,
                    stale_runtime.snapshot_scope_issuer.active,
                    stale_boundary.terminal_receipt,
                )
            )
            return False

        def quarantine(self) -> None:
            super().quarantine()
            stale_stage_observations.append(
                (
                    "connector_quarantined",
                    stale_physical.closed,
                    self.quarantined,
                    stale_runtime.snapshot_scope_issuer.active,
                    stale_boundary.terminal_receipt,
                )
            )

    class ReplacementPhysical(_V12PinnedPhysicalConnection):
        def __init__(self) -> None:
            super().__init__()
            self.closed = False

        def close(self) -> None:
            super().close()
            self.closed = True

    stale_stage_observations: list[tuple[str, bool, bool, bool, object | None]] = []
    stale_physical = StalePinnedPhysical()
    stale_connector = ReplacedPhysicalConnector(stale_physical)
    stale_boundary, _stale_connector, stale_runtime = _prepared(modules, stale_connector)
    stale_scope = stale_boundary._scope
    assert stale_scope is not None
    stale_release_observations: list[tuple[object | None, bool, bool, bool, bool]] = []
    stale_original_release = stale_scope._release

    def observe_stale_release(owner: object) -> None:
        stale_release_observations.append(
            (
                stale_boundary.terminal_receipt,
                stale_physical.closed,
                stale_connector.quarantined,
                replacement.closed,
                stale_runtime.snapshot_scope_issuer.active,
            )
        )
        stale_original_release(owner)
        stale_release_observations.append(
            (
                stale_boundary.terminal_receipt,
                stale_physical.closed,
                stale_connector.quarantined,
                replacement.closed,
                stale_runtime.snapshot_scope_issuer.active,
            )
        )

    stale_scope._release = observe_stale_release
    replacement = ReplacementPhysical()
    replacement.info.transaction_status = TransactionStatus.INTRANS
    replacement.autocommit = False
    stale_connector._connection = replacement  # type: ignore[assignment]
    stale_primary = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed")
    stale_caught = None
    try:
        stale_boundary.abort_preserving(stale_primary)
    except BaseException as error:
        stale_caught = error
    stale_receipt = stale_boundary.terminal_receipt
    terminal_checks.extend(
        (
            ("stale pinned primary identity", stale_caught is stale_primary),
            ("stale pinned rollback attempted once", stale_physical.rollback_attempts == 1),
            ("replacement never rolled back as pinned", replacement.rollback_attempts == 0),
            ("exact stale quarantine rejected once", stale_connector.quarantine_if_current_calls == [stale_physical]),
            ("stale pinned physical fenced", stale_physical.closed and stale_physical.close_attempts == 1),
            ("replacement connector quarantined", stale_connector.quarantined and replacement.close_attempts == 1),
            (
                "stale cleanup receipt truthful after fencing",
                stale_receipt is not None
                and (
                    stale_receipt.outcome,
                    stale_receipt.cleanup_attempted,
                    stale_receipt.cleanup_succeeded,
                    stale_receipt.cleanup_error_reason,
                    stale_receipt.connection_quarantined,
                )
                == ("aborted", True, False, "snapshot_cleanup_failed", True),
            ),
            (
                "stale fencing and quarantine precede receipt publication",
                stale_stage_observations
                == [
                    ("exact_quarantine_rejected", False, False, True, None),
                    ("stale_fenced", True, False, True, None),
                    ("connector_quarantined", True, True, True, None),
                ],
            ),
            (
                "stale receipt precedes issuer reuse",
                stale_receipt is not None
                and stale_release_observations
                == [
                    (stale_receipt, True, True, True, True),
                    (stale_receipt, True, True, True, False),
                ],
            ),
            ("stale connector blocks business io", _raises(lambda: stale_connector.connection) is not None),
        )
    )
    assert [name for name, passed in terminal_checks if not passed] == []


def test_close_if_active_translates_cleanup_failure_to_route_error() -> None:
    """Preserve an explicit primary on abort and report standalone close failure."""

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    connector.rollback_error = RuntimeError("secret rollback failure")
    primary = modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1("catalog_observation_failed")
    caught = None
    try:
        try:
            raise primary
        except BaseException as active_failure:
            boundary.abort_preserving(active_failure)
    except BaseException as error:
        caught = error
    close_result = boundary.close_if_active()

    receipt = boundary.terminal_receipt
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    quarantine_count = sum(event[0] == "quarantine" for event in connector.events)
    replay_close = boundary.close_if_active()
    close_checks: list[tuple[str, bool]] = [
        ("primary close receipt frozen", receipt is not None),
        ("primary close identity", caught is primary),
        ("primary close result", close_result is None),
        (
            "primary close links closed",
            caught is not None and caught.__cause__ is caught.__context__ is None,
        ),
        ("primary close rollback once", rollback_count == 1),
        ("primary close quarantine once", quarantine_count == 1),
        ("primary close connector quarantined", connector.quarantined is True),
        (
            "primary close receipt truthful",
            receipt is not None
            and (
                receipt.outcome,
                receipt.cleanup_attempted,
                receipt.cleanup_succeeded,
                receipt.cleanup_error_reason,
                receipt.connection_quarantined,
                replay_close,
                boundary.terminal_receipt is receipt,
                sum(event[0] == "rollback" for event in connector.events),
                sum(event[0] == "quarantine" for event in connector.events),
            )
            == (
                "aborted",
                True,
                False,
                "snapshot_cleanup_failed",
                True,
                None,
                True,
                rollback_count,
                quarantine_count,
            ),
        ),
        ("primary close receipt redacted", receipt is not None and "secret" not in repr(receipt)),
        ("primary close failure redacted", caught is not None and "secret" not in str(caught)),
    ]

    ordinary, ordinary_connector, _ordinary_runtime = _prepared(modules)
    ordinary_connector.rollback_error = RuntimeError("ordinary cleanup secret")
    ordinary_caught = None
    try:
        ordinary.close_if_active()
    except BaseException as error:
        ordinary_caught = error
    ordinary_receipt = ordinary.terminal_receipt
    close_checks.extend(
        (
            ("ordinary close receipt frozen", ordinary_receipt is not None),
            ("ordinary close failure reported", getattr(ordinary_caught, "reason", None) == "snapshot_cleanup_failed"),
            ("ordinary close did not return success", ordinary_caught is not None),
            (
                "ordinary close receipt truthful",
                ordinary_receipt is not None
                and (
                    ordinary_receipt.outcome,
                    ordinary_receipt.cleanup_succeeded,
                    ordinary_receipt.cleanup_error_reason,
                    ordinary_receipt.connection_quarantined,
                )
                == ("aborted", False, "snapshot_cleanup_failed", True),
            ),
        )
    )

    import asyncio

    cancelled, cancelled_connector, _cancelled_runtime = _prepared(modules)
    close_cancellation = asyncio.CancelledError("close cleanup cancelled")
    cancelled_connector.rollback_error = close_cancellation
    cancelled_caught = None
    try:
        cancelled.close_if_active()
    except BaseException as error:
        cancelled_caught = error
    cancelled_receipt = cancelled.terminal_receipt
    close_checks.extend(
        (
            ("cancelled close receipt frozen", cancelled_receipt is not None),
            ("cancelled close identity", cancelled_caught is close_cancellation),
            (
                "cancelled close links closed",
                cancelled_caught is not None and cancelled_caught.__cause__ is cancelled_caught.__context__ is None,
            ),
            (
                "cancelled close receipt truthful",
                cancelled_receipt is not None
                and (
                    cancelled_receipt.outcome,
                    cancelled_receipt.cleanup_succeeded,
                    cancelled_receipt.cleanup_error_reason,
                    cancelled_receipt.connection_quarantined,
                )
                == ("aborted", False, "snapshot_cleanup_failed", True),
            ),
        )
    )

    from psycopg.pq import TransactionStatus

    quarantined, quarantined_connector, _quarantined_runtime = _prepared(modules)
    pinned = quarantined_connector._connection
    assert pinned is not None
    pinned.info.transaction_status = TransactionStatus.INTRANS
    pinned.autocommit = False
    quarantined_connector.quarantined = True
    with pytest.raises(Exception) as quarantined_failure:
        quarantined.close_if_active()
    assert quarantined_failure.value.reason == "snapshot_cleanup_failed"
    quarantined_receipt = quarantined.terminal_receipt
    pinned_safe = pinned.closed or (
        pinned.info.transaction_status is TransactionStatus.IDLE and pinned.autocommit is True
    )
    close_checks.extend(
        (
            ("pre-quarantined receipt frozen", quarantined_receipt is not None),
            (
                "pre-quarantined physical safe or failure recorded",
                pinned_safe
                or (
                    quarantined_receipt is not None
                    and quarantined_receipt.cleanup_succeeded is False
                    and quarantined_receipt.connection_quarantined is True
                ),
            ),
        )
    )
    assert [name for name, passed in close_checks if not passed] == []


def test_real_full_extract_path_uses_only_and_same_prepared_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = _CopyConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)
    strategy = PostgresFullExtractStrategy(connector, _Logger())
    captured: dict[str, Any] = {}
    artifact = _sealed_artifact(tmp_path)

    def export(query: Any, *_args: Any, **kwargs: Any) -> FileExportArtifact:
        captured["query"] = query.as_string(None)
        captured["kwargs"] = kwargs
        kwargs["prepared_boundary"].complete(artifact)
        return artifact

    monkeypatch.setattr(strategy, "_export_to_file", export)
    monkeypatch.setattr(strategy, "_internal_query_authorized", lambda _config: False)
    config = LoadConfig(
        source_conn_id="postgres_orders",
        target_conn_id="mssql_dwh",
        source_schema="sales",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        options={
            "sink_type": "mssql",
            modules["boundary"].MSSQL_PREPARED_POSTGRES_SOURCE_BOUNDARY_OPTION: boundary,
        },
    )
    begin_count = sum(event[0] == "begin" for event in connector.events)
    result = strategy.extract(config, None)

    assert result.artifact is artifact
    assert 'FROM ONLY "sales"."orders"' in captured["query"]
    assert captured["kwargs"]["prepared_boundary"] is boundary
    assert captured["kwargs"]["snapshot_lease"] is None
    assert sum(event[0] == "begin" for event in connector.events) == begin_count
    assert boundary.terminal_receipt is not None
    assert boundary.terminal_receipt.outcome == "completed"


class _V12ConnectionCancellationConnector(_CopyConnector):
    def __init__(self) -> None:
        super().__init__()
        self.connection_cancellation: BaseException | None = None

    @property
    def connection(self):
        pending = self.connection_cancellation
        if pending is not None:
            self.connection_cancellation = None
            raise pending
        return super().connection


def test_v12_active_connection_cancellation_propagates_by_identity() -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = _V12ConnectionCancellationConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)
    cancellation = asyncio.CancelledError("active connector access cancelled")
    connector.connection_cancellation = cancellation
    caught = None
    try:
        boundary.require_active_for_copy(connector)
    except BaseException as error:
        caught = error
    finally:
        close_result = boundary.close_if_active()

    receipt = boundary.terminal_receipt
    assert receipt is not None
    assert (caught, caught.__cause__ if caught else object(), caught.__context__ if caught else object()) == (
        cancellation,
        None,
        None,
    )
    assert (
        connector.connection_cancellation,
        connector.copy_calls,
        receipt.outcome,
        receipt.cleanup_attempted,
        receipt.cleanup_succeeded,
        receipt.cleanup_error_reason,
        receipt.connection_quarantined,
        close_result,
        sum(event[0] == "rollback" for event in connector.events),
    ) == (None, 0, "aborted", True, True, None, False, None, 1)


def test_v12_terminal_connection_cancellation_propagates_by_identity(tmp_path: Path) -> None:
    import asyncio

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()

    class CompletedConnectionCancellationConnector(_V12ConnectionCancellationConnector):
        def __init__(self) -> None:
            super().__init__()
            self.cancel_when_lifecycle_completed = False
            self.observed_lifecycle: object | None = None
            self.completed_receipts_observed_during_connection_access: list[object] = []

        @property
        def connection(self):
            current_receipt = getattr(self.observed_lifecycle, "receipt", None)
            if self.cancel_when_lifecycle_completed and getattr(current_receipt, "complete", False):
                pending = self.connection_cancellation
                if pending is not None:
                    self.completed_receipts_observed_during_connection_access.append(current_receipt)
                    self.connection_cancellation = None
                    raise pending
            if self.quarantined or self._connection is None:
                raise RuntimeError("postgres_connector.quarantined")
            return self._connection

    completed_connector = CompletedConnectionCancellationConnector()
    completed, _completed_connector, completed_runtime = _prepared(modules, completed_connector)
    completed_lifecycle_receipt = completed.lifecycle.receipt
    assert completed_lifecycle_receipt is not None
    completed_lifecycle_receipt_bytes = repr(completed_lifecycle_receipt).encode()
    completed_connector.observed_lifecycle = completed.lifecycle
    completed_cancellation = asyncio.CancelledError("completed pinned access cancelled")
    completed_connector.connection_cancellation = completed_cancellation
    completed_connector.cancel_when_lifecycle_completed = True
    completed_caught = None
    try:
        completed.complete(_sealed_artifact(tmp_path))
    except BaseException as error:
        completed_caught = error
    completed_receipt = completed.terminal_receipt
    completed_current_lifecycle_receipt = completed.lifecycle.receipt
    completed_scope = completed._scope
    assert completed_scope is not None
    completed_rollback_count = sum(event[0] == "rollback" for event in completed_connector.events)
    completed_repeat_caught = None
    try:
        completed_repeat_result = completed.close_if_active()
    except BaseException as error:
        completed_repeat_result = object()
        completed_repeat_caught = error
    cancellation_checks: list[tuple[str, bool]] = [
        ("completed connection receipt frozen", completed_receipt is not None),
        ("completed connection cancellation identity", completed_caught is completed_cancellation),
        (
            "completed connection cancellation links closed",
            completed_caught is not None and completed_caught.__cause__ is completed_caught.__context__ is None,
        ),
        (
            "completed issued lifecycle receipt remains frozen and incomplete",
            repr(completed_lifecycle_receipt).encode() == completed_lifecycle_receipt_bytes
            and completed_lifecycle_receipt.complete is False,
        ),
        (
            "completed lifecycle receipt replaced immutably",
            completed_current_lifecycle_receipt is not None
            and completed_current_lifecycle_receipt is not completed_lifecycle_receipt
            and completed_current_lifecycle_receipt.complete is True,
        ),
        (
            "completed lifecycle replacement shared atomically",
            completed_current_lifecycle_receipt
            is completed.snapshot_lease.lifecycle.receipt
            is completed_scope.lifecycle.receipt
            is completed_scope._verified.lifecycle.receipt,
        ),
        (
            "completed connection observed only current replacement",
            completed_connector.completed_receipts_observed_during_connection_access
            == [completed_current_lifecycle_receipt],
        ),
        ("completed issuer released", completed_runtime.snapshot_scope_issuer.active is False),
        ("completed cleanup rollback once", completed_rollback_count == 1),
        ("completed repeat close suppressed", completed_repeat_caught is None),
        ("completed repeat close returns none", completed_repeat_result is None),
        ("completed repeat keeps receipt identity", completed.terminal_receipt is completed_receipt),
        (
            "completed repeat does not clean again",
            sum(event[0] == "rollback" for event in completed_connector.events) == completed_rollback_count,
        ),
        (
            "completed connection receipt truthful",
            completed_receipt is not None
            and (
                completed_receipt.outcome,
                completed_receipt.cleanup_attempted,
                completed_receipt.cleanup_succeeded,
                completed_receipt.cleanup_error_reason,
                completed_receipt.connection_quarantined,
            )
            == ("completed", True, True, None, False),
        ),
    ]

    connector = _V12ConnectionCancellationConnector()
    boundary, _connector, _runtime = _prepared(modules, connector)
    issued_lifecycle_receipt = boundary.lifecycle.receipt
    assert issued_lifecycle_receipt is not None
    issued_lifecycle_receipt_bytes = repr(issued_lifecycle_receipt).encode()
    cancellation = asyncio.CancelledError("terminal cleanup cancelled")
    connector.rollback_error = cancellation
    caught = None
    try:
        boundary.complete(_sealed_artifact(tmp_path))
    except BaseException as error:
        caught = error

    receipt = boundary.terminal_receipt
    current_lifecycle_receipt = boundary.lifecycle.receipt
    scope = boundary._scope
    assert scope is not None
    rollback_count = sum(event[0] == "rollback" for event in connector.events)
    rollback_close = boundary.close_if_active()
    cancellation_checks.extend(
        (
            ("rollback cancellation receipt frozen", receipt is not None),
            ("rollback cancellation attempted once", rollback_count == 1),
            ("rollback cancellation identity", caught is cancellation),
            (
                "rollback cancellation links closed",
                caught is not None and caught.__cause__ is caught.__context__ is None,
            ),
            (
                "rollback issued lifecycle receipt remains frozen and incomplete",
                repr(issued_lifecycle_receipt).encode() == issued_lifecycle_receipt_bytes
                and issued_lifecycle_receipt.complete is False,
            ),
            (
                "rollback lifecycle receipt replaced immutably",
                current_lifecycle_receipt is not None
                and current_lifecycle_receipt is not issued_lifecycle_receipt
                and current_lifecycle_receipt.complete is True,
            ),
            (
                "rollback lifecycle replacement shared atomically",
                current_lifecycle_receipt
                is boundary.snapshot_lease.lifecycle.receipt
                is scope.lifecycle.receipt
                is scope._verified.lifecycle.receipt,
            ),
            (
                "rollback cancellation receipt truthful",
                receipt is not None
                and (
                    receipt.outcome,
                    receipt.cleanup_attempted,
                    receipt.cleanup_succeeded,
                    receipt.cleanup_error_reason,
                    receipt.connection_quarantined,
                    rollback_close,
                    boundary.terminal_receipt is receipt,
                    sum(event[0] == "rollback" for event in connector.events),
                )
                == ("completed", True, False, "snapshot_cleanup_failed", True, None, True, rollback_count),
            ),
        )
    )

    bootstrap = __import__(
        "dpone.runtime.bootstrap_postgres_source_authority",
        fromlist=["_require_source_schema_runtime"],
    )
    runtime_type = modules["runtime"].PostgresMssqlSourceSchemaRuntimeV1
    runtime = object.__new__(runtime_type)
    bootstrap_cancellation = asyncio.CancelledError("bootstrap bundle validation cancelled")
    bootstrap_caught = None
    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(
            runtime_type,
            "require_exact_bundle",
            lambda _self: (_ for _ in ()).throw(bootstrap_cancellation),
        )
        try:
            bootstrap._require_source_schema_runtime(runtime)
        except BaseException as error:
            bootstrap_caught = error
    cancellation_checks.extend(
        (
            ("bootstrap cancellation identity", bootstrap_caught is bootstrap_cancellation),
            (
                "bootstrap cancellation links closed",
                bootstrap_caught is not None and bootstrap_caught.__cause__ is bootstrap_caught.__context__ is None,
            ),
        )
    )

    import importlib

    try:
        copy_module = importlib.import_module("dpone.runtime.connectors.postgres_copy_stream")
    except ModuleNotFoundError:
        pytest.fail("approved implementation missing: dpone.runtime.connectors.postgres_copy_stream", pytrace=False)

    class CopyStream:
        def __init__(self, failure: BaseException | None) -> None:
            self.failure = failure

        def __enter__(self) -> object:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def read(self) -> bytes:
            if self.failure is not None:
                raise self.failure
            return b""

    class Cursor:
        def __init__(self, failure: BaseException | None) -> None:
            self.failure = failure

        def __enter__(self) -> object:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def copy(self, *_args: object) -> CopyStream:
            return CopyStream(self.failure)

    class Connection:
        def __init__(self, failure: BaseException | None) -> None:
            self.failure = failure

        def cursor(self) -> Cursor:
            return Cursor(self.failure)

    class Output:
        def __init__(self, close_failure: BaseException) -> None:
            self.close_failure = close_failure
            self.close_attempts = 0

        def close(self) -> None:
            self.close_attempts += 1
            raise self.close_failure

        def write(self, _payload: bytes) -> None:
            return None

    def copy_outcome(
        primary: BaseException | None,
        close_failure: BaseException,
        *,
        compress: bool,
    ) -> tuple[BaseException | None, int, int]:
        from contextlib import ExitStack
        from unittest.mock import patch

        opener_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        output = Output(close_failure)

        def output_opener(*args: object, **kwargs: object) -> Output:
            opener_calls.append((args, kwargs))
            return output

        exporter = copy_module.PostgresCopyFileExporter(Connection(primary))
        caught_copy = None
        with ExitStack() as stack:
            stack.enter_context(patch.object(copy_module, "open", output_opener, create=True))
            stack.enter_context(patch.object(copy_module.gzip, "open", output_opener))
            try:
                exporter.export(
                    "SELECT 1",
                    str(tmp_path / "copy.bin"),
                    format="BINARY",
                    compress=compress,
                    compress_level=1,
                    buffer_size=8192,
                    logger=None,
                    params=(),
                )
            except BaseException as error:
                caught_copy = error
        return caught_copy, len(opener_calls), output.close_attempts

    for compress in (False, True):
        copy_primary = RuntimeError(f"active copy primary {compress}")
        copy_caught, primary_opens, primary_closes = copy_outcome(
            copy_primary,
            RuntimeError("secondary output close"),
            compress=compress,
        )
        copy_cancellation = asyncio.CancelledError(f"active copy cancellation {compress}")
        cancellation_caught, cancellation_opens, cancellation_closes = copy_outcome(
            copy_cancellation,
            RuntimeError("secondary cancellation close"),
            compress=compress,
        )
        close_cancellation = asyncio.CancelledError(f"output close cancelled {compress}")
        close_caught, close_opens, close_closes = copy_outcome(None, close_cancellation, compress=compress)
        cancellation_checks.extend(
            (
                (f"copy primary identity {compress}", copy_caught is copy_primary),
                (
                    f"copy primary links closed {compress}",
                    copy_caught is not None and copy_caught.__cause__ is copy_caught.__context__ is None,
                ),
                (f"copy cancellation identity {compress}", cancellation_caught is copy_cancellation),
                (
                    f"copy cancellation links closed {compress}",
                    cancellation_caught is not None
                    and cancellation_caught.__cause__ is cancellation_caught.__context__ is None,
                ),
                (f"copy close cancellation identity {compress}", close_caught is close_cancellation),
                (
                    f"copy close cancellation links closed {compress}",
                    close_caught is not None and close_caught.__cause__ is close_caught.__context__ is None,
                ),
                (
                    f"copy opener and close once {compress}",
                    (primary_opens, primary_closes, cancellation_opens, cancellation_closes, close_opens, close_closes)
                    == (1, 1, 1, 1, 1, 1),
                ),
            )
        )
    assert [name for name, passed in cancellation_checks if not passed] == []


def test_v12_terminal_admission_revalidates_the_retained_exact_lease(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    lease = boundary.snapshot_lease
    lease_type = type(lease)
    original = lease_type.require_for
    calls: list[object] = []

    def observe(candidate: object, connector: object) -> object:
        if candidate is lease:
            calls.append(connector)
        return original(candidate, connector)

    monkeypatch.setattr(lease_type, "require_for", observe)
    boundary.complete(_sealed_artifact(tmp_path))

    assert (calls, boundary.terminal_receipt.outcome) == ([connector], "completed")


def test_v12_terminal_admission_rejects_an_already_terminal_lifecycle(tmp_path: Path) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary, connector, _runtime = _prepared(modules)
    boundary.lifecycle.complete()
    error = _raises(lambda: boundary.complete(_sealed_artifact(tmp_path)))

    assert (
        getattr(error, "reason", None),
        boundary.terminal_receipt.outcome,
        sum(event[0] == "rollback" for event in connector.events),
    ) == ("snapshot_lease_mismatch", "aborted", 1)


def _v12_legacy_constructor_values() -> tuple[Any, object, object, object]:
    from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
        issue_repeatable_read_snapshot_lease,
    )

    connector = SimpleNamespace(
        connection=object(),
        commit_transaction=lambda: None,
        rollback=lambda: None,
    )
    lease = issue_repeatable_read_snapshot_lease(
        connector=connector,
        lifecycle=ExtractionLifecycleAuthority(),
        raw_snapshot_token="10:20:",
    )
    return connector, lease, object(), object()


@pytest.mark.parametrize("form", ("positional", "keyword", "mixed"))
def test_v12_legacy_constructor_preserves_exact_four_field_contract(form: str) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary_type = modules["boundary"].PreparedPostgresSourceBoundary
    connector, lease, source_identity, schema_projection = _v12_legacy_constructor_values()
    if form == "positional":
        boundary = boundary_type(connector, lease, source_identity, schema_projection)
    elif form == "keyword":
        boundary = boundary_type(
            connector=connector,
            snapshot_lease=lease,
            source_identity=source_identity,
            schema_projection=schema_projection,
        )
    else:
        boundary = boundary_type(
            connector,
            lease,
            source_identity=source_identity,
            schema_projection=schema_projection,
        )

    assert (
        boundary.connector,
        boundary.snapshot_lease,
        boundary.source_identity,
        boundary.schema_projection,
    ) == (connector, lease, source_identity, schema_projection)


@pytest.mark.parametrize(
    "rejection",
    ("unknown_argument", "ignored_positional_prefix", "non_exact_lease", "lease_subclass", "other_connector"),
)
def test_v12_legacy_constructor_rejects_only_outside_the_exact_compatibility_shape(rejection: str) -> None:
    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    boundary_type = modules["boundary"].PreparedPostgresSourceBoundary
    connector, lease, source_identity, schema_projection = _v12_legacy_constructor_values()

    def construct() -> object:
        if rejection == "unknown_argument":
            return boundary_type(
                connector=connector,
                snapshot_lease=lease,
                source_identity=source_identity,
                schema_projection=schema_projection,
                unexpected=True,
            )
        if rejection == "ignored_positional_prefix":
            return boundary_type(object(), connector, lease, source_identity, schema_projection)
        if rejection == "non_exact_lease":
            return boundary_type(connector, object(), source_identity, schema_projection)
        if rejection == "lease_subclass":
            lease_subclass = type("LeaseSubclass", (type(lease),), {})
            return boundary_type(connector, object.__new__(lease_subclass), source_identity, schema_projection)
        other_connector = SimpleNamespace(
            connection=connector.connection, commit_transaction=lambda: None, rollback=lambda: None
        )
        return boundary_type(other_connector, lease, source_identity, schema_projection)

    if rejection in {"unknown_argument", "ignored_positional_prefix"}:
        error = _raises(construct)
        assert type(error) is TypeError and "secret" not in str(error)
    else:
        legacy = construct()
        assert type(legacy) is boundary_type
        error = _raises(lambda: legacy.require_active_for_copy(legacy.connector))
        assert type(error) is modules["authority"].PostgresMssqlSourceSchemaAuthorityErrorV1
        assert error.reason == "exact_type_violation"


class _V12PinnedPhysicalConnection:
    def __init__(self) -> None:
        from psycopg.pq import TransactionStatus

        self.info = SimpleNamespace(transaction_status=TransactionStatus.IDLE)
        self.autocommit = True
        self.rollback_attempts = 0
        self.close_attempts = 0

    def rollback(self) -> None:
        from psycopg.pq import TransactionStatus

        self.rollback_attempts += 1
        self.info.transaction_status = TransactionStatus.IDLE
        self.autocommit = True

    def close(self) -> None:
        self.close_attempts += 1


@pytest.mark.parametrize("terminal", ("complete", "abort", "close"))
def test_v12_terminal_cleanup_targets_the_pinned_session_after_connector_replacement(
    terminal: str,
    tmp_path: Path,
) -> None:
    from psycopg.pq import TransactionStatus

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    connector = FakeCatalogConnector()
    pinned = _V12PinnedPhysicalConnection()
    connector._connection = pinned  # type: ignore[assignment]
    boundary, _connector, _runtime = _prepared(modules, connector)
    replacement = _V12PinnedPhysicalConnection()
    replacement.info.transaction_status = TransactionStatus.INTRANS
    replacement.autocommit = False
    connector._connection = replacement  # type: ignore[assignment]
    caught = None
    try:
        if terminal == "complete":
            boundary.complete(_sealed_artifact(tmp_path))
        elif terminal == "abort":
            boundary.abort_preserving(RuntimeError("primary"))
        else:
            boundary.close_if_active()
    except BaseException as error:
        caught = error

    assert (
        pinned.rollback_attempts,
        pinned.info.transaction_status,
        replacement.rollback_attempts,
        replacement.info.transaction_status,
        getattr(caught, "reason", None),
        boundary.terminal_receipt.outcome,
    ) == (
        1,
        TransactionStatus.IDLE,
        0,
        TransactionStatus.INTRANS,
        "snapshot_lease_mismatch" if terminal == "complete" else None,
        "aborted",
    )


class _V12RacePhysicalConnection(_V12PinnedPhysicalConnection):
    def __init__(self, entered: Any, release: Any) -> None:
        super().__init__()
        self._entered = entered
        self._release = release
        self._attempt_lock = __import__("threading").Lock()

    def rollback(self) -> None:
        with self._attempt_lock:
            self.rollback_attempts += 1
        self._entered.set()
        if not self._release.wait(5):
            raise RuntimeError("bounded terminal cleanup did not resume")
        raise RuntimeError("synthetic terminal cleanup failure")


class _V12RaceConnector(FakeCatalogConnector):
    def __init__(self, physical: _V12RacePhysicalConnection) -> None:
        super().__init__()
        self._connection = physical  # type: ignore[assignment]
        self.quarantine_attempts = 0

    def rollback(self) -> None:
        self.events.append(("rollback", "", ()))
        connection: Any = self.connection
        connection.rollback()
        connection.autocommit = True

    def quarantine(self) -> None:
        self.quarantine_attempts += 1
        super().quarantine()


@pytest.mark.parametrize(
    ("owner_action", "waiter_action", "expected_outcome"),
    (
        pytest.param("complete", "complete", "completed", id="complete-complete"),
        pytest.param("abort", "abort", "aborted", id="abort-abort"),
        pytest.param("close", "close", "aborted", id="close-close"),
        pytest.param("complete", "cancel_abort", "completed", id="controlled-mixed"),
    ),
)
def test_v12_terminal_races_are_linearizable_and_cleanup_once(
    owner_action: str,
    waiter_action: str,
    expected_outcome: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    import asyncio
    import threading

    from tests.test_postgres_mssql_r1_source_schema_semantic_inventory import _feature_modules

    modules = _feature_modules()
    cleanup_entered = threading.Event()
    cleanup_release = threading.Event()
    physical = _V12RacePhysicalConnection(cleanup_entered, cleanup_release)
    connector = _V12RaceConnector(physical)
    boundary, _connector, runtime = _prepared(modules, connector)
    _sealed_artifact(tmp_path).require_integrity_receipt()
    scope = boundary._scope
    assert scope is not None
    original_release = scope._release
    release_observations: list[tuple[object, bool]] = []

    def observed_release(owner: object) -> None:
        release_observations.append((scope.terminal_receipt, runtime.snapshot_scope_issuer.active))
        original_release(owner)
        release_observations.append((scope.terminal_receipt, runtime.snapshot_scope_issuer.active))

    scope._release = observed_release
    waiter_wait_entered = threading.Event()
    release_waiter_cancellation = threading.Event()
    waiter_identity: list[int] = []
    intercepted_wait = threading.local()
    cancellation = asyncio.CancelledError("waiting caller cancelled")
    original_wait = threading.Condition.wait

    def controlled_wait(condition: threading.Condition, timeout: float | None = None) -> bool:
        is_waiter = threading.get_ident() in waiter_identity
        if is_waiter and not getattr(intercepted_wait, "active", False):
            intercepted_wait.active = True
            try:
                waiter_wait_entered.set()
                if waiter_action == "cancel_abort":
                    if not release_waiter_cancellation.wait(5):
                        raise RuntimeError("bounded waiter cancellation was not released")
                    raise cancellation
            finally:
                intercepted_wait.active = False
        return original_wait(condition, timeout)

    monkeypatch.setattr(threading.Condition, "wait", controlled_wait)
    returned: dict[str, object] = {}
    failures: dict[str, BaseException] = {}
    primaries: dict[str, BaseException] = {}

    def invoke(label: str, action: str) -> None:
        try:
            if label == "waiter":
                waiter_identity.append(threading.get_ident())
            if action == "complete":
                returned[label] = scope.complete_after_artifact_seal()
            elif action == "abort":
                primary = RuntimeError(label)
                primaries[label] = primary
                returned[label] = scope.abort_preserving(primary)
            elif action == "close":
                returned[label] = scope.close_if_active()
            else:
                returned[label] = scope.abort_preserving(cancellation)
        except BaseException as error:
            failures[label] = error

    owner = threading.Thread(target=invoke, args=("owner", owner_action))
    owner.start()
    cleanup_started = cleanup_entered.wait(5)
    waiter = threading.Thread(target=invoke, args=("waiter", waiter_action))
    waiter.start()
    waiter_started = waiter_wait_entered.wait(5)
    waiter_blocked_with_owner = (
        waiter.is_alive() and owner.is_alive() and physical.rollback_attempts == 1 and boundary.terminal_receipt is None
    )
    if waiter_action == "cancel_abort":
        release_waiter_cancellation.set()
        waiter.join(5)
        waiter_cancelled_inside_wait = failures.get("waiter") is cancellation and not waiter.is_alive()
        owner_unaffected_by_waiter_cancellation = (
            owner.is_alive() and physical.rollback_attempts == 1 and boundary.terminal_receipt is None
        )
    else:
        waiter_cancelled_inside_wait = True
        owner_unaffected_by_waiter_cancellation = True
    cleanup_release.set()
    owner.join(5)
    waiter.join(5)
    receipt = boundary.terminal_receipt
    active = getattr(runtime.snapshot_scope_issuer, "active", None)
    expected_failures = (
        {"owner": primaries["owner"], "waiter": primaries["waiter"]}
        if owner_action == waiter_action == "abort"
        else {"waiter": cancellation}
        if waiter_action == "cancel_abort"
        else {}
    )

    assert (
        cleanup_started,
        waiter_started,
        waiter_blocked_with_owner,
        waiter_cancelled_inside_wait,
        owner_unaffected_by_waiter_cancellation,
        owner.is_alive(),
        waiter.is_alive(),
        physical.rollback_attempts,
        connector.quarantine_attempts,
        receipt.outcome,
        receipt.cleanup_succeeded,
        receipt.connection_quarantined,
        all(value is receipt for value in returned.values()),
        failures,
        active,
        release_observations,
    ) == (
        True,
        True,
        True,
        True,
        True,
        False,
        False,
        1,
        1,
        expected_outcome,
        False,
        True,
        True,
        expected_failures,
        False,
        [(receipt, True), (receipt, False)],
    )
    for label, failure in failures.items():
        assert failure is expected_failures[label]
        assert failure.__cause__ is failure.__context__ is None
    if owner_action == waiter_action == "abort":
        assert returned == {}
    if (owner_action, waiter_action) == ("complete", "cancel_abort"):
        assert set(returned) == {"owner"}
        assert returned["owner"] is receipt
        assert failures["waiter"] is cancellation
        assert failures["waiter"].__cause__ is failures["waiter"].__context__ is None
        assert receipt.cleanup_attempted is True
        assert receipt.cleanup_error_reason == "snapshot_cleanup_failed"
        assert boundary.terminal_receipt is receipt

        from psycopg.pq import TransactionStatus

        class SuccessfulRacePhysical(_V12RacePhysicalConnection):
            def __init__(self, entered: threading.Event, release: threading.Event) -> None:
                super().__init__(entered, release)

            def rollback(self) -> None:
                self.rollback_attempts += 1
                self._entered.set()
                if not self._release.wait(5):
                    raise RuntimeError("bounded successful cleanup did not resume")
                self.info.transaction_status = TransactionStatus.IDLE
                self.autocommit = True

        def assert_complete_waiter_projects_aborted_owner(owner_terminal_action: str) -> None:
            projection_cleanup_entered = threading.Event()
            projection_cleanup_release = threading.Event()
            projection_physical = SuccessfulRacePhysical(projection_cleanup_entered, projection_cleanup_release)
            projection_connector = _V12RaceConnector(projection_physical)
            projection_boundary, _projection_connector, projection_runtime = _prepared(modules, projection_connector)
            projection_scope = projection_boundary._scope
            assert projection_scope is not None
            projection_original_release = projection_scope._release
            projection_release_observations: list[tuple[object, bool]] = []

            def observe_projection_release(owner: object) -> None:
                projection_release_observations.append(
                    (projection_scope.terminal_receipt, projection_runtime.snapshot_scope_issuer.active)
                )
                projection_original_release(owner)
                projection_release_observations.append(
                    (projection_scope.terminal_receipt, projection_runtime.snapshot_scope_issuer.active)
                )

            projection_scope._release = observe_projection_release
            projection_wait_entered = threading.Event()
            projection_waiter_identity: list[int] = []
            projection_intercepted_wait = threading.local()

            def observe_projection_wait(
                condition: threading.Condition,
                timeout: float | None = None,
            ) -> bool:
                is_projection_waiter = threading.get_ident() in projection_waiter_identity
                if is_projection_waiter and not getattr(projection_intercepted_wait, "active", False):
                    projection_intercepted_wait.active = True
                    try:
                        projection_wait_entered.set()
                    finally:
                        projection_intercepted_wait.active = False
                return original_wait(condition, timeout)

            monkeypatch.setattr(threading.Condition, "wait", observe_projection_wait)
            projection_returned: dict[str, object] = {}
            projection_failures: dict[str, BaseException] = {}
            projection_primary = RuntimeError(f"{owner_terminal_action} owner")

            def invoke_projection(label: str, action: str) -> None:
                try:
                    if label == "waiter":
                        projection_waiter_identity.append(threading.get_ident())
                    if action == "complete":
                        projection_returned[label] = projection_scope.complete_after_artifact_seal()
                    elif action == "abort":
                        projection_returned[label] = projection_scope.abort_preserving(projection_primary)
                    else:
                        projection_returned[label] = projection_scope.close_if_active()
                except BaseException as error:
                    projection_failures[label] = error

            projection_owner = threading.Thread(
                target=invoke_projection,
                args=("owner", owner_terminal_action),
            )
            projection_owner.start()
            projection_cleanup_started = projection_cleanup_entered.wait(5)
            projection_waiter = threading.Thread(target=invoke_projection, args=("waiter", "complete"))
            projection_waiter.start()
            projection_waiter_started = projection_wait_entered.wait(5)
            projection_waiter_blocked = (
                projection_owner.is_alive()
                and projection_waiter.is_alive()
                and projection_physical.rollback_attempts == 1
                and projection_boundary.terminal_receipt is None
            )
            projection_cleanup_release.set()
            projection_owner.join(5)
            projection_waiter.join(5)
            projection_receipt = projection_boundary.terminal_receipt
            projection_waiter_failure = projection_failures.get("waiter")

            assert (
                projection_cleanup_started,
                projection_waiter_started,
                projection_waiter_blocked,
                projection_owner.is_alive(),
                projection_waiter.is_alive(),
                projection_physical.rollback_attempts,
                projection_connector.quarantine_attempts,
                projection_receipt is not None and projection_receipt.outcome,
                projection_receipt is not None and projection_receipt.cleanup_succeeded,
                "waiter" in projection_returned,
                getattr(projection_waiter_failure, "reason", None),
                projection_waiter_failure is not None
                and projection_waiter_failure.__cause__ is projection_waiter_failure.__context__ is None,
                projection_runtime.snapshot_scope_issuer.active,
                projection_release_observations,
            ) == (
                True,
                True,
                True,
                False,
                False,
                1,
                0,
                "aborted",
                True,
                False,
                "snapshot_lease_mismatch",
                True,
                False,
                [(projection_receipt, True), (projection_receipt, False)],
            )
            if owner_terminal_action == "abort":
                assert projection_failures.get("owner") is projection_primary
                assert "owner" not in projection_returned
            else:
                assert set(projection_returned) == {"owner"}
                assert projection_returned["owner"] is projection_receipt
                assert "owner" not in projection_failures

        assert_complete_waiter_projects_aborted_owner("abort")
        assert_complete_waiter_projects_aborted_owner("close")
