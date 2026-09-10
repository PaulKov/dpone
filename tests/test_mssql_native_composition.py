"""Concrete context assembly owns independent sessions and rejects wrong database."""

from contextlib import contextmanager
from threading import Event
from types import SimpleNamespace

import pytest

from dpone.manifest.mssql_native_policy import native_limits
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from tests.test_mssql_native_policy import config


def _context(tmp_path, target):
    return compose_native_stage_context(
        store=SimpleNamespace(assert_lease=lambda lease: None),
        plan=SimpleNamespace(run_id="synthetic"),
        lease=SimpleNamespace(target_id="target"),
        wire_contract=build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic"),
        limits=native_limits(config()),
        work_dir=tmp_path,
        target_connector=target,
        importer_connection=lambda: pytest.fail("preparation must not open importers"),
        bcp_options_factory=BcpOptions,
        database="synthetic",
        schema="dbo",
        row_source=lambda: pytest.fail("preparation must not open the source"),
        journal_factory=lambda: None,
        cancelled=Event(),
        required_target_headroom_bytes=1024,
    )


class _PreparationSession:
    def __init__(self, *, database="synthetic", release_code=0, close_error=None):
        self.database = database
        self.release_code = release_code
        self.close_error = close_error
        self.closed = False
        self.calls = []

    def get_records(self, sql, params=None):
        assert not self.closed
        self.calls.append(sql)
        if sql == "SELECT DB_NAME()":
            return [(self.database,)]
        return [(self.release_code if "sp_releaseapplock" in sql else 0,)]

    def close(self):
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


def test_composed_importers_are_independent_and_closed(tmp_path):
    closed = []

    @contextmanager
    def connection():
        connector = SimpleNamespace(get_records=lambda sql: [("synthetic",)])
        try:
            yield connector
        finally:
            closed.append(connector)

    context = compose_native_stage_context(
        store=SimpleNamespace(assert_lease=lambda lease: None),
        plan=SimpleNamespace(run_id="synthetic"),
        lease=object(),
        wire_contract=build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT synthetic"),
        limits=native_limits(config()),
        work_dir=tmp_path,
        target_connector=SimpleNamespace(get_records=lambda sql: [("wrong",)]),
        importer_connection=connection,
        bcp_options_factory=BcpOptions,
        database="synthetic",
        schema="dbo",
        row_source=lambda: iter(()),
        journal_factory=lambda: None,
        cancelled=Event(),
        required_target_headroom_bytes=1024,
    )
    with context.executor.importer_factory() as first, context.executor.importer_factory() as second:
        assert first.connector is not second.connector
    assert len(closed) == 2
    with pytest.raises(ValueError, match="database_mismatch"):
        context.capacity_check(1)


def test_preparation_rejects_reused_target_without_closing_it(tmp_path):
    target = _PreparationSession()
    target.open_session = lambda **kwargs: target
    context = _context(tmp_path, target)
    with pytest.raises(ValueError, match="preparation_session_reused"), context.preparation_scope():
        pytest.fail("shared target session must not enter preparation")
    assert not target.closed
    assert target.calls == []


def test_preparation_rejects_and_closes_wrong_database(tmp_path):
    session = _PreparationSession(database="wrong")
    context = _context(tmp_path, SimpleNamespace(open_session=lambda **kwargs: session))
    with pytest.raises(ValueError, match="preparation_database_mismatch"), context.preparation_scope():
        pytest.fail("wrong database must not enter preparation")
    assert session.closed
    assert session.calls == ["SELECT DB_NAME()"]


@pytest.mark.parametrize("release_code,close_error", [(-999, None), (0, OSError("close failed"))])
def test_preparation_reports_cleanup_failure_after_success(tmp_path, release_code, close_error):
    session = _PreparationSession(release_code=release_code, close_error=close_error)
    context = _context(tmp_path, SimpleNamespace(open_session=lambda **kwargs: session))
    with pytest.raises(Exception, match="writer_release_failed|close failed"), context.preparation_scope():
        pass
    assert session.closed


def test_preparation_preserves_primary_error_when_cleanup_also_fails(tmp_path):
    session = _PreparationSession(release_code=-999, close_error=OSError("close failed"))
    context = _context(tmp_path, SimpleNamespace(open_session=lambda **kwargs: session))
    primary = ValueError("publication failed")
    with pytest.raises(ValueError) as raised, context.preparation_scope():
        raise primary
    assert raised.value is primary
    assert session.closed
    assert any("lock cleanup failed" in note for note in primary.__notes__)
    assert any("session cleanup failed" in note for note in primary.__notes__)
