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
