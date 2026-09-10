"""Real native coordinators over bounded spawned workers and synthetic SQL rows.

These fixtures prove lifecycle structure and corruption rejection. SQL storage
and BCP are synthetic; they establish neither live type fidelity nor performance.
"""

from collections import Counter
from contextlib import contextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_chunks_journal import NativeChunkJournal
from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.mssql_native_chunks import BoundedNativeChunks
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer, NativeStageContext
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import MSSQLFullRefreshStrategy
from tests.test_mssql_native_staged_prepare import MemoryConnector
from tests.test_mssql_native_staged_recovery import prepared_fixture


class DeliveryConnector(MemoryConnector):
    """Store one integer workload; use real file bytes and typed digest services."""

    def __init__(self, tamper=None):
        super().__init__()
        self.tables.clear()
        self.read_counts = Counter()
        self.tamper = tamper

    def fetch_schema_columns(self, *args, **kwargs):
        return [SimpleNamespace(name="n", dtype="int", nullable=False, collation=None)]

    def bcp_import(self, schema, table, path, *, options, database):
        assert options.file_format == "native"
        content = Path(path).read_bytes()
        assert len(content) % 4 == 0
        rows = [{"n": int.from_bytes(content[i : i + 4], "little", signed=True)} for i in range(0, len(content), 4)]
        self.tables[self.qualified_name(schema, table, database=database)].extend(rows)
        return len(rows)

    def get_records(self, sql, params=(), **kwargs):
        if "reserved_page_count" in sql:
            return [(0,)]
        return super().get_records(sql, params, **kwargs)

    def get_records_iterator(self, sql):
        table = sql.split(" FROM ", 1)[1]
        kind = "prepared" if "dpone_native_prepared_" in table else "raw"
        self.read_counts[table] += 1
        if self.tamper and self.tamper[:2] == (kind, self.read_counts[table]):
            column = self.tamper[2]
            self.tables[table][0][column] = "tampered" if column.startswith("__dpone__") else 8
        return super().get_records_iterator(sql)


def delivery_fixture(tmp_path, rows, *, tamper=None, observer=None):
    connector = DeliveryConnector(tamper)
    manager = MSSQLStagingManager(connector)
    strategy = MSSQLFullRefreshStrategy(connector, SimpleNamespace(), manager)
    config = LoadConfig(
        "source",
        "target",
        "default",
        "events",
        "dbo",
        "target",
        target_database="db",
        staging_database="db",
        staging_schema="stage",
        options={"__dpone_load_identity": {"run_id": "run", "load_id": "load"}},
    )
    wire = build_mssql_bcp_native_contract(schema=(("n", "int"),), query="fixture", target_format="mssql_native")
    encoder = MssqlNativeEncoder(wire, max_row_bytes=4)
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", wire.type_layout_hash)
    limits = NativeChunkLimits(
        max_total_encoded_bytes=10000,
        stage_allocated_bytes_stop_threshold=10000,
        max_rows=2,
        max_bytes=4096,
        max_row_bytes=4,
        parallelism=1,
    )

    @contextmanager
    def importer_factory():
        yield MssqlNativeChunkImporter(
            connector,
            database="db",
            schema="stage",
            columns=wire.columns,
            encode_row=encoder.encode_row,
            assert_lease=store.assert_lease,
            mutation_scope=lambda *args: nullcontext(),
            options_factory=BcpOptions,
            observer=observer,
        )

    executor = BoundedNativeChunks(
        store=store, importer_factory=importer_factory, work_dir=tmp_path / "files", limits=limits, observer=observer
    )

    def verify(receipts):
        with importer_factory() as importer:
            for receipt in receipts:
                importer.inspect(plan, receipt, lease)

    closed = []

    def source():
        try:
            yield from rows
        finally:
            closed.append(True)

    context = NativeStageContext(
        plan,
        wire,
        executor,
        lease,
        source,
        verify,
        lambda receipts: None,
        lambda extra: store.assert_lease(lease),
        lambda: NativeChunkJournal(store, lease, plan),
        nullcontext,
        max_row_bytes=4,
        observer=observer,
    )
    base = prepared_fixture()
    payload = SimpleNamespace(
        schema=(("n", "int"),),
        relation_schema=None,
        relation_metadata=None,
        relation_dialect=None,
        target_projection=None,
        mssql_transaction_admission=base.admission,
        mssql_target_mutation_plan=base.mutation_plan,
        require_completed_extraction=lambda: base.source_lifecycle,
    )
    sink = SimpleNamespace(connector=connector, _strategy_map={LoadStrategy.FULL_REFRESH: strategy})
    return MssqlNativeStagePreparer(sink, lambda *args: context), config, payload, connector, context, closed


@pytest.mark.parametrize("rows", [[], [(7,), (7,), (7,)]])
def test_integrated_fresh_delivery_keeps_four_raw_and_two_prepared_readbacks(tmp_path, rows):
    preparer, config, payload, connector, context, closed = delivery_fixture(tmp_path, rows)
    prepared = preparer.stage(config, payload)
    preparer.reverify(prepared)
    raw_counts = [count for table, count in connector.read_counts.items() if "dpone_native_prepared_" not in table]
    prepared_counts = [count for table, count in connector.read_counts.items() if "dpone_native_prepared_" in table]
    assert raw_counts == [4] * max(1, (len(rows) + 1) // 2)
    assert prepared_counts == [2]
    assert prepared.staging.row_count == len(rows)
    assert prepared.staging.consumed_payload_evidence.actual_native_rows == len(rows)
    assert context.journal_factory().completed().rows == len(rows)
    assert closed == [True]
    assert not any(sql.startswith("UPDATE n SET ") for sql in connector.statements)


@pytest.mark.parametrize(
    "tamper,code",
    [(("raw", boundary, "n"), "typed_digest_mismatch") for boundary in range(1, 5)]
    + [
        (("prepared", 1, "n"), "prepared_digest_mismatch"),
        (("prepared", 2, "n"), "prepared_content_changed"),
        (("prepared", 2, "__dpone__load_id"), "prepared_content_changed"),
    ],
)
def test_integrated_tamper_is_rejected_at_each_retained_boundary(tmp_path, tamper, code):
    prepublication = tamper[:2] in {("raw", 4), ("prepared", 2)}
    preparer, config, payload, connector, context, closed = delivery_fixture(
        tmp_path, [(7,)], tamper=None if prepublication else tamper
    )
    if prepublication:
        # Establish the durable prepared boundary before enabling corruption;
        # rejection during stage() cannot prove independent prepublication checks.
        prepared = preparer.stage(config, payload)
        assert context.journal_factory().publication.state()["phase"] == "prepared"
        connector.tamper = tamper
        with pytest.raises(ValueError, match=code):
            preparer.reverify(prepared)
    else:
        with pytest.raises(ValueError, match=code):
            preparer.stage(config, payload)
    state = context.journal_factory().publication.state()
    assert state is None or state["phase"] in {"preparing", "prepared"}
    assert closed == [True]


@pytest.mark.parametrize("rows", [[], [(7,), (7,), (7,)]])
def test_observed_delivery_reports_actual_boundaries_and_preserves_journal(tmp_path, rows):
    import os

    from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver

    observer = BoundedNativeDeliveryObserver()
    preparer, config, payload, connector, context, closed = delivery_fixture(tmp_path, rows, observer=observer)
    prepared = preparer.stage(config, payload)
    preparer.reverify(prepared)
    report = observer.snapshot()
    assert report["status"] == "PASS"
    spans = report["observations"]
    assert {item["reason"] for item in spans if item["phase"] == "raw_verify"} == {
        "import",
        "immediate_inspection",
        "preparation",
        "prepublication",
    }
    assert [item["reason"] for item in spans if item["phase"] == "prepared_verify"] == ["preparation", "prepublication"]
    assert len([item for item in spans if item["phase"] == "bcp"]) == (2 if rows else 0)
    encoded = [item for item in spans if item["phase"] == "encode"]
    assert encoded and all(item["process_id"] != os.getpid() for item in encoded)
    frames = [item for item in spans if item["phase"] == "frame_build"]
    assert all(item["metrics"]["source_read_work_seconds"]["availability"] == "measured" for item in frames)
    assert all(item["metrics"]["source_adapt_work_seconds"]["value"] is None for item in frames)
    assert all("schema_version" not in item for item in context.journal_factory().completed().observations)
    assert closed == [True]
