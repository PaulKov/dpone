"""Synthetic source contract checks; these do not certify a live route."""

import pytest

from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource
from tests.test_mssql_native_policy import config

RAW_ENGINES = ("MergeTree", "ReplicatedMergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree")
LEGACY_SETTINGS = {
    "strings_as_bytes": True,
    "max_block_size": 65536,
    "skip_unavailable_shards": 0,
    "read_overflow_mode": "throw",
    "result_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
    "use_query_cache": 0,
}


class Guard:
    def __init__(self):
        self.active = False
        self.events = []

    def __enter__(self):
        self.active = True
        self.events.append("enter")
        return self

    def __exit__(self, *args):
        self.active = False
        self.events.append("exit")


class Connector:
    """One synthetic physical session with inherited final=1 and optional denial."""

    driver = "native"

    def __init__(self, guard, engine="MergeTree", database_engine="Atomic", failure=None):
        self.connection = self
        self.guard = guard
        self.engine = engine
        self.database_engine = database_engine
        self.failure = failure
        self.inherited_settings = {"final": 1}
        self.selects = []
        self.catalog_reads = []
        self.disconnected = False
        self.stream_closed = False

    def get_records(self, query, params=None, as_dict=False):
        assert self.guard.active
        self.catalog_reads.append(query)
        if "system.tables" in query:
            return [
                {
                    "engine": self.engine,
                    "database_engine": self.database_engine,
                    "uuid": "11111111-1111-4111-8111-111111111111",
                }
            ]
        return [{"name": "value", "type": "UInt64", "default_kind": ""}]

    def execute_iter(self, query, params, **kwargs):
        assert self.guard.active
        self.selects.append((query, params, kwargs))
        if self.failure == "denied":
            assert kwargs["settings"]["final"] == 0
            raise RuntimeError("synthetic final override denied")

        def rows():
            try:
                yield [("value", "String" if self.failure == "schema" else "UInt64")]
                if self.failure == "empty":
                    return
                effective = {**self.inherited_settings, **kwargs["settings"]}
                yield (7,)
                if self.failure == "guard":
                    self.guard.active = False
                    raise RuntimeError("synthetic guard lost")
                if effective["final"] == 0:
                    yield (7,)
            finally:
                self.stream_closed = True

        return rows()

    def disconnect(self):
        self.disconnected = True


def source_case(engine="MergeTree", *, raw=True, database_engine="Atomic", failure=None):
    value = config()
    value.source_schema, value.source_table = "synthetic", "events"
    if raw:
        value.options["native_transfer"]["source_read"] = {"mode": "raw_single_query"}
    guard = Guard()
    connector = Connector(guard, engine, database_engine, failure)
    source = ClickHouseNativeSource(connector, schema_guard_factory=lambda cfg: guard)
    return value, guard, connector, source


@pytest.mark.parametrize("engine", RAW_ENGINES)
def test_raw_admission_preserves_duplicates_over_inherited_final(engine):
    value, guard, connector, source = source_case(engine)
    result = source.extract(value, query_id="synthetic-query")
    try:
        assert list(result.artifact.iter_native_rows()) == [{"value": 7}, {"value": 7}]
        assert result.artifact.rows_exported == 2
        assert len(connector.catalog_reads) == 2
        assert len(connector.selects) == 1
        query, params, kwargs = connector.selects[0]
        assert query == "SELECT `value` FROM `synthetic`.`events`"
        assert params == {}
        assert kwargs == {
            "query_id": "synthetic-query",
            "with_column_types": True,
            "settings": {**LEGACY_SETTINGS, "final": 0},
        }
        assert connector.inherited_settings == {"final": 1}
        assert guard.active and connector.stream_closed
    finally:
        result.artifact.cleanup()
    assert connector.disconnected and guard.events == ["enter", "exit"]


@pytest.mark.parametrize("raw", [False, True])
@pytest.mark.parametrize(
    "engine",
    [
        *RAW_ENGINES,
        "Distributed",
        "SummingMergeTree",
        "CollapsingMergeTree",
        "AggregatingMergeTree",
        "VersionedCollapsingMergeTree",
        "Memory",
        "View",
    ],
)
@pytest.mark.parametrize("database_engine", ["Atomic", "Ordinary", "Replicated"])
def test_closed_engine_database_matrix(raw, engine, database_engine):
    value, guard, connector, source = source_case(engine, raw=raw, database_engine=database_engine)
    admitted = database_engine == "Atomic" and engine in (RAW_ENGINES if raw else ("MergeTree",))
    if admitted:
        result = source.extract(value)
        result.artifact.cleanup()
    else:
        diagnostic = "source_engine_unsupported" if raw else "plain_mergetree_atomic_required"
        with pytest.raises(ValueError, match=diagnostic):
            source.extract(value)
        assert not connector.selects
    assert connector.disconnected and not guard.active


def test_legacy_query_settings_are_exactly_unchanged():
    value, _, connector, source = source_case(raw=False)
    result = source.extract(value, query_id="legacy-query")
    try:
        assert list(result.artifact.iter_native_rows()) == [{"value": 7}]
        assert connector.selects == [
            (
                "SELECT `value` FROM `synthetic`.`events`",
                {},
                {"query_id": "legacy-query", "with_column_types": True, "settings": LEGACY_SETTINGS},
            )
        ]
    finally:
        result.artifact.cleanup()


@pytest.mark.parametrize(
    "failure,diagnostic",
    [("denied", "final override denied"), ("schema", "source_schema_changed"), ("guard", "guard lost")],
)
def test_raw_failure_never_retries_or_reports_eof(failure, diagnostic):
    value, guard, connector, source = source_case(failure=failure)
    result = source.extract(value)
    try:
        with pytest.raises((ValueError, RuntimeError), match=diagnostic):
            list(result.artifact.iter_native_rows())
        assert getattr(result.artifact, "rows_exported", None) is None
        assert len(connector.selects) == 1
    finally:
        result.artifact.cleanup()
    assert connector.disconnected and guard.events == ["enter", "exit"]


def test_raw_cancellation_closes_stream_and_preserves_incomplete_count():
    value, guard, connector, source = source_case()
    result = source.extract(value)
    iterator = result.artifact.iter_native_rows()
    assert next(iterator) == {"value": 7}
    iterator.close()
    assert connector.stream_closed and getattr(result.artifact, "rows_exported", None) is None
    result.artifact.cleanup()
    assert connector.disconnected and not guard.active
    assert len(connector.selects) == 1


@pytest.mark.parametrize(
    "mutation,diagnostic",
    [
        ({"uuid": "00000000-0000-0000-0000-000000000000"}, "source_uuid_required"),
        ({"type": "Array(UInt64)"}, "source_type_unsupported"),
        ({"type": "DateTime64(9)"}, "source_type_unsupported"),
        ({"default_kind": "MATERIALIZED"}, "ordinary_columns_required"),
    ],
)
def test_raw_mode_preserves_uuid_and_scalar_admission(mutation, diagnostic):
    value, guard, connector, source = source_case("ReplacingMergeTree")
    get_records = connector.get_records

    def metadata(query, params=None, as_dict=False):
        records = get_records(query, params, as_dict)
        scope = "system.tables" if "uuid" in mutation else "system.columns"
        if scope in query:
            records[0].update(mutation)
        return records

    connector.get_records = metadata
    with pytest.raises(ValueError, match=diagnostic):
        source.extract(value)
    assert not connector.selects and connector.disconnected and not guard.active


@pytest.mark.parametrize("tables", [[], [{"engine": "MergeTree", "database_engine": "Atomic"}] * 2])
def test_raw_relation_must_be_unique(tables):
    from dpone.runtime.sources.clickhouse_native_admission import admit_native_relation

    with pytest.raises(ValueError, match="source_engine_unsupported"):
        admit_native_relation(tables, "raw_single_query")


@pytest.mark.parametrize("policy", [None, {}, {"mode": "FINAL"}, {"mode": "raw_single_query", "extra": True}, False])
def test_invalid_authored_mode_fails_before_guard_and_catalog(policy):
    value, guard, connector, source = source_case()
    value.options["native_transfer"]["source_read"] = policy
    with pytest.raises(ValueError, match="source_read_invalid"):
        source.extract(value)
    assert guard.events == [] and connector.catalog_reads == [] and connector.selects == []


def test_raw_empty_query_reaches_eof_without_a_count_probe():
    value, guard, connector, source = source_case(failure="empty")
    result = source.extract(value)
    try:
        assert list(result.artifact.iter_native_rows()) == []
        assert result.artifact.rows_exported == 0
        assert len(connector.selects) == 1
    finally:
        result.artifact.cleanup()
    assert connector.disconnected and connector.stream_closed and not guard.active
