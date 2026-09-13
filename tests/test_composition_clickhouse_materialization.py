"""Independent bounded ClickHouse materialization; scripted I/O is not live proof."""

from decimal import Decimal
from uuid import UUID

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn as Column
from dpone.contracts.composition_snapshot_materialization import snapshot_content_sha256

COLUMNS = (Column("id", "Int32"), Column("amount", "Nullable(Decimal(38, 9))"), Column("guid", "UUID"))
GUID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def digest(rows):
    return snapshot_content_sha256(COLUMNS, rows, max_rows=10, max_bytes=4096)


def test_typed_multiset_exact_decimal_guid_null_and_multiplicity():
    rows = [(1, Decimal("12345678901234567890123456789.123456789"), UUID(GUID)), (2, None, GUID)]
    assert digest(rows) == digest(list(reversed(rows)))
    assert digest(rows) != digest(rows + [rows[0]])
    assert digest(rows) != digest([(1, Decimal("12345678901234567890123456789.123456788"), GUID), rows[1]])
    assert digest(rows) != digest([(1, None, GUID), rows[1]])
    assert digest([]).startswith("sha256:")


@pytest.mark.parametrize(
    "row",
    [
        (True, None, GUID),
        (2**31, None, GUID),
        (1, 1.1, GUID),
        (1, Decimal("0.0000000001"), GUID),
        (1, None, "bad"),
        (1, None),
        (1, Decimal("NaN"), GUID),
    ],
)
def test_lossy_invalid_or_partial_values_reject(row):
    with pytest.raises(CompositionAdmissionError):
        digest([row])


def test_rows_and_bytes_are_bounded():
    with pytest.raises(CompositionAdmissionError):
        snapshot_content_sha256(COLUMNS, [(1, None, GUID)], max_rows=1, max_bytes=1)
    with pytest.raises(CompositionAdmissionError):
        digest([(1, None, GUID)] * 11)


def _catalog(
    *, rows=None, physical=None, excluded=0, visibility=True, columns=COLUMNS, exchanged=False, observed_count=2
):
    from dpone.app.composition_clickhouse_catalog import ClickHouseHttpSnapshotCatalog
    from tests.composition_snapshot_helpers import intent
    from tests.test_composition_clickhouse_publication import _catalog_bodies, _json_compact, _ScriptedCatalogHttp

    bodies = {
        "AS capture_engine": _json_compact(
            tuple(
                (name, "String")
                for name in (
                    "capture_engine",
                    "partition_key",
                    "sorting_key",
                    "primary_key",
                    "sampling_key",
                    "storage_policy",
                    "engine_full",
                )
            ),
            [["MergeTree", "", "tuple()", "tuple()", "", "default", "MergeTree ORDER BY tuple()"]],
        ),
        "AS observed_count": _json_compact((("observed_count", "UInt64"),), [[observed_count]]),
        "excluded_tables": _json_compact(
            (("excluded_tables", "UInt64"), ("excluded_nodes", "UInt64")), [[excluded, 0]]
        ),
        "AS uuid": _json_compact(
            (("service_uuid", "String"), ("uuid", "String")),
            [[intent().target.service_id, intent().target.database_id]],
        ),
        "toUInt64(position)": _json_compact(
            tuple(
                zip(
                    ("name", "type", "position", "default_kind", "default_expression", "compression_codec"),
                    ("String", "String", "UInt64", "String", "String", "String"),
                    strict=True,
                )
            ),
            [[c.name, c.type_name, i, "", "", ""] for i, c in enumerate(columns, 1)],
        ),
        "AS feature_0": _json_compact(
            tuple(
                zip(
                    (
                        "engine",
                        "partition_key",
                        "sorting_key",
                        "primary_key",
                        "sampling_key",
                        "storage_policy",
                        "engine_full",
                    )
                    + tuple(f"feature_{i}" for i in range(8)),
                    ("String",) * 7 + ("UInt64",) * 8,
                    strict=True,
                )
            ),
            [
                physical
                or ["MergeTree", "", "tuple()", "tuple()", "", "default", "MergeTree ORDER BY tuple()", *([0] * 8)]
            ],
        ),
        "toString(`id`)": _json_compact(
            tuple((c.name, "Nullable(String)" if c.type_name.startswith("Nullable(") else "String") for c in columns),
            rows if rows is not None else [["1", "12345678901234567890123456789.123456789", GUID], ["2", None, GUID]],
        ),
        **_catalog_bodies(),
    }
    if exchanged:
        value = intent()
        bodies["total_bytes"] = _catalog_bodies(
            tables=[
                [value.target.target_table, value.generation.new_generation_uuid, "MergeTree", 50, 2],
                [value.target.generation_table, value.generation.old_target_uuid, "MergeTree", 40, 3],
            ]
        )["total_bytes"]
    return ClickHouseHttpSnapshotCatalog(
        _ScriptedCatalogHttp(bodies),
        columns=columns,
        max_rows=10,
        require_visibility=(lambda value: b"protected same-principal complete visibility") if visibility else None,
    )


def test_catalog_classifies_independent_typed_b_and_does_not_echo_claims():
    from tests.composition_snapshot_helpers import intent

    value = intent()
    catalog = _catalog()
    assert catalog.can_classify_publication()
    observed = catalog.inspect(value)
    assert observed.generation_content_sha256 == digest(
        [(1, Decimal("12345678901234567890123456789.123456789"), GUID), (2, None, GUID)]
    )
    assert observed.generation_content_sha256 != value.generation.content_sha256
    assert observed.generation_rows == 2
    assert observed.schema_sha256[0] == observed.schema_sha256[1] is not None
    assert observed.physical_sha256[0] == observed.physical_sha256[1] is not None


@pytest.mark.parametrize(
    "kwargs",
    [{"visibility": False}, {"excluded": 1}, {"rows": [["1", 1.1, GUID]]}, {"rows": [["1", "0.0000000001", GUID]]}],
)
def test_catalog_rejects_incomplete_visibility_global_dependencies_and_lossy_rows(kwargs):
    from tests.composition_snapshot_helpers import intent

    with pytest.raises(CompositionAdmissionError):
        _catalog(**kwargs).inspect(intent())


def test_catalog_rejects_unmodeled_scalar_before_capability():
    with pytest.raises(CompositionAdmissionError):
        _catalog(columns=(Column("id", "Float64"),))


@pytest.mark.parametrize("exchanged,expected", [(False, "NOT_PUBLISHED"), (True, "PUBLISHED")])
def test_catalog_classifies_before_and_after_exchange_using_observed_uuid_b(exchanged, expected):
    from dataclasses import replace

    from dpone.contracts.composition_snapshot import classify_snapshot
    from tests.composition_snapshot_helpers import intent

    value = intent()
    observed = _catalog(exchanged=exchanged).inspect(value)
    # Independent source producer would seal these same canonical algorithms.
    generation = replace(
        value.generation,
        rows=2,
        content_sha256=digest([(1, Decimal("12345678901234567890123456789.123456789"), GUID), (2, None, GUID)]),
        schema_sha256=observed.schema_sha256[0],
        physical_sha256=observed.physical_sha256[0],
    )
    assert classify_snapshot(replace(value, generation=generation), observed) == expected
    assert classify_snapshot(value, observed) == "COMMIT_UNKNOWN"


def test_catalog_rejects_uuid_change_during_read():
    from tests.composition_snapshot_helpers import intent
    from tests.test_composition_clickhouse_publication import _catalog_bodies

    catalog = _catalog()
    original = catalog._http.request
    calls = 0

    def request(**kwargs):
        nonlocal calls
        if "total_bytes" in kwargs["payload"].decode():
            calls += 1
            if calls > 1:
                catalog._http.bodies["total_bytes"] = _catalog_bodies(tables=[])["total_bytes"]
        return original(**kwargs)

    catalog._http.request = request
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_changed"):
        catalog.inspect(intent())


@pytest.mark.parametrize("body", [b"{", b'{"meta":[],"data":[],"rows":0}', b'{"meta":[],"data":[],"rows":0}ERROR'])
def test_catalog_rejects_partial_or_malformed_materialization_response(body):
    from tests.composition_snapshot_helpers import intent

    catalog = _catalog()
    catalog._http.bodies["toUInt64(position)"] = body
    with pytest.raises(CompositionAdmissionError):
        catalog.inspect(intent())


def test_catalog_rejects_complete_json_with_partial_data_against_independent_count():
    from tests.composition_snapshot_helpers import intent

    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_count"):
        _catalog(rows=[["1", "0.000000000", GUID]]).inspect(intent())


def test_empty_generation_is_explicitly_observed_and_hashed():
    from tests.composition_snapshot_helpers import intent

    observed = _catalog(rows=[], observed_count=0).inspect(intent())
    assert observed.generation_rows == 0
    assert observed.generation_content_sha256 == digest([])


def test_complete_materialization_observation_has_one_aggregate_byte_budget():
    from tests.composition_snapshot_helpers import intent

    catalog = _catalog()
    catalog._max_content_bytes = 100
    with pytest.raises(CompositionAdmissionError, match="snapshot_materialization_budget"):
        catalog.inspect(intent())


def test_capture_catalog_observes_without_constructing_publication_intent(monkeypatch):
    from dpone.contracts.composition_snapshot import SnapshotPublicationIntent
    from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject, attempt_snapshot_target
    from tests.composition_snapshot_helpers import digest as original_digest
    from tests.composition_snapshot_helpers import intent

    value = intent()
    catalog = _catalog()
    subject = SnapshotCaptureSubject(
        value.attempt,
        attempt_snapshot_target(value.target, value.attempt),
        original_digest("binding"),
        ("db", "dbo", "table"),
        value.limits,
    )
    monkeypatch.setattr(
        SnapshotPublicationIntent, "__post_init__", lambda _: (_ for _ in ()).throw(AssertionError("invented intent"))
    )
    observed = catalog.observe_capture(subject, COLUMNS)
    assert observed.target == subject.target
    assert observed.target_uuid == value.generation.old_target_uuid
    assert observed.generation_uuid is None
    assert observed.generation_content_sha256 is None


def test_visibility_drift_after_content_never_certifies():
    from tests.composition_snapshot_helpers import intent

    catalog = _catalog()
    versions = iter([b"complete visibility before", b"permissions changed"])
    catalog._visibility = lambda _: next(versions)
    with pytest.raises(CompositionAdmissionError, match="snapshot_catalog_visibility_changed"):
        catalog.inspect(intent())


@pytest.mark.parametrize(
    "index,value", [(1, "toYYYYMM(id)"), (2, "id"), (4, "id"), (5, "other"), (6, "MergeTree ORDER BY id")]
)
def test_capture_rejects_incompatible_existing_physical_profile(index, value, monkeypatch):
    from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject, attempt_snapshot_target
    from tests.composition_snapshot_helpers import intent

    prepared = intent()
    subject = SnapshotCaptureSubject(
        prepared.attempt,
        attempt_snapshot_target(prepared.target, prepared.attempt),
        prepared.generation.content_sha256,
        ("db", "dbo", "table"),
        prepared.limits,
    )
    catalog = _catalog()
    original = catalog._query

    def query(sql, *args):
        if "AS capture_engine" in sql:
            row = ["MergeTree", "", "tuple()", "tuple()", "", "default", "MergeTree ORDER BY tuple()"]
            row[index] = value
            return [tuple(row)]
        return original(sql, *args)

    monkeypatch.setattr(catalog, "_query", query)
    with pytest.raises(CompositionAdmissionError, match="snapshot_capture_physical_profile"):
        catalog.observe_capture(subject, COLUMNS)


def test_capture_capability_requires_visibility_and_bounds_without_fabricated_columns():
    from dpone.app.composition_clickhouse_catalog import ClickHouseHttpSnapshotCatalog

    http = _catalog()._http
    catalog = ClickHouseHttpSnapshotCatalog(http, require_visibility=lambda _: b"pins")
    assert catalog.can_observe_capture()
    assert not catalog.can_classify_publication()
    assert not ClickHouseHttpSnapshotCatalog(http).can_observe_capture()


@pytest.mark.parametrize("text", ["x" * 70000, "ж" * 70000])
def test_catalog_materializes_wide_captured_string_within_explicit_budget(text):
    from tests.composition_snapshot_helpers import intent

    columns = (Column("id", "String"),)
    expected = snapshot_content_sha256(columns, [(text,)], max_rows=10, max_bytes=1024 * 1024)
    observed = _catalog(columns=columns, rows=[[text]], observed_count=1).inspect(intent())
    assert observed.generation_content_sha256 == expected


def test_catalog_string_decoder_enforces_utf8_byte_budget():
    from dpone.contracts.composition_snapshot_materialization import _catalog_response_cell

    assert _catalog_response_cell("жж", "Nullable(String)", 4) == "жж"
    with pytest.raises(CompositionAdmissionError):
        _catalog_response_cell("жж", "Nullable(String)", 3)
