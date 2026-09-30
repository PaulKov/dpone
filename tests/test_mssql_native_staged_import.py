"""Independent native attempt counts and typed content must all agree."""

from contextlib import nullcontext
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter


def importer(
    tmp_path,
    *,
    vendor=2,
    server_rows=(1, 1),
    rejected=False,
    target_digest_contract=None,
    dtype="int",
    persisted_hash_layout=False,
):
    class Connector:
        queries = []

        def quote_identifier(self, name):
            return f"[{name}]"

        def qualified_name(self, schema, table, *, database=None):
            return f"[{database}].[{schema}].[{table}]"

        def begin(self):
            pass

        def commit_transaction(self):
            pass

        def rollback(self):
            pass

        def execute_query(self, query, params=()):
            self.queries.append(query)
            if "sp_addextendedproperty" in query:
                self.owner = params[0]

        def fetch_schema_columns(self, *args, **kwargs):
            return [SimpleNamespace(name="n", dtype=dtype, nullable=False, collation=None)]

        def get_records(self, sql, params=()):
            if "extended_properties" in sql:
                return [(self.owner,)]
            if "OBJECT_ID" in sql:
                return [(11,)]
            return [(len(server_rows),)]

        def get_records_iterator(self, sql):
            return iter({"n": value} for value in server_rows)

        def bcp_import(self, schema, table, path, *, options, database):
            if rejected:
                from pathlib import Path

                Path(options.error_file).write_text("rejected")
            return vendor

    def encode(row):
        return int(row["n"]).to_bytes(8 if dtype == "bigint" else 4, "little", signed=True)

    value = MssqlNativeChunkImporter(
        Connector(),
        options_factory=BcpOptions,
        database="db",
        schema="stage",
        columns=(SimpleNamespace(name="n", source_type=dtype, nullable=False),),
        encode_row=encode,
        assert_lease=lambda lease: None,
        mutation_scope=lambda plan, attempt, lease: nullcontext(),
        target_digest_contract=target_digest_contract,
        persisted_hash_layout=persisted_hash_layout,
    )
    path = tmp_path / "data.bin"
    path.write_bytes(encode({"n": 1}) * 2)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    file = EncodedNativeFile(
        path,
        0,
        2,
        path.stat().st_size,
        sha256(path.read_bytes()).hexdigest(),
        value.digest_rows(({"n": 1}, {"n": 1}))[1],
    )
    return value, plan, file


def test_persisted_hash_layout_creates_closed_technical_columns_and_fixed_width_queries(tmp_path):
    contract = build_mssql_bcp_native_contract(schema=[("n", "bigint")], query="SELECT n")
    value, plan, _ = importer(
        tmp_path,
        target_digest_contract=contract,
        dtype="bigint",
        persisted_hash_layout=True,
    )

    value._create_owned_stage(plan, "attempt", "stage")
    ddl = value.connector.queries[0]
    assert "[__dpone__native_row_hash] binary(32) NOT NULL" in ddl
    assert "[__dpone__mutation_version] rowversion NOT NULL" in ddl

    value.connector.get_records = lambda sql, params=(): [(0, 0, None, *([0] * 8))]
    observed, _ = value._target_digest_observation("stage", 0)
    assert (observed.rows, observed.mutation_watermark) == (0, 0)

    value.connector.get_records = lambda sql, params=(): [(0, None)]
    repeat = value._repeat_mutation_watermark("stage", 0)
    assert (repeat.rows, repeat.mutation_watermark) == (0, 0)


@pytest.mark.parametrize("reported", ["timestamp", "rowversion", " TIMESTAMP "])
def test_framework_rowversion_alias_is_normalized_without_broadening_business_types(reported):
    column = SimpleNamespace(name="__dpone__mutation_version", dtype=reported, nullable=False)
    assert MssqlNativeChunkImporter._normalize_stage_column(column) == (
        "__dpone__mutation_version",
        "timestamp",
        False,
    )
    with pytest.raises(ValueError, match="unsupported MSSQL physical type"):
        MssqlNativeChunkImporter._normalize_stage_column(
            SimpleNamespace(name="business_timestamp", dtype="timestamp", nullable=False)
        )


def test_native_attempt_verifies_duplicates_and_consumed_file_evidence(tmp_path):
    value, plan, file = importer(tmp_path)
    receipt = value.import_file(plan, file, "attempt", object())
    assert receipt.rows == 2
    assert receipt.consumed_part_evidence["declared_rows"] == 2
    assert value.inspect(plan, receipt, object()) == receipt


@pytest.mark.parametrize(
    "kwargs,code",
    [
        ({"vendor": 1}, "vendor_count"),
        ({"server_rows": (1,)}, "server_count"),
        ({"server_rows": (1, 2)}, "typed_digest"),
        ({"rejected": True}, "rejects"),
    ],
)
def test_disagreeing_authorities_fail_closed(tmp_path, kwargs, code):
    value, plan, file = importer(tmp_path, **kwargs)
    with pytest.raises(ValueError, match=code):
        value.import_file(plan, file, "attempt", object())


def test_tampered_file_rejected_before_import(tmp_path):
    value, plan, file = importer(tmp_path)
    file.path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="file_identity"):
        value.import_file(plan, file, "attempt", object())


def test_foreign_receipt_cannot_address_other_stage(tmp_path):
    value, plan, file = importer(tmp_path)
    receipt = value.import_file(plan, file, "attempt", object())
    with pytest.raises(ValueError, match="stage_identity"):
        value.inspect(plan, replace(receipt, stage_id="foreign"), object())


def test_settlement_refuses_foreign_table_with_same_name(tmp_path):
    value, plan, file = importer(tmp_path)
    value.connector.owner = "another-invocation"
    with pytest.raises(ValueError, match="owner_mismatch"):
        value.settle(plan, "attempt", object())


def test_target_local_raw_verification_returns_only_aggregate_and_never_iterates_rows(tmp_path):
    contract = build_mssql_bcp_native_contract(schema=[("n", "bigint")], query="SELECT n")
    value, _, file = importer(tmp_path, target_digest_contract=contract, dtype="bigint")
    digest_word = int.from_bytes(sha256((1).to_bytes(8, "little", signed=True)).digest(), "big")
    limbs = [((digest_word >> (32 * (7 - index))) & (2**32 - 1)) * 2 for index in range(8)]
    queries = []

    def aggregate(sql, params=()):
        queries.append(sql)
        return [(2, 0, *limbs)]

    value.connector.get_records = aggregate
    value.connector.get_records_iterator = lambda sql: pytest.fail("business-row readback forbidden")
    assert value._verify_contents("stage", file.rows, file.typed_digest) == (2 * digest_word) % (1 << 256)
    assert len(queries) == 1 and "#dpone_target_hashes" in queries[0]


@pytest.mark.parametrize(
    ("row", "code"),
    [
        ((2, 0, *([0] * 8)), "typed_digest_mismatch"),
        ((3, 1, *([0] * 8)), "target_digest_count_overflow"),
    ],
)
def test_target_local_raw_mismatch_and_overflow_reject_without_row_iterator(tmp_path, row, code):
    contract = build_mssql_bcp_native_contract(schema=[("n", "bigint")], query="SELECT n")
    value, _, file = importer(tmp_path, target_digest_contract=contract, dtype="bigint")
    value.connector.get_records = lambda sql, params=(): [row]
    value.connector.get_records_iterator = lambda sql: pytest.fail("business-row readback forbidden")
    with pytest.raises(ValueError, match=code):
        value._verify_contents("stage", file.rows, file.typed_digest)
