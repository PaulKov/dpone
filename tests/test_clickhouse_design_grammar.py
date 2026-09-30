"""The complete CREATE grammar fails closed instead of stripping SQL clauses."""

from __future__ import annotations

import importlib

import pytest


def _grammar():
    return importlib.import_module("dpone.adapters.clickhouse_design_grammar")


def test_quoted_column_names_and_full_roundtrip():
    from dpone.contracts.clickhouse_authority import AuthoritySubject, OperationBinding

    api = importlib.import_module("dpone.contracts.clickhouse_observation")
    columns = (api.CandidateColumn("a`b\\c.'", "UInt32"), api.CandidateColumn("雪", "Nullable(String)"))
    design = api.CandidateDesign(columns, (columns[0].name,), (), (columns[0].name,))
    binding = OperationBinding("d:op", AuthoritySubject("d", "server", "db", "target"), "candidate", 1)
    sql = _grammar().render_candidate_create(binding, design)
    assert "IF NOT EXISTS" not in sql
    assert _grammar().parse_table_design(sql) == design


@pytest.mark.parametrize(
    "keys",
    [
        "ORDER BY tuple()",
        "ORDER BY id",
        "PRIMARY KEY id ORDER BY (id, n)",
        "PARTITION BY (id, n) ORDER BY (id, n) PRIMARY KEY id",
    ],
)
def test_closed_keys(keys):
    design = _grammar().parse_table_design(f"CREATE TABLE db.t (id Int32, n UInt64) ENGINE = MergeTree {keys}")
    assert design.columns[0].name == "id"


@pytest.mark.parametrize(
    "extra",
    [
        "SETTINGS index_granularity = 8192",
        "TTL id + INTERVAL 1 DAY",
        "COMMENT 'x'",
        "SAMPLE BY id",
        "; DROP TABLE db.t",
        "/* comment */",
        "-- comment",
        "garbage",
    ],
)
def test_unknown_trailing_clauses_are_rejected(extra):
    with pytest.raises(ValueError):
        _grammar().parse_table_design(f"CREATE TABLE db.t (id Int32) ENGINE = MergeTree ORDER BY id {extra}")


@pytest.mark.parametrize(
    "columns",
    [
        "id Int32 DEFAULT 0",
        "id Int32 ALIAS 0",
        "id Int32 MATERIALIZED 0",
        "id Int32 CODEC(ZSTD)",
        "id Int32 COMMENT 'x'",
        "id Int32, INDEX i id TYPE minmax GRANULARITY 1",
        "id Int32, PROJECTION p (SELECT id)",
        "id Int32, CONSTRAINT c CHECK id > 0",
        "id Int32, id Int32",
    ],
)
def test_unknown_column_clauses_are_rejected(columns):
    with pytest.raises(ValueError):
        _grammar().parse_table_design(f"CREATE TABLE db.t ({columns}) ENGINE = MergeTree ORDER BY tuple()")


@pytest.mark.parametrize(
    "keys",
    [
        "ORDER BY id + 1",
        "ORDER BY missing",
        "ORDER BY id PRIMARY KEY n",
        "ORDER BY id ORDER BY id",
        "PARTITION BY text ORDER BY id",
        "PARTITION BY nullable ORDER BY id",
    ],
)
def test_expressions_invalid_primary_and_partitions_reject(keys):
    with pytest.raises(ValueError):
        _grammar().parse_table_design(
            f"CREATE TABLE db.t (id Int32, n Int32, text String, nullable Nullable(Int32)) ENGINE = MergeTree {keys}"
        )


def test_identity_is_separate_from_design_and_literals_are_not_erased():
    grammar = _grammar()
    sql = "CREATE TABLE db.t UUID '00112233-4455-6677-8899-aabbccddeeff' (`when` DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY tuple()"
    design = grammar.parse_table_design(sql)
    assert design == grammar.parse_table_design(sql.replace("db.t", "other.candidate"))
    with pytest.raises(ValueError):
        grammar.parse_table_design(sql.replace("'UTC'", "'Europe/Moscow'"))
    with pytest.raises(ValueError):
        grammar.parse_table_design(sql.replace("MergeTree", "ReplacingMergeTree"))
