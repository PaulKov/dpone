"""Live parity checks for the bounded target-local SQL digest kernel."""

from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
from tests.integration.mssql.mssql_target_local_p1_cases import digest_cases
from tests.integration.mssql.mssql_target_local_p1_support import configured_target_local_stand

from dpone.runtime.native_wire_models import stable_hash
from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]

_FIXTURES = Path(__file__).parents[2] / "fixtures" / "mssql-target-local-p1"


@pytest.fixture(scope="module")
def stand():
    return configured_target_local_stand()


@pytest.mark.parametrize("case", digest_cases(), ids=lambda case: case.name)
def test_fixture_descriptor_is_bound_to_generated_schema(case) -> None:
    descriptor = json.loads((_FIXTURES / f"{case.name}-v1.json").read_text(encoding="utf-8"))

    assert descriptor["source"] == "deterministic-synthetic-only"
    assert descriptor["business_column_count"] == len(case.columns)
    assert descriptor["row_count"] == len(case.rows)
    assert "sha256:" + descriptor["schema_sha256"] == stable_hash(case.columns)


@pytest.mark.parametrize("case", digest_cases(), ids=lambda case: case.name)
def test_python_and_sql_target_digest_are_identical(stand, case) -> None:
    table = stand.unique_table(case.name)
    try:
        stand.create_case_table(table, case)
        expected = case.python_digest()
        sql = build_target_digest_sql(stand.qualified(table), case.contract, len(case.rows))

        raw = stand.single_record(sql)
        actual = decode_target_digest_row(raw, expected_rows=len(case.rows))

        assert len(raw) == 10
        assert actual == expected
        assert stand.table_rows(table) == len(case.rows)
    finally:
        stand.drop_table(table)


def test_digest_rejects_count_overflow_and_detects_value_mutation(stand) -> None:
    case = digest_cases()[0]
    table = stand.unique_table("drift")
    try:
        stand.create_case_table(table, case)
        baseline = decode_target_digest_row(
            stand.single_record(build_target_digest_sql(stand.qualified(table), case.contract, len(case.rows))),
            expected_rows=len(case.rows),
        )

        stand.sql(f"UPDATE {stand.qualified(table)} SET [text_value] = N'mutated' WHERE [row_key] = 1;")
        changed = decode_target_digest_row(
            stand.single_record(build_target_digest_sql(stand.qualified(table), case.contract, len(case.rows))),
            expected_rows=len(case.rows),
        )
        assert changed.rows == baseline.rows
        assert changed.typed_digest != baseline.typed_digest

        with pytest.raises(ValueError, match="target_digest_count_overflow"):
            decode_target_digest_row(
                stand.single_record(build_target_digest_sql(stand.qualified(table), case.contract, len(case.rows) - 1)),
                expected_rows=len(case.rows) - 1,
            )
    finally:
        stand.drop_table(table)


def test_digest_is_stable_across_repeated_barrier_observations(stand) -> None:
    case = digest_cases()[1]
    table = stand.unique_table("stable")
    try:
        stand.create_case_table(table, case)
        sql = build_target_digest_sql(stand.qualified(table), case.contract, len(case.rows))

        observations = [stand.single_record(sql) for _ in range(3)]

        assert all(len(row) == 10 for row in observations)
        assert observations == [observations[0]] * 3
        assert all(isinstance(value, (int, Decimal)) for value in observations[0])
    finally:
        stand.drop_table(table)
