"""Hermetic contracts for postgres_to_postgres_identity_v1."""

from __future__ import annotations

import pytest

from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.type_system.source_sink.profiles import build_default_profiles


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("integer", "integer"),
        ("bytea", "bytea"),
        ("jsonb", "jsonb"),
        ("uuid", "uuid"),
        ("numeric(18,4)", "numeric(18,4)"),
        ("character varying(255)", "character varying(255)"),
        ("time without time zone", "time without time zone"),
    ],
)
def test_postgres_postgres_identity_core_types(source_type: str, expected: str) -> None:
    decision = build_default_profiles()[("postgres", "postgres")].resolve(source_type)
    assert decision.target_type == expected
    assert decision.lossless is True


def test_postgres_postgres_type_matrix_registers_pair() -> None:
    matrix = PairTypeMatrixService().build(source="postgres", sink="postgres")
    assert matrix["profile"] == "postgres_to_postgres_identity_v1"
    assert matrix["entries"]
