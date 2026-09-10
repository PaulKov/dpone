"""Verification budgets do not shrink the authored business row allowance."""

import pytest

from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_verification import verification_allowance


def wire(schema):
    return build_mssql_bcp_native_contract(schema=schema, query="profile")


def test_allowance_counts_exact_metadata_capacity_and_business_null_framing():
    business = wire((("n", "int"),))
    prepared = wire(
        (("n", "int nullable"), ("__dpone__load_id", "varchar(26)"), ("__dpone__loaded_at", "datetime2(7)"))
    )
    allowance = verification_allowance(prepared, business)
    assert allowance.overhead_bytes == 1 + 2 + 26 + 8
    assert allowance.null_metadata == ()


def test_unbounded_generated_placeholder_cannot_expand_verification_memory():
    allowance = verification_allowance(
        wire((("n", "int"), ("__dpone__meta", "nvarchar(max) nullable"))), wire((("n", "int"),))
    )
    assert allowance.overhead_bytes == 8
    allowance.require_null_metadata({"n": 1, "__dpone__meta": None})
    with pytest.raises(ValueError, match="unbounded_metadata_changed"):
        allowance.require_null_metadata({"n": 1, "__dpone__meta": "changed"})
