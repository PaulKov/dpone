"""Actual transaction observations, not context objects, select a control scope."""

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_activation import CompositionAdmissionError

SERVICE = "11111111-1111-4111-8111-111111111111"


def context(*, state=((1, 1, "Exclusive", 42),), authority=None):
    class Cursor:
        def __init__(self):
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, sql, *parameters):
            self.calls.append((sql, parameters))
            return self

        def fetchall(self):
            if len(self.calls) == 1:
                return state
            return ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE),) if authority is None else authority

    return SimpleNamespace(cursor=Cursor(), schema="dpone_control", table=lambda _: "untrusted_table")


def test_current_transaction_identity_is_observed_and_canonical_scope_is_used():
    ledger = context()
    assert require_shared_transaction_in(ledger, expected_service_id=SERVICE) == 42
    statements = " ".join(sql for sql, _ in ledger.cursor.calls)
    assert "[dpone_control].[composition_authority]" in statements
    assert "untrusted_table" not in statements
    assert "CURRENT_TRANSACTION_ID()" in statements
    assert ledger.cursor.calls[0][1] == ("dpone:composition-control:v1",)


@pytest.mark.parametrize(
    "state",
    [
        (),
        ((1, 1, "Exclusive", 42),) * 2,
        ((1, 1, "Exclusive"),),
        ((0, 0, "NoLock", None),),
        ((1, -1, "NoLock", None),),
        ((1, 1, "Shared", 42),),
        ((1, 1, None, 42),),
        ((True, 1, "Exclusive", 42),),
        ((1, True, "Exclusive", 42),),
        ((1, 1, "Exclusive", True),),
        ((1, 1, "Exclusive", 0),),
        ((1, 1, "Exclusive", -1),),
        ((1, 1, "Exclusive", 2**63),),
    ],
)
def test_invalid_transaction_never_reads_protected_originals(state):
    ledger = context(state=state)
    with pytest.raises(CompositionAdmissionError, match="shared_transaction"):
        require_shared_transaction_in(ledger, expected_service_id=SERVICE)
    assert len(ledger.cursor.calls) == 1


@pytest.mark.parametrize("previous", [41, 0, -1, True, "42", 2**63])
def test_replaced_or_invalid_transaction_pin_rejects(previous: Any):
    with pytest.raises(CompositionAdmissionError, match="shared_transaction"):
        require_shared_transaction_in(context(), expected_service_id=SERVICE, transaction_id=previous)


def test_same_transaction_can_resume_without_changing_its_identity():
    assert require_shared_transaction_in(context(), expected_service_id=SERVICE, transaction_id=42) == 42


@pytest.mark.parametrize(
    "authority",
    [
        (),
        ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE),) * 2,
        ((1, 99, SERVICE),),
        ((1, COMPOSITION_MSSQL_SCHEMA_VERSION, "foreign"),),
        ((True, COMPOSITION_MSSQL_SCHEMA_VERSION, SERVICE),),
    ],
)
def test_missing_or_foreign_authority_rejects(authority):
    with pytest.raises(CompositionAdmissionError, match="control_authority"):
        require_shared_transaction_in(context(authority=authority), expected_service_id=SERVICE)


def test_invalid_schema_cannot_select_another_catalog():
    ledger = context()
    ledger.schema = "x]; private-canary"
    with pytest.raises(CompositionAdmissionError, match="control_schema") as caught:
        require_shared_transaction_in(ledger, expected_service_id=SERVICE)
    assert "private-canary" not in str(caught.value)
    assert not ledger.cursor.calls


@pytest.mark.parametrize("service", [None, True, "", "foreign", "AAAAAAAA-1111-4111-8111-111111111111"])
def test_invalid_configuration_pin_cannot_become_authority(service: Any):
    # An external service pin is a canonical UUID, not a caller-selected label.
    ledger = context(authority=((1, COMPOSITION_MSSQL_SCHEMA_VERSION, service),))
    with pytest.raises(CompositionAdmissionError, match="control_service_id"):
        require_shared_transaction_in(ledger, expected_service_id=service)
