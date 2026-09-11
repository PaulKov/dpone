"""Strict trust observations and the real helper query, without SQL execution."""

import json
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.integration.composition import nonproduction_mssql_trust_live_support as support
from tests.integration.composition.test_nonproduction_mssql_trust_live import DoomedTransactionCursor
from tests.test_nonproduction_mssql_trust_live_support import FaultCursor


@pytest.mark.parametrize("transaction_id", [None, 1, 2**63 - 1])
def test_transaction_identity_observation_preserves_null_or_positive_bigint(transaction_id):
    payload = {"transaction_id": transaction_id}
    assert json.loads(support.observation_document(payload)) == payload


@pytest.mark.parametrize("transaction_id", [True, False, 0, -1, 2**63, 1.0, "1", "PWD=secret", [], {}])
def test_transaction_identity_observation_rejects_noncanonical_values(transaction_id):
    with pytest.raises(ValueError, match="unsafe_trust_observation"):
        support.observation_document({"transaction_id": transaction_id})


def test_doomed_wrapper_records_all_four_actual_shared_helper_fields():
    observed = [(1, -1, "NoLock", None)]
    cursor = FaultCursor([None, [(0, 1, 1, "Exclusive")], [(2627, 1, -1)], None, observed, None])
    observations = {}

    def record(name, payload):
        observations[name] = json.loads(support.observation_document(payload))

    boundary = DoomedTransactionCursor(cursor, record)
    with pytest.raises(CompositionAdmissionError, match="shared_transaction"):
        require_shared_transaction_in(
            SimpleNamespace(cursor=boundary, schema="control"),
            expected_service_id="00000000-0000-0000-0000-000000000001",
        )
    assert len(cursor.calls) == 1 and cursor.calls[0][1] == (support.COMPOSITION_MSSQL_LEDGER_LOCK,)
    assert "CURRENT_TRANSACTION_ID()" in cursor.calls[0][0]
    assert boundary.precondition_rows == tuple(observed) and boundary.complete
    assert cursor.index == len(cursor.sets)
    assert observations["transaction_doomed"] == {
        "result_rows": 1,
        "transaction_count": 1,
        "xact_state": -1,
        "lock_mode": "NoLock",
        "transaction_id": None,
    }
