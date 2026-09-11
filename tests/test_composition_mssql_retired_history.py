"""Historical terminal reads remain valid while a legitimate successor runs."""

from dataclasses import replace
from typing import cast

import pytest

from dpone.adapters import composition_mssql_existing_operation as kernel
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.ports.composition_sql import CompositionSqlContext
from tests.composition_mssql_store_helpers import SERVICE_ID, mixed_request
from tests.test_composition_mssql_existing_operation import _owner_state
from tests.test_composition_mssql_operations import add_operation
from tests.test_composition_mssql_ownership import SharedSql
from tests.test_composition_mssql_shared_history import overlapping_qualification


def retired_with_successor(state="ACTIVE"):
    context = SharedSql()
    reference = context.add_owner()
    old = add_operation(context, state="SUCCEEDED")
    _owner_state(context, reference, "RETIRED")
    original = mixed_request()
    successor = replace(
        original, context=replace(original.context, activation_id="10000000-0000-4000-8000-000000000099")
    )
    context.add_owner(successor)
    if state != "PREPARED":
        add_operation(context)
    context.owners[next(key for key in context.owners if key != reference.owner_key)] = (
        *context.owners[next(key for key in context.owners if key != reference.owner_key)][:5],
        state,
    )
    return context, reference, old


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "RETIRING"])
def test_existing_terminal_attempt_can_be_read_while_successor_owns_same_guards(state):
    context, _, old = retired_with_successor(state)
    seen = []
    occurrence, receipt = kernel.require_existing_execution_in(
        context,
        old,
        expected_service_id=SERVICE_ID,
        terminal_validator=lambda _, parent, proof: seen.append((parent, proof)),
    )
    assert receipt.attempt == old and receipt.state == "SUCCEEDED"
    assert occurrence.receipt.state == "RETIRED"
    assert len(seen) == 1 and seen[0][1] == receipt


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "RETIRING"])
def test_retired_owner_read_audits_all_its_proofs_without_requiring_successor_terminal(state):
    from dpone.adapters.composition_mssql_operations import _occurrence
    from dpone.adapters.composition_mssql_ownership import read_shared_owner_in

    context, reference, old = retired_with_successor(state)
    observed = read_shared_owner_in(context, reference, expected_service_id=SERVICE_ID)
    assert observed is not None
    seen = []
    kernel.require_retired_execution_in(
        context,
        _occurrence(observed),
        expected_service_id=SERVICE_ID,
        terminal_validator=lambda _, parent, proof: seen.append((parent, proof)),
    )
    assert len(seen) == 1 and seen[0][1].attempt == old


def test_unsupported_qualification_successor_does_not_become_a_historical_permission():
    context = SharedSql()
    reference = context.add_owner()
    old = add_operation(context, state="SUCCEEDED")
    _owner_state(context, reference, "RETIRED")
    context.add_owner(overlapping_qualification())
    with pytest.raises(CompositionAdmissionError, match="shared_qualification_blocker"):
        kernel.require_existing_execution_in(
            cast(CompositionSqlContext, context),
            old,
            expected_service_id=SERVICE_ID,
            terminal_validator=lambda *_: None,
        )


def inspect_retirement(context, reference, *, terminal_validator=lambda *_: None):
    from dpone.adapters.composition_mssql_operations import _occurrence
    from dpone.adapters.composition_mssql_ownership import read_shared_owner_in

    observed = read_shared_owner_in(context, reference, expected_service_id=SERVICE_ID)
    assert observed is not None
    return kernel.require_retired_execution_in(
        context,
        _occurrence(observed),
        expected_service_id=SERVICE_ID,
        terminal_validator=terminal_validator,
    )


@pytest.mark.parametrize("damage", ["old_partition", "successor_partition", "successor_original", "retired_running"])
def test_historical_phase_still_rejects_incomplete_or_malformed_global_originals(damage):
    context, reference, old = retired_with_successor()
    key = old.attempt_sha256
    if damage in {"old_partition", "successor_partition"}:
        chosen = key if damage == "old_partition" else next(k for k in context.operations if k != key)
        context.operation_domains = [row for row in context.operation_domains if row[0] != chosen]
    elif damage == "successor_original":
        chosen = next(k for k in context.operations if k != key)
        record = context.operations[chosen]
        context.operations[chosen] = (*record[:5], record[5] + b" ", *record[6:])
    else:
        record = context.operations[key]
        context.operations[key] = (*record[:6], "RUNNING", None, None, None)
    with pytest.raises(CompositionAdmissionError):
        inspect_retirement(context, reference)


def test_retired_terminal_callback_cannot_replace_the_actual_transaction():
    context, reference, _ = retired_with_successor()

    def replaced(*_):
        context.transaction = (1, 1, "Exclusive", 8)

    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        inspect_retirement(context, reference, terminal_validator=replaced)


def test_terminal_proof_refusal_still_blocks_retired_owner_read():
    context, reference, _ = retired_with_successor()

    def rejected(*_):
        raise CompositionAdmissionError("missing_original_proof")

    with pytest.raises(CompositionAdmissionError, match="missing_original_proof"):
        inspect_retirement(context, reference, terminal_validator=rejected)


def test_full_history_scan_must_contain_the_exact_retired_owner(monkeypatch):
    context, reference, _ = retired_with_successor()
    original = kernel.iter_shared_owners_in

    def missing(*args, **kwargs):
        return (owner for owner in original(*args, **kwargs) if owner.reference != reference)

    monkeypatch.setattr(kernel, "iter_shared_owners_in", missing)
    with pytest.raises(CompositionAdmissionError, match="occurrence_identity"):
        inspect_retirement(context, reference)
