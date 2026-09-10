"""Existing-operation guard regressions; SQL doubles confer no live certification."""

from dataclasses import replace
from typing import cast

import pytest

from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_operations import require_execution_attempt_in
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.ports.composition_sql import CompositionSqlContext
from tests.composition_mssql_store_helpers import SERVICE_ID, domain_guard, mixed_request
from tests.test_composition_activation_contract import digest
from tests.test_composition_mssql_operations import add_operation, attempt_for
from tests.test_composition_mssql_ownership import SharedSql
from tests.test_composition_mssql_shared_history import no_terminal, overlapping_qualification
from tests.test_composition_qualification_operation import operation as qualification_operation
from tests.test_composition_qualification_operation import owner as qualification_owner


def test_exact_running_existing_operation_can_be_inspected_without_fresh_admission() -> None:
    context = SharedSql()
    context.add_owner()
    attempt = add_operation(context)
    # The shared double self-types its mutable cursor; this use only reads it.
    occurrence, receipt = require_existing_execution_in(
        cast(CompositionSqlContext, context), attempt, expected_service_id=SERVICE_ID, terminal_validator=no_terminal
    )
    assert occurrence.request == mixed_request() and occurrence.receipt.state == "ACTIVE"
    assert receipt.attempt == attempt and receipt.state == "RUNNING"
    assert context.operations[attempt.attempt_sha256][5] == encode_attempt_identity(attempt)
    assert len(context.operations) == 1


def _journal(*, owner_state="ACTIVE", state="RUNNING"):
    context = SharedSql()
    reference = context.add_owner()
    attempt = add_operation(context, state=state)
    _owner_state(context, reference, owner_state)
    return context, attempt, reference


def _owner_state(context, reference, state):
    context.owners[reference.owner_key] = (*context.owners[reference.owner_key][:5], state)
    if state == "RETIRED":
        context.domains = {guard: (*row[:4], None) for guard, row in context.domains.items()}


def _inspect(context, attempt, terminal_validator=no_terminal):
    return require_existing_execution_in(
        context, attempt, expected_service_id=SERVICE_ID, terminal_validator=terminal_validator
    )


def test_fresh_admission_still_rejects_the_same_retained_operation():
    context, attempt, _ = _journal()
    _inspect(context, attempt)
    with pytest.raises(CompositionAdmissionError) as caught:
        require_execution_attempt_in(context, attempt, expected_service_id=SERVICE_ID, terminal_validator=no_terminal)
    assert caught.value.reason == "attempt_replay"


@pytest.mark.parametrize("owner_state", ["ACTIVE", "RETIRING"])
@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_nonretired_current_receipt_is_phase_neutral_without_terminal_callback(owner_state, state):
    context, attempt, _ = _journal(owner_state=owner_state, state=state)
    occurrence, receipt = _inspect(context, attempt)
    assert occurrence.receipt.state == owner_state and receipt.state == state
    if owner_state == "RETIRING":
        with pytest.raises(CompositionAdmissionError) as caught:
            occurrence.require_state("ACTIVE")
        assert caught.value.reason == "occurrence_state"


def test_prepared_parent_with_an_operation_is_not_recovery_ready():
    context, attempt, _ = _journal(owner_state="PREPARED")
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "occurrence_state"


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_disjoint_active_workload_does_not_require_other_operation_terminal_callback(state):
    context, attempt, _ = _journal()
    add_operation(context, attempt_for(context, workload_id="c_generated_данные"), state=state)
    occurrence, receipt = _inspect(context, attempt)
    assert occurrence.receipt.state == "ACTIVE" and receipt.attempt == attempt


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_only_the_observed_operation_is_exempt_from_overlapping_nonterminal_conflict(state):
    context, attempt, _ = _journal()
    add_operation(context, attempt_for(context, try_number=2), state=state)
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_conflict"


def test_other_overlapping_terminal_operation_uses_exact_original_callback_scope():
    context, attempt, _ = _journal()
    other = add_operation(context, attempt_for(context, try_number=2), state="FAILED")
    seen = []

    def terminal(observed_context, occurrence, receipt):
        assert observed_context is context and occurrence.request == mixed_request()
        seen.append(receipt)

    _, receipt = _inspect(context, attempt, terminal)
    assert receipt.attempt == attempt and len(seen) == 1
    assert seen[0].attempt == other and seen[0].state == "FAILED"


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_retired_current_owner_cannot_exempt_its_nonterminal_operation(state):
    context, attempt, _ = _journal(owner_state="RETIRED", state=state)
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "unresolved_attempt"


def test_retired_current_owner_requires_every_operation_proof_including_current_and_disjoint():
    context, attempt, reference = _journal(state="SUCCEEDED")
    disjoint = add_operation(context, attempt_for(context, workload_id="c_generated_данные"), state="FAILED")
    _owner_state(context, reference, "RETIRED")
    seen = []

    def terminal(observed_context, occurrence, receipt):
        assert observed_context is context and occurrence.receipt.state == "RETIRED"
        seen.append(receipt.attempt)

    occurrence, receipt = _inspect(context, attempt, terminal)
    assert occurrence.receipt.state == "RETIRED" and receipt.attempt == attempt
    assert {value.attempt_sha256 for value in seen} == {attempt.attempt_sha256, disjoint.attempt_sha256}


def test_retired_disjoint_terminal_proof_rejection_cannot_be_skipped():
    context, attempt, reference = _journal(state="SUCCEEDED")
    disjoint = add_operation(context, attempt_for(context, workload_id="c_generated_данные"), state="FAILED")
    _owner_state(context, reference, "RETIRED")

    def reject_disjoint(observed_context, occurrence, receipt):
        if receipt.attempt == disjoint:
            raise CompositionAdmissionError("terminal_proof_identity")

    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt, reject_disjoint)
    assert caught.value.reason == "terminal_proof_identity"


def test_previous_retired_intersecting_parent_requires_its_disjoint_terminal_proof():
    context, previous, reference = _journal(state="SUCCEEDED")
    disjoint = add_operation(context, attempt_for(context, workload_id="c_generated_данные"), state="SUCCEEDED")
    _owner_state(context, reference, "RETIRED")
    original = mixed_request()
    successor = replace(
        original, context=replace(original.context, activation_id="55555555-5555-4555-8555-555555555555")
    )
    context.add_owner(successor)
    current = add_operation(context)

    def reject_disjoint(observed_context, occurrence, receipt):
        assert receipt.attempt != current
        if receipt.attempt == disjoint:
            raise CompositionAdmissionError("terminal_proof_identity")

    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, current, reject_disjoint)
    assert caught.value.reason == "terminal_proof_identity"


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "SEALING", "SEALED", "TRANSFERRED", "RETIRED"])
def test_every_overlapping_qualification_state_blocks_existing_inspection(state):
    context, attempt, _ = _journal(owner_state="RETIRED", state="SUCCEEDED")
    context.add_owner(overlapping_qualification(), state)
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "shared_qualification_blocker"


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_unrelated_well_formed_qualification_history_allows_existing_inspection(state):
    context, attempt, _ = _journal()
    context.add_owner(qualification_owner())
    add_operation(context, qualification_operation(), state=state)
    assert _inspect(context, attempt)[1].attempt == attempt


def test_missing_existing_operation_is_not_admission():
    context, attempt, _ = _journal()
    context.operations.clear()
    context.operation_domains.clear()
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_missing"


def test_same_normalized_hash_requires_exact_original_caller_bytes():
    context, attempt, _ = _journal()
    changed = replace(attempt, dag_run_id=attempt.dag_run_id.replace("\\", "/"))
    assert changed.attempt_sha256 == attempt.attempt_sha256
    assert encode_attempt_identity(changed) != encode_attempt_identity(attempt)
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, changed)
    assert caught.value.reason == "attempt_identity"


def test_full_operation_scan_must_actually_contain_current_original():
    context, attempt, _ = _journal()

    def remove_before_scan(database, sql, parameters):
        if "SELECT TOP (1) p.guard_id FROM [dpone_control].[composition_operation_domains]" in sql:
            database.operations.clear()
            database.operation_domains.clear()

    context.after_execute = remove_before_scan
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_missing"


def test_same_hash_original_substitution_during_scan_rejects():
    context, attempt, _ = _journal()
    changed = replace(attempt, dag_run_id=attempt.dag_run_id.replace("\\", "/"))

    def substitute(database, sql, parameters):
        if "ORDER BY operation_key;" in sql and database.results:
            row = database.operations[attempt.attempt_sha256]
            replacement = (*row[:5], encode_attempt_identity(changed), *row[6:])
            database.operations[attempt.attempt_sha256] = replacement
            database.results = [replacement]

    context.after_execute = substitute
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_identity"


def test_changed_current_owner_during_owner_scan_rejects():
    context, attempt, reference = _journal()

    def change_owner(database, sql, parameters):
        if "ORDER BY owner_key;" in sql and database.results:
            _owner_state(database, reference, "RETIRING")
            database.results = [database.owners[reference.owner_key]]

    context.after_execute = change_owner
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "occurrence_identity"


@pytest.mark.parametrize("partition", ["owner", "operation"])
def test_deleted_unrelated_qualification_partition_rejects_globally(partition):
    context, attempt, _ = _journal()
    reference = context.add_owner(qualification_owner())
    qualification = add_operation(context, qualification_operation())
    if partition == "owner":
        context.owner_domains = [row for row in context.owner_domains if row[0] != reference.owner_key]
    else:
        context.operation_domains = [row for row in context.operation_domains if row[0] != qualification.operation_key]
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason in {"guard_partition", "attempt_partition"}


def test_deleted_later_disjoint_operation_partition_never_passes_partial_history():
    context, attempt, _ = _journal()
    others = [
        add_operation(context, attempt_for(context, workload_id="c_generated_данные", try_number=n))
        for n in range(1, 10)
    ]
    last = max(other.attempt_sha256 for other in others)
    context.operation_domains = [row for row in context.operation_domains if row[0] != last]
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_partition"


@pytest.mark.parametrize(
    "transaction,reason",
    [
        ((0, 0, "NoLock", None), "shared_transaction"),
        ((1, 1, "NoLock", 7), "shared_transaction"),
        ((1, 1, "Exclusive", 9), "shared_transaction_identity"),
    ],
)
def test_terminal_callback_cannot_end_replace_or_unlock_transaction(transaction, reason):
    context, attempt, _ = _journal()
    add_operation(context, attempt_for(context, try_number=2), state="SUCCEEDED")

    def terminal(database, occurrence, receipt):
        database.transaction = transaction

    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt, terminal)
    assert caught.value.reason == reason


@pytest.mark.parametrize("damage", ["owner", "receipt", "document", "missing"])
def test_final_full_read_catches_current_record_changed_after_its_scan_page(damage):
    context, attempt, reference = _journal()
    other = next(
        attempt_for(context, try_number=n)
        for n in range(2, 100)
        if attempt_for(context, try_number=n).attempt_sha256 > attempt.attempt_sha256
    )
    add_operation(context, other, state="SUCCEEDED")

    def mutate_after_current_page(database, occurrence, receipt):
        assert receipt.attempt == other
        row = database.operations[attempt.attempt_sha256]
        if damage == "owner":
            _owner_state(database, reference, "RETIRING")
        elif damage == "receipt":
            database.operations[attempt.attempt_sha256] = (*row[:6], "COMMIT_UNKNOWN", *row[7:])
        elif damage == "document":
            changed = replace(attempt, dag_run_id=attempt.dag_run_id.replace("\\", "/"))
            database.operations[attempt.attempt_sha256] = (*row[:5], encode_attempt_identity(changed), *row[6:])
        else:
            database.operations.pop(attempt.attempt_sha256)
            database.operation_domains = [r for r in database.operation_domains if r[0] != attempt.attempt_sha256]

    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt, mutate_after_current_page)
    assert caught.value.reason == ("attempt_missing" if damage == "missing" else "attempt_identity")


def test_invalid_authority_rejects_before_any_original_read():
    context, attempt, _ = _journal()
    context.authority = [(1, -1, SERVICE_ID)]
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "control_authority"
    assert not any("composition_operations]" in sql for sql, _ in context.statements)


def test_unrelated_retired_execution_owner_needs_only_structural_readback():
    context, prior, reference = _journal(state="SUCCEEDED")
    _owner_state(context, reference, "RETIRED")
    original = mixed_request()
    resources = []
    for resource in original.resources:
        physical = digest("unrelated:" + resource.guard_id)
        resources.append(
            replace(
                resource,
                physical_subject_sha256=physical,
                guard_id=domain_guard(resource.connector, resource.service_id, physical),
            )
        )
    successor = replace(
        original,
        context=replace(original.context, activation_id="55555555-5555-4555-8555-555555555555"),
        resources=tuple(sorted(resources, key=lambda resource: resource.guard_id)),
    )
    context.add_owner(successor)
    current = add_operation(context)
    assert not {guard for guard, _ in current.guard_epochs}.intersection(guard for guard, _ in prior.guard_epochs)
    assert _inspect(context, current)[1].attempt == current


def test_complete_owner_scan_must_contain_the_observed_parent():
    context, attempt, _ = _journal()

    def omit_parent(database, sql, parameters):
        if "ORDER BY owner_key;" in sql:
            database.results = []

    context.after_execute = omit_parent
    with pytest.raises(CompositionAdmissionError) as caught:
        _inspect(context, attempt)
    assert caught.value.reason == "attempt_parent"
