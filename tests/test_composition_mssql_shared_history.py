"""Complete history, independent workload scope and transaction continuity tests.

Injected callbacks only model the terminal-verifier boundary. They never stand
in for protected original proof, issuance or gate certification.
"""

from dataclasses import replace
from typing import Any

import pytest

from dpone.adapters.composition_mssql_operations import (
    iter_shared_operations_in,
    read_shared_operation_in,
    require_execution_attempt_in,
    require_execution_history_in,
)
from dpone.adapters.composition_mssql_ownership import (
    iter_shared_owners_in,
    read_shared_owner_in,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_store_helpers import SERVICE_ID, mixed_request
from tests.test_composition_mssql_operations import add_operation, attempt_for
from tests.test_composition_mssql_ownership import SharedSql
from tests.test_composition_qualification_operation import operation as qualification_operation
from tests.test_composition_qualification_operation import owner as qualification_owner


def no_terminal(context: Any, occurrence: Any, receipt: Any) -> None:
    raise AssertionError("No relevant execution terminal is present")


def overlapping_qualification() -> Any:
    request = mixed_request()
    resource = next(r for r in request.resources if r.connector == "mssql")
    original = qualification_owner()
    claims = tuple(
        sorted(
            (
                replace(c, service_id=resource.service_id, physical_subject_sha256=resource.physical_subject_sha256)
                if c.connector == "mssql"
                else c
                for c in original.claims
            ),
            key=lambda c: c.guard_id,
        )
    )
    return replace(original, claims=claims)


def test_disjoint_running_workload_allows_fresh_exact_selected_attempt() -> None:
    database = SharedSql()
    database.add_owner()
    existing = add_operation(database, attempt_for(database, workload_id="c_generated_данные"))
    attempt = attempt_for(database)
    assert not set(attempt.guard_epochs).intersection(existing.guard_epochs)
    occurrence = require_execution_attempt_in(
        database, attempt, expected_service_id=SERVICE_ID, terminal_validator=no_terminal
    )
    assert occurrence.request == mixed_request() and occurrence.receipt.state == "ACTIVE"
    assert len(database.operations) == 1  # Reading never reserves the fresh attempt.


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_overlapping_nonterminal_operations_block_fresh_and_whole_history(state: str) -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database, state=state)
    with pytest.raises(CompositionAdmissionError, match="attempt_conflict"):
        require_execution_attempt_in(
            database,
            attempt_for(database, try_number=2),
            expected_service_id=SERVICE_ID,
            terminal_validator=no_terminal,
        )
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        require_execution_history_in(
            database, mixed_request(), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
        )


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_every_existing_exact_operation_rejects_fresh_replay(state: str) -> None:
    database = SharedSql()
    database.add_owner()
    original = add_operation(database, state=state)
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        require_execution_attempt_in(database, original, expected_service_id=SERVICE_ID, terminal_validator=no_terminal)


@pytest.mark.parametrize("state", ["PREPARED", "RETIRING", "RETIRED"])
def test_fresh_attempt_needs_observed_active_parent(state: str) -> None:
    database = SharedSql()
    reference = database.add_owner()
    attempt = attempt_for(database)
    database.owners[reference.owner_key] = (*database.owners[reference.owner_key][:5], state)
    if state == "RETIRED":
        database.domains = {g: (*r[:4], None) for g, r in database.domains.items()}
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        require_execution_attempt_in(database, attempt, expected_service_id=SERVICE_ID, terminal_validator=no_terminal)


@pytest.mark.parametrize("state", ["PREPARED", "ACTIVE", "SEALING", "SEALED", "TRANSFERRED", "RETIRED"])
def test_every_overlapping_qualification_state_blocks_without_operations(state: str) -> None:
    database = SharedSql()
    database.add_owner(overlapping_qualification(), state)
    with pytest.raises(CompositionAdmissionError, match="shared_qualification_blocker"):
        require_execution_history_in(
            database, mixed_request(), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
        )


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN", "SUCCEEDED", "FAILED"])
def test_unrelated_well_formed_qualification_operations_do_not_deny_execution(state: str) -> None:
    database = SharedSql()
    database.add_owner()
    database.add_owner(qualification_owner())
    add_operation(database, qualification_operation(), state=state)
    require_execution_attempt_in(
        database, attempt_for(database), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
    )
    require_execution_history_in(
        database, mixed_request(), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
    )


@pytest.mark.parametrize("state", ["TRANSFERRED", "RETIRED"])
def test_released_qualification_original_still_blocks_selected_reacquired_scope(state: str) -> None:
    database = SharedSql()
    database.add_owner(overlapping_qualification(), state)
    database.add_owner()
    with pytest.raises(CompositionAdmissionError, match="shared_qualification_blocker"):
        require_execution_attempt_in(
            database, attempt_for(database), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
        )
    # The other workload does not touch the qualification's retained MSSQL scope.
    require_execution_attempt_in(
        database,
        attempt_for(database, workload_id="c_generated_данные"),
        expected_service_id=SERVICE_ID,
        terminal_validator=no_terminal,
    )


@pytest.mark.parametrize("partition", ["owner", "operation"])
def test_deleted_unrelated_qualification_partition_rejects_globally(partition: str) -> None:
    database = SharedSql()
    database.add_owner()
    reference = database.add_owner(qualification_owner())
    add_operation(database, qualification_operation())
    if partition == "owner":
        database.owner_domains = [
            r
            for r in database.owner_domains
            if not (r[0] == reference.owner_key and r[1] == qualification_owner().claims[0].guard_id)
        ]
    else:
        database.operation_domains.pop()
    with pytest.raises(CompositionAdmissionError, match="guard_partition|attempt_partition"):
        require_execution_attempt_in(
            database, attempt_for(database), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
        )


@pytest.mark.parametrize(
    "iterator,table", [(iter_shared_owners_in, "owners"), (iter_shared_operations_in, "operations")]
)
@pytest.mark.parametrize(
    "change,reason",
    [
        ((0, 0, "NoLock", None), "shared_transaction"),
        ((1, 1, "NoLock", 7), "shared_transaction"),
        ((1, 1, "Exclusive", 8), "shared_transaction_identity"),
    ],
)
def test_stream_resume_checks_actual_transaction_and_lock(iterator: Any, table: str, change: Any, reason: str) -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database)
    stream = iterator(database, expected_service_id=SERVICE_ID)
    next(stream)
    database.transaction = change
    with pytest.raises(CompositionAdmissionError, match=reason):
        next(stream)


@pytest.mark.parametrize(
    "iterator,column", [(iter_shared_owners_in, "owner_key"), (iter_shared_operations_in, "operation_key")]
)
def test_iterator_rejects_nonmonotonic_next_page(iterator: Any, column: str) -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database)
    stream = iterator(database, expected_service_id=SERVICE_ID)
    next(stream)
    record = next(iter(database.owners.values() if column == "owner_key" else database.operations.values()))

    def repeat_page(context: SharedSql, sql: str, parameters: Any) -> None:
        if f"WHERE {column}>?" in sql:
            context.results = [record]

    database.after_execute = repeat_page
    with pytest.raises(CompositionAdmissionError, match="shared_owner_order|shared_operation_order"):
        next(stream)


@pytest.mark.parametrize(
    "change,reason",
    [
        ((0, 0, "NoLock", None), "shared_transaction"),
        ((1, 1, "NoLock", 7), "shared_transaction"),
        ((1, 1, "Exclusive", 9), "shared_transaction_identity"),
    ],
)
def test_terminal_callback_cannot_replace_transaction_or_release_lock(change: Any, reason: str) -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database, state="SUCCEEDED")

    def terminal(context: SharedSql, occurrence: Any, receipt: Any) -> None:
        assert context is database and receipt.state == "SUCCEEDED"
        context.transaction = change

    with pytest.raises(CompositionAdmissionError, match=reason):
        require_execution_attempt_in(
            database, attempt_for(database, try_number=2), expected_service_id=SERVICE_ID, terminal_validator=terminal
        )


def test_streaming_history_consumes_every_original_across_nested_callback_cursor_use() -> None:
    database = SharedSql()
    reference = database.add_owner()
    expected = {
        add_operation(database, attempt_for(database, try_number=n), state="SUCCEEDED").attempt_sha256
        for n in range(1, 65)
    }
    observed: list[str] = []

    def terminal(context: SharedSql, occurrence: Any, receipt: Any) -> None:
        assert context is database and occurrence.request == mixed_request()
        reread = read_shared_owner_in(context, reference, expected_service_id=SERVICE_ID)
        assert reread is not None and reread.subject == occurrence.request
        observed.append(receipt.attempt.attempt_sha256)

    require_execution_attempt_in(
        database, attempt_for(database, try_number=65), expected_service_id=SERVICE_ID, terminal_validator=terminal
    )
    assert observed == sorted(expected)
    pages = [sql for sql, _ in database.statements if "ORDER BY operation_key;" in sql]
    assert len(pages) == 65 and all("TOP (1)" in sql for sql in pages)


def test_later_unrelated_operation_corruption_cannot_be_skipped() -> None:
    database = SharedSql()
    database.add_owner()
    for n in range(1, 9):
        add_operation(database, attempt_for(database, workload_id="c_generated_данные", try_number=n))
    final_key = max(database.operations)
    row = database.operations[final_key]
    database.operations[final_key] = (*row[:5], row[5] + b"\n", *row[6:])
    with pytest.raises(CompositionAdmissionError, match="attempt_persistence_readback"):
        require_execution_attempt_in(
            database, attempt_for(database), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
        )
    assert len([sql for sql, _ in database.statements if "ORDER BY operation_key;" in sql]) == 8


def test_terminal_label_never_substitutes_for_injected_original_proof_verification() -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database, state="SUCCEEDED")

    def reject_proof(context: Any, occurrence: Any, receipt: Any) -> None:
        raise CompositionAdmissionError("terminal_proof_identity")

    with pytest.raises(CompositionAdmissionError, match="terminal_proof_identity"):
        require_execution_history_in(
            database, mixed_request(), expected_service_id=SERVICE_ID, terminal_validator=reject_proof
        )


def test_rebound_same_request_hash_still_compares_exact_original_bytes() -> None:
    database = SharedSql()
    original = mixed_request()
    changed = replace(
        original, workloads=tuple(replace(w, workload_id=w.workload_id + "\\x") for w in original.workloads)
    )
    slash = replace(original, workloads=tuple(replace(w, workload_id=w.workload_id + "/x") for w in original.workloads))
    assert changed.request_sha256 == slash.request_sha256
    database.add_owner(changed)
    with pytest.raises(CompositionAdmissionError, match="occurrence_identity"):
        require_execution_history_in(database, slash, expected_service_id=SERVICE_ID, terminal_validator=no_terminal)


def test_failed_or_changed_authority_precedes_any_original_read() -> None:
    database = SharedSql()
    reference = database.add_owner()
    database.authority = [(1, -1, SERVICE_ID)]
    with pytest.raises(CompositionAdmissionError, match="control_authority"):
        read_shared_owner_in(database, reference, expected_service_id=SERVICE_ID)
    assert not any("composition_owners]" in sql for sql, _ in database.statements)


def test_orphan_partitions_reject_even_without_any_original_page() -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database)
    database.operations.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_partition"):
        list(iter_shared_operations_in(database, expected_service_id=SERVICE_ID))
    database.owners.clear()
    with pytest.raises(CompositionAdmissionError, match="guard_partition"):
        list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID))


def test_empty_operation_end_page_also_requires_transaction_continuity() -> None:
    database = SharedSql()

    def replace_transaction(context: SharedSql, sql: str, parameters: Any) -> None:
        if "ORDER BY operation_key;" in sql:
            context.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = replace_transaction
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        list(iter_shared_operations_in(database, expected_service_id=SERVICE_ID))


def test_operation_read_rejects_replacement_during_nested_owner_read() -> None:
    database = SharedSql()
    database.add_owner()
    attempt = add_operation(database)

    def replace_transaction(context: SharedSql, sql: str, parameters: Any) -> None:
        if "operation_document END" in sql:
            context.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = replace_transaction
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        read_shared_operation_in(database, attempt.attempt_sha256, expected_service_id=SERVICE_ID)


def test_deleted_whole_owner_history_cannot_hide_retained_domain_epoch() -> None:
    database = SharedSql()
    database.add_owner(qualification_owner(), "RETIRED")
    database.owners.clear()
    database.owner_domains.clear()
    with pytest.raises(CompositionAdmissionError, match="guard_partition"):
        list(iter_shared_owners_in(database, expected_service_id=SERVICE_ID))


def test_retired_intersecting_parent_requires_terminal_proof_for_disjoint_workload() -> None:
    database = SharedSql()
    original = mixed_request()
    reference = database.add_owner(original)
    add_operation(database, attempt_for(database), state="SUCCEEDED")
    disjoint = add_operation(database, attempt_for(database, workload_id="c_generated_данные"), state="SUCCEEDED")
    database.owners[reference.owner_key] = (*database.owners[reference.owner_key][:5], "RETIRED")
    database.domains = {guard: (*row[:4], None) for guard, row in database.domains.items()}
    successor = replace(
        original, context=replace(original.context, activation_id="55555555-5555-4555-8555-555555555555")
    )
    database.add_owner(successor)
    selected = attempt_for(database)
    assert not {guard for guard, _ in selected.guard_epochs}.intersection(guard for guard, _ in disjoint.guard_epochs)

    def reject_disjoint_proof(context: Any, occurrence: Any, receipt: Any) -> None:
        assert occurrence.receipt.state == "RETIRED"
        if receipt.attempt == disjoint:
            raise CompositionAdmissionError("terminal_proof_identity")

    with pytest.raises(CompositionAdmissionError, match="terminal_proof_identity"):
        require_execution_attempt_in(
            database, selected, expected_service_id=SERVICE_ID, terminal_validator=reject_disjoint_proof
        )


def test_active_parent_disjoint_terminal_workload_requires_no_selected_scope_callback() -> None:
    database = SharedSql()
    database.add_owner()
    add_operation(database, attempt_for(database, workload_id="c_generated_данные"), state="SUCCEEDED")
    require_execution_attempt_in(
        database, attempt_for(database), expected_service_id=SERVICE_ID, terminal_validator=no_terminal
    )
