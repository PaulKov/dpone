"""Inspect retained execution under its original transaction and complete history.

This guard grants no fresh admission, worker permit or issuance authority. It
reopens the existing operation before considering its narrow conflict exemption,
and observes it again after the complete mixed-family scan. Calling phases keep
explicit state requirements: issuance needs ACTIVE/RUNNING, while closing and
recovery may inspect a retained RETIRING occurrence without renewing admission.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.adapters.composition_mssql_operations import (
    _occurrence,
    _receipt,
    iter_shared_operations_in,
    read_shared_operation_in,
)
from dpone.adapters.composition_mssql_ownership import SharedOwnerRecord, iter_shared_owners_in, read_shared_owner_in
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_activation import CompositionActivationOccurrence, CompositionAdmissionError
from dpone.contracts.composition_ownership import CompositionOwnerReference
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    encode_attempt_identity,
    require_composition_attempt_scope,
)

if TYPE_CHECKING:
    from dpone.ports.composition_sql import CompositionSqlContext, ExecutionTerminalValidator


def _require_owner_history(owner: SharedOwnerRecord, observed: SharedOwnerRecord, guards: frozenset[str]) -> None:
    """Compare each reopened owner before applying any operation-scope exemption."""
    if owner.reference == observed.reference:
        if owner != observed:
            raise CompositionAdmissionError("occurrence_identity")
    elif guards.intersection(guard for guard, _ in owner.guard_epochs):
        if owner.reference.owner_kind == "qualification":
            raise CompositionAdmissionError("shared_qualification_blocker")
        if owner.state != "RETIRED" and observed.state != "RETIRED":
            raise CompositionAdmissionError("historical_occurrence_unresolved")


def require_existing_execution_in(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    *,
    expected_service_id: str,
    terminal_validator: ExecutionTerminalValidator,
) -> tuple[CompositionActivationOccurrence, CompositionAttemptReceipt]:
    """Return only an unchanged independently observed operation and occurrence.

    The owning boundary audits the exact catalog before this read. Every original
    owner and operation is streamed under the same independently observed actual
    transaction. Only the exact already observed nonretired operation is exempt
    from competing-operation rejection and proof verification. An independently
    observed RETIRED parent instead permits structurally valid current execution
    successors; all overlapping retired owners retain complete terminal closure,
    including formerly disjoint operations. Overlapping qualification still blocks.
    Private kernel projections preserve the existing execution receipt contracts;
    neither they nor a caller-supplied digest establish durable acknowledgement.
    """
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(attempt) is not CompositionAttemptIdentity:
        raise CompositionAdmissionError("attempt_identity")
    original = encode_attempt_identity(attempt)
    observed = read_shared_operation_in(context, attempt.attempt_sha256, expected_service_id=expected_service_id)
    if observed is None:
        raise CompositionAdmissionError("attempt_missing")
    if observed.identity != attempt or observed.identity_document != original:
        raise CompositionAdmissionError("attempt_identity")
    occurrence, receipt = _occurrence(observed.owner), _receipt(observed)
    if occurrence.receipt.state not in {"ACTIVE", "RETIRING", "RETIRED"}:
        raise CompositionAdmissionError("occurrence_state")
    guards = require_composition_attempt_scope(occurrence, receipt.attempt)
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    found_owner = False
    for owner in iter_shared_owners_in(context, expected_service_id=expected_service_id):
        _require_owner_history(owner, observed.owner, guards)
        if owner.reference == observed.owner.reference:
            if found_owner:
                raise CompositionAdmissionError("occurrence_identity")
            found_owner = True
    if not found_owner:
        raise CompositionAdmissionError("attempt_parent")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    found_operation = False
    for operation in iter_shared_operations_in(context, expected_service_id=expected_service_id):
        _require_owner_history(operation.owner, observed.owner, guards)
        current = operation.operation_key == observed.operation_key
        if current:
            if found_operation or operation != observed:
                raise CompositionAdmissionError("attempt_identity")
            found_operation = True
        if operation.owner.reference.owner_kind == "qualification":
            continue
        candidate = _receipt(operation)
        retired = operation.owner.state == "RETIRED"
        if retired and candidate.state not in {"SUCCEEDED", "FAILED"}:
            raise CompositionAdmissionError("unresolved_attempt")
        if not retired and (current or observed.owner.state == "RETIRED"):
            # Independently observed retirement makes this a historical read.
            # Successor activity cannot invalidate the original closed writer.
            continue
        overlaps = bool(guards.intersection(guard for guard, _ in candidate.attempt.guard_epochs))
        if candidate.state not in {"SUCCEEDED", "FAILED"}:
            if overlaps:
                raise CompositionAdmissionError("attempt_conflict")
        elif overlaps or (retired and guards.intersection(guard for guard, _ in operation.owner.guard_epochs)):
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            terminal_validator(context, _occurrence(operation.owner), candidate)
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    if not found_operation:
        raise CompositionAdmissionError("attempt_missing")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    # Reopen the complete owner, operation and partitions after callbacks too.
    final = read_shared_operation_in(context, observed.operation_key, expected_service_id=expected_service_id)
    if final is None:
        raise CompositionAdmissionError("attempt_missing")
    if final != observed:
        raise CompositionAdmissionError("attempt_identity")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return occurrence, receipt


def require_retired_execution_in(
    context: CompositionSqlContext,
    occurrence: CompositionActivationOccurrence,
    *,
    expected_service_id: str,
    terminal_validator: ExecutionTerminalValidator,
) -> None:
    """Audit retained retirement while legitimate execution successors may run.

    Unlike fresh acquisition/retirement mutation, this read never asks a successor
    to retire. Every original is still structurally read, unsupported overlapping
    qualification blocks, and every operation of each overlapping retired owner
    needs its complete original proof. No current activity is business evidence.
    """
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(occurrence) is not CompositionActivationOccurrence:
        raise CompositionAdmissionError("occurrence_identity")
    occurrence.require_state("RETIRED")
    reference = CompositionOwnerReference("execution", occurrence.request.activation_id)
    observed = read_shared_owner_in(context, reference, expected_service_id=expected_service_id)
    if observed is None or _occurrence(observed) != occurrence:
        raise CompositionAdmissionError("occurrence_identity")
    guards = frozenset(guard for guard, _ in observed.guard_epochs)
    found = False
    for owner in iter_shared_owners_in(context, expected_service_id=expected_service_id):
        _require_owner_history(owner, observed, guards)
        if owner.reference == reference:
            if found:
                raise CompositionAdmissionError("occurrence_identity")
            found = True
    if not found:
        raise CompositionAdmissionError("occurrence_identity")
    for operation in iter_shared_operations_in(context, expected_service_id=expected_service_id):
        _require_owner_history(operation.owner, observed, guards)
        if operation.owner.reference.owner_kind != "execution" or operation.owner.state != "RETIRED":
            continue
        receipt = _receipt(operation)
        if receipt.state not in {"SUCCEEDED", "FAILED"}:
            raise CompositionAdmissionError("unresolved_attempt")
        if guards.intersection(guard for guard, _ in operation.owner.guard_epochs):
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            terminal_validator(context, _occurrence(operation.owner), receipt)
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    final = read_shared_owner_in(context, reference, expected_service_id=expected_service_id)
    if final != observed:
        raise CompositionAdmissionError("occurrence_identity")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
