"""Complete mixed-family operation audits inside the caller's pinned transaction.

These readers never reserve an operation or acknowledge execution. Fresh attempt
admission rejects every replay. Existing-operation issuance and recovery retain
separate root-owned guards; qualification terminal verification is unavailable.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.adapters.composition_mssql_ownership import (
    SharedOwnerRecord,
    _read_owner_key_in,
    _rows,
    _table,
    iter_shared_owners_in,
)
from dpone.adapters.composition_mssql_transaction import require_shared_transaction_in
from dpone.contracts.composition_control import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptReceipt,
    decode_attempt_identity,
    encode_activation_request,
    require_composition_attempt_scope,
    require_digest,
)
from dpone.contracts.composition_qualification_operation import (
    CompositionQualificationOperation,
    CompositionQualificationOwner,
)

if TYPE_CHECKING:
    from dpone.ports.composition_sql import CompositionSqlContext, ExecutionTerminalValidator


@dataclass(frozen=True, slots=True)
class SharedOperationRecord:
    """Exact original operation, its complete observed owner and proof selectors."""

    owner: SharedOwnerRecord
    identity: CompositionAttemptIdentity | CompositionQualificationOperation
    identity_document: bytes
    operation_key: str
    replay_key: str
    state: str
    closed_gates_sha256: str | None
    quiescence_sha256: str | None
    outcome_evidence_sha256: str | None


def _occurrence(owner: SharedOwnerRecord) -> CompositionActivationOccurrence:
    if type(owner.subject) is not CompositionActivationRequest:
        raise CompositionAdmissionError("attempt_parent")
    return CompositionActivationOccurrence(
        owner.subject, CompositionActivationReceipt(owner.subject_sha256, owner.state, owner.guard_epochs)
    )


def _receipt(operation: SharedOperationRecord) -> CompositionAttemptReceipt:
    if type(operation.identity) is not CompositionAttemptIdentity:
        raise CompositionAdmissionError("shared_qualification_blocker")
    return CompositionAttemptReceipt(
        operation.identity,
        operation.state,
        operation.closed_gates_sha256,
        operation.quiescence_sha256,
        operation.outcome_evidence_sha256,
    )


def _operation_columns() -> str:
    return (
        "operation_key,operation_family,owner_key,owner_subject_sha256,replay_key,"
        "CASE WHEN DATALENGTH(operation_document) BETWEEN 1 AND "
        "CASE operation_family WHEN 'execution' THEN 8388608 WHEN 'qualification' THEN 1048576 ELSE 0 END "
        "THEN operation_document END,state,closed_gates_sha256,quiescence_sha256,outcome_evidence_sha256"
    )


def _operation_record(
    context: CompositionSqlContext, row: tuple[Any, ...], *, expected_service_id: str
) -> SharedOperationRecord:
    if len(row) != 10:
        raise CompositionAdmissionError("attempt_identity")
    key, family, owner_key, parent, replay, document, state, closed, quiescent, outcome = row
    require_digest(key)
    require_digest(replay)
    owner = _read_owner_key_in(context, owner_key, expected_service_id=expected_service_id)
    if owner is None or (family, parent) != (owner.reference.owner_kind, owner.subject_sha256):
        raise CompositionAdmissionError("attempt_parent_identity")
    identity: CompositionAttemptIdentity | CompositionQualificationOperation
    if family == "execution":
        identity = decode_attempt_identity(document, key)
        require_composition_attempt_scope(_occurrence(owner), identity)
        expected_replay = identity.attempt_sha256
    else:
        if type(owner.subject) is not CompositionQualificationOwner:
            raise CompositionAdmissionError("attempt_parent_identity")
        identity = CompositionQualificationOperation.from_bytes(document, expected_sha256=key)
        identity.require_owner(owner.subject)
        expected_replay = identity.invocation_key
        selected = {guard for guard, _ in identity.guard_epochs}
        if identity.guard_epochs != tuple(pair for pair in owner.guard_epochs if pair[0] in selected):
            raise CompositionAdmissionError("attempt_guard_epochs")
    if replay != expected_replay:
        raise CompositionAdmissionError("attempt_identity")
    unique = _rows(
        context,
        f"SELECT TOP (2) operation_key FROM {_table(context, 'operations')} WITH (HOLDLOCK) "
        "WHERE operation_family=? AND replay_key=?;",
        family,
        replay,
    )
    if unique != ((key,),):
        raise CompositionAdmissionError("attempt_replay")
    partitions = _rows(
        context,
        f"SELECT TOP ({len(identity.guard_epochs) + 1}) owner_key,guard_id,fencing_epoch "
        f"FROM {_table(context, 'operation_domains')} WITH (HOLDLOCK) WHERE operation_key=? ORDER BY guard_id;",
        key,
    )
    if partitions != tuple((owner_key, guard, epoch) for guard, epoch in identity.guard_epochs) or any(
        type(row[2]) is not int or not 1 <= row[2] < 2**63 for row in partitions
    ):
        raise CompositionAdmissionError("attempt_partition")
    if type(state) is not str or state not in {"RUNNING", "SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
        raise CompositionAdmissionError("attempt_state")
    evidence = (closed, quiescent, outcome)
    if state in {"SUCCEEDED", "FAILED"} and any(value is None for value in evidence):
        raise CompositionAdmissionError("terminal_evidence")
    for digest in evidence:
        if digest is not None:
            require_digest(digest)
    return SharedOperationRecord(owner, identity, document, key, replay, state, closed, quiescent, outcome)


def read_shared_operation_in(
    context: CompositionSqlContext, operation_key: str, *, expected_service_id: str
) -> SharedOperationRecord | None:
    """Reopen a complete exact original; absence never means unsupported history."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    require_digest(operation_key)
    rows = _rows(
        context,
        f"SELECT TOP (2) {_operation_columns()} FROM {_table(context, 'operations')} WITH (HOLDLOCK) "
        "WHERE operation_key=?;",
        operation_key,
    )
    if len(rows) > 1:
        raise CompositionAdmissionError("attempt_identity")
    result = _operation_record(context, rows[0], expected_service_id=expected_service_id) if rows else None
    if result is None and _rows(
        context,
        f"SELECT TOP (1) guard_id FROM {_table(context, 'operation_domains')} WITH (HOLDLOCK) WHERE operation_key=?;",
        operation_key,
    ):
        raise CompositionAdmissionError("attempt_partition")
    if result is not None and result.operation_key != operation_key:
        raise CompositionAdmissionError("attempt_identity")
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return result


def iter_shared_operations_in(
    context: CompositionSqlContext, *, expected_service_id: str
) -> Iterator[SharedOperationRecord]:
    """Stream every original and retain only one bounded page and its parent."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    orphans = _rows(
        context,
        f"SELECT TOP (1) p.guard_id FROM {_table(context, 'operation_domains')} p WITH (HOLDLOCK) "
        f"LEFT JOIN {_table(context, 'operations')} o WITH (HOLDLOCK) ON o.operation_key=p.operation_key "
        f"LEFT JOIN {_table(context, 'owner_domains')} d WITH (HOLDLOCK) "
        "ON d.owner_key=p.owner_key AND d.guard_id=p.guard_id AND d.fencing_epoch=p.fencing_epoch "
        "WHERE o.operation_key IS NULL OR d.guard_id IS NULL OR o.owner_key<>p.owner_key;",
    )
    if orphans:
        raise CompositionAdmissionError("attempt_partition")
    prior: str | None = None
    while True:
        require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
        page = _rows(
            context,
            f"SELECT TOP (1) {_operation_columns()} FROM {_table(context, 'operations')} WITH (HOLDLOCK) "
            + ("" if prior is None else "WHERE operation_key>? ")
            + "ORDER BY operation_key;",
            *(() if prior is None else (prior,)),
        )
        if not page:
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            return
        if len(page) != 1 or len(page[0]) != 10:
            raise CompositionAdmissionError("attempt_identity")
        require_digest(page[0][0])
        if prior is not None and page[0][0] <= prior:
            raise CompositionAdmissionError("shared_operation_order")
        record = _operation_record(context, page[0], expected_service_id=expected_service_id)
        prior = record.operation_key
        require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
        yield record


def _require_history_in(
    context: CompositionSqlContext,
    request: CompositionActivationRequest,
    guards: frozenset[str],
    replay: str | None,
    *,
    expected_service_id: str,
    terminal_validator: ExecutionTerminalValidator,
) -> None:
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    original = encode_activation_request(request)
    for owner in iter_shared_owners_in(context, expected_service_id=expected_service_id):
        if owner.reference.owner_kind == "execution" and owner.reference.owner_id == request.activation_id:
            if (owner.subject_sha256, owner.subject_document) != (request.request_sha256, original):
                raise CompositionAdmissionError("occurrence_identity")
        elif guards.intersection(guard for guard, _ in owner.guard_epochs):
            if owner.reference.owner_kind == "qualification":
                raise CompositionAdmissionError("shared_qualification_blocker")
            if owner.state != "RETIRED":
                raise CompositionAdmissionError("historical_occurrence_unresolved")
    for operation in iter_shared_operations_in(context, expected_service_id=expected_service_id):
        if replay is not None and (
            operation.operation_key == replay
            or (operation.owner.reference.owner_kind == "execution" and operation.replay_key == replay)
        ):
            raise CompositionAdmissionError("attempt_replay")
        overlaps = bool(guards.intersection(guard for guard, _ in operation.identity.guard_epochs))
        if operation.owner.reference.owner_kind == "qualification":
            if overlaps:
                raise CompositionAdmissionError("shared_qualification_blocker")
            continue
        receipt = _receipt(operation)
        if receipt.state not in {"SUCCEEDED", "FAILED"}:
            if overlaps or operation.owner.state == "RETIRED":
                raise CompositionAdmissionError("attempt_conflict" if replay is not None else "unresolved_attempt")
        elif overlaps or (
            operation.owner.state == "RETIRED"
            and guards.intersection(guard for guard, _ in operation.owner.guard_epochs)
        ):
            # Legacy retired-parent readback requires its complete terminal closure.
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
            terminal_validator(context, _occurrence(operation.owner), receipt)
            require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)


def require_execution_history_in(
    context: CompositionSqlContext,
    request: CompositionActivationRequest,
    *,
    expected_service_id: str,
    terminal_validator: ExecutionTerminalValidator,
) -> None:
    """Audit whole-request acquisition/retirement history without changing state."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(request) is not CompositionActivationRequest:
        raise CompositionAdmissionError("attempt_parent")
    request.__post_init__()
    _require_history_in(
        context,
        request,
        frozenset(resource.guard_id for resource in request.resources),
        None,
        expected_service_id=expected_service_id,
        terminal_validator=terminal_validator,
    )
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)


def require_execution_attempt_in(
    context: CompositionSqlContext,
    attempt: CompositionAttemptIdentity,
    *,
    expected_service_id: str,
    terminal_validator: ExecutionTerminalValidator,
) -> CompositionActivationOccurrence:
    """Check only fresh admission; any existing operation requires separate recovery."""
    transaction = require_shared_transaction_in(context, expected_service_id=expected_service_id)
    if type(attempt) is not CompositionAttemptIdentity:
        raise CompositionAdmissionError("attempt_identity")
    attempt.__post_init__()
    parents = _rows(
        context,
        f"SELECT TOP (2) owner_key FROM {_table(context, 'owners')} WITH (HOLDLOCK) "
        "WHERE owner_kind='execution' AND subject_sha256=?;",
        attempt.activation_request_sha256,
    )
    if len(parents) != 1 or len(parents[0]) != 1:
        raise CompositionAdmissionError("attempt_parent")
    owner = _read_owner_key_in(context, parents[0][0], expected_service_id=expected_service_id)
    if owner is None:
        raise CompositionAdmissionError("attempt_parent")
    occurrence = _occurrence(owner).require_state("ACTIVE")
    guards = require_composition_attempt_scope(occurrence, attempt)
    _require_history_in(
        context,
        occurrence.request,
        guards,
        attempt.attempt_sha256,
        expected_service_id=expected_service_id,
        terminal_validator=terminal_validator,
    )
    require_shared_transaction_in(context, expected_service_id=expected_service_id, transaction_id=transaction)
    return occurrence
