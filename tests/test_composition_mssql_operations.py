"""Operation original projections and partition regressions, without live SQL."""

from dataclasses import replace
from typing import Any

import pytest

from dpone.adapters.composition_mssql_operations import (
    SharedOperationRecord,
    iter_shared_operations_in,
    read_shared_operation_in,
)
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_persistence import decode_activation_request, encode_attempt_identity
from dpone.contracts.composition_qualification_operation import CompositionQualificationOperation
from tests.composition_mssql_store_helpers import SERVICE_ID
from tests.test_composition_activation_contract import digest
from tests.test_composition_mssql_ownership import SharedSql
from tests.test_composition_qualification_operation import operation as qualification_operation
from tests.test_composition_qualification_operation import owner as qualification_owner


def attempt_for(
    database: SharedSql, *, workload_id: str = "a_native", try_number: int = 1
) -> CompositionAttemptIdentity:
    row = next(r for r in database.owners.values() if r[1] == "execution" and r[5] == "ACTIVE")
    request = decode_activation_request(row[4], row[3])
    workload = next(w for w in request.workloads if w.workload_id == workload_id)
    guards = {r.guard_id for r in request.resources if set(r.write_subjects).intersection(workload.write_subjects)}
    epochs = tuple(sorted((r[1], r[3]) for r in database.owner_domains if r[0] == row[0] and r[1] in guards))
    return CompositionAttemptIdentity(
        request.request_sha256,
        workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("plan"),
        "run/α\\fixture",
        "execute",
        try_number,
        -1,
        epochs,
    )


def add_operation(database: SharedSql, identity: Any = None, *, state: str = "RUNNING") -> Any:
    identity = attempt_for(database) if identity is None else identity
    if type(identity) is CompositionAttemptIdentity:
        owner = next(
            r for r in database.owners.values() if r[1] == "execution" and r[3] == identity.activation_request_sha256
        )
        key, replay, document = identity.attempt_sha256, identity.attempt_sha256, encode_attempt_identity(identity)
    else:
        owner = database.owners[identity.owner_key]
        key, replay, document = identity.operation_key, identity.invocation_key, identity.to_bytes()
    proof = digest("selected proof") if state in {"SUCCEEDED", "FAILED"} else None
    database.operations[key] = (key, owner[1], owner[0], owner[3], replay, document, state, proof, proof, proof)
    database.operation_domains.extend((key, owner[0], guard, epoch) for guard, epoch in identity.guard_epochs)
    return identity


def test_operation_record_is_immutable() -> None:
    assert SharedOperationRecord.__dataclass_params__.frozen


def test_exact_execution_projection_preserves_legacy_bytes_and_hashes() -> None:
    database = SharedSql()
    reference = database.add_owner()
    original = add_operation(database)
    record = read_shared_operation_in(database, original.attempt_sha256, expected_service_id=SERVICE_ID)
    assert record is not None and record.identity == original
    assert record.owner.reference == reference
    assert record.identity_document == encode_attempt_identity(original)
    assert record.operation_key == record.replay_key == original.attempt_sha256
    assert b"\\\\" in record.identity_document
    assert list(iter_shared_operations_in(database, expected_service_id=SERVICE_ID)) == [record]


def test_exact_qualification_projection_recomputes_distinct_replay_hash() -> None:
    database = SharedSql()
    database.add_owner(qualification_owner())
    original = add_operation(database, qualification_operation())
    record = read_shared_operation_in(database, original.operation_key, expected_service_id=SERVICE_ID)
    assert record is not None and type(record.identity) is CompositionQualificationOperation
    assert record.identity_document == original.to_bytes()
    assert record.operation_key != record.replay_key == original.invocation_key


@pytest.mark.parametrize(
    "damage,reason",
    [
        ("partition_missing", "attempt_partition"),
        ("partition_extra", "attempt_partition"),
        ("epoch", "attempt_partition"),
        ("boolean_epoch", "attempt_partition"),
        ("owner", "attempt_partition"),
        ("family", "attempt_parent_identity"),
        ("parent", "attempt_parent_identity"),
        ("replay", "attempt_identity"),
        ("state", "attempt_state"),
        ("terminal", "terminal_evidence"),
        ("document", "attempt_persistence_readback"),
    ],
)
def test_corrupt_execution_rows_reject(damage: str, reason: str) -> None:
    database = SharedSql()
    database.add_owner()
    original = add_operation(database)
    key = original.attempt_sha256
    row = list(database.operations[key])
    partition = database.operation_domains[0]
    if damage == "partition_missing":
        database.operation_domains.clear()
    elif damage == "partition_extra":
        database.operation_domains.append((*partition[:2], digest("extra"), 1))
    elif damage in {"epoch", "boolean_epoch"}:
        database.operation_domains[0] = (*partition[:3], 2 if damage == "epoch" else True)
    elif damage == "owner":
        database.operation_domains[0] = (key, digest("different owner"), *partition[2:])
    else:
        index, value = {
            "family": (1, "qualification"),
            "parent": (3, digest("changed")),
            "replay": (4, digest("wrong replay")),
            "state": (6, "RUNNING "),
            "terminal": (6, "SUCCEEDED"),
            "document": (5, row[5] + b"\n"),
        }[damage]
        row[index] = value
        database.operations[key] = tuple(row)
    with pytest.raises(CompositionAdmissionError, match=reason):
        read_shared_operation_in(database, key, expected_service_id=SERVICE_ID)


def test_changed_qualification_subject_cannot_refingerprint_same_invocation() -> None:
    database = SharedSql()
    database.add_owner(qualification_owner())
    original = add_operation(database, qualification_operation())
    changed = add_operation(database, replace(original, work_item_sha256=digest("changed work item")))
    assert original.operation_key != changed.operation_key
    assert original.invocation_key == changed.invocation_key
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        list(iter_shared_operations_in(database, expected_service_id=SERVICE_ID))


@pytest.mark.parametrize("epoch", [2, 2**63 - 1])
def test_qualification_original_epochs_cannot_change_owner_epoch(epoch: int) -> None:
    database = SharedSql()
    database.add_owner(qualification_owner())
    original = qualification_operation()
    changed = add_operation(
        database, replace(original, guard_epochs=tuple((g, epoch) for g, _ in original.guard_epochs))
    )
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        read_shared_operation_in(database, changed.operation_key, expected_service_id=SERVICE_ID)


@pytest.mark.parametrize("bad_key", ["", "!", "sha256:" + "A" * 64])
def test_unfiltered_first_operation_page_rejects_malformed_keys(bad_key: str) -> None:
    database = SharedSql()
    database.add_owner()
    original = add_operation(database)
    row = database.operations[original.attempt_sha256]
    database.operations[original.attempt_sha256] = (bad_key, *row[1:])
    with pytest.raises(CompositionAdmissionError, match="digest"):
        list(iter_shared_operations_in(database, expected_service_id=SERVICE_ID))
    pages = [sql for sql, _ in database.statements if "ORDER BY operation_key;" in sql]
    assert pages and "WHERE" not in pages[0]


def test_absence_and_orphan_operation_partition_are_distinct() -> None:
    database = SharedSql()
    database.add_owner()
    identity = attempt_for(database)
    assert read_shared_operation_in(database, identity.attempt_sha256, expected_service_id=SERVICE_ID) is None
    add_operation(database, identity)
    database.operations.clear()
    with pytest.raises(CompositionAdmissionError, match="attempt_partition"):
        read_shared_operation_in(database, identity.attempt_sha256, expected_service_id=SERVICE_ID)


def test_oversized_operation_document_is_not_fetched() -> None:
    database = SharedSql()
    database.add_owner(qualification_owner())
    original = add_operation(database, qualification_operation())
    row = database.operations[original.operation_key]
    database.operations[original.operation_key] = (*row[:5], b" " * (1048576 + 1), *row[6:])
    with pytest.raises(CompositionAdmissionError):
        read_shared_operation_in(database, original.operation_key, expected_service_id=SERVICE_ID)
