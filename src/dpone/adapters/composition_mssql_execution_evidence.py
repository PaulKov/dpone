"""Retain observed proof originals in the producer's existing SQL transaction.

This persistence primitive performs no observation and creates no authority.
Only protected composition-root producers call it after complete scope, issued
principal and outcome/closure checks. Workers never receive its connection.
"""

from hashlib import sha256
from typing import Protocol

from dpone.adapters.composition_execution_evidence_schema import require_execution_evidence_schema
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptProof, encode_attempt_proof
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.ports.composition_sql import CompositionSqlContext


class TransactionalCompositionSqlContext(CompositionSqlContext, Protocol):
    """A protected composition ledger with observable transaction continuity."""

    def require_transaction(self, transaction_id: int | None = None) -> int: ...


def persist_execution_proof(
    context: TransactionalCompositionSqlContext,
    proof: CompositionAttemptProof,
    document: bytes,
    *,
    create: bool = True,
) -> None:
    """Append or compare both exact originals; fresh outer readback proves commit."""
    proof.__post_init__()
    if (
        type(document) is not bytes
        or not 0 < len(document) <= 8388608
        or proof.evidence_sha256 != "sha256:" + sha256(document).hexdigest()
        or canonical_json_bytes(strict_json_object(document)) != document
    ):
        raise CompositionAdmissionError("execution_proof_original")
    transaction = context.require_transaction()
    require_execution_evidence_schema(context.cursor, context.schema)
    context.require_transaction(transaction)
    selector = (proof.attempt_sha256, proof.kind, proof.evidence_sha256)
    select = (
        f"SELECT TOP (2) CASE WHEN DATALENGTH(evidence_document) BETWEEN 1 AND 8388608 "
        f"THEN evidence_document END FROM {context.table('execution_evidence')} WITH (UPDLOCK,HOLDLOCK) "
        "WHERE operation_key=? AND kind=? AND evidence_sha256=?;"
    )
    _append_or_compare(
        context,
        select,
        selector,
        (document,),
        f"INSERT INTO {context.table('execution_evidence')} (operation_key,kind,evidence_sha256,evidence_document) VALUES (?,?,?,?);",
        (*selector, document),
        create,
    )
    selector = (proof.attempt_sha256, proof.kind, proof.proof_sha256)
    expected = ("execution", encode_attempt_proof(proof))
    select = (
        f"SELECT TOP (2) operation_family, CASE WHEN DATALENGTH(proof_document) BETWEEN 1 AND 8388608 "
        f"THEN proof_document END FROM {context.table('proofs')} WITH (UPDLOCK,HOLDLOCK) "
        "WHERE operation_key=? AND kind=? AND proof_sha256=?;"
    )
    _append_or_compare(
        context,
        select,
        selector,
        expected,
        f"INSERT INTO {context.table('proofs')} (operation_key,kind,proof_sha256,operation_family,proof_document) VALUES (?,?,?,?,?);",
        (*selector, *expected),
        create,
    )
    context.require_transaction(transaction)


def _append_or_compare(context, select, selector, expected, insert, values, create):
    transaction = context.require_transaction()
    context.cursor.execute(select, *selector)
    rows = tuple(tuple(row) for row in context.cursor.fetchall())
    context.require_transaction(transaction)
    if not rows:
        if not create:
            raise CompositionAdmissionError("execution_proof_missing")
        context.cursor.execute(insert, *values)
    elif rows != (expected,):
        raise CompositionAdmissionError("execution_proof_conflict")
    context.require_transaction(transaction)
    context.cursor.execute(select, *selector)
    if tuple(tuple(row) for row in context.cursor.fetchall()) != (expected,):
        raise CompositionAdmissionError("execution_proof_readback")
    context.require_transaction(transaction)
