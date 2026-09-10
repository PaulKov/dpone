"""Trusted SQL gate observations; session disappearance never infers data outcome."""

from __future__ import annotations

import json

from dpone.adapters.composition_mssql_issuance import MssqlEnrollment
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.dbapi_lifecycle import row
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
    encode_attempt_proof,
)


def observe_quiescence(ledger: CompositionMssqlLedger, sid: bytes, enrollments: tuple[MssqlEnrollment, ...]) -> None:
    """Require zero original-SID sessions, including incomplete authentication.

    Caller has already read the exact irreversible CLOSED gate, disabled login,
    installed synchronous LOGON policy and complete server visibility. No timing
    delay, process status or empty-prefix heuristic substitutes for this barrier.
    Database transaction inspection also blocks orphan/unattributed transactions.
    Only this exact trusted controller transaction is excluded: its catalog
    reads may enlist it in the target database, but it executes no business DML.
    """
    ledger.cursor.execute(
        "SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE original_security_id=?;",
        sid,
    )
    if row(ledger.cursor) != (0,):
        raise CompositionAdmissionError("login_sessions_not_quiescent")
    ledger.cursor.execute(
        "SELECT COUNT_BIG(*) FROM sys.dm_tran_session_transactions t JOIN sys.dm_exec_sessions s "
        "ON s.session_id=t.session_id WHERE s.original_security_id=?;",
        sid,
    )
    if row(ledger.cursor) != (0,):
        raise CompositionAdmissionError("login_transactions_not_quiescent")
    ledger.cursor.execute("SELECT transaction_id FROM sys.dm_tran_current_transaction;")
    observer = tuple(tuple(value) for value in ledger.cursor.fetchall())
    if len(observer) != 1 or len(observer[0]) != 1 or type(observer[0][0]) is not int or observer[0][0] <= 0:
        raise CompositionAdmissionError("login_observer_transaction")
    for enrollment in enrollments:
        ledger.cursor.execute(
            "SELECT COUNT_BIG(*) FROM sys.dm_tran_database_transactions "
            "WHERE database_id=? AND database_transaction_type<>3 AND transaction_id<>?;",
            enrollment.database_id,
            observer[0][0],
        )
        if row(ledger.cursor) != (0,):
            raise CompositionAdmissionError("login_unattributed_transactions")


def gate_proof(
    attempt: CompositionAttemptIdentity, *, service_id: str, sid: bytes, kind: str, evidence: dict[str, object]
) -> CompositionAttemptProof:
    """A SQL producer certifies its own exact principal only, never ClickHouse."""
    if kind not in {"CLOSED_GATES", "QUIESCENCE"}:
        raise CompositionAdmissionError("login_proof_kind")
    return CompositionAttemptProof(
        kind,
        attempt.attempt_sha256,
        attempt.activation_request_sha256,
        composition_attempt_epoch_subject(attempt),
        (CompositionProofAuthority("mssql", service_id, "mssql-sid:" + sid.hex()),),
        canonical_fingerprint(evidence),
    )


def persist_gate_proof(
    ledger: CompositionMssqlLedger, proof: CompositionAttemptProof, evidence: dict[str, object]
) -> None:
    """Create-or-compare exact observed bytes; never update previous evidence."""
    if (
        proof.kind not in {"CLOSED_GATES", "QUIESCENCE"}
        or len(proof.authorities) != 1
        or proof.authorities[0].connector != "mssql"
        or proof.evidence_sha256 != canonical_fingerprint(evidence)
    ):
        raise CompositionAdmissionError("login_proof_evidence")
    document = json.dumps(evidence, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ledger.cursor.execute(
        f"SELECT evidence_document FROM {ledger.table('mssql_gate_evidence')} WITH (UPDLOCK, HOLDLOCK) "
        "WHERE evidence_sha256=? AND attempt_sha256=?;",
        proof.evidence_sha256,
        proof.attempt_sha256,
    )
    observed = row(ledger.cursor)
    if observed is None:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('mssql_gate_evidence')} (evidence_sha256, attempt_sha256, evidence_document) "
            "VALUES (?, ?, ?);",
            proof.evidence_sha256,
            proof.attempt_sha256,
            document,
        )
    elif observed != (document,):
        raise CompositionAdmissionError("login_evidence_readback")
    encoded = encode_attempt_proof(proof)
    ledger.cursor.execute(
        f"SELECT activation_request_sha256, guard_epochs_sha256, proof_document FROM {ledger.table('proofs')} "
        "WITH (UPDLOCK, HOLDLOCK) WHERE attempt_sha256=? AND kind=? AND proof_sha256=?;",
        proof.attempt_sha256,
        proof.kind,
        proof.proof_sha256,
    )
    observed = row(ledger.cursor)
    expected = (proof.activation_request_sha256, proof.guard_epochs_sha256, encoded)
    if observed is None:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('proofs')} (attempt_sha256, kind, proof_sha256, activation_request_sha256, "
            "guard_epochs_sha256, proof_document) VALUES (?, ?, ?, ?, ?, ?);",
            proof.attempt_sha256,
            proof.kind,
            proof.proof_sha256,
            *expected,
        )
    elif observed != expected:
        raise CompositionAdmissionError("login_proof_readback")
