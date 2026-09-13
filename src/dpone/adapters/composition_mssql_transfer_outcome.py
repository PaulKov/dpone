"""Persist ordinary transfer observations as exact parent-attempt OUTCOME proofs.

The observer reopens the committed generic receipt and actual target/state
identity on a fresh connection. A process return value, exit status or
caller-selected digest is never an outcome proof.
"""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from dpone.contracts.strict_json import canonical_json_bytes

TransactionFactory = Callable[[], AbstractContextManager[Any]]
ProofWriter = Callable[..., None]
ObserveTransfer = Callable[[CompositionAttemptIdentity], "CompositionTransferObservation"]

_OUTCOME_REASONS = {
    "SUCCEEDED": "transfer_receipt_verified",
    "FAILED": "protected_rollback_no_mutation",
    "COMMIT_UNKNOWN": "transfer_outcome_unverified",
}


@dataclass(frozen=True, slots=True)
class CompositionTransferObservation:
    """Independent business evidence; no executor result is accepted here."""

    receipt_matches: bool
    row_evidence: bool
    content_evidence: bool
    rollback_protected: bool
    no_mutation: bool
    evidence_document: bytes | None = None

    @property
    def state(self) -> str:
        succeeded = self.receipt_matches and self.row_evidence and self.content_evidence
        failed = self.rollback_protected and self.no_mutation
        if succeeded and not failed:
            return "SUCCEEDED"
        if failed and not succeeded:
            return "FAILED"
        return "COMMIT_UNKNOWN"

    @property
    def reason(self) -> str:
        return _OUTCOME_REASONS[self.state]


class CompositionMssqlTransferOutcomeObserver:
    """Create, commit and independently reopen one immutable transfer OUTCOME."""

    def __init__(
        self,
        *,
        transaction: TransactionFactory,
        expected_service_id: str,
        principal_id: str,
        observe: ObserveTransfer,
        persist: ProofWriter | None = None,
        resolve_principal: Callable[[CompositionAttemptIdentity], str] | None = None,
    ) -> None:
        self._transaction = transaction
        self._service_id = expected_service_id
        self._principal_id = principal_id
        self._observe = observe
        self._persist = persist
        self._resolve_principal = resolve_principal

    def can_prove_outcome(self) -> bool:
        """The default unknown observer cannot seal SUCCEEDED or proven FAILED."""

        return bool(getattr(self._observe, "proves_outcome", False))

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        attempt.__post_init__()
        principal = self._resolve_principal(attempt) if self._resolve_principal else self._principal_id
        try:
            observed = self._observe(attempt)
        except Exception:
            observed = CompositionTransferObservation(False, False, False, False, False)
        if type(observed) is not CompositionTransferObservation:
            raise CompositionAdmissionError("transfer_outcome_observation")
        import json

        evidence = {}
        if observed.evidence_document is not None:
            evidence["observation"] = json.loads(observed.evidence_document)
        document = canonical_json_bytes(
            {
                "schema": "dpone.composition-transfer-outcome-evidence.v1",
                "attempt_sha256": attempt.attempt_sha256,
                "state": observed.state,
                "reason": observed.reason,
                "receipt_matches": observed.receipt_matches,
                "row_evidence": observed.row_evidence,
                "content_evidence": observed.content_evidence,
                "rollback_protected": observed.rollback_protected,
                "no_mutation": observed.no_mutation,
                **evidence,
            }
        )
        proof = CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            (CompositionProofAuthority("mssql", self._service_id, principal),),
            "sha256:" + sha256(document).hexdigest(),
            observed.state,
        )
        write = self._persist or persist_execution_proof
        with self._transaction() as ledger:
            write(ledger, proof, document, create=True)
        with self._transaction() as ledger:
            write(ledger, proof, document, create=False)
        return proof.require_attempt(attempt)


def resolve_transfer_principal(ledger: Any, attempt: CompositionAttemptIdentity, service_id: str) -> str:
    """Resolve the closed, immutable issued SID under protected ledger authority.

    A service UUID or a caller-selected login name cannot stand in for this
    per-attempt principal. Closure and OUTCOME must name the same retained SID.
    """
    attempt.__post_init__()
    ledger.cursor.execute(
        "SELECT TOP (2) g.login_sid FROM "
        + ledger.table("login_gates")
        + " g WITH (HOLDLOCK) JOIN "
        + ledger.table("issued_authorities")
        + " a WITH (HOLDLOCK) ON a.operation_key=g.operation_key "
        "WHERE g.operation_key=? AND g.operation_family='execution' AND g.gate_state='CLOSED' "
        "AND g.disabled_evidence_sha256 IS NOT NULL AND a.connector='mssql' AND a.service_id=? "
        "AND a.principal_id='mssql-sid:'+LOWER(CONVERT(varchar(32),g.login_sid,2));",
        attempt.attempt_sha256,
        service_id,
    )
    rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
    if len(rows) != 1 or len(rows[0]) != 1 or type(rows[0][0]) is not bytes or len(rows[0][0]) != 16:
        raise CompositionAdmissionError("transfer_issued_sid")
    return "mssql-sid:" + rows[0][0].hex()


__all__ = ["CompositionMssqlTransferOutcomeObserver", "CompositionTransferObservation", "resolve_transfer_principal"]
