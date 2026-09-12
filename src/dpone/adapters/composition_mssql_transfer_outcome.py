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
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
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

    @property
    def state(self) -> str:
        if self.receipt_matches and self.row_evidence and self.content_evidence:
            return "SUCCEEDED"
        if self.rollback_protected and self.no_mutation:
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
    ) -> None:
        self._transaction = transaction
        self._service_id = expected_service_id
        self._principal_id = principal_id
        self._observe = observe
        self._persist = persist

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        attempt.__post_init__()
        try:
            observed = self._observe(attempt)
        except Exception:
            observed = CompositionTransferObservation(False, False, False, False, False)
        if type(observed) is not CompositionTransferObservation:
            raise CompositionAdmissionError("transfer_outcome_observation")
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
            }
        )
        proof = CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            (CompositionProofAuthority("mssql", self._service_id, self._principal_id),),
            "sha256:" + sha256(document).hexdigest(),
            observed.state,
        )
        write = self._persist or persist_execution_proof
        with self._transaction() as ledger:
            write(ledger, proof, document, create=True)
        with self._transaction() as ledger:
            write(ledger, proof, document, create=False)
        return proof.require_attempt(attempt)


__all__ = ["CompositionMssqlTransferOutcomeObserver", "CompositionTransferObservation"]
