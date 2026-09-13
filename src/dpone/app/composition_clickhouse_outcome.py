"""Persist ClickHouse publication state as a parent-attempt OUTCOME proof."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
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


class CompositionClickHouseOutcomeObserver:
    """OUTCOME follows the snapshot record, never process exit or HTTP 200."""

    def __init__(
        self,
        *,
        transaction: TransactionFactory,
        service_id: str,
        read_publication_state: Callable[[CompositionAttemptIdentity], str],
        read_authorities: Callable[[CompositionAttemptIdentity], tuple[CompositionProofAuthority, ...]],
        persist: ProofWriter | None = None,
    ) -> None:
        self._transaction = transaction
        self._service_id = service_id
        self._read_state = read_publication_state
        self._read_authorities = read_authorities
        self._persist = persist

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        attempt.__post_init__()
        try:
            authorities = self._read_authorities(attempt)
        except Exception:
            raise CompositionAdmissionError("clickhouse_outcome") from None
        try:
            state = self._read_state(attempt)
        except Exception:
            state = "COMMIT_UNKNOWN"
        if state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}:
            state = "COMMIT_UNKNOWN"
        document = canonical_json_bytes(
            {
                "schema": "dpone.composition-clickhouse-outcome-evidence.v1",
                "service_id": self._service_id,
                "attempt_sha256": attempt.attempt_sha256,
                "state": state,
            }
        )
        proof = CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            authorities,
            "sha256:" + sha256(document).hexdigest(),
            state,
        )
        write = self._persist or persist_execution_proof
        with self._transaction() as ledger:
            write(ledger, proof, document, create=True)
        with self._transaction() as ledger:
            write(ledger, proof, document, create=False)
        return proof.require_attempt(attempt)


def publication_state(record: Any) -> str:
    """Map a snapshot record onto worker OUTCOME; only PUBLISHED is success."""

    state = getattr(record, "state", None)
    if state == "PUBLISHED":
        return "SUCCEEDED"
    if state == "NOT_PUBLISHED":
        return "COMMIT_UNKNOWN"
    return "COMMIT_UNKNOWN"


__all__ = ["CompositionClickHouseOutcomeObserver", "publication_state"]
