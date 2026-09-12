"""Persist dbt business observations as exact parent-attempt OUTCOME proofs."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from hashlib import sha256
from typing import Any, Protocol

from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.contracts.composition_dbt_outcome import (
    DbtCaptureError,
    DbtDispatchIntent,
    DbtNativeOutcome,
)
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object


class DbtOutcomeObserver(Protocol):
    def observe(self, attempt: CompositionAttemptIdentity) -> DbtNativeOutcome: ...


TransactionFactory = Callable[[], AbstractContextManager[Any]]
IntentReader = Callable[[CompositionAttemptIdentity], DbtDispatchIntent]
ProofWriter = Callable[..., None]

_OUTCOME_REASONS = {
    "SUCCEEDED": "native_invocation_verified",
    "FAILED": "protected_undispatched_closure",
    "COMMIT_UNKNOWN": "native_outcome_unverified",
}


class CompositionDbtOutcomeProofProducer:
    """Create, commit and independently reopen one immutable OUTCOME proof."""

    def __init__(
        self,
        *,
        transaction: TransactionFactory,
        expected_service_id: str,
        intent_reader: IntentReader,
        outcome_observer: DbtOutcomeObserver,
        persist: ProofWriter | None = None,
    ) -> None:
        self._transaction = transaction
        self._service_id = expected_service_id
        self._read_intent = intent_reader
        self._observe_outcome = outcome_observer
        self._persist = persist

    def observe(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        attempt.__post_init__()
        intent = self._read_intent(attempt)
        outcome = self._observe_outcome.observe(attempt)
        document = self._document(attempt, intent, outcome)
        proof = CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            (
                CompositionProofAuthority(
                    "mssql",
                    self._service_id,
                    intent.sql_principal_sid,
                ),
            ),
            "sha256:" + sha256(document).hexdigest(),
            outcome.state,
        )
        # The module global stays the default so an injected writer and a
        # patched producer resolve the same persistence at call time.
        write = self._persist or persist_execution_proof
        with self._transaction() as ledger:
            write(ledger, proof, document, create=True)
        with self._transaction() as ledger:
            write(ledger, proof, document, create=False)
        return proof.require_attempt(attempt)

    @staticmethod
    def _document(
        attempt: CompositionAttemptIdentity,
        intent: DbtDispatchIntent,
        outcome: DbtNativeOutcome,
    ) -> bytes:
        if type(intent) is not DbtDispatchIntent or intent.attempt != attempt:
            raise DbtCaptureError("outcome_intent")
        intent.__post_init__()
        if (
            type(outcome) is not DbtNativeOutcome
            or _OUTCOME_REASONS.get(outcome.state) != outcome.reason
            or outcome.intent_sha256 != intent.intent_sha256
            or (outcome.state == "SUCCEEDED") != (outcome.materialization_original is not None)
        ):
            raise DbtCaptureError("outcome_observation")
        materialization = None
        if outcome.materialization_original is not None:
            try:
                materialization = strict_json_object(outcome.materialization_original)
                if canonical_json_bytes(materialization) != outcome.materialization_original:
                    raise ValueError("non-canonical materialization")
            except Exception:
                raise DbtCaptureError("outcome_materialization") from None
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-dbt-outcome-evidence.v1",
                "attempt_sha256": attempt.attempt_sha256,
                "intent_sha256": intent.intent_sha256,
                "state": outcome.state,
                "reason": outcome.reason,
                "materialization": materialization,
            }
        )


__all__ = ["CompositionDbtOutcomeProofProducer"]
