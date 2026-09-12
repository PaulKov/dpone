"""Source-free prepared runners for sealed MSSQL R1 V3 effects."""

from __future__ import annotations

from dpone.ports import mssql_r1_v3 as r1
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_effect_uow import (
    MssqlR1V3EffectOutcome,
    MssqlR1V3EffectUnitOfWork,
)


class MssqlR1V3PreparedRunnerError(RuntimeError):
    """The target-only prepared effect cannot be executed safely."""


class MssqlR1PreparedEffectRunnerV3:
    """Execute exact SEALED bytes and retry only after proven noncommit."""

    def __init__(
        self,
        *,
        preparation: r1.MssqlTargetAuthorityPreparationV3Port,
        unit_of_work: MssqlR1V3EffectUnitOfWork,
        owner_id_digest: bytes,
        source_mode: object,
        maximum_takeovers: int = 1,
    ) -> None:
        self._preparation = preparation
        self._unit_of_work = unit_of_work
        self._owner_id_digest = _digest(owner_id_digest, "owner_id_digest")
        self._source_mode = source_mode
        if isinstance(maximum_takeovers, bool) or not isinstance(maximum_takeovers, int) or maximum_takeovers < 0:
            raise ValueError("maximum_takeovers must be a non-negative integer")
        self._maximum_takeovers = maximum_takeovers

    def resume(self, effect_key: bytes) -> r1.MssqlR1SealedRunResultV3:
        """Use pre-source authority; never reinterpret OPEN artifacts as SEALED."""

        _digest(effect_key, "effect_key")
        outcome = self._preparation.pre_source(effect_key)
        if outcome.status is r1.MssqlR1PreSourceStatusV3.REPLAY_COMMITTED:
            if not isinstance(outcome.replay, r1.CommittedEffectReplayProofV3):
                raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.committed_replay_proof_required")
            self._validate_request(outcome.replay.sealed_request)
            if outcome.replay.sealed_request.identity.effect_key != effect_key:
                raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.committed_replay_effect_mismatch")
            return r1.MssqlR1SealedRunResultV3(outcome.replay.receipt, True)
        if outcome.status is r1.MssqlR1PreSourceStatusV3.SEALED_RESUME:
            assert outcome.attempt is not None
            if outcome.attempt.request.identity.effect_key != effect_key:
                raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.sealed_resume_effect_mismatch")
            return self.run_sealed(outcome.attempt)
        reason = outcome.blocker or "sealed_request_required"
        raise MssqlR1V3PreparedRunnerError(f"postgres_mssql_r1.{reason}")

    def run_sealed(self, attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> r1.MssqlR1SealedRunResultV3:
        self._validate(attempt)
        current = attempt
        for takeover_count in range(self._maximum_takeovers + 1):
            result = self._unit_of_work.execute(current)
            if result.outcome is not MssqlR1V3EffectOutcome.KNOWN_NOT_COMMITTED:
                assert result.receipt is not None
                return r1.MssqlR1SealedRunResultV3(
                    result.receipt,
                    result.outcome is MssqlR1V3EffectOutcome.COMMITTED_AFTER_FRESH_PROOF,
                )
            if takeover_count == self._maximum_takeovers:
                break
            successor = self._preparation.take_over_sealed(
                current.request.identity.effect_key,
                self._owner_id_digest,
            )
            self._validate_successor(current, successor)
            current = successor
        raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.known_noncommit_takeover_limit")

    def _validate(self, attempt: object) -> None:
        if not isinstance(attempt, r1.MssqlR1EffectAttemptEnvelopeV3):
            raise r1.MssqlR1V3ContractError("prepared runner requires an exact V3 attempt")
        self._validate_request(attempt.request)
        if attempt.owner_id_digest != self._owner_id_digest:
            raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.sealed_attempt_owner_mismatch")

    def _validate_request(self, request: object) -> None:
        if not isinstance(request, r1.MssqlR1EffectRequestV3):
            raise r1.MssqlR1V3ContractError("prepared runner requires an exact V3 sealed request")
        if request.identity.source_mode is not self._source_mode:
            raise r1.MssqlR1V3ContractError("prepared runner mode differs from exact sealed V3 contracts")

    def _validate_successor(
        self,
        previous: r1.MssqlR1EffectAttemptEnvelopeV3,
        successor: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        self._validate(successor)
        if (
            successor.request != previous.request
            or successor.owner_id_digest != self._owner_id_digest
            or successor.operation_epoch != previous.operation_epoch + 1
            or successor.operation_projection_revision != previous.operation_projection_revision + 1
            or successor.server_lease_expires_at <= previous.server_lease_expires_at
        ):
            raise MssqlR1V3PreparedRunnerError("postgres_mssql_r1.sealed_takeover_not_adjacent")


def _digest(value: object, field: str) -> bytes:
    if not isinstance(value, bytes) or len(value) != 32:
        raise r1.MssqlR1V3ContractError(f"{field} must be exactly 32 bytes")
    return value


__all__ = ["MssqlR1PreparedEffectRunnerV3", "MssqlR1V3PreparedRunnerError"]
