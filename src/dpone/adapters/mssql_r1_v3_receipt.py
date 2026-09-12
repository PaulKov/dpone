"""Canonical receipt adapter for target-observed MSSQL R1 V3 evidence."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dpone.contracts.mssql_r1_v3_receipt_projection import build_receipt_v3, receipt_body_for_observation

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
    from dpone.contracts.mssql_r1_v3_quality import (
        MssqlQualityEvidenceV3,
    )
    from dpone.contracts.mssql_r1_v3_receipt import (
        MssqlR1EffectReceiptV3,
        MssqlR1ReceiptObservationV3,
    )
    from dpone.contracts.mssql_r1_v3_transaction_authority import MssqlAdmittedGenerationAuthoritySetV3
    from dpone.ports.mssql_r1_v3_effect_runtime import MssqlR1TransactionV3


class MssqlR1ReceiptObservationV3Port(Protocol):
    def observe(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        quality: MssqlQualityEvidenceV3,
    ) -> MssqlR1ReceiptObservationV3: ...


class MssqlR1V3ReceiptBuilder:
    """Build V3 receipts without accepting caller metrics or digests."""

    def __init__(self, observations: MssqlR1ReceiptObservationV3Port) -> None:
        self._observations = observations

    def build(
        self,
        transaction: MssqlR1TransactionV3,
        attempt: MssqlR1EffectAttemptEnvelopeV3,
        admitted: MssqlAdmittedGenerationAuthoritySetV3,
        quality: MssqlQualityEvidenceV3,
    ) -> MssqlR1EffectReceiptV3:
        observation = self._observations.observe(transaction, attempt, quality)
        body = receipt_body_for_observation(attempt, quality, observation)
        return build_receipt_v3(
            attempt,
            body,
            committed_at=observation.committed_at,
            admitted_authorities=admitted,
        )


__all__ = ["MssqlR1ReceiptObservationV3Port", "MssqlR1V3ReceiptBuilder"]
