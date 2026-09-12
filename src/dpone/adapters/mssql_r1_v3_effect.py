"""Mode-specific adapters for the shared MSSQL R1 V3 effect UoW."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from dpone.contracts.postgres_mssql_correctness_profile import SourceMode

if TYPE_CHECKING:
    from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
    from dpone.contracts.mssql_r1_v3_quality import MssqlBatchQualityEvidenceV3, MssqlXminQualityEvidenceV3
    from dpone.ports import mssql_r1_v3_effect_runtime as r1


@dataclass(frozen=True, slots=True)
class MssqlR1BatchEffectV3:
    mutation: r1.MssqlBatchMutationV3Port
    quality: r1.MssqlBatchQualityV3Port
    source_mode: SourceMode = SourceMode.BATCH_FULL_REFRESH

    def mutate(self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.mutation.mutate(transaction, attempt)

    def update_row_hashes(self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.mutation.update_row_hashes(transaction, attempt)

    def evaluate(
        self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3
    ) -> MssqlBatchQualityEvidenceV3:
        return self.quality.evaluate(transaction, attempt)


@dataclass(frozen=True, slots=True)
class MssqlR1XminEffectV3:
    mutation: r1.MssqlXminMutationV3Port
    quality: r1.MssqlXminQualityV3Port
    source_mode: SourceMode = SourceMode.XMIN_CURRENT_STATE

    def mutate(self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.mutation.apply_delta(transaction, attempt)

    def update_row_hashes(self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.mutation.update_row_hashes(transaction, attempt)

    def evaluate(
        self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3
    ) -> MssqlXminQualityEvidenceV3:
        return self.quality.evaluate(transaction, attempt)

    def write_checkpoint(self, transaction: r1.MssqlR1TransactionV3, attempt: MssqlR1EffectAttemptEnvelopeV3) -> None:
        self.mutation.write_checkpoint(transaction, attempt)


__all__ = ["MssqlR1BatchEffectV3", "MssqlR1XminEffectV3"]
