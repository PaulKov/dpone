"""Closed XMin mutation sequence for the MSSQL R1 V3 target UoW."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

from dpone.ports import mssql_r1_v3_effect_runtime as r1

if TYPE_CHECKING:
    from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_quality import MssqlR1V3RenderedAdmission


class MssqlR1V3XminMutationError(RuntimeError):
    """The retained request cannot authorize the closed XMin mutation path."""

    def __init__(self, code: str, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(f"postgres_mssql_r1.{code}:{reason}")


class MssqlR1V3MutationCommand(Protocol):
    """Execute one exact retained statement on the caller-owned transaction."""

    def execute(
        self,
        transaction: r1.MssqlR1TransactionV3,
        statement: object,
    ) -> None: ...


class MssqlR1XminMutationProviderV3:
    """Expose only admitted XMin mutation steps without classifying rows.

    The enclosing target UoW owns the global D/U/I -> sidecar -> quality ->
    receipt -> checkpoint sequence.  This provider deliberately cannot claim
    that cross-component ordering by itself.
    """

    def __init__(self, command: MssqlR1V3MutationCommand, admission: MssqlR1V3RenderedAdmission) -> None:
        self._command = command
        self._admission = admission

    def apply_delta(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        statements = self._statements(attempt)
        for step in ("delete", "update", "insert"):
            self._command.execute(transaction, statements[step])

    def update_row_hashes(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        self._command.execute(transaction, self._statements(attempt)["row_hash"])

    def write_checkpoint(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        self._command.execute(transaction, self._statements(attempt)["checkpoint"])

    def _statements(
        self,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> dict[str, object]:
        if not isinstance(attempt, r1.MssqlR1EffectAttemptEnvelopeV3):
            raise MssqlR1V3XminMutationError(
                "exact_v3_attempt_required", "provider accepts only an exact V3 attempt envelope"
            )
        plan = attempt.request.mutation_plan
        if not isinstance(plan, r1.R1XminMutationPlanV1):
            raise MssqlR1V3XminMutationError("xmin_plan_required", "XMin mutation requires a sealed XMin mutation plan")
        statements = self._admission.admit(attempt)
        retained = attempt.request.rendered_bundle.statements
        if (
            statements != retained
            or len(statements) != len(retained)
            or any(type(item) is not type(reference) for item, reference in zip(statements, retained))
        ):
            raise MssqlR1V3XminMutationError(
                "mutation_bundle_invalid", "renderer admission did not return exact retained V3 statements"
            )
        observed_steps = tuple(item.step_kind.value for item in retained)
        if observed_steps != tuple(item.value for item in plan.template_set.ordered_steps):
            raise MssqlR1V3XminMutationError(
                "mutation_bundle_invalid", "retained statements differ from the closed XMin sequence"
            )
        if plan.complete_keys_manifest.observed_row_count == 0 and (
            "empty_refresh" not in tuple(item.value for item in attempt.request.generation.authority_set.purposes)
        ):
            raise MssqlR1V3XminMutationError(
                "empty_refresh_authority_required",
                "an exact empty complete-key set requires empty-refresh authority before mutation",
            )
        return {item.step_kind.value: item for item in retained}


__all__ = [
    "MssqlR1V3MutationCommand",
    "MssqlR1V3XminMutationError",
    "MssqlR1XminMutationProviderV3",
]
