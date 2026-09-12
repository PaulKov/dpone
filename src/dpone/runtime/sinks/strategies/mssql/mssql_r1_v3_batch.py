"""Closed Batch mutation sequence for the MSSQL R1 V3 target UoW."""

from __future__ import annotations

from typing import Protocol

from dpone.ports import mssql_r1_v3 as r1


class _MssqlR1V3BatchMutationCommand(Protocol):
    """Execute one retained Batch statement on the caller-owned transaction."""

    def execute(
        self,
        transaction: r1.MssqlR1TransactionV3,
        statement: object,
    ) -> None: ...


class _MssqlR1V3BatchRenderedAdmission(Protocol):
    """Independently re-admit the retained Batch renderer bundle."""

    def admit(self, attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> tuple[object, ...]: ...


class MssqlR1V3BatchMutationError(RuntimeError):
    """The retained request cannot authorize the closed Batch mutation path."""

    def __init__(self, code: str, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(f"postgres_mssql_r1.{code}:{reason}")


class MssqlR1BatchMutationProviderV3:
    """Execute only independently re-admitted Batch publication statements."""

    def __init__(
        self,
        command: _MssqlR1V3BatchMutationCommand,
        admission: _MssqlR1V3BatchRenderedAdmission,
    ) -> None:
        self._command = command
        self._admission = admission

    def mutate(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        self._command.execute(transaction, self._statements(attempt)["batch_publication"])

    def update_row_hashes(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> None:
        self._command.execute(transaction, self._statements(attempt)["row_hash"])

    def _statements(self, attempt: object) -> dict[str, object]:
        if not isinstance(attempt, r1.MssqlR1EffectAttemptEnvelopeV3):
            raise MssqlR1V3BatchMutationError(
                "exact_v3_attempt_required", "provider accepts only an exact V3 attempt envelope"
            )
        plan = attempt.request.mutation_plan
        if not isinstance(plan, r1.R1BatchMutationPlanV1):
            raise MssqlR1V3BatchMutationError(
                "batch_plan_required", "Batch mutation requires a sealed Batch mutation plan"
            )
        admitted = self._admission.admit(attempt)
        retained = attempt.request.rendered_bundle.statements
        if (
            admitted != retained
            or len(admitted) != len(retained)
            or any(type(item) is not type(reference) for item, reference in zip(admitted, retained, strict=True))
        ):
            raise MssqlR1V3BatchMutationError(
                "mutation_bundle_invalid", "renderer admission did not return exact retained V3 statements"
            )
        expected = tuple(item.value for item in plan.template_set.ordered_steps)
        if tuple(item.step_kind.value for item in retained) != expected:
            raise MssqlR1V3BatchMutationError(
                "mutation_bundle_invalid", "retained statements differ from the closed Batch sequence"
            )
        if plan.stage_manifest.observed_row_count == 0 and "empty_refresh" not in tuple(
            item.value for item in attempt.request.authority_set.purposes
        ):
            raise MssqlR1V3BatchMutationError(
                "empty_refresh_authority_required", "an empty Batch payload requires exact empty-refresh authority"
            )
        return {item.step_kind.value: item for item in retained}


__all__ = ["MssqlR1BatchMutationProviderV3", "MssqlR1V3BatchMutationError"]
