"""Closed, transaction-local quality providers for MSSQL R1 V3."""

from __future__ import annotations

from typing import NoReturn, Protocol

from dpone.ports import mssql_r1_v3 as r1

_MAX_SQL_BIGINT = 2**63 - 1


class MssqlR1V3QualityError(RuntimeError):
    """A closed target-local probe did not produce exact usable evidence."""

    def __init__(self, code: str, reason: str) -> None:
        self.code = code
        self.reason = reason
        super().__init__(f"postgres_mssql_r1.{code}:{reason}")


class MssqlR1V3QualityQuery(Protocol):
    """Execute one retained quality statement on the caller-owned transaction."""

    def query(
        self,
        transaction: r1.MssqlR1TransactionV3,
        statement: object,
    ) -> tuple[tuple[object, ...], ...]: ...


class MssqlR1V3RenderedAdmission(Protocol):
    """Independently re-admit retained renderer bytes before every SQL step."""

    def admit(self, attempt: r1.MssqlR1EffectAttemptEnvelopeV3) -> tuple[object, ...]: ...


class MssqlR1BatchQualityProviderV3:
    """Turn one exact Batch probe row into canonical V3 evidence."""

    def __init__(self, query: MssqlR1V3QualityQuery, admission: MssqlR1V3RenderedAdmission) -> None:
        self._query = query
        self._admission = admission

    def evaluate(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> r1.MssqlBatchQualityEvidenceV3:
        attempt = _exact_attempt(attempt)
        plan = attempt.request.mutation_plan
        if not isinstance(plan, r1.R1BatchMutationPlanV1):
            _fail("batch_plan_required", "Batch quality requires a sealed Batch mutation plan")
        row = _exact_probe_row(self._query.query(transaction, _quality_statement(attempt, self._admission, plan)), 8)
        _require_count_fields(row)
        try:
            evidence = r1.MssqlBatchQualityEvidenceV3(plan.probe_contract_digest, *row)  # type: ignore[arg-type]
        except r1.MssqlR1V3ContractError as exc:
            raise MssqlR1V3QualityError("quality_equation_failed", str(exc)) from exc
        if evidence.staged_rows != plan.stage_manifest.observed_row_count:
            _fail("quality_equation_failed", "Batch quality count differs from the sealed stage manifest")
        return evidence


class MssqlR1XminQualityProviderV3:
    """Validate the complete XMin D/I/U/N/A and target-key proof."""

    def __init__(self, query: MssqlR1V3QualityQuery, admission: MssqlR1V3RenderedAdmission) -> None:
        self._query = query
        self._admission = admission

    def evaluate(
        self,
        transaction: r1.MssqlR1TransactionV3,
        attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    ) -> r1.MssqlXminQualityEvidenceV3:
        attempt = _exact_attempt(attempt)
        plan = attempt.request.mutation_plan
        if not isinstance(plan, r1.R1XminMutationPlanV1):
            _fail("xmin_plan_required", "XMin quality requires a sealed XMin mutation plan")
        row = _exact_probe_row(self._query.query(transaction, _quality_statement(attempt, self._admission, plan)), 19)
        _require_count_fields(row[:-1])
        if not isinstance(row[-1], bool):
            _fail("probe_invalid", "checkpoint predecessor proof must be boolean")
        try:
            evidence = r1.MssqlXminQualityEvidenceV3(plan.probe_contract_digest, *row)  # type: ignore[arg-type]
        except r1.MssqlR1V3ContractError as exc:
            raise MssqlR1V3QualityError("quality_equation_failed", str(exc)) from exc
        if (
            evidence.expected_present_keys != plan.delta_manifest.observed_row_count
            or evidence.complete_key_count != plan.complete_keys_manifest.observed_row_count
        ):
            _fail("quality_equation_failed", "XMin quality counts differ from the sealed stage manifests")
        return evidence


def _quality_statement(
    attempt: r1.MssqlR1EffectAttemptEnvelopeV3,
    admission: MssqlR1V3RenderedAdmission,
    plan: r1.R1BatchMutationPlanV1 | r1.R1XminMutationPlanV1,
) -> object:
    statements = admission.admit(attempt)
    retained = attempt.request.rendered_bundle.statements
    if (
        statements != retained
        or len(statements) != len(retained)
        or any(type(item) is not type(reference) for item, reference in zip(statements, retained))
    ):
        _fail("probe_contract_invalid", "renderer admission did not return exact retained V3 statements")
    admitted = tuple(item for item in retained if item.step_kind.value == "quality")
    if len(admitted) != 1:
        _fail("probe_contract_invalid", "retained mutation bundle must contain exactly one quality statement")
    statement = admitted[0]
    if statement.result_contract_digest != plan.probe_contract_digest:
        _fail("probe_contract_invalid", "quality result contract differs from the sealed probe contract")
    return statement


def _exact_attempt(attempt: object) -> r1.MssqlR1EffectAttemptEnvelopeV3:
    if not isinstance(attempt, r1.MssqlR1EffectAttemptEnvelopeV3):
        _fail("exact_v3_attempt_required", "provider accepts only an exact V3 attempt envelope")
    return attempt


def _exact_probe_row(rows: tuple[tuple[object, ...], ...], field_count: int) -> tuple[object, ...]:
    if not isinstance(rows, tuple) or not rows:
        _fail("probe_incomplete", "quality probe returned no row")
    if len(rows) > 1:
        _fail("probe_duplicate", "quality probe returned more than one row")
    row = rows[0]
    if not isinstance(row, tuple) or len(row) != field_count:
        _fail("probe_incomplete", "quality probe row has an incomplete result shape")
    return row


def _require_count_fields(values: tuple[object, ...]) -> None:
    if any(
        isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_SQL_BIGINT for value in values
    ):
        _fail("probe_invalid", "quality counts must fit non-negative SQL bigint")


def _fail(code: str, reason: str) -> NoReturn:
    raise MssqlR1V3QualityError(code, reason)


__all__ = [
    "MssqlR1BatchQualityProviderV3",
    "MssqlR1V3QualityError",
    "MssqlR1V3QualityQuery",
    "MssqlR1V3RenderedAdmission",
    "MssqlR1XminQualityProviderV3",
]
