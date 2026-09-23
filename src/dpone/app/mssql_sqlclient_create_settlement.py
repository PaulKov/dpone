"""Settle only the original successful v2 CREATE/departure evidence chain.

No outcome/proof arguments are accepted. Directory ACKs do not grant writer
credentials, prepare a stage, or certify a live SQL route.
"""

from typing import Any

from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureOutcome
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
from dpone.contracts.mssql_tds_api import (
    CreateKind,
    SqlClientDeparturePlanV2,
    SqlClientDepartureResultV2,
    reconstruct_departure_chain,
)
from dpone.contracts.mssql_tds_directory import TdsLocalContainment, TdsRemoteSettlement, parent_digest
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_original_continuation import CREATE_ERROR as ERROR
from dpone.services.mssql_tds_original_continuation import CreateSettlement


def _proofs(custody: CreateSettlement) -> tuple[TdsLocalContainment, TdsRemoteSettlement]:
    state, outcome = custody.retained, custody.outcome
    if type(outcome) is not SqlClientCreateDepartureOutcome:
        raise ValueError(ERROR)
    helper, created = outcome.helper_outcome, outcome.create_outcome
    if (
        type(helper.plan) is not SqlClientDeparturePlanV2
        or type(helper.result) is not SqlClientDepartureResultV2
        or helper.plan is not state.plan
        or helper.request is not state.request
        or helper.result is not state.result
    ):
        raise ValueError(ERROR)
    state.validate_create()
    plan, request, result = helper.plan, helper.request, helper.result
    if (
        state.startup is None
        or helper.local_exit.identity != state.startup.process
        or not helper.local_exit.reaped
        or helper.local_exit.exit_code != 0
    ):
        raise ValueError(ERROR)
    subject = plan.helper_id, parent_digest(plan.attempt)
    receipts = {receipt.kind: receipt for receipt in helper.receipts}
    if len(receipts) != len(Kind):
        raise ValueError(ERROR)
    chain = reconstruct_departure_chain(plan, request, state.startup, result, helper.local_exit)
    for record in chain:
        kind, expected = record.kind, record.receipt
        if receipts[kind] != expected or state.expected[kind] != expected or state.receipts[kind] is not receipts[kind]:
            raise ValueError(ERROR)
    create_receipts = {receipt.kind: receipt for receipt in created.receipts}
    if (
        created.provenance is None
        or created.local_exit.identity != created.provenance.startup.process
        or not created.local_exit.reaped
    ):
        raise ValueError(ERROR)
    operation = state.create_identity.operation_id
    return (
        TdsLocalContainment(
            subject[1],
            operation,
            process_identity_digest(created.local_exit.identity),
            create_receipts[CreateKind.LOCAL_EXIT].payload_sha256,
        ),
        TdsRemoteSettlement(
            subject[1],
            operation,
            session_authority_digest(result.departure.observer.authority).hex(),
            receipts[Kind.EXCLUSION].payload_sha256,
        ),
    )


def settle_sqlclient_create_departure(attempt: Any, *, admitted_factory: Any, pool: Any, deadline: float):
    """Consume trusted registration; uncertainty retains partial original ACKs."""
    deadline_nanoseconds(deadline)
    if type(attempt) is not TdsAttempt:
        raise ValueError(ERROR)
    attempt._owned()
    attempt._assert_composition_origin(admitted_factory)
    custody = attempt._create_settlement
    if (
        type(custody) is not CreateSettlement
        or custody.attempt is not attempt
        or custody.factory is not admitted_factory
        or custody.pool is not pool
    ):
        raise ValueError(ERROR)
    try:
        custody.assert_current(min(deadline, custody.deadline))
        local, remote = _proofs(custody)
        return custody.execute(local, remote, deadline=deadline)
    except BaseException:
        custody.failed = attempt._poisoned = True
        raise
