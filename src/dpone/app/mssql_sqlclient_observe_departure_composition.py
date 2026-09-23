"""Trusted original PREPARED-to-OBSERVE settlement composition.

Caller inputs configure bounded execution; the original attempt supplies all
operation, preparation, process and acknowledgement authority. Success reports
local observations only and neither enables a backend nor certifies a route.
"""

import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import current_thread
from time import monotonic
from typing import cast
from uuid import uuid4

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.adapters.mssql_sqlclient_observe_containment_evidence_actor import SqlClientObserveContainmentEvidenceActor
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import (
    PRODUCTION_DEPARTURE_INFRASTRUCTURE,
)
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import (
    admitted_departure_launcher as _admission,
)
from dpone.app.mssql_sqlclient_departure_supervisor import _run_departure
from dpone.app.mssql_sqlclient_observe_departure_supervision import (
    SqlClientObserveDepartureUnknown,
    _ObserveDepartureRetention,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.mssql_sqlclient_departure_models import (
    OriginalKind,
    SqlClientDatabasePrincipal,
    SqlClientDepartureEvidenceReceipt,
    SqlClientObserveContainmentReceipt,
    SqlClientObserveDeparturePlan,
    SqlClientObserverAdmission,
    TdsAttemptSnapshot,
    TdsConnectionMaterial,
    TdsDirectorySnapshot,
    WindowOutcomeUnknown,
)
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_tds_api import authority_digest, validate_observer_admission
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientObserveContainmentEvidenceGateway
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_original_continuation import PreparationTransition


@dataclass(frozen=True)
class SqlClientObserveSettlementOutcome:
    """Original PREPARED and final directory observations, without reusable authority."""

    prepared: TdsAttemptSnapshot
    directory: TdsDirectorySnapshot
    containment_receipt: SqlClientObserveContainmentReceipt
    helper_receipts: tuple[SqlClientDepartureEvidenceReceipt, ...]


def _bind_plan(
    state: _ObserveDepartureRetention,
    admission: tuple[bytes, str, str, str, int],
    startup_deadline: float,
) -> None:
    origin = state.owner.origin
    receipts = {record.kind: receipt for record, receipt, observation in origin.original_evidence}
    principal = origin.management_incarnation.authority.principal_resolution.principal
    containment = state.owner.containment_capture
    assert containment is not None and origin.expected is not None and origin.evidence_receipt is not None
    plan = SqlClientObserveDeparturePlan(
        helper_id=state.helper_id,
        attempt=origin.expected.state.identity,
        ownership=origin.expected.state.ownership,
        observe_operation=origin.identity,
        observe_process=origin.startup.process,
        original_registration_artifact_sha256=receipts[OriginalKind.REGISTRATION].payload_sha256,
        original_authority_artifact_sha256=receipts[OriginalKind.AUTHORITY].payload_sha256,
        original_authority_sha256=authority_digest(origin.authority),
        original_containment_artifact_sha256=containment[2].payload_sha256,
        preparation_artifact_sha256=origin.evidence_receipt.payload_sha256,
        original=origin.authority.session,
        database=origin.authority.database,
        management_admission=origin.management_admission,
        principal=SqlClientDatabasePrincipal(principal.principal_id, principal.name, principal.sid),
        observer_admission=state.observer_admission,
        implementation_sha256=admission[2],
        package_root=admission[3],
        admission_sha256=admission[1],
        max_address_space_bytes=admission[4],
        startup_deadline=startup_deadline,
        operation_deadline=state.owner.deadline,
    )
    state.owner.bind_plan(plan)


def settle_prepared_observe(
    attempt: TdsAttempt,
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    pool: TdsActorPool,
    evidence_root: Path,
    departure_launcher: PythonSqlClientDepartureLauncher,
    observer_admission: SqlClientObserverAdmission,
    management_credentials: Callable[[], TdsConnectionMaterial],
    deadline: float,
    helper_startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
) -> SqlClientObserveSettlementOutcome:
    """Consume the original successful preparation once, before external callbacks.

    The supplier provides the separately admitted verifier login. The actual
    helper, six original evidence ACKs and original reaped process are required;
    caller-created requests, results, proofs and outcomes are never accepted.
    """
    if (
        type(attempt) is not TdsAttempt
        or type(admitted_factory) is not _AdmittedSqlClientStoreFactory
        or type(pool) is not TdsActorPool
        or type(departure_launcher) is not PythonSqlClientDepartureLauncher
        or type(observer_admission) is not SqlClientObserverAdmission
        or type(evidence_root) is not type(Path())
        or not evidence_root.is_absolute()
        or not callable(clock)
        or not callable(management_credentials)
    ):
        raise ValueError("mssql_native.sqlclient_observe_departure_arguments_invalid")
    for value in (deadline, helper_startup_timeout, termination_timeout):
        deadline_nanoseconds(value)
    origin = attempt._prepared_origin
    if type(origin) is not PreparationTransition:
        raise WindowOutcomeUnknown("mssql_native.sqlclient_prepared_origin_required")
    owner = attempt._prepared_observe_settlement(uuid4(), deadline=deadline)
    helper = DepartureHelperCustody(clock, os.getpid(), current_thread())
    state = _ObserveDepartureRetention(owner, helper, observer_admission, clock)
    try:
        with owner.sequence():
            end = owner.deadline
            state.guard(end)
            if (
                origin.factory is not admitted_factory
                or origin.pool is not pool
                or attempt._composition_origin is not admitted_factory
            ):
                raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_departure_origin_changed")
            state.guard(end)
            validate_observer_admission(observer_admission)
            if (observer_admission.server, observer_admission.database) != (
                origin.management_admission.server,
                origin.management_admission.database,
            ):
                raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_departure_admission_changed")
            state.guard(end)
            admission = _admission(departure_launcher)
            state.guard(end)
            startup_deadline = min(end, clock() + helper_startup_timeout)
            state.guard(end)
            deadline_nanoseconds(startup_deadline)

            @contextmanager
            def factory() -> Iterator[CreateOnlyEvidenceWriterV1]:
                yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

            try:
                gateway = pool.open(
                    lambda d, c: SqlClientObserveContainmentEvidenceActor(
                        factory, origin.identity, origin.startup.process, d, c
                    ),
                    deadline=end,
                )
            except TdsJournalActorUnknown as error:
                owner.retain_containment_gateway(cast(SqlClientObserveContainmentEvidenceGateway | None, error.gateway))
                raise
            owner.retain_containment_gateway(gateway)
            state.guard(end)
            owner.acknowledge_containment(deadline=end)
            state.guard(end)
            _bind_plan(state, admission, startup_deadline)
            state.guard(end)
            _run_departure(
                state,
                departure_launcher,
                evidence_root,
                management_credentials,
                PRODUCTION_DEPARTURE_INFRASTRUCTURE,
            )
            state.guard(end)
            owner.acknowledge_settlement(deadline=end)
            state.guard(end)
            helper.close_evidence(end)
            state.guard(end)
            owner.close_containment(deadline=end)
            state.guard(end)
            pool.assert_deadline(deadline=end)
            assert origin.expected is not None and owner.remote_ack is not None
            assert owner.containment_capture is not None
            outcome = SqlClientObserveSettlementOutcome(
                deepcopy(origin.expected),
                deepcopy(owner.remote_ack),
                owner.containment_capture[2],
                tuple(state.receipts[kind] for kind in Kind),
            )
            state.guard(end)
            owner.finish(deadline=end)
            state.guard(end)
        return outcome
    except BaseException:
        state.faulted = True
        try:
            state.capture_budget(termination_timeout)
            if state.containment_deadline is not None:
                state.close(deadline=state.containment_deadline)
        except BaseException:
            pass
        raise SqlClientObserveDepartureUnknown(state) from None
