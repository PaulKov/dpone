"""Trusted original CREATE-to-departure composition on the caller's actor pool.

The caller supplies an acknowledged original CREATE reservation, complete owner
lease, immutable admitted launchers, and separately admitted creator identity.
Success leaves attempt/directory progression unchanged. It does not prepare a
worker, settle SQL state, or certify a complete data route.
"""

from collections.abc import Callable
from dataclasses import fields, replace
from pathlib import Path
from time import monotonic
from typing import cast
from uuid import UUID, uuid4

from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.adapters.mssql_tds_coordinator_process import PythonTdsCoordinatorLauncher
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import (
    PRODUCTION_DEPARTURE_INFRASTRUCTURE,
)
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import (
    admitted_departure_launcher as _admission,
)
from dpone.app.mssql_sqlclient_departure_supervision import (
    SqlClientCreateDepartureOutcome,
    SqlClientCreateDepartureUnknown,
    _DepartureRetention,
)
from dpone.app.mssql_sqlclient_departure_supervisor import _run_departure
from dpone.app.mssql_sqlclient_stage_locator_composition import (
    _AdmittedSqlClientStoreFactory,
    create_sqlclient_stage_locator,
)
from dpone.app.mssql_tds_attempt_composition import StoreFactory
from dpone.app.mssql_tds_coordinator_composition import create_tds_coordinator, open_tds_coordinator_evidence
from dpone.app.mssql_tds_coordinator_supervision import TdsCoordinatorFailure, TdsCoordinatorSupervisionUnknown
from dpone.app.mssql_tds_coordinator_supervisor import run_tds_coordinator
from dpone.app.mssql_tds_create_provenance import _identity as validate_original_create_identity
from dpone.app.mssql_tds_create_provenance import _request as validate_original_create_request
from dpone.contracts.mssql_tds_api import (
    create_command_digest,
    decode_create_request,
    encode_create_request,
    validate_observer_admission,
)
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_operation_models import (
    CreateKind,
    SqlClientDatabasePrincipal,
    SqlClientDeparturePlan,
    SqlClientDeparturePlanV2,
    SqlClientObserverAdmission,
    SqlClientStageLocator,
    TdsConnectionMaterial,
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorIdentity,
    TdsCreateRequest,
    TdsDatabaseObservation,
    WindowLease,
)
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorGateway
from dpone.ports.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceGateway
from dpone.services.mssql_tds_attempt import TdsAttempt, _ShutdownCapability


def _run_sqlclient_create_departure(
    attempt: TdsAttempt,
    request: TdsCreateRequest,
    create_identity: TdsCoordinatorIdentity,
    *,
    version: int,
    observer_admission: SqlClientObserverAdmission | None,
    pool: TdsActorPool,
    store_factory: StoreFactory,
    lease: WindowLease,
    evidence_root: Path,
    create_launcher: PythonTdsCoordinatorLauncher,
    departure_launcher: PythonSqlClientDepartureLauncher,
    creator_admission: SqlClientObserverAdmission,
    creator_principal: SqlClientDatabasePrincipal,
    create_connection_material: Callable[[], TdsConnectionMaterial],
    observer_connection_material: Callable[[], TdsConnectionMaterial],
    operation_deadline: float,
    create_startup_timeout: float,
    helper_startup_timeout: float,
    termination_timeout: float,
    clock: Callable[[], float] = monotonic,
) -> SqlClientCreateDepartureOutcome:
    """Run actual CREATE then six helper ACKs under one original attempt authority.

    No reservation retry or outcome callback exists. Failures retain all partial
    capabilities through SqlClientCreateDepartureUnknown.close(deadline=...).
    Cleanup uses one captured budget and never closes the caller's shared pool.
    """
    state = _DepartureRetention(attempt, pool, uuid4(), request, create_identity, clock)
    expected_database = expected_server = domain_id = None
    try:
        for value in (operation_deadline, create_startup_timeout, helper_startup_timeout, termination_timeout):
            deadline_nanoseconds(value)
        if not all(
            callable(v) for v in (clock, store_factory, create_connection_material, observer_connection_material)
        ):
            raise ValueError("mssql_native.sqlclient_departure_arguments_invalid")
        # These validators reconstruct every original nested DTO before any codec
        # can normalize enum/string, integer/bool, tuple/list or subclass aliases.
        validate_original_create_identity(create_identity)
        validate_original_create_request(request)
        if type(version) is not int or version not in (1, 2):
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
        if version == 2:
            if type(store_factory) is not _AdmittedSqlClientStoreFactory:
                raise ValueError("mssql_native.sqlclient_state_domain_admission_required")
            attempt._assert_composition_origin(store_factory)
            for uuid_value in (store_factory.domain_id, request.object_nonce, create_identity.operation_id):
                if (
                    type(uuid_value) is not UUID
                    or type(uuid_value.int) is not int
                    or not 1 <= uuid_value.int <= 2**128 - 1
                ):
                    raise ValueError("mssql_native.sqlclient_departure_uuid_invalid")
            domain_id = UUID(int=store_factory.domain_id.int)
            if observer_admission is None:
                raise ValueError("mssql_native.sqlclient_departure_observer_binding")
            validate_observer_admission(observer_admission)
            validate_observer_admission(creator_admission)
            assert observer_admission is not None
            if (observer_admission.server, observer_admission.database) != (
                creator_admission.server,
                creator_admission.database,
            ):
                raise ValueError("mssql_native.sqlclient_departure_observer_binding")
            expected_server = replace(creator_admission.server)
            expected_database = TdsDatabaseObservation(
                creator_admission.database.database_name,
                creator_admission.database.database_id,
                UUID(creator_admission.database.database_guid),
            )
            # Reconstruct nested originals after strict validation, before any effect.
            state.observer_admission = replace(
                observer_admission,
                server=replace(observer_admission.server),
                database=replace(observer_admission.database),
                login=replace(observer_admission.login),
                transport=replace(observer_admission.transport),
            )
        elif observer_admission is not None:
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
        create = _admission(create_launcher)
        helper = _admission(departure_launcher)
        state.create_admission, _, state.create_source, state.create_root, _ = create
        if (
            type(create_identity) is not TdsCoordinatorIdentity
            or type(request) is not TdsCreateRequest
            or request.parent != create_identity.parent
            or create_command_digest(request) != create_identity.command_sha256
            or create_identity.implementation_sha256 != state.create_source
        ):
            raise ValueError("mssql_native.sqlclient_departure_create_binding")
        state.original_request = decode_create_request(encode_create_request(request))
        state.create_identity = replace(create_identity, parent=replace(create_identity.parent))
        if version == 2:
            state.create_identity = replace(
                state.create_identity, operation_id=UUID(int=create_identity.operation_id.int)
            )
            request, create_identity = state.original_request, state.create_identity
        if not isinstance(evidence_root, Path) or not evidence_root.is_absolute():
            raise ValueError("mssql_native.sqlclient_departure_evidence_root")
        with attempt._create_departure_sequence(state.helper_id, create_identity, deadline=operation_deadline) as (
            parent,
            directory,
            shutdown,
        ):
            state.attempt_shutdown = shutdown
            state.owner = owner = replace(parent.state.ownership)
            if (
                type(lease) is not WindowLease
                or (lease.target_id, lease.owner, lease.fence)
                != (parent.state.identity.target_key, owner.owner, owner.fence)
                or type(lease.fence) is not int
            ):
                raise ValueError("mssql_native.sqlclient_departure_lease_binding")
            state.guard(operation_deadline)
            try:
                state.create_writer = create_tds_coordinator(
                    pool,
                    store_factory,
                    create_identity,
                    directory.state.limits,
                    lease,
                    supervisor_token=owner.supervisor_id,
                    deadline=operation_deadline,
                )
            except TdsJournalActorUnknown as error:
                state.create_writer = cast(TdsCoordinatorGateway | None, error.gateway)
                raise
            state.guard(operation_deadline)
            if version == 2:
                assert type(store_factory) is _AdmittedSqlClientStoreFactory
                assert domain_id is not None and expected_server is not None and expected_database is not None
                locator = SqlClientStageLocator(
                    domain_id,
                    expected_server,
                    expected_database,
                    request.object_nonce,
                    create_identity,
                    owner,
                    directory.state.limits,
                )
                try:
                    create_sqlclient_stage_locator(
                        admitted_factory=store_factory,
                        locator=locator,
                        request=request,
                        lease=lease,
                        pool=pool,
                        deadline=operation_deadline,
                    )
                except TdsJournalActorUnknown as error:
                    state.locator_gateway = cast(_ShutdownCapability | None, error.gateway)
                    raise
                state.guard(operation_deadline)
                pool.assert_deadline(deadline=operation_deadline)
            try:
                state.create_evidence = open_tds_coordinator_evidence(
                    pool, evidence_root, coordinator_identity_digest(create_identity), deadline=operation_deadline
                )
            except TdsJournalActorUnknown as error:
                state.create_evidence = cast(TdsCoordinatorEvidenceGateway | None, error.gateway)
                raise
            state.guard(operation_deadline)
            evidence_initial = state.create_evidence.observation
            if type(evidence_initial) is not TdsCoordinatorEvidenceObservation or replace(
                evidence_initial
            ) != TdsCoordinatorEvidenceObservation(coordinator_identity_digest(create_identity)):
                raise ValueError("mssql_native.sqlclient_departure_initial_create_evidence")
            initial = state.create_writer.observation.snapshot
            if (
                initial is None
                or initial.state.identity != create_identity
                or initial.state.ownership != owner
                or initial.state.execution_owner != owner
            ):
                raise ValueError("mssql_native.sqlclient_departure_initial_create_binding")
            replace(initial)
            replace(initial.state)
            replace(initial.state.identity)
            replace(initial.state.identity.parent)
            replace(initial.state.ownership)
            replace(initial.state.execution_owner)
            created = run_tds_coordinator(
                state.create_writer,
                state.create_evidence,
                create_launcher,
                request,
                state.create_admission,
                create_connection_material,
                pool=pool,
                operation_deadline=operation_deadline,
                startup_timeout=create_startup_timeout,
                termination_timeout=termination_timeout,
                clock=clock,
                _parent_assertion=state.guard,
                _expected_database=expected_database,
            )
            state.create_outcome = created
            state.create_writer_closed = state.create_evidence_closed = True
            state.guard(operation_deadline)
            state.validate_create()
            if version == 2:
                assert type(store_factory) is _AdmittedSqlClientStoreFactory
                state.seal_create(store_factory, locator, lease, operation_deadline)
            assert created.provenance is not None and created.response.evidence is not None
            receipts = {receipt.kind: receipt for receipt in created.receipts}
            startup_deadline = min(operation_deadline, clock() + helper_startup_timeout)
            deadline_nanoseconds(startup_deadline)
            common_plan = SqlClientDeparturePlan(
                helper_id=state.helper_id,
                attempt=parent.state.identity,
                ownership=owner,
                create_operation=create_identity,
                create_process=created.provenance.startup.process,
                create_result_sha256=receipts[CreateKind.RESULT].payload_sha256,
                create_local_exit_sha256=receipts[CreateKind.LOCAL_EXIT].payload_sha256,
                original=created.response.evidence.session,
                database=created.response.evidence.database,
                creator_admission=creator_admission,
                principal=creator_principal,
                implementation_sha256=helper[2],
                package_root=helper[3],
                admission_sha256=helper[1],
                max_address_space_bytes=helper[4],
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
            )
            if version == 2:
                assert state.observer_admission is not None
                configured = state.observer_admission
                state.plan = SqlClientDeparturePlanV2(
                    **{f.name: getattr(common_plan, f.name) for f in fields(common_plan) if f.name != "schema"},
                    observer_admission=replace(
                        configured,
                        server=replace(configured.server),
                        database=replace(configured.database),
                        login=replace(configured.login),
                        transport=replace(configured.transport),
                    ),
                )
            else:
                state.plan = common_plan
            helper_outcome = _run_departure(
                state,
                departure_launcher,
                evidence_root,
                observer_connection_material,
                PRODUCTION_DEPARTURE_INFRASTRUCTURE,
            )
            outcome = SqlClientCreateDepartureOutcome(created, helper_outcome)
            state.guard(operation_deadline)
            pool.assert_deadline(deadline=operation_deadline)
            from dpone.services.mssql_tds_original_continuation import CreateSettlement

            settlement = CreateSettlement(attempt, outcome, state, store_factory, parent, directory, operation_deadline)
        return cast(SqlClientCreateDepartureOutcome, settlement.activate_after_context())
    except BaseException as error:
        state.faulted = True
        if isinstance(error, (TdsCoordinatorFailure, TdsCoordinatorSupervisionUnknown)):
            state.create_retained = error.retained
        try:
            state.capture_budget(termination_timeout)
            if state.containment_deadline is not None:
                state.close(deadline=state.containment_deadline)
        except BaseException:
            pass
        raise SqlClientCreateDepartureUnknown(state) from None


from dpone.app.mssql_sqlclient_create_departure_entrypoints import (  # noqa: E402
    run_sqlclient_create_departure as run_sqlclient_create_departure,
)
from dpone.app.mssql_sqlclient_create_departure_entrypoints import (  # noqa: E402
    run_sqlclient_create_departure_v2 as run_sqlclient_create_departure_v2,
)
