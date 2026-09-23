"""Parent-neutral policy for one bounded SQLClient departure transcript."""

from collections.abc import Callable, Mapping
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast, overload

from dpone.app import mssql_sqlclient_departure_request as credentials
from dpone.app.mssql_sqlclient_departure_custody import decode_startup, encode_startup
from dpone.app.mssql_sqlclient_departure_lifecycle import legacy_strategy
from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureChildSession,
    DepartureEvidenceSession,
    DepartureOwnerAuthority,
    DepartureRunFacts,
    DepartureRunner,
    Kind,
    TdsChildExit,
    TdsCoordinatorStartup,
)
from dpone.app.mssql_sqlclient_departure_supervision import (
    SqlClientDepartureEvidenceGateway,
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureOutcome,
    _DepartureRetention,
)
from dpone.app.mssql_sqlclient_observe_departure_supervision import _ObserveDepartureRetention
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceReceipt,
    TdsConnectionMaterial,
)
from dpone.contracts.mssql_tds_api import ipc, ipc_v2, observe_ipc
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, _integer
from dpone.ports.mssql_sqlclient_departure_infrastructure import (
    DepartureInfrastructure,
    DepartureLauncher,
    DepartureProcess,
)

SqlClientDeparturePlan = ipc.SqlClientDeparturePlan
SqlClientDepartureRequest = ipc.SqlClientDepartureRequest
SqlClientDepartureResult = ipc.SqlClientDepartureResult
SqlClientDeparturePlanV2 = ipc_v2.SqlClientDeparturePlanV2
SqlClientDepartureRequestV2 = ipc_v2.SqlClientDepartureRequestV2
SqlClientDepartureResultV2 = ipc_v2.SqlClientDepartureResultV2
SqlClientObserveDeparturePlan = observe_ipc.SqlClientObserveDeparturePlan
SqlClientObserveDepartureRequest = observe_ipc.SqlClientObserveDepartureRequest
SqlClientObserveDepartureResult = observe_ipc.SqlClientObserveDepartureResult
State = _DepartureRetention | _ObserveDepartureRetention
Request = object
Result = object


def _admission(
    launcher: DepartureLauncher,
    infrastructure: DepartureInfrastructure,
) -> tuple[bytes, str, str, str, int]:
    """Capture canonical nonsecret constructor facts before any allocation."""
    admission = launcher.admission
    canonical = infrastructure.canonicalize_admission(admission)
    digest = launcher.admission_sha256
    source, root, address_space = (
        launcher.implementation_sha256,
        str(launcher.package_root),
        launcher.max_address_space_bytes,
    )
    _hash(source)
    _integer(address_space, 1, 2**63 - 1)
    if canonical != admission or sha256(canonical).hexdigest() != digest or not Path(root).is_absolute():
        raise ValueError("mssql_native.sqlclient_departure_admission_binding")
    return canonical, digest, source, root, address_space


class _LegacyOwner(DepartureOwnerAuthority):
    def __init__(self, state: State) -> None:
        self._state, self._observe = state, type(state) is _ObserveDepartureRetention

    def guard(self, deadline: float) -> None:
        self._state.guard(deadline)

    def bind_request(self, value: Request) -> None:
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            assert type(value) is SqlClientObserveDepartureRequest
            self._state.capture_helper_request(value)
        else:
            assert type(self._state) is _DepartureRetention
            self._state.request = cast(SqlClientDepartureRequest | SqlClientDepartureRequestV2, value)

    def bind_result(self, value: Result) -> None:
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            assert type(value) is SqlClientObserveDepartureResult
            self._state.capture_helper_result(value)
        else:
            assert type(self._state) is _DepartureRetention
            self._state.result = cast(SqlClientDepartureResult | SqlClientDepartureResultV2, value)

    def validate_request_owner(self) -> None:
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            self._state.validate_observe()

    def validate_final_owner(self) -> None:
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            self._state.validate_observe()
        else:
            assert type(self._state) is _DepartureRetention
            self._state.validate_create()


class _LegacyEvidence(DepartureEvidenceSession):
    def __init__(self, state: State) -> None:
        self._state = state

    def persist(self, kind: Kind, payload: bytes) -> SqlClientDepartureEvidenceReceipt:
        return self._state.persist(kind, payload)

    def receipts(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]:
        return self._state.receipts

    def expected(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]:
        return self._state.expected

    def close(self, deadline: float) -> None:
        self._state.helper.close_evidence(deadline)

    def assert_closed(self, deadline: float) -> None:
        self._state.pool.assert_deadline(deadline=deadline)


class _LegacyChild(DepartureChildSession):
    def __init__(self, state: State, child: DepartureProcess, supplier: Callable[[], TdsConnectionMaterial]):
        self._state, self._child = state, child
        self._supplier: Callable[[], TdsConnectionMaterial] | None = supplier
        self._observe = type(state) is _ObserveDepartureRetention

    def startup(self, deadline: float) -> TdsCoordinatorStartup:
        value = self._child.startup(deadline=deadline)
        self._state.helper.record_startup(value)
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            self._state.capture_helper_startup()
        startup = self._state.startup
        assert startup is not None
        return startup if self._observe else decode_startup(encode_startup(startup))

    def deliver(self, request: Request, deadline: float) -> None:
        supplier, self._supplier = self._supplier, None
        if supplier is None:
            raise ValueError("mssql_native.sqlclient_departure_one_shot")
        material = body = None
        try:
            self._state.guard(deadline)
            material = supplier()
            self._state.guard(deadline)
            if type(request) is SqlClientObserveDepartureRequest:
                body = credentials.encode_observe_departure_credentials(
                    credentials.SqlClientObserveDepartureCredentials(request=request, connection_material=material)
                )
            elif type(request) is SqlClientDepartureRequestV2:
                body = credentials.encode_departure_credentials_v2(
                    credentials.SqlClientDepartureCredentialsV2(request=request, connection_material=material)
                )
            elif type(request) is SqlClientDepartureRequest:
                body = credentials.encode_departure_credentials(
                    credentials.SqlClientDepartureCredentials(request=request, connection_material=material)
                )
            else:
                raise ValueError("mssql_native.sqlclient_departure_version_invalid")
            self._state.guard(deadline)
            self._child.send_request(body, deadline=deadline)
            self._state.guard(deadline)
        finally:
            del material, body, supplier

    def receive(self, deadline: float) -> bytes:
        try:
            raw = self._child.receive_result(deadline=deadline)
        finally:
            if self._observe:
                assert type(self._state) is _ObserveDepartureRetention
                self._state.capture_result()
            else:
                self._state.capture_result()
        if type(raw) is not bytes or raw != self._state.raw_result:
            raise ValueError("mssql_native.sqlclient_departure_result_transport_binding")
        return raw

    def settle(self, deadline: float) -> TdsChildExit:
        self._state.helper.record_exit(self._child.wait(deadline=deadline))
        local_exit = self._state.local_exit
        assert local_exit is not None
        if self._observe:
            assert type(self._state) is _ObserveDepartureRetention
            self._state.capture_helper_exit(local_exit)
        return local_exit

    def close(self) -> None:
        if self._observe:
            self._state.helper.close_child()
        else:
            assert type(self._state) is _DepartureRetention
            self._state.close_child()


def _evidence_factory(
    state: State,
    root: Path,
    facts: DepartureRunFacts,
    deadline: float,
    infrastructure: DepartureInfrastructure,
) -> _LegacyEvidence:
    subject = facts.helper_id, attempt_identity_digest(facts.plan.attempt)

    try:
        gateway = cast(
            SqlClientDepartureEvidenceGateway,
            cast(Any, state.pool).open(
                lambda d, c: infrastructure.evidence_actor(
                    lambda: infrastructure.evidence_writer(root), *subject, d, c
                ),
                deadline=deadline,
            ),
        )
    except BaseException as error:
        matched, retained = infrastructure.journal_failure(error)
        if not matched:
            raise
        _bind_evidence(state, cast(SqlClientDepartureEvidenceGateway | None, retained))
        raise error
    _bind_evidence(state, gateway)
    if type(state) is _DepartureRetention:
        observed = gateway.observation
        if type(observed) is not SqlClientDepartureEvidenceObservation or replace(
            observed
        ) != SqlClientDepartureEvidenceObservation(*subject):
            raise ValueError("mssql_native.sqlclient_departure_initial_evidence_invalid")
        state.initial_observation = observed
    return _LegacyEvidence(state)


def _bind_evidence(state: State, gateway: SqlClientDepartureEvidenceGateway | None) -> None:
    if type(state) is _ObserveDepartureRetention:
        state.bind_helper_evidence(gateway)
    else:
        state.helper.retain_evidence(gateway)


def _launch_factory(
    state: State,
    launcher: DepartureLauncher,
    supplier: Callable[[], TdsConnectionMaterial],
    facts: DepartureRunFacts,
    infrastructure: DepartureInfrastructure,
) -> _LegacyChild:
    plan = facts.plan
    try:
        child = infrastructure.spawn(launcher, plan.startup_deadline, plan.operation_deadline)
    except BaseException as error:
        matched, retained = infrastructure.launch_failure(error)
        if not matched:
            raise
        state.helper.retain_unresolved(cast(Any, retained))
        raise error
    state.helper.retain_child(cast(Any, child))
    if type(state) is _ObserveDepartureRetention:
        state.capture_helper_child()
    state.helper.record_process(child.identity)
    if type(state) is _ObserveDepartureRetention:
        state.capture_helper_process()
    state.helper.record_declared_startup(child.declared_startup)
    return _LegacyChild(state, child, supplier)


@overload
def _run_departure(
    state: _DepartureRetention,
    launcher: DepartureLauncher,
    evidence_root: Path,
    supplier: Callable[[], TdsConnectionMaterial],
    infrastructure: DepartureInfrastructure,
) -> SqlClientDepartureOutcome: ...


@overload
def _run_departure(
    state: _ObserveDepartureRetention,
    launcher: DepartureLauncher,
    evidence_root: Path,
    supplier: Callable[[], TdsConnectionMaterial],
    infrastructure: DepartureInfrastructure,
) -> None: ...


def _run_departure(
    state: State,
    launcher: DepartureLauncher,
    evidence_root: Path,
    supplier: Callable[[], TdsConnectionMaterial],
    infrastructure: DepartureInfrastructure,
) -> SqlClientDepartureOutcome | None:
    """Preserve the exact legacy type guard, then delegate one transcript."""
    if type(state) not in (_DepartureRetention, _ObserveDepartureRetention):
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    plan = state.plan
    if (type(state) is _ObserveDepartureRetention and type(plan) is not SqlClientObserveDeparturePlan) or (
        type(state) is _DepartureRetention and type(plan) not in (SqlClientDeparturePlan, SqlClientDeparturePlanV2)
    ):
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    assert plan is not None
    observe = type(state) is _ObserveDepartureRetention
    facts = DepartureRunFacts(
        state.helper_id,
        plan,
        observe,
        legacy_strategy(plan, state.observer_admission, observe),
    )
    runner = DepartureRunner(
        facts,
        _LegacyOwner(state),
        lambda value, deadline: _evidence_factory(state, evidence_root, value, deadline, infrastructure),
        lambda value: _launch_factory(state, launcher, supplier, value, infrastructure),
    )
    completion = runner.run()
    if observe:
        assert type(completion.request) is SqlClientObserveDepartureRequest
        assert type(completion.result) is SqlClientObserveDepartureResult
        return None
    return SqlClientDepartureOutcome(
        cast(SqlClientDeparturePlan | SqlClientDeparturePlanV2, completion.plan),
        cast(SqlClientDepartureRequest | SqlClientDepartureRequestV2, completion.request),
        cast(SqlClientDepartureResult | SqlClientDepartureResultV2, completion.result),
        completion.local_exit,
        completion.receipts,
    )
