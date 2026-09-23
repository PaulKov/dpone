"""Explicit retained OBSERVE lifecycle with original parent evidence custody.

Collection is one-use. Selected observation always targets the frozen request.
The caller must close this handle; UNKNOWN retains cleanup capabilities, never
permission to replay commands, reconnect SQL, or claim remote settlement.
"""

import secrets
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from time import monotonic
from typing import Any, NoReturn
from uuid import uuid4

from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_dependency_bundle import (
    SqlClientObserveDependencies,
    sqlclient_observe_dependencies,
)
from dpone.app.mssql_sqlclient_grant_inventory_composition import (
    SqlClientGrantInventoryCollector,
    SqlClientGrantInventoryUnknown,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory
from dpone.contracts.mssql_sqlclient_observe import MAX_REQUEST_PAYLOAD_BYTES, decode_request, encode_request
from dpone.contracts.mssql_sqlclient_observe_handshake import (
    encode_credentials,
    validate_request_accepted,
)
from dpone.contracts.mssql_tds_api import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_operation_models import (
    CONTROL_PAYLOAD_BYTES,
    MAX_CREDENTIAL_PAYLOAD_BYTES,
    SqlClientDatabasePrincipal,
    SqlClientObserveCredentials,
    SqlClientObserveRequest,
    TdsChildExit,
    TdsConnectionMaterial,
    TdsCoordinatorAuthority,
    TdsCoordinatorCommand,
    TdsCoordinatorIdentity,
    TdsCoordinatorStartup,
    WindowLease,
)
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds, deadline_seconds
from dpone.ports.mssql_tds_coordinator import TdsCoordinatorGateway
from dpone.ports.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceGateway
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown, TdsUnresolvedLaunch
from dpone.services.mssql_tds_attempt import TdsAttempt
from dpone.services.mssql_tds_attempt_continuation import TdsObserveContinuation
from dpone.services.mssql_tds_original_continuation import PreparationTransition

_ERROR = "mssql_native.sqlclient_observe_unknown"
_observe_dependencies = sqlclient_observe_dependencies


class SqlClientObserveUnknown(WindowOutcomeUnknown):
    """Actual child/actors remain retained; cleanup is the only operation."""

    def __init__(self, retained: "SqlClientRetainedObserve") -> None:
        self.retained = retained
        super().__init__(_ERROR)

    def close(self, *, deadline: float) -> None:
        self.retained.close(deadline=deadline)


class SqlClientRetainedObserve:
    """Composition-owned original attempt, coordinator, process and actor pool."""

    def __init__(
        self,
        attempt: TdsAttempt,
        request: SqlClientObserveRequest,
        factory: _AdmittedSqlClientStoreFactory,
        pool: TdsActorPool,
        termination_timeout: float,
        dependencies: SqlClientObserveDependencies | None = None,
    ) -> None:
        self.attempt, self.request, self.factory, self.pool = attempt, request, factory, pool
        self.helper_id = uuid4()
        self.operation_deadline = deadline_seconds(request.operation_deadline_ns)
        self.termination_timeout = termination_timeout
        self.dependencies = dependencies or _observe_dependencies()
        self.identity: TdsCoordinatorIdentity | None = None
        self.child: Any | None = None
        self.unresolved: TdsUnresolvedLaunch | None = None
        self.writer: TdsCoordinatorGateway | None = None
        self.evidence: TdsCoordinatorEvidenceGateway | None = None
        self.continuation: TdsObserveContinuation | None = None
        self.catalog: Any | None = None
        self.authority: TdsCoordinatorAuthority | None = None
        self.startup_receipt: TdsCoordinatorStartup | None = None
        self.collector: SqlClientGrantInventoryCollector | None = None
        self._contained_exit: TdsChildExit | None = None
        self._faulted = self._used = self._closed = self._active = False
        self._cleanup_deadline: float | None = None
        self._cleanup_capture_started = False
        self._closed_gateways: set[int] = set()
        self._unknown_gateways: set[int] = set()
        self._unresolved_close_started = False
        self._failed_gateways: list = []
        self._observations: list = []

    @property
    def channel(self):
        assert self.child is not None
        return self.child.channel

    def assert_current(self) -> None:
        try:
            if self._faulted or self._closed or monotonic() >= self.operation_deadline:
                raise SqlClientObserveUnknown(self)
            assert self.continuation is not None
            preparation = self.attempt._preparation
            if type(preparation) is PreparationTransition and preparation.attempted:
                preparation.assert_current(include_sql=False)
            else:
                self.continuation.assert_current()
            if self.child is not None:
                self.child.assert_current()
                self.dependencies.require_quiet(self.channel)
            self.pool.assert_deadline(deadline=self.operation_deadline)
        except BaseException as error:
            self._failed(error)

    def _failed(self, error: BaseException) -> NoReturn:
        self._faulted = True
        if isinstance(error, TdsJournalActorUnknown) and error.gateway is not None:
            self._failed_gateways.append(error.gateway)
        if isinstance(error, SqlClientGrantInventoryUnknown) and error.gateway is not None:
            self._failed_gateways.append(error.gateway)
        try:
            # A failed first clock/property read cannot authorize a later budget.
            cleanup_deadline = self._capture_cleanup(None)
            self.close(deadline=cleanup_deadline)
        except BaseException:
            pass
        raise SqlClientObserveUnknown(self) from None

    def collect_authenticated(self, *, reader_factory: Any):
        try:
            if self._used:
                raise ValueError(_ERROR)
            self._used = True
            assert self.identity is not None and self.catalog is not None and self.continuation is not None
            with self.attempt._observe_sequence(self.helper_id, self.identity, deadline=self.operation_deadline):
                self.assert_current()
                principal = self.request.writer_principal
                self.collector = SqlClientGrantInventoryCollector.from_catalog(
                    self.catalog,
                    management_admission=self.request.management_admission,
                    writer_admission=self.request.writer_admission,
                    writer_principal=SqlClientDatabasePrincipal(principal.principal_id, principal.name, principal.sid),
                    admitted_factory=self.factory,
                    pool=self.pool,
                    limits=self.request.limits,
                    deadline=self.operation_deadline,
                    continuation=self.continuation,
                )
                result = self.collector.collect_authenticated(reader_factory=reader_factory)
                self._observations.append(result)
                self.assert_current()
            return result
        except BaseException as error:
            self._failed(error)

    def observe_selected(self):
        try:
            assert self.identity is not None and self.catalog is not None
            with self.attempt._observe_sequence(self.helper_id, self.identity, deadline=self.operation_deadline):
                self.assert_current()
                result = self.catalog.observe_selected()
                self._observations.append(result)
                self.assert_current()
            return result
        except BaseException as error:
            self._failed(error)

    def _capture_cleanup(self, supplied: float | None) -> float:
        if self._cleanup_deadline is None:
            if self._cleanup_capture_started:
                raise SqlClientObserveUnknown(self)
            self._cleanup_capture_started = True
            original = self.child.cleanup_deadline if self.child is not None else None
            captured = original if original is not None else monotonic() + self.termination_timeout
            deadline_nanoseconds(captured)
            self._cleanup_deadline = captured
        if supplied is not None:
            self._cleanup_deadline = min(self._cleanup_deadline, supplied)
        return self._cleanup_deadline

    def close(self, *, deadline: float) -> None:
        deadline_nanoseconds(deadline)
        if self._closed:
            return
        self._faulted = True
        try:
            end = self._capture_cleanup(deadline)
            if self.unresolved is not None:
                if self._unresolved_close_started:
                    raise SqlClientObserveUnknown(self)
                self.unresolved.contain(deadline=end)
                self._unresolved_close_started = True
                self.unresolved.close()
                self.unresolved = None
            if self.child is not None:
                contained = self.child.contain(deadline=end)
                if self.startup_receipt is not None:
                    if type(contained) is not TdsChildExit:
                        raise ValueError(_ERROR)
                    replace(contained)
                    replace(contained.identity)
                    if not contained.reaped or contained.identity != self.startup_receipt.process:
                        raise ValueError(_ERROR)
                    self._contained_exit = contained
                self.child.close()
            if self._active:
                self.attempt._end_observe(self.helper_id)
                self._active = False
            failed = False
            for gateway in (*self._failed_gateways, self.evidence, self.writer):
                if gateway is None or id(gateway) in self._closed_gateways:
                    continue
                if id(gateway) in self._unknown_gateways:
                    failed = True
                    continue
                self._unknown_gateways.add(id(gateway))
                try:
                    gateway.close(deadline=end)
                    self._closed_gateways.add(id(gateway))
                    self._unknown_gateways.remove(id(gateway))
                except BaseException:
                    failed = True
            if failed:
                raise ValueError(_ERROR)
            self._closed = True
        except BaseException:
            raise SqlClientObserveUnknown(self) from None


def open_sqlclient_observe(
    attempt: TdsAttempt,
    request: SqlClientObserveRequest,
    *,
    pool: TdsActorPool,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    lease: WindowLease,
    evidence_root: Path,
    launcher: Any,
    connection_material: Callable[[], TdsConnectionMaterial],
    startup_timeout: float,
    termination_timeout: float,
) -> SqlClientRetainedObserve:
    """Reserve, register and hold OBSERVE; no fabricated CREATE or writer grant."""
    # Every original request and composition binding is checked before reservation.
    if type(attempt) is not TdsAttempt or type(admitted_factory) is not _AdmittedSqlClientStoreFactory:
        raise ValueError(_ERROR)
    attempt._assert_composition_origin(admitted_factory)
    frozen = decode_request(encode_request(request))
    deadline_nanoseconds(startup_timeout)
    deadline_nanoseconds(termination_timeout)
    dependencies = _observe_dependencies()
    if type(launcher) is not dependencies.launcher_type or not callable(connection_material):
        raise ValueError(_ERROR)
    typed_launcher: Any = launcher
    typed_launcher.assert_installation()
    parent = attempt.lifecycle
    if (
        frozen.parent != parent.state.identity
        or type(lease) is not WindowLease
        or (lease.target_id, lease.owner, lease.fence)
        != (parent.state.identity.target_key, parent.state.ownership.owner, parent.state.ownership.fence)
    ):
        raise ValueError(_ERROR)
    handle = SqlClientRetainedObserve(attempt, frozen, admitted_factory, pool, termination_timeout, dependencies)
    end = handle.operation_deadline
    try:
        pool.assert_deadline(deadline=end)
        reserved = attempt.reserve_operation(
            uuid4(), TdsCoordinatorCommand.OBSERVE, frozen.command_sha256, deadline=end
        )
        identity = handle.identity = TdsObserveContinuation.reserved_identity(
            frozen, reserved, typed_launcher.implementation_sha256
        )
        handle.writer = dependencies.create_coordinator(
            pool,
            admitted_factory,
            identity,
            reserved.state.limits,
            lease,
            supervisor_token=parent.state.ownership.supervisor_id,
            deadline=end,
        )
        try:
            attempt._begin_observe(handle.helper_id, identity, deadline=end)
        finally:
            # Begin may latch the association before a later authority check fails.
            handle._active = attempt._observe_helper_id == handle.helper_id
        handle.continuation = TdsObserveContinuation(attempt, handle.helper_id, identity, handle.writer, deadline=end)
        with attempt._observe_sequence(handle.helper_id, identity, deadline=end):
            handle.assert_current()
            handle.evidence = dependencies.open_evidence(
                pool, evidence_root, handle.continuation.operation_sha256, deadline=end
            )
            handle.assert_current()
            handle.continuation.bind_evidence(handle.evidence, typed_launcher.admission)
            startup_end = min(end, monotonic() + startup_timeout)
            handle.assert_current()
            try:
                handle.child = typed_launcher.spawn(
                    startup_deadline=startup_end, operation_deadline=end, termination_timeout=termination_timeout
                )
            except TdsLaunchUnknown as error:
                handle.unresolved = error.launch
                raise
            startup = handle.startup_receipt = handle.child.startup()
            handle.assert_current()
            handle.continuation.register_process(startup, typed_launcher.admission)
            handle.assert_current()
            dependencies.write_frame(
                handle.channel, encode_request(frozen), deadline=end, limit=MAX_REQUEST_PAYLOAD_BYTES
            )
            ack = dependencies.read_frame(handle.channel, deadline=end, limit=CONTROL_PAYLOAD_BYTES)
            validate_request_accepted(ack, frozen, startup)
            handle.assert_current()
            handle.continuation.acknowledge_credentials()
            handle.assert_current()
            material = connection_material()
            nonce = secrets.token_bytes(32)
            try:
                credentials = SqlClientObserveCredentials(
                    frozen.command_sha256,
                    identity,
                    replace(parent.state.ownership),
                    startup.process,
                    startup.launch_nonce,
                    nonce,
                    material,
                    dependencies.decode_connection_admission(typed_launcher.admission)[1],
                )
                payload = encode_credentials(credentials, frozen)
                try:
                    handle.assert_current()
                    dependencies.write_frame(handle.channel, payload, deadline=end, limit=MAX_CREDENTIAL_PAYLOAD_BYTES)
                finally:
                    del payload
            finally:
                del material
            authority = handle.authority = handle.continuation.register_session(
                dependencies.read_frame(handle.channel, deadline=end, limit=CONTROL_PAYLOAD_BYTES), frozen, nonce
            )
            handle.catalog = dependencies.catalog_type(handle, frozen, credentials, authority)
            del credentials
            handle.assert_current()
        attempt._observe_origin = handle
        return handle
    except BaseException as error:
        handle._failed(error)
        raise AssertionError("unreachable")
