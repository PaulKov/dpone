"""Private state-domain factory admission and acknowledged locator composition.

Root retains this exact wrapper and injects it into every downstream SqlClient
actor before attempt initialization. A matching UUID is not factory authority:
trusted single-store custody is required; cloned/rolled-back state is unsupported.
The caller retains UNKNOWN gateways and its shared actor pool for containment.
"""

import sys
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from uuid import UUID

from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.adapters.mssql_tds_coordinator_journal import TdsCoordinatorJournal
from dpone.adapters.mssql_tds_directory_journal import TdsCoordinatorDirectoryJournal
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.app.mssql_tds_attempt_composition import StoreFactory
from dpone.contracts.mssql_tds_api import (
    SqlClientStageLocator,
    SqlClientStageLocatorSnapshot,
    SqlClientStageLookup,
    TdsCreateRequest,
    WindowLease,
    WindowRecord,
    _integer,
    _text,
    decode_create_request,
    decode_stage_locator,
    encode_create_request,
    encode_stage_locator,
    validate_locator_request,
    validate_state_domain_record,
)
from dpone.ports.bounded_window import WindowStore
from dpone.services.mssql_sqlclient_stage_locator import (
    SqlClientStageLocatorJournal,
    admit_state_domain,
    require_state_domain,
)


class SqlClientStateDomainActor(_ActorCore[WindowStore, WindowRecord]):
    """One bounded initial read/create/readback under the original target lease."""

    def __init__(self, factory: StoreFactory, lease: WindowLease, deadline: float, clock: Callable[[], float]) -> None:
        if type(lease) is not WindowLease:
            raise ValueError("mssql_native.sqlclient_domain_lease_invalid")
        _text(lease.target_id)
        _text(lease.owner)
        _integer(lease.fence, 1)
        self._lease = WindowLease(lease.target_id, lease.owner, lease.fence)
        super().__init__(factory, deadline, clock, WindowRecord)

    def _initial(self, backend: WindowStore) -> WindowRecord:
        record = admit_state_domain(backend, self._lease)
        validate_state_domain_record(record)
        return WindowRecord(record.revision, record.payload)

    def _dispatch(self, backend: WindowStore, command: _ActorCommand[WindowRecord]) -> WindowRecord:
        raise TdsJournalActorUnknown(self)


@dataclass(frozen=True)
class CreateSqlClientStageLocator:
    locator: SqlClientStageLocator
    request: TdsCreateRequest
    lease: WindowLease


@dataclass(frozen=True)
class ReadSqlClientStageLocator:
    lookup: SqlClientStageLookup


class SqlClientStageLocatorActor(_ActorCore[WindowStore, SqlClientStageLocatorSnapshot]):
    """One explicit create or read initialization; read mode cannot be upgraded."""

    def __init__(
        self,
        factory: StoreFactory,
        domain: WindowRecord,
        initialization: CreateSqlClientStageLocator | ReadSqlClientStageLocator,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        validate_state_domain_record(domain)
        self._domain = WindowRecord(domain.revision, domain.payload)
        if type(initialization) is CreateSqlClientStageLocator:
            validate_locator_request(initialization.locator, initialization.request)
            lease = initialization.lease
            if type(lease) is not WindowLease:
                raise ValueError("mssql_native.sqlclient_locator_lease_invalid")
            _text(lease.target_id)
            _text(lease.owner)
            _integer(lease.fence, 1)
            self._initialization: CreateSqlClientStageLocator | ReadSqlClientStageLocator = CreateSqlClientStageLocator(
                decode_stage_locator(encode_stage_locator(initialization.locator)),
                decode_create_request(encode_create_request(initialization.request)),
                WindowLease(lease.target_id, lease.owner, lease.fence),
            )
        elif type(initialization) is ReadSqlClientStageLocator:
            lookup = initialization.lookup
            if type(lookup) is not SqlClientStageLookup:
                raise ValueError("mssql_native.sqlclient_locator_lookup_invalid")
            # Snapshot exact validated nested values without sharing mutable shells.
            lookup.__post_init__()
            server, database = lookup.server, lookup.database
            self._initialization = ReadSqlClientStageLocator(
                SqlClientStageLookup(
                    UUID(int=lookup.state_domain_id.int),
                    type(server)(
                        server.server_name, server.machine_name, server.instance_name, server.physical_machine_name
                    ),
                    type(database)(database.name, database.database_id, UUID(int=database.database_guid.int)),
                    lookup.owner_binding,
                    UUID(int=lookup.object_nonce.int),
                )
            )
        else:
            raise ValueError("mssql_native.sqlclient_locator_initialization_invalid")
        super().__init__(factory, deadline, clock, SqlClientStageLocatorSnapshot)

    def _initial(self, backend: WindowStore) -> SqlClientStageLocatorSnapshot:
        parents = TdsAttemptJournal(backend, backend="mssql_sqlclient")
        directories = TdsCoordinatorDirectoryJournal(backend, parent_observer=parents)
        coordinators = TdsCoordinatorJournal(backend, directories)
        journal = SqlClientStageLocatorJournal(
            backend,
            self._domain,
            parent_observer=parents,
            directory_observer=directories,
            coordinator_observer=coordinators,
        )
        request = self._initialization
        if isinstance(request, CreateSqlClientStageLocator):
            return journal.create(request.locator, request.request, request.lease)
        return journal.read(request.lookup)

    def _dispatch(
        self, backend: WindowStore, command: _ActorCommand[SqlClientStageLocatorSnapshot]
    ) -> SqlClientStageLocatorSnapshot:
        raise TdsJournalActorUnknown(self)


class _AdmittedSqlClientStoreFactory:
    """Root-owned callable retaining original factory and exact acknowledged marker.

    Construction is private to successful admission composition. This is not an
    unforgeable security token against arbitrary Python executing in-process.
    """

    def __init__(self, factory: StoreFactory, record: WindowRecord) -> None:
        self._domain_id = validate_state_domain_record(record)
        self._factory = factory
        self._record = WindowRecord(record.revision, record.payload)

    @property
    def domain_id(self) -> UUID:
        """Discovery identity only, never sufficient to reconstruct an admission."""
        return self._domain_id

    def __call__(self) -> AbstractContextManager[WindowStore]:
        return self._context()

    @contextmanager
    def _context(self) -> Iterator[WindowStore]:
        context = self._factory()
        store = context.__enter__()
        try:
            require_state_domain(store, self._record)
            yield store
        finally:
            # Preserve validation/body failures even if a backend suppresses them.
            context.__exit__(*sys.exc_info())


def admit_sqlclient_state_domain(
    *, store_factory: StoreFactory, lease: WindowLease, pool: TdsActorPool, deadline: float
) -> _AdmittedSqlClientStoreFactory:
    """Admit before attempt initialization; return only after actual actor exit.

    On timeout or uncertain teardown TdsJournalActorUnknown retains the actual
    actor gateway. The caller must retain it/pool; this function never retries.
    """
    actor = pool.open(lambda end, clock: SqlClientStateDomainActor(store_factory, lease, end, clock), deadline=deadline)
    record = actor.snapshot
    actor.close(deadline=deadline)
    pool.assert_deadline(deadline=deadline)
    return _AdmittedSqlClientStoreFactory(store_factory, record)


def _locator(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    initialization: CreateSqlClientStageLocator | ReadSqlClientStageLocator,
    pool: TdsActorPool,
    deadline: float,
) -> SqlClientStageLocatorSnapshot:
    if type(admitted_factory) is not _AdmittedSqlClientStoreFactory:
        raise ValueError("mssql_native.sqlclient_state_domain_admission_required")
    actor = pool.open(
        lambda end, clock: SqlClientStageLocatorActor(
            admitted_factory, admitted_factory._record, initialization, end, clock
        ),
        deadline=deadline,
    )
    snapshot = actor.snapshot
    actor.close(deadline=deadline)
    pool.assert_deadline(deadline=deadline)
    return snapshot


def create_sqlclient_stage_locator(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    locator: SqlClientStageLocator,
    request: TdsCreateRequest,
    lease: WindowLease,
    pool: TdsActorPool,
    deadline: float,
) -> SqlClientStageLocatorSnapshot:
    """Acknowledge discovery CAS and actor teardown before coordinator CREATE."""
    return _locator(
        admitted_factory=admitted_factory,
        initialization=CreateSqlClientStageLocator(locator, request, lease),
        pool=pool,
        deadline=deadline,
    )


def read_sqlclient_stage_locator(
    *,
    admitted_factory: _AdmittedSqlClientStoreFactory,
    lookup: SqlClientStageLookup,
    pool: TdsActorPool,
    deadline: float,
) -> SqlClientStageLocatorSnapshot:
    """Bounded point resolution and original journal reads, never replay authority."""
    return _locator(
        admitted_factory=admitted_factory,
        initialization=ReadSqlClientStageLocator(lookup),
        pool=pool,
        deadline=deadline,
    )
