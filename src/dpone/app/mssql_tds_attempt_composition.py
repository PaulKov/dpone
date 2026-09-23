"""Explicit two-actor TDS composition; backend I/O stays on actor threads.

Discovery reads both journals through bounded actors and grants no writer
authority. Recovery accepts exact snapshots; missing directories are never
created implicitly. Whole-route attempt enumeration remains a caller obligation.
"""

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass

from dpone.adapters.mssql_coordinator_worker_capabilities import (
    TdsActorPool,
    TdsAttemptJournal,
    TdsAttemptObserverActor,
    TdsCoordinatorDirectoryJournal,
    TdsDirectoryActor,
    TdsJournalActor,
    TdsJournalActorUnknown,
)
from dpone.contracts.mssql_coordinator_worker_capabilities import (
    TdsAttemptIdentity,
    TdsAttemptOwnership,
    TdsAttemptPhase,
    TdsAttemptSnapshot,
    WindowLease,
    initial_state,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits, TdsDirectorySnapshot, initial_directory
from dpone.contracts.mssql_tds_suspension import TdsAttemptSuspension
from dpone.ports.mssql_coordinator_worker_capabilities import (
    AssertDirectoryAuthority,
    CreateDirectory,
    ReadDirectory,
    ResumeDirectory,
    TakeOverDirectory,
    TdsAttemptObserver,
    TdsAttemptWriter,
    TdsDirectoryStore,
    WindowStore,
)
from dpone.services.mssql_tds_attempt import TdsAttempt, TdsAttemptUnknown, _ShutdownCapability

StoreFactory = Callable[[], AbstractContextManager[WindowStore]]


def create_tds_attempt(
    pool: TdsActorPool,
    store_factory: StoreFactory,
    identity: TdsAttemptIdentity,
    limits: TdsDirectoryLimits,
    lease: WindowLease,
    *,
    supervisor_token: str,
    deadline: float,
    backend: str = "mssql_python",
) -> TdsAttempt:
    """Acknowledge exact parent intent before creating its directory.

    Each actor enters its own injected store context. Construction failure retains
    partial durable state and pool reservations; no usable attempt is returned.
    The caller retains the pool and any TdsAttemptUnknown for bounded teardown.
    """
    owner = TdsAttemptOwnership(lease.owner, lease.fence, supervisor_token)
    planned = initial_state(identity, owner, backend=backend)
    initial_directory(identity, limits, schema_version=planned.schema_version)  # Before effects.
    if identity.target_key != lease.target_id:
        raise ValueError("mssql_native.tds_attempt_lease_mismatch")

    @contextmanager
    def lifecycle_factory() -> Iterator[TdsAttemptWriter]:
        with store_factory() as store:
            yield TdsAttemptJournal(store, backend=backend).create(identity, lease, supervisor_token=supervisor_token)

    return _open(
        pool,
        store_factory,
        lifecycle_factory,
        CreateDirectory(identity, limits, lease, supervisor_token),
        owner,
        deadline=deadline,
    )


def recover_tds_attempt(
    pool: TdsActorPool,
    store_factory: StoreFactory,
    observed_parent: TdsAttemptSnapshot,
    observed_directory: TdsDirectorySnapshot,
    limits: TdsDirectoryLimits,
    lease: WindowLease,
    *,
    supervisor_token: str,
    deadline: float,
) -> TdsAttempt:
    """Take over two exact saved observations under a newer, distinct owner.

    Snapshots must originate from trusted bounded discovery; this function neither
    performs discovery on the supervisor nor synthesizes absent records. Partial
    takeover failure is unresolved and requires a further newer-fence observation.
    """
    if type(observed_parent) is not TdsAttemptSnapshot or type(observed_directory) is not TdsDirectorySnapshot:
        raise ValueError("mssql_native.tds_attempt_recovery_observations_required")
    parent, directory = observed_parent.state, observed_directory.state
    owner = TdsAttemptOwnership(lease.owner, lease.fence, supervisor_token)
    prior_directory_owner = observed_directory.ownership
    if (
        parent.identity != directory.parent
        or parent.schema_version != directory.schema_version
        or directory.limits != limits
        or prior_directory_owner.fence > parent.ownership.fence
        or (prior_directory_owner.fence == parent.ownership.fence and prior_directory_owner != parent.ownership)
        or lease.target_id != parent.identity.target_key
        or owner.fence <= parent.ownership.fence
        or owner.supervisor_id in (parent.ownership.supervisor_id, prior_directory_owner.supervisor_id)
    ):
        raise ValueError("mssql_native.tds_attempt_recovery_binding_mismatch")

    @contextmanager
    def lifecycle_factory() -> Iterator[TdsAttemptWriter]:
        with store_factory() as store:
            yield TdsAttemptJournal(store).take_over(observed_parent, lease, supervisor_token=supervisor_token)

    return _open(
        pool,
        store_factory,
        lifecycle_factory,
        TakeOverDirectory(observed_directory, lease, supervisor_token),
        owner,
        deadline=deadline,
    )


def resume_tds_attempt(
    pool: TdsActorPool,
    store_factory: StoreFactory,
    suspension: TdsAttemptSuspension,
    lease: WindowLease,
    *,
    deadline: float,
) -> TdsAttempt:
    """Consume one local suspension; any opening failure requires a newer fence."""
    if type(suspension) is not TdsAttemptSuspension:
        raise ValueError("mssql_native.tds_attempt_suspension_required")
    claim = suspension.consume()
    observed_parent, observed_directory = claim.observations
    parent, directory = observed_parent.state, observed_directory.state
    owner = parent.ownership
    if (
        parent.identity != directory.parent
        or parent.schema_version != directory.schema_version
        or observed_directory.ownership != owner
        or (owner.owner, owner.fence) != (lease.owner, lease.fence)
        or lease.target_id != parent.identity.target_key
    ):
        raise ValueError("mssql_native.tds_attempt_resume_binding_mismatch")

    @contextmanager
    def lifecycle_factory() -> Iterator[TdsAttemptWriter]:
        with store_factory() as store:
            yield TdsAttemptJournal(store, backend=parent.backend or "mssql_python").resume(claim, lease)

    return _open(
        pool,
        store_factory,
        lifecycle_factory,
        ResumeDirectory(claim, lease),
        owner,
        deadline=deadline,
    )


def _open(
    pool: TdsActorPool,
    store_factory: StoreFactory,
    lifecycle_factory: Callable[[], AbstractContextManager[TdsAttemptWriter]],
    initialization: CreateDirectory | TakeOverDirectory | ResumeDirectory,
    owner: TdsAttemptOwnership,
    *,
    deadline: float,
) -> TdsAttempt:
    gateways: list[_ShutdownCapability] = []

    @contextmanager
    def directory_factory() -> Iterator[TdsDirectoryStore]:
        with store_factory() as store:
            yield TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))

    try:
        lifecycle = pool.open(
            lambda actor_deadline, actor_clock: TdsJournalActor(lifecycle_factory, actor_deadline, actor_clock),
            deadline=deadline,
        )
        gateways.append(lifecycle)
        lifecycle.assert_authority(deadline=deadline)
        parent = lifecycle.snapshot.state
        if isinstance(initialization, CreateDirectory):
            identity, limits = initialization.parent, initialization.limits
            if parent.phase is not TdsAttemptPhase.CREATION_INTENT:
                raise ValueError("mssql_native.tds_attempt_creation_intent_required")
        elif isinstance(initialization, TakeOverDirectory):
            identity, limits = initialization.observed.state.parent, initialization.observed.state.limits
        else:
            _, suspended_directory = initialization.claim.observations
            identity, limits = parent.identity, suspended_directory.state.limits
        if parent.identity != identity or parent.ownership != owner:
            raise ValueError("mssql_native.tds_attempt_parent_binding_mismatch")
        directory = pool.open(
            lambda actor_deadline, actor_clock: TdsDirectoryActor(
                directory_factory, initialization, actor_deadline, actor_clock
            ),
            deadline=deadline,
        )
        gateways.append(directory)
        snapshot = directory.execute(AssertDirectoryAuthority(), deadline=deadline)
        if (
            snapshot.state.parent != identity
            or snapshot.state.limits != limits
            or snapshot.ownership != owner
            or snapshot.state.schema_version != parent.schema_version
        ):
            raise ValueError("mssql_native.tds_attempt_directory_binding_mismatch")
        lifecycle.assert_authority(deadline=deadline)
        return TdsAttempt(
            lifecycle,
            directory,
            _fresh_creation=type(initialization) is CreateDirectory,
            _composition_origin=store_factory,
        )
    except BaseException as error:
        # Failed initialization can retain a live actor even without returning it.
        raise _close_unknown(gateways, error, deadline) from None


@dataclass(frozen=True)
class TdsAttemptObservations:
    """Two acknowledged reads, including absence, without cross-record atomicity.

    Identity and limits bind even an absent pair to the requested attempt. An
    orphan directory is returned for diagnosis rather than hidden by a missing
    parent. Recovery must revalidate both exact snapshots through fenced CAS.
    """

    identity: TdsAttemptIdentity
    limits: TdsDirectoryLimits
    parent: TdsAttemptSnapshot | None
    directory: TdsDirectorySnapshot | None

    def __post_init__(self) -> None:
        if type(self.identity) is not TdsAttemptIdentity or type(self.limits) is not TdsDirectoryLimits:
            raise ValueError("mssql_native.tds_attempt_observations_invalid")
        if self.parent is not None and (
            type(self.parent) is not TdsAttemptSnapshot or self.parent.state.identity != self.identity
        ):
            raise ValueError("mssql_native.tds_attempt_observations_invalid")
        if self.directory is not None and (
            type(self.directory) is not TdsDirectorySnapshot
            or self.directory.state.parent != self.identity
            or self.directory.state.limits != self.limits
        ):
            raise ValueError("mssql_native.tds_attempt_observations_invalid")

        if self.parent is not None and self.directory is not None:
            if self.parent.state.schema_version != self.directory.state.schema_version:
                raise ValueError("mssql_native.tds_attempt_observations_invalid")


def observe_tds_attempt(
    pool: TdsActorPool,
    store_factory: StoreFactory,
    identity: TdsAttemptIdentity,
    limits: TdsDirectoryLimits,
    *,
    deadline: float,
) -> TdsAttemptObservations:
    """Read both records and finish both contexts within one absolute deadline.

    Each actor enters its own store context and is closed before the next actor
    opens, allowing capacity-one discovery. Directory lookup is mandatory even
    after acknowledged parent absence. No partial pair is returned on a failed
    read or teardown; retain the raised TdsAttemptUnknown and run-scoped pool.
    """
    initial_directory(identity, limits)
    gateways: list[_ShutdownCapability] = []

    @contextmanager
    def lifecycle_factory() -> Iterator[TdsAttemptObserver]:
        with store_factory() as store:
            yield TdsAttemptJournal(store)

    @contextmanager
    def directory_factory() -> Iterator[TdsDirectoryStore]:
        with store_factory() as store:
            yield TdsCoordinatorDirectoryJournal(store, parent_observer=TdsAttemptJournal(store))

    try:
        lifecycle = pool.open(
            lambda actor_deadline, actor_clock: TdsAttemptObserverActor(
                lifecycle_factory, identity, actor_deadline, actor_clock
            ),
            deadline=deadline,
        )
        gateways.append(lifecycle)
        parent = lifecycle.observation.snapshot
        lifecycle.close(deadline=deadline)
        directory = pool.open(
            lambda actor_deadline, actor_clock: TdsDirectoryActor(
                directory_factory, ReadDirectory(identity, limits), actor_deadline, actor_clock
            ),
            deadline=deadline,
        )
        gateways.append(directory)
        observed_directory = directory.observation.snapshot
        directory.close(deadline=deadline)
        pool.assert_deadline(deadline=deadline)
        return TdsAttemptObservations(identity, limits, parent, observed_directory)
    except BaseException as error:
        raise _close_unknown(gateways, error, deadline) from None


def _close_unknown(gateways: list[_ShutdownCapability], error: BaseException, deadline: float) -> TdsAttemptUnknown:
    """Retain partial initialization handles and bound every cleanup attempt."""
    if isinstance(error, TdsJournalActorUnknown) and error.gateway is not None and error.gateway not in gateways:
        gateways.append(error.gateway)
    unknown = TdsAttemptUnknown(tuple(gateways))
    try:
        unknown.close(deadline=deadline)
    except BaseException:
        pass
    return unknown
