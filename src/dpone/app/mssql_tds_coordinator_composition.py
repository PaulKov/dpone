"""Compose independent coordinator journal and evidence owners on an injected pool."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from dpone.adapters.mssql_coordinator_worker_capabilities import (
    DescriptorPinnedCreateOnlyEvidenceWriter,
    TdsAttemptJournal,
    TdsCoordinatorActor,
    TdsCoordinatorDirectoryJournal,
    TdsCoordinatorEvidenceActor,
    TdsCoordinatorJournal,
    TdsJournalActorUnknown,
    _ActorCore,
)
from dpone.app.mssql_tds_attempt_composition import StoreFactory
from dpone.contracts.mssql_coordinator_worker_capabilities import TdsCoordinatorIdentity, TdsCoordinatorSnapshot
from dpone.contracts.mssql_tds_api import TdsDirectoryLimits, WindowLease
from dpone.ports.mssql_coordinator_worker_capabilities import (
    AssertCoordinatorAuthority,
    CoordinatorInitialization,
    CreateCoordinator,
    CreateOnlyEvidenceWriterV1,
    ReadCoordinator,
    TakeOverCoordinator,
    TdsCoordinatorEvidenceGateway,
    TdsCoordinatorGateway,
    TdsCoordinatorStore,
)

ActorT = TypeVar("ActorT", bound=_ActorCore[Any, Any])


class TdsActorPoolCapability(Protocol):
    """Run-scoped actor allocation required by coordinator composition."""

    def open(
        self,
        build: Callable[[float, Callable[[], float]], ActorT],
        *,
        deadline: float,
    ) -> ActorT: ...

    def assert_deadline(self, *, deadline: float) -> None: ...


@dataclass(frozen=True)
class TdsCoordinatorObservation:
    """An exact bounded read after successful teardown, including bound absence."""

    identity: TdsCoordinatorIdentity
    limits: TdsDirectoryLimits
    snapshot: TdsCoordinatorSnapshot | None

    def __post_init__(self) -> None:
        if (
            type(self.identity) is not TdsCoordinatorIdentity
            or type(self.limits) is not TdsDirectoryLimits
            or (
                self.snapshot is not None
                and (type(self.snapshot) is not TdsCoordinatorSnapshot or self.snapshot.state.identity != self.identity)
            )
        ):
            raise ValueError("mssql_native.tds_coordinator_observation_invalid")


def create_tds_coordinator(
    pool: TdsActorPoolCapability,
    store_factory: StoreFactory,
    identity: TdsCoordinatorIdentity,
    limits: TdsDirectoryLimits,
    lease: WindowLease,
    *,
    supervisor_token: str,
    deadline: float,
) -> TdsCoordinatorGateway:
    """Create original intent from an existing exact durable reservation.

    No parent or directory is created or changed. The returned gateway has
    acknowledged its current lease, directory binding and saved revision.
    """
    return _open(pool, store_factory, CreateCoordinator(identity, limits, lease, supervisor_token), deadline)


def recover_tds_coordinator(
    pool: TdsActorPoolCapability,
    store_factory: StoreFactory,
    observed: TdsCoordinatorSnapshot,
    limits: TdsDirectoryLimits,
    lease: WindowLease,
    *,
    supervisor_token: str,
    deadline: float,
) -> TdsCoordinatorGateway:
    """Take over exact existing state after the caller recovers directory ownership.

    A stale or absent observation cannot create a replacement. Recovery preserves
    original execution ownership and grants no permission to replay execution.
    """
    return _open(pool, store_factory, TakeOverCoordinator(observed, limits, lease, supervisor_token), deadline)


def observe_tds_coordinator(
    pool: TdsActorPoolCapability,
    store_factory: StoreFactory,
    identity: TdsCoordinatorIdentity,
    limits: TdsDirectoryLimits,
    *,
    deadline: float,
) -> TdsCoordinatorObservation:
    """Read and finish the context within the same absolute deadline.

    No usable observation escapes failed teardown. Retain the pool and an unknown
    exception's gateway for subsequent bounded cleanup; late replies are excluded.
    """
    gateway = _open(pool, store_factory, ReadCoordinator(identity, limits), deadline)
    try:
        observation = TdsCoordinatorObservation(identity, limits, gateway.observation.snapshot)
        gateway.close(deadline=deadline)
        pool.assert_deadline(deadline=deadline)
        return observation
    except BaseException as error:
        _cleanup(gateway, error, deadline)
        raise


def _open(
    pool: TdsActorPoolCapability,
    store_factory: StoreFactory,
    initialization: CoordinatorInitialization,
    deadline: float,
) -> TdsCoordinatorGateway:
    @contextmanager
    def factory() -> Iterator[TdsCoordinatorStore]:
        with store_factory() as store:
            observer = TdsAttemptJournal(store)
            directory = TdsCoordinatorDirectoryJournal(store, parent_observer=observer)
            yield TdsCoordinatorJournal(store, directory)

    gateway: TdsCoordinatorGateway | None = None
    try:
        gateway = pool.open(
            lambda actor_deadline, actor_clock: TdsCoordinatorActor(
                factory, initialization, actor_deadline, actor_clock
            ),
            deadline=deadline,
        )
        if not isinstance(initialization, ReadCoordinator):
            gateway.execute(AssertCoordinatorAuthority(), deadline=deadline)
        return gateway
    except BaseException as error:
        _cleanup(gateway, error, deadline)
        raise


def _cleanup(gateway: TdsCoordinatorGateway | None, error: BaseException, deadline: float) -> None:
    """Preserve the original failure and its capability, including final-clock failure."""
    if isinstance(error, TdsJournalActorUnknown) and error.gateway is None and isinstance(gateway, TdsCoordinatorActor):
        error.gateway = gateway
    target = error.gateway if isinstance(error, TdsJournalActorUnknown) and error.gateway is not None else gateway
    if target is not None:
        try:
            target.close(deadline=deadline)
        except BaseException:
            pass


def open_tds_coordinator_evidence(
    pool: TdsActorPoolCapability, evidence_root: Path, operation_sha256: str, *, deadline: float
) -> TdsCoordinatorEvidenceGateway:
    """Admit one evidence owner before spawn, without creating a private pool.

    A timeout preserves the actual actor reservation until its thread terminates.
    Closing the returned gateway closes only that operation; other run actors
    remain usable. This helper neither creates directories nor validates SQL
    evidence: the trusted supervisor performs full semantic checks before write.
    """
    if not isinstance(evidence_root, Path) or not evidence_root.is_absolute():
        raise ValueError("mssql_native.tds_evidence_root_invalid")

    @contextmanager
    def factory() -> Iterator[CreateOnlyEvidenceWriterV1]:
        yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

    return pool.open(
        lambda actor_deadline, actor_clock: TdsCoordinatorEvidenceActor(
            factory, operation_sha256, actor_deadline, actor_clock
        ),
        deadline=deadline,
    )
