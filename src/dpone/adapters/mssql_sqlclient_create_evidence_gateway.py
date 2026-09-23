"""Bounded adapter gateway for SQLClient CREATE evidence persistence and reads."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any

from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown, _ActorCommand, _ActorCore
from dpone.adapters.mssql_tds_coordinator_evidence_actor import TdsCoordinatorEvidenceReadActor
from dpone.ports.bounded_window import WindowStore

StoreContextFactory = Callable[[], AbstractContextManager[WindowStore]]


@dataclass(frozen=True)
class SqlClientCreateEvidenceGatewayBundle:
    """Closed store capability and its one actor-thread initial operation."""

    store_factory: StoreContextFactory
    initial: Callable[[WindowStore], Any]
    snapshot_type: type[Any]


class SqlClientCreateEvidenceActor(_ActorCore[WindowStore, Any]):
    """Explicit immutable create or read mode, one actor-owned store context."""

    def __init__(
        self,
        bundle: SqlClientCreateEvidenceGatewayBundle,
        locator: Any,
        seal: Any | None,
        lease: Any | None,
        deadline: float,
        clock: Callable[[], float],
    ) -> None:
        if type(bundle) is not SqlClientCreateEvidenceGatewayBundle:
            raise ValueError("mssql_native.sqlclient_state_domain_admission_required")
        self._initial_operation = bundle.initial
        super().__init__(bundle.store_factory, deadline, clock, bundle.snapshot_type)

    def _initial(self, backend: WindowStore) -> Any:
        return self._initial_operation(backend)

    def _dispatch(self, backend: WindowStore, command: _ActorCommand[Any]) -> Any:
        raise TdsJournalActorUnknown(self)


def observe_create_evidence(
    *,
    bundle: SqlClientCreateEvidenceGatewayBundle,
    locator: Any,
    pool: TdsActorPool,
    deadline: float,
    actor_type: type[SqlClientCreateEvidenceActor],
    seal: Any | None = None,
    lease: Any | None = None,
) -> Any:
    """Run one bounded CREATE-evidence actor and acknowledge its teardown."""
    actor = pool.open(lambda end, clock: actor_type(bundle, locator, seal, lease, end, clock), deadline=deadline)
    observation = actor.snapshot
    actor.close(deadline=deadline)
    pool.assert_deadline(deadline=deadline)
    return observation


def read_coordinator_evidence(
    *,
    reader_factory: PinnedEvidenceReadFactory,
    receipts: tuple[Any, ...],
    pool: TdsActorPool,
    deadline: float,
) -> tuple[bytes, ...]:
    """Read the exact sealed coordinator files and acknowledge actor teardown."""
    actor = pool.open(
        lambda end, clock: TdsCoordinatorEvidenceReadActor(reader_factory, receipts, end, clock), deadline=deadline
    )
    payloads = actor.snapshot
    actor.close(deadline=deadline)
    pool.assert_deadline(deadline=deadline)
    return payloads
