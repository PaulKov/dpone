"""Observation projection and evidence settlement for observe-departure custody."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, overload
from uuid import UUID

from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_departure_custody import DepartureHelperCustody
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    SqlClientObserverAdmission,
    TdsChildExit,
    TdsCoordinatorStartup,
    TdsProcessIdentity,
    WindowOutcomeUnknown,
)
from dpone.contracts.mssql_sqlclient_observe_departure import validate_observe_departure_binding
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.services.mssql_tds_observe_settlement import ObserveSettlement

if TYPE_CHECKING:
    from collections.abc import Callable

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ObserveDepartureSnapshot:
    """Immutable point-in-time view used by cleanup and compatibility readers."""

    owner: ObserveSettlement
    helper: DepartureHelperCustody
    pool: TdsActorPool
    helper_id: UUID
    plan: SqlClientObserveDeparturePlan | None
    request: SqlClientObserveDepartureRequest | None
    result: SqlClientObserveDepartureResult | None
    receipts: Mapping[Kind, SqlClientDepartureEvidenceReceipt]
    expected: Mapping[Kind, SqlClientDepartureEvidenceReceipt]
    child: SqlClientDepartureProcess | None
    process: TdsProcessIdentity | None
    startup: TdsCoordinatorStartup | None
    raw_result: bytes | None
    local_exit: TdsChildExit | None
    containment_deadline: float | None
    helper_evidence: SqlClientDepartureEvidenceGateway | None

    @classmethod
    def capture(cls, owner: ObserveSettlement, helper: DepartureHelperCustody) -> ObserveDepartureSnapshot:
        """Detach collection projections while retaining exact capability identities."""
        records = owner.helper_records
        return cls(
            owner=owner,
            helper=helper,
            pool=owner.origin.pool,
            helper_id=owner.helper_id,
            plan=owner.plan,
            request=owner.request,
            result=owner.result,
            receipts=MappingProxyType({kind: value[2] for kind, value in records.items() if value[4] is not None}),
            expected=MappingProxyType({kind: value[1] for kind, value in records.items()}),
            child=helper.child,
            process=helper.process,
            startup=helper.startup,
            raw_result=helper.raw_result,
            local_exit=helper.local_exit,
            containment_deadline=helper.containment_deadline,
            helper_evidence=helper.helper_evidence,
        )


class ObserveSnapshotProvider(Protocol):
    def snapshot(self) -> ObserveDepartureSnapshot: ...


class ObserveSnapshotField(Generic[T]):
    """Read-only compatibility descriptor backed by a fresh projection."""

    def __init__(self, getter: Callable[[ObserveDepartureSnapshot], T]) -> None:
        self._getter = getter

    @overload
    def __get__(self, instance: None, owner: type[Any]) -> ObserveSnapshotField[T]: ...

    @overload
    def __get__(self, instance: ObserveSnapshotProvider, owner: type[Any]) -> T: ...

    def __get__(self, instance: ObserveSnapshotProvider | None, owner: type[Any]) -> ObserveSnapshotField[T] | T:
        if instance is None:
            return self
        return self._getter(instance.snapshot())

    def __set__(self, instance: ObserveSnapshotProvider, value: T) -> None:
        raise AttributeError("observe-departure projections are read-only")


def observe_field(getter: Callable[[ObserveDepartureSnapshot], T]) -> ObserveSnapshotField[T]:
    return ObserveSnapshotField(getter)


class ObserveDepartureSettlement:
    """Validate and persist evidence without owning helper cleanup orchestration."""

    def __init__(self, observer_admission: SqlClientObserverAdmission) -> None:
        self._observer_admission = observer_admission

    def validate(self, snapshot: ObserveDepartureSnapshot) -> None:
        if snapshot.request is None or snapshot.startup is None or snapshot.plan is None:
            raise WindowOutcomeUnknown("mssql_native.sqlclient_observe_departure_unknown")
        validate_observe_departure_binding(
            snapshot.request,
            startup=snapshot.startup,
            admission_sha256=snapshot.plan.admission_sha256,
            startup_deadline=snapshot.plan.startup_deadline,
            operation_deadline=snapshot.plan.operation_deadline,
            max_address_space_bytes=snapshot.plan.max_address_space_bytes,
            observer_admission=self._observer_admission,
        )

    def persist(
        self,
        snapshot: ObserveDepartureSnapshot,
        kind: Kind,
        payload: bytes,
        guard: Callable[[float], None],
    ) -> SqlClientDepartureEvidenceReceipt:
        plan = snapshot.plan
        assert plan is not None
        guard(plan.operation_deadline)
        record = SqlClientDepartureEvidenceRecord(
            snapshot.helper_id,
            attempt_identity_digest(plan.attempt),
            kind,
            payload,
            observe_plan=plan if kind is Kind.LAUNCH_INTENT else None,
            observe_request=None if kind is Kind.LAUNCH_INTENT else snapshot.request,
            observer_admission=self._observer_admission,
        )
        receipt = snapshot.owner.write_helper_record(record, deadline=plan.operation_deadline)
        guard(plan.operation_deadline)
        return receipt
