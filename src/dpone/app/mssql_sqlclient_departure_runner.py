# Parent-neutral one-shot ordering for a six-ACK departure transcript.

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from types import FunctionType
from typing import ParamSpec, Protocol, TypeVar
from uuid import UUID

from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    TdsAttemptIdentity,
    TdsChildExit,
    TdsCoordinatorStartup,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_validation import _hash, deadline_nanoseconds

_P = ParamSpec("_P")
_T = TypeVar("_T")


class DeparturePlan(Protocol):
    @property
    def helper_id(self) -> UUID: ...
    @property
    def attempt(self) -> TdsAttemptIdentity: ...
    @property
    def implementation_sha256(self) -> str: ...
    @property
    def package_root(self) -> str: ...
    @property
    def admission_sha256(self) -> str: ...
    @property
    def startup_deadline(self) -> float: ...
    @property
    def operation_deadline(self) -> float: ...


class DepartureOwnerAuthority(Protocol):
    def guard(self, deadline: float) -> None: ...
    def bind_request(self, value: object) -> None: ...
    def bind_result(self, value: object) -> None: ...
    def validate_request_owner(self) -> None: ...
    def validate_final_owner(self) -> None: ...


class DepartureEvidenceSession(Protocol):
    def persist(self, kind: Kind, payload: bytes) -> SqlClientDepartureEvidenceReceipt: ...
    def receipts(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]: ...
    def expected(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]: ...
    def close(self, deadline: float) -> None: ...
    def assert_closed(self, deadline: float) -> None: ...


class DepartureChildSession(Protocol):
    def startup(self, deadline: float) -> TdsCoordinatorStartup: ...
    def deliver(self, request: object, deadline: float) -> None: ...
    def receive(self, deadline: float) -> bytes: ...
    def settle(self, deadline: float) -> TdsChildExit: ...
    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class DepartureRecordContext:
    facts: "DepartureRunFacts"
    startup: TdsCoordinatorStartup | None
    request: object | None
    result: object | None
    local_exit: TdsChildExit | None
    records: tuple[SqlClientDepartureEvidenceRecord, ...]


MakeRequest = Callable[[object, TdsCoordinatorStartup], object]
ValidateRequest = Callable[[object, object], None]
DecodeResult = Callable[[object, bytes, object], object]
MakeRecord = Callable[[object, Kind, DepartureRecordContext], SqlClientDepartureEvidenceRecord]


@dataclass(frozen=True, slots=True)
class DepartureOperationStrategy:
    context: object
    make_request_fn: MakeRequest
    validate_request_fn: ValidateRequest
    decode_fn: DecodeResult
    record_fn: MakeRecord

    def __post_init__(self) -> None:
        params = getattr(type(self.context), "__dataclass_params__", None)
        if not is_dataclass(self.context) or params is None or not params.frozen:
            raise ValueError("mssql_native.sqlclient_departure_strategy_invalid")
        try:
            _immutable_snapshot(self.context)
        except (TypeError, ValueError, AttributeError):
            raise ValueError("mssql_native.sqlclient_departure_strategy_invalid") from None
        for operation in _strategy_operations(self):
            if (
                type(operation) is not FunctionType
                or operation.__closure__ is not None
                or "<locals>" in operation.__qualname__
                or not operation.__module__
            ):
                raise ValueError("mssql_native.sqlclient_departure_strategy_invalid")

    def make_request(self, startup: TdsCoordinatorStartup) -> object:
        return self.make_request_fn(self.context, startup)

    def validate_request(self, request: object) -> None:
        self.validate_request_fn(self.context, request)

    def decode(self, raw: bytes, request: object) -> object:
        return self.decode_fn(self.context, raw, request)

    def record(self, kind: Kind, context: DepartureRecordContext) -> SqlClientDepartureEvidenceRecord:
        return self.record_fn(self.context, kind, context)


@dataclass(frozen=True, slots=True)
class DepartureRunFacts:
    helper_id: UUID
    plan: DeparturePlan
    retain_evidence_on_success: bool
    operation: DepartureOperationStrategy
    _plan_snapshot: object = field(init=False, repr=False, compare=False)
    _context_snapshot: object = field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        try:
            if type(self.helper_id) is not UUID or self.helper_id != self.plan.helper_id:
                raise ValueError
            if type(self.retain_evidence_on_success) is not bool or not isinstance(
                self.plan.attempt, TdsAttemptIdentity
            ):
                raise ValueError
            _hash(self.plan.implementation_sha256)
            _hash(self.plan.admission_sha256)
            attempt_identity_digest(self.plan.attempt)
            deadline_nanoseconds(self.plan.startup_deadline)
            deadline_nanoseconds(self.plan.operation_deadline)
            if (
                self.plan.startup_deadline > self.plan.operation_deadline
                or not Path(self.plan.package_root).is_absolute()
            ):
                raise ValueError
            operation = self.operation
            if type(operation) is not DepartureOperationStrategy:
                raise ValueError
            operation.__post_init__()
            object.__setattr__(self, "_plan_snapshot", _immutable_snapshot(self.plan))
            object.__setattr__(self, "_context_snapshot", _immutable_snapshot(operation.context))
        except (ValueError, TypeError, AttributeError, OverflowError):
            raise ValueError("mssql_native.sqlclient_departure_strategy_invalid") from None


@dataclass(frozen=True, slots=True)
class DepartureRunCompletion:
    plan: DeparturePlan
    request: object
    result: object
    local_exit: TdsChildExit
    receipts: tuple[SqlClientDepartureEvidenceReceipt, ...]


class DepartureEvidenceFactory(Protocol):
    def __call__(self, facts: DepartureRunFacts, deadline: float) -> DepartureEvidenceSession: ...


class DepartureLaunchFactory(Protocol):
    def __call__(self, facts: DepartureRunFacts) -> DepartureChildSession: ...


class DepartureRunner:
    def __init__(
        self,
        facts: DepartureRunFacts,
        owner: DepartureOwnerAuthority,
        evidence_factory: DepartureEvidenceFactory,
        launch_factory: DepartureLaunchFactory,
    ) -> None:
        if type(facts.operation) is not DepartureOperationStrategy:
            raise ValueError("mssql_native.sqlclient_departure_strategy_invalid")
        self._facts, self._owner = facts, owner
        self._evidence_factory, self._launch_factory = evidence_factory, launch_factory
        self._strategy = facts.operation
        self._identities = (facts.plan, self._strategy.context, *_strategy_operations(self._strategy))
        self._snapshots = (facts._plan_snapshot, facts._context_snapshot)
        self._consumed = False
        self._assert_strategy()

    def _assert_strategy(self) -> None:
        if (
            type(self._facts.operation) is not DepartureOperationStrategy
            or self._facts.operation is not self._strategy
            or not all(
                current is admitted
                for current, admitted in zip(
                    (self._facts.plan, self._strategy.context, *_strategy_operations(self._strategy)),
                    self._identities,
                    strict=True,
                )
            )
            or (_immutable_snapshot(self._facts.plan), _immutable_snapshot(self._strategy.context)) != self._snapshots
        ):
            raise ValueError("mssql_native.sqlclient_departure_strategy_invalid")
        self._strategy.__post_init__()

    def _call(self, callback: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
        self._assert_strategy()
        try:
            return callback(*args, **kwargs)
        finally:
            self._assert_strategy()

    def run(self) -> DepartureRunCompletion:
        self._assert_strategy()
        if self._consumed:
            raise ValueError("mssql_native.sqlclient_departure_one_shot")
        self._consumed = True
        return self._execute()

    def _emit(
        self, evidence: DepartureEvidenceSession, kind: Kind, context: DepartureRecordContext
    ) -> SqlClientDepartureEvidenceRecord:
        record = self._call(self._strategy.record, kind, context)
        if type(record) is not SqlClientDepartureEvidenceRecord:
            raise ValueError("mssql_native.sqlclient_departure_record_invalid")
        record.__post_init__()
        expected_order = tuple(Kind)[: len(context.records) + 1]
        if (
            record.kind is not kind
            or (record.helper_id, record.attempt_sha256)
            != (self._facts.helper_id, attempt_identity_digest(self._facts.plan.attempt))
            or tuple(item.kind for item in (*context.records, record)) != expected_order
        ):
            raise ValueError("mssql_native.sqlclient_departure_record_invalid")
        receipt = self._call(evidence.persist, kind, record.payload)
        if receipt != record.receipt:
            raise ValueError("mssql_native.sqlclient_departure_record_invalid")
        return record

    def _execute(self) -> DepartureRunCompletion:
        f, owner, plan = self._facts, self._owner, self._facts.plan
        deadline, records = plan.operation_deadline, []
        self._call(owner.guard, deadline)
        evidence = self._call(self._evidence_factory, f, deadline)
        context = DepartureRecordContext(f, None, None, None, None, ())
        records.append(self._emit(evidence, Kind.LAUNCH_INTENT, context))
        self._call(owner.guard, deadline)
        child = self._call(self._launch_factory, f)
        self._call(owner.guard, plan.startup_deadline)
        startup = self._call(child.startup, plan.startup_deadline)
        if type(startup) is not TdsCoordinatorStartup:
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        startup.__post_init__()
        startup.process.__post_init__()
        if (startup.implementation_sha256, startup.package_root) != (
            plan.implementation_sha256,
            plan.package_root,
        ):
            raise ValueError("mssql_native.sqlclient_departure_startup_binding")
        self._call(owner.guard, plan.startup_deadline)
        observe = f.retain_evidence_on_success
        if observe:
            request = self._call(self._strategy.make_request, startup)
            self._call(owner.bind_request, request)
            self._call(owner.validate_request_owner)
            self._call(self._strategy.validate_request, request)
        context = DepartureRecordContext(f, startup, request if observe else None, None, None, tuple(records))
        records.append(self._emit(evidence, Kind.REGISTRATION, context))
        if not observe:
            request = self._call(self._strategy.make_request, startup)
            self._call(self._strategy.validate_request, request)
            self._call(owner.bind_request, request)
        context = DepartureRecordContext(f, startup, request, None, None, tuple(records))
        records.append(self._emit(evidence, Kind.CREDENTIAL_INTENT, context))
        self._call(child.deliver, request, deadline)
        self._call(owner.guard, deadline)
        raw = self._call(child.receive, deadline)
        if type(raw) is not bytes:
            raise ValueError("mssql_native.sqlclient_departure_result_transport_binding")
        result = self._call(self._strategy.decode, raw, request)
        self._call(owner.bind_result, result)
        context = DepartureRecordContext(f, startup, request, result, None, tuple(records))
        records.append(self._emit(evidence, Kind.RESULT, context))
        self._call(owner.guard, deadline)
        local_exit = self._call(child.settle, deadline)
        if type(local_exit) is not TdsChildExit:
            raise ValueError("mssql_native.sqlclient_departure_nonzero_exit")
        startup.__post_init__()
        startup.process.__post_init__()
        local_exit.__post_init__()
        local_exit.identity.__post_init__()
        if local_exit.exit_code != 0 or local_exit.reaped is not True or local_exit.identity != startup.process:
            raise ValueError("mssql_native.sqlclient_departure_nonzero_exit")
        self._call(owner.guard, deadline)
        self._call(child.close)
        self._call(owner.guard, deadline)
        context = DepartureRecordContext(f, startup, request, result, local_exit, tuple(records))
        records.append(self._emit(evidence, Kind.LOCAL_EXIT, context))
        self._call(owner.validate_final_owner)
        context = DepartureRecordContext(f, startup, request, result, local_exit, tuple(records))
        records.append(self._emit(evidence, Kind.EXCLUSION, context))
        if len(records) != 6 or tuple(item.kind for item in records) != tuple(Kind):
            raise ValueError("mssql_native.sqlclient_departure_record_invalid")
        receipts, expected = self._call(evidence.receipts), self._call(evidence.expected)
        if any(receipts[item.kind] != item.receipt or expected[item.kind] != item.receipt for item in records):
            raise ValueError("mssql_native.sqlclient_departure_chain_changed")
        acknowledged = tuple(receipts[item.kind] for item in records)
        self._call(owner.guard, deadline)
        if not f.retain_evidence_on_success:
            self._call(evidence.close, deadline)
            self._call(owner.guard, deadline)
            self._call(evidence.assert_closed, deadline)
            self._call(owner.guard, deadline)
        return DepartureRunCompletion(plan, request, result, local_exit, acknowledged)


def _strategy_operations(strategy: DepartureOperationStrategy) -> tuple[object, ...]:
    return strategy.make_request_fn, strategy.validate_request_fn, strategy.decode_fn, strategy.record_fn


def _immutable_snapshot(value: object) -> object:
    """Snapshot exact frozen values without invoking repr, pickle, or user codecs."""
    kind = type(value)
    nominal = kind.__module__, kind.__qualname__
    if value is None or kind in (bool, int, str):
        return nominal, value
    if kind is float and isinstance(value, float):
        return nominal, value.hex()
    if kind is bytes and isinstance(value, bytes):
        return nominal, value.hex()
    if kind is UUID and isinstance(value, UUID):
        return nominal, value.hex
    if kind is datetime and isinstance(value, datetime):
        return nominal, value.isoformat(timespec="microseconds"), value.fold
    if isinstance(value, Enum):
        return nominal, value.name
    if kind is tuple and isinstance(value, tuple):
        return nominal, tuple(_immutable_snapshot(item) for item in value)
    params = getattr(kind, "__dataclass_params__", None)
    if not is_dataclass(value) or params is None or not params.frozen:
        raise ValueError("mssql_native.sqlclient_departure_strategy_invalid")
    return nominal, tuple((field.name, _immutable_snapshot(getattr(value, field.name))) for field in fields(value))
