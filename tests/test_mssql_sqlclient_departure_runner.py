"""Focused contract tests for the parent-neutral one-shot departure seam."""

from collections.abc import Iterator, Mapping
from dataclasses import FrozenInstanceError, dataclass, replace
from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.app.mssql_sqlclient_departure_lifecycle import legacy_strategy
from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureOperationStrategy,
    DepartureRecordContext,
    DepartureRunFacts,
    DepartureRunner,
)
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from dpone.contracts import mssql_sqlclient_departure_chain as chain
from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_ipc import (
    SqlClientDeparturePlan,
    SqlClientDepartureRequest,
    SqlClientDepartureResult,
)
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import (
    decode_departure_result,
    encode_departure_result,
    make_departure_result,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity, TdsChildExit
from tests.test_mssql_sqlclient_create_departure_codec import sample
from tests.test_mssql_sqlclient_create_departure_composition import harness as harness
from tests.test_mssql_sqlclient_departure_ipc import request

_EVENTS: list[str] = []
_PRODUCED: list[SqlClientDepartureEvidenceRecord] = []


class _ReverseMapping(Mapping):
    def __init__(self, values):
        self._values = values

    def __getitem__(self, key):
        return self._values[key]

    def __iter__(self) -> Iterator:
        return iter(tuple(reversed(self._values)))

    def __len__(self) -> int:
        return len(self._values)


@dataclass(frozen=True, slots=True)
class _DistinctPlan:
    base: SqlClientDeparturePlan

    @property
    def helper_id(self) -> UUID:
        return self.base.helper_id

    @property
    def attempt(self) -> TdsAttemptIdentity:
        return self.base.attempt

    @property
    def implementation_sha256(self) -> str:
        return self.base.implementation_sha256

    @property
    def package_root(self) -> str:
        return self.base.package_root

    @property
    def admission_sha256(self) -> str:
        return self.base.admission_sha256

    @property
    def startup_deadline(self) -> float:
        return self.base.startup_deadline

    @property
    def operation_deadline(self) -> float:
        return self.base.operation_deadline


@dataclass(frozen=True, slots=True)
class _DistinctRequest:
    startup: TdsCoordinatorStartup
    base: SqlClientDepartureRequest


@dataclass(frozen=True, slots=True)
class _DistinctResult:
    marker: str
    base: SqlClientDepartureResult


@dataclass(frozen=True, slots=True)
class _DistinctOperation:
    plan: SqlClientDeparturePlan
    records: tuple[str, ...] = ("admitted",)


def _distinct_request(context: object, startup: TdsCoordinatorStartup) -> object:
    assert type(context) is _DistinctOperation
    _EVENTS.append("make")
    return _DistinctRequest(startup, SqlClientDepartureRequest(plan=context.plan, startup=startup))


def _distinct_validate(context: object, value: object) -> None:
    assert type(context) is _DistinctOperation and type(value) is _DistinctRequest
    assert value.base.plan == context.plan and value.base.startup == value.startup
    _EVENTS.append("validate")


def _distinct_decode(context: object, raw: bytes, value: object) -> object:
    assert type(context) is _DistinctOperation and type(value) is _DistinctRequest
    _EVENTS.append("decode")
    return _DistinctResult("distinct", decode_departure_result(raw, request=value.base))


def _distinct_record(context: object, kind: Kind, value: DepartureRecordContext) -> SqlClientDepartureEvidenceRecord:
    assert type(context) is _DistinctOperation
    assert type(value.facts.plan) is _DistinctPlan and value.facts.plan.base == context.plan
    _EVENTS.append(f"record:{kind.value}")
    subject = value.facts.helper_id, attempt_identity_digest(context.plan.attempt)
    request = value.request
    result = value.result
    if kind is Kind.LAUNCH_INTENT:
        payload = chain.departure_launch_payload(context.plan)
    elif kind is Kind.REGISTRATION:
        assert value.startup is not None
        payload = chain.departure_registration_payload(
            context.plan, value.startup, value.records[0].receipt.payload_sha256
        )
    elif kind is Kind.CREDENTIAL_INTENT:
        if (
            type(request) is not _DistinctRequest
            or request.startup != value.startup
            or request.base.plan != context.plan
        ):
            raise ValueError("distinct_request_binding")
        payload = chain.departure_credential_payload(request.base, value.records[1].receipt.payload_sha256)
    elif kind is Kind.RESULT:
        if type(request) is not _DistinctRequest or request.base.plan != context.plan:
            raise ValueError("distinct_request_binding")
        assert type(result) is _DistinctResult
        payload = chain.departure_result_payload(request.base, result.base, value.records[2].receipt.payload_sha256)
    elif kind is Kind.LOCAL_EXIT:
        assert type(request) is _DistinctRequest and value.local_exit is not None
        payload = chain.departure_local_exit_payload(
            request.base,
            value.local_exit,
            value.records[1].receipt.payload_sha256,
            value.records[3].receipt.payload_sha256,
        )
    else:
        assert type(request) is _DistinctRequest and type(result) is _DistinctResult
        assert value.startup is not None and value.local_exit is not None
        record = chain.reconstruct_departure_chain(
            context.plan, request.base, value.startup, result.base, value.local_exit
        )[5]
        _PRODUCED.append(record)
        return record
    record = (
        SqlClientDepartureEvidenceRecord(*subject, kind, payload, result_context=request.base)
        if kind is Kind.RESULT and type(request) is _DistinctRequest
        else SqlClientDepartureEvidenceRecord(*subject, kind, payload)
    )
    _PRODUCED.append(record)
    return record


def _strategy(plan: SqlClientDeparturePlan):
    return DepartureOperationStrategy(
        _DistinctOperation(plan),
        _distinct_request,
        _distinct_validate,
        _distinct_decode,
        _distinct_record,
    )


def _legacy_values():
    legacy = request()
    exited = TdsChildExit(legacy.startup.process, 0, True)
    result = make_departure_result(legacy, sample())
    return legacy, exited, result, encode_departure_result(result, request=legacy)


def test_runner_preserves_six_ack_order_before_dependent_effects(harness, monkeypatch):
    acknowledged = []
    original = SqlClientDepartureEvidenceActor.write

    def write(self, record, *, deadline):
        receipt = original(self, record, deadline=deadline)
        acknowledged.append(record.kind)
        return receipt

    monkeypatch.setattr(SqlClientDepartureEvidenceActor, "write", write)
    harness.hook = lambda label: (
        (
            acknowledged == [Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT]
            if label == "observer"
            else True
        )
        or pytest.fail("credentials dispatched before three ACKs")
    )

    outcome = harness.run()

    assert acknowledged == list(Kind)
    assert [receipt.kind for receipt in outcome.helper_outcome.receipts] == list(Kind)


def test_runner_callback_reentry_sticks_before_credential_dispatch(harness):
    def hook(label):
        if label == "observer":
            try:
                _ = harness.attempt.directory
            except BaseException:
                pass

    harness.hook = hook
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()

    assert caught.value.retained.faulted is True
    assert "helper.credentials" not in harness.events
    assert list(caught.value.retained.receipts) == [Kind.LAUNCH_INTENT, Kind.REGISTRATION, Kind.CREDENTIAL_INTENT]


def test_runner_consumes_binding_before_first_callback():
    plan = request().plan
    effects = []

    class Owner:
        def guard(self, deadline):
            effects.append(("guard", deadline))

    def evidence_factory(facts, deadline):
        effects.append(("open", deadline))
        raise RuntimeError("synthetic stop")

    facts = DepartureRunFacts(plan.helper_id, plan, False, legacy_strategy(plan, None, False))
    runner = DepartureRunner(facts, Owner(), evidence_factory, lambda facts: pytest.fail("unexpected launch"))
    with pytest.raises(RuntimeError, match="synthetic stop"):
        runner.run()
    with pytest.raises(ValueError, match="one_shot"):
        runner.run()

    assert effects == [("guard", plan.operation_deadline), ("open", plan.operation_deadline)]
    with pytest.raises(FrozenInstanceError):
        facts.helper_id = UUID(int=1)


def test_distinct_operation_strategy_flows_without_observe_types():
    legacy, exited, result, raw_result = _legacy_values()
    _EVENTS.clear()
    _PRODUCED.clear()

    class Owner:
        def guard(self, deadline):
            _EVENTS.append("guard")

        def bind_request(self, value):
            _EVENTS.append("bind-request")
            assert type(value) is _DistinctRequest

        def bind_result(self, value):
            _EVENTS.append("bind-result")
            assert type(value) is _DistinctResult

        def validate_request_owner(self):
            pytest.fail("CREATE flow cannot use OBSERVE validation")

        def validate_final_owner(self):
            _EVENTS.append("validate-final")

    class Evidence:
        def __init__(self):
            self.values = {}
            self.acknowledged = ()

        def persist(self, kind, payload):
            _EVENTS.append(kind.value)
            record = _PRODUCED[-1]
            assert payload == record.payload
            self.values[kind] = record.receipt
            return record.receipt

        def receipts(self):
            return _ReverseMapping(self.values)

        def expected(self):
            return self.values

        def close(self, deadline):
            _EVENTS.append("evidence-close")
            self.acknowledged = tuple(self.values[kind] for kind in Kind)
            self.values.clear()

        def assert_closed(self, deadline):
            _EVENTS.append("evidence-closed")

    class Child:
        def startup(self, deadline):
            return legacy.startup

        def deliver(self, value, deadline):
            _EVENTS.append("deliver")
            assert type(value) is _DistinctRequest

        def receive(self, deadline):
            return raw_result

        def settle(self, deadline):
            return exited

        def close(self):
            _EVENTS.append("child-close")

    evidence = Evidence()
    plan = _DistinctPlan(legacy.plan)
    facts = DepartureRunFacts(plan.helper_id, plan, False, _strategy(legacy.plan))

    completion = DepartureRunner(facts, Owner(), lambda facts, deadline: evidence, lambda facts: Child()).run()

    assert type(completion.plan) is _DistinctPlan
    assert type(completion.request) is _DistinctRequest and type(completion.result) is _DistinctResult
    assert completion.result.base == result
    assert tuple(receipt.kind for receipt in completion.receipts) == tuple(Kind)
    assert all(receipt is actor for receipt, actor in zip(completion.receipts, evidence.acknowledged, strict=True))
    assert _EVENTS.index("registration") < _EVENTS.index("make") < _EVENTS.index("validate")
    assert _EVENTS.index("validate") < _EVENTS.index("bind-request") < _EVENTS.index("credential_intent")
    assert _EVENTS.count("deliver") == _EVENTS.count("decode") == 1
    assert [item for item in _EVENTS if item.startswith("record:")] == [f"record:{kind.value}" for kind in Kind]


def test_strategy_rejects_closure_and_mutable_context_before_effects():
    legacy, _, _, _ = _legacy_values()

    def closure(context, startup):
        return legacy, context, startup

    with pytest.raises(ValueError, match="strategy_invalid"):
        DepartureOperationStrategy(
            _DistinctOperation(legacy.plan), closure, _distinct_validate, _distinct_decode, _distinct_record
        )

    @dataclass(frozen=True)
    class MutableContext:
        values: list[int]

    with pytest.raises(ValueError, match="strategy_invalid"):
        DepartureOperationStrategy(
            MutableContext([]), _distinct_request, _distinct_validate, _distinct_decode, _distinct_record
        )


def test_changed_distinct_record_context_is_rejected_before_persistence():
    legacy, _, _, _ = _legacy_values()
    plan = _DistinctPlan(legacy.plan)
    facts = DepartureRunFacts(plan.helper_id, plan, False, _strategy(legacy.plan))
    r0 = _distinct_record(
        facts.operation.context, Kind.LAUNCH_INTENT, DepartureRecordContext(facts, None, None, None, None, ())
    )
    r1 = _distinct_record(
        facts.operation.context,
        Kind.REGISTRATION,
        DepartureRecordContext(facts, legacy.startup, None, None, None, (r0,)),
    )
    changed = replace(legacy.plan, helper_id=UUID(int=1))
    bad = _DistinctRequest(legacy.startup, SqlClientDepartureRequest(plan=changed, startup=legacy.startup))
    with pytest.raises(ValueError, match="distinct_request_binding"):
        _distinct_record(
            facts.operation.context,
            Kind.CREDENTIAL_INTENT,
            DepartureRecordContext(facts, legacy.startup, bad, None, None, (r0, r1)),
        )


def test_strategy_identity_mutation_is_caught_after_callback_before_evidence():
    legacy, _, _, _ = _legacy_values()
    plan = _DistinctPlan(legacy.plan)
    operation = _strategy(legacy.plan)
    facts = DepartureRunFacts(plan.helper_id, plan, False, operation)
    effects = []

    class Owner:
        def guard(self, deadline):
            effects.append("guard")
            object.__setattr__(operation.context, "records", ())

    def evidence_factory(facts, deadline):
        effects.append("evidence")
        pytest.fail("mutated strategy reached evidence")

    with pytest.raises(ValueError, match="strategy_invalid"):
        DepartureRunner(facts, Owner(), evidence_factory, lambda facts: object()).run()
    assert effects == ["guard"]


def test_plan_value_mutation_is_caught_after_callback_before_evidence():
    legacy, _, _, _ = _legacy_values()
    plan = _DistinctPlan(legacy.plan)
    facts = DepartureRunFacts(plan.helper_id, plan, False, _strategy(legacy.plan))
    effects = []

    class Owner:
        def guard(self, deadline):
            effects.append("guard")
            object.__setattr__(plan.base, "helper_id", UUID(int=1))

    def evidence_factory(facts, deadline):
        effects.append("evidence")
        pytest.fail("mutated plan reached evidence")

    with pytest.raises(ValueError, match="strategy_invalid"):
        DepartureRunner(facts, Owner(), evidence_factory, lambda facts: object()).run()
    assert effects == ["guard"]


@pytest.mark.parametrize("mutation", ["unreaped", "identity", "malformed"])
def test_local_exit_is_revalidated_and_bound_before_child_close(harness, monkeypatch, mutation):
    from dpone.app.mssql_sqlclient_departure_supervisor import _LegacyChild

    runner_closes = []
    monkeypatch.setattr(_LegacyChild, "close", lambda self: runner_closes.append(True))
    identity = harness.helper_startup.process
    if mutation == "unreaped":
        local_exit = TdsChildExit(identity, 0, False)
    elif mutation == "identity":
        local_exit = TdsChildExit(replace(identity, pid=identity.pid + 1), 0, True)
    else:
        local_exit = TdsChildExit(identity, 0, True)
        object.__setattr__(local_exit, "reaped", 1)
    harness.helper.wait = lambda **kwargs: local_exit

    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run()

    assert runner_closes == []
    assert list(caught.value.retained.receipts) == [
        Kind.LAUNCH_INTENT,
        Kind.REGISTRATION,
        Kind.CREDENTIAL_INTENT,
        Kind.RESULT,
    ]
