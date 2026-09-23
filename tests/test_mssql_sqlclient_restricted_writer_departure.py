"""P9b strategy integration through the real six-record DepartureRunner."""

import pytest

from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureOperationStrategy,
    DepartureRunFacts,
    DepartureRunner,
)
from dpone.app.mssql_sqlclient_restricted_writer_departure import (
    _Child,
    _close_resources,
    _decode_result,
    _Evidence,
    _make_request,
    _record,
    _StrategyContext,
    _validate_request,
)
from dpone.contracts import mssql_sqlclient_restricted_writer_settlement_codec as codec
from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.contracts.mssql_tds_worker import TdsChildExit
from dpone.contracts.strict_json import strict_json_object
from tests.test_mssql_sqlclient_restricted_writer_settlement_codec import settlement_fixture


def test_child_delivers_credentials_exactly_once():
    _, request, _ = settlement_fixture()
    writes = []
    supplies = []

    class Child:
        def send_request(self, payload, *, deadline):
            writes.append((payload, deadline))

    def material():
        supplies.append(True)
        return TdsConnectionMaterial(
            "localhost",
            1433,
            request.plan.grant_evidence.authority.database.name,
            request.plan.management_admission.login.name,
            "secret",
        )

    _Child(Child(), material).deliver(request, 2.0)
    assert len(writes) == 1 and supplies == [True]
    assert codec.decode_credentials(writes[0][0]).request == request


def test_departure_cleanup_never_truth_coerces_first_failure_and_replay_is_noop():
    effects = []

    class BoolBomb(BaseException):
        def __bool__(self):
            raise AssertionError("cleanup failure truth coercion")

    first = BoolBomb("first")

    class Child:
        def close(self):
            effects.append("child")
            raise first

    class Gateway:
        def close(self, *, deadline):
            effects.append(("evidence", deadline))
            raise RuntimeError("second")

    resources = {
        "child": _Child(Child(), lambda: None),
        "evidence": _Evidence(Gateway(), object(), 1.0),
    }

    with pytest.raises(BoolBomb) as caught:
        _close_resources(resources, 3.0)
    assert caught.value is first
    assert effects == ["child", ("evidence", 3.0)]

    _close_resources(resources, 3.0)
    assert effects == ["child", ("evidence", 3.0)]


def test_retained_request_emits_only_fields_admitted_for_each_six_ack_kind():
    plan, expected_request, expected_result = settlement_fixture()
    raw_result = codec.encode_result(expected_result, expected_request)

    class Owner:
        request = result = None

        def guard(self, deadline):
            assert deadline in (plan.startup_deadline, plan.operation_deadline)

        def bind_request(self, value):
            assert self.request is None
            self.request = value

        def bind_result(self, value):
            assert self.result is None
            self.result = value

        def validate_request_owner(self):
            assert self.request is not None and self.request.plan is plan

        def validate_final_owner(self):
            codec.validate_result(self.result, self.request)

    class Evidence:
        def __init__(self):
            self.values = {}
            self.payloads = {}

        def persist(self, kind, payload):
            record = SqlClientDepartureEvidenceRecord(
                plan.helper_id,
                attempt_identity_digest(plan.attempt),
                kind,
                payload,
                restricted_writer_context=codec.RestrictedWriterDepartureEvidenceContext(plan),
            )
            self.values[kind] = record.receipt
            self.payloads[kind] = strict_json_object(payload)
            return record.receipt

        def receipts(self):
            return self.values

        def expected(self):
            return self.values

        def close(self, deadline):
            raise AssertionError("retained evidence must remain open on success")

        def assert_closed(self, deadline):
            raise AssertionError("retained evidence must remain open on success")

    class Child:
        def startup(self, deadline):
            return expected_request.startup

        def deliver(self, request, deadline):
            assert request.plan is plan

        def receive(self, deadline):
            return raw_result

        def settle(self, deadline):
            return TdsChildExit(expected_request.startup.process, 0, True)

        def close(self):
            return None

    strategy = DepartureOperationStrategy(
        _StrategyContext(plan),
        _make_request,
        _validate_request,
        _decode_result,
        _record,
    )
    facts = DepartureRunFacts(plan.helper_id, plan, True, strategy)
    evidence = Evidence()

    completion = DepartureRunner(facts, Owner(), lambda facts, deadline: evidence, lambda facts: Child()).run()

    assert tuple(receipt.kind for receipt in completion.receipts) == tuple(Kind)
    for index, kind in enumerate(Kind):
        keys = evidence.payloads[kind]
        assert ("startup" in keys) is (index >= 1)
        assert ("request_sha256" in keys) is (index >= 2)
        assert ("result_sha256" in keys) is (index >= 3)
        assert ("local_exit" in keys) is (index >= 4)
