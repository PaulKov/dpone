"""Raw-cursor contracts, not driver or SQL route qualification."""

from threading import Event

import pytest

from dpone.adapters.dbt_mssql_physical_protocol_transport import StrictPhysicalProtocolTransport
from dpone.contracts.dbt_mssql_physical_protocol import ATTACH_COLUMNS, attach_parameters
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from tests.support.dbt_mssql_physical import plan_set_document
from tests.test_dbt_physical_transport_delivery import packet_payload


class Cursor:
    def __init__(self, row):
        self.description = [(name,) for name in ATTACH_COLUMNS]
        self.rows, self.extra, self.closed, self.cancelled = [row], None, False, False
        self.calls = []
        self.failure = None

    def execute(self, sql, parameters):
        self.calls.append((sql, parameters))
        if self.failure == "execute":
            raise RuntimeError("private driver text")

    def fetchone(self):
        if self.failure == "fetch":
            raise RuntimeError("private driver text")
        return self.rows.pop(0) if self.rows else None

    def nextset(self):
        if self.failure == "nextset":
            raise RuntimeError("private driver text")
        return self.extra

    def close(self):
        self.closed = True
        if self.failure == "close":
            raise RuntimeError("private driver text")

    def cancel(self):
        self.cancelled = True


class Handle:
    def __init__(self, cursor):
        self.raw, self.calls = cursor, 0

    def cursor(self):
        self.calls += 1
        return self.raw


def case(*, clock=lambda: 2, cancellation=None, generation_bytes=65536):
    raw = packet_payload()
    raw["limits"]["max_generation_bytes"] = generation_bytes
    plan = plan_set_document()["models"][0]
    raw["generation_id"] = plan["generation_id"]
    raw["model_database"]["database_name"] = plan["spec"]["relation"]["database"]
    raw["trusted_profile"]["reference"] = plan["spec"]["resource_bounds"]
    executor = {
        "schema": "dpone.native-source-executor-binding.v1",
        "generation_id": raw["generation_id"],
        "invocation_id": raw["executor_invocation_id"],
        "guard_epoch": raw["guard_epoch"],
        "reservation": raw["plan_set"],
        "profile": raw["trusted_profile"]["reference"],
        "command": raw["command_plan"],
    }
    row = (
        1,
        raw["registration_id"],
        raw["registration_sha256"],
        raw["generation_id"],
        raw["executor_invocation_id"],
        raw["plan_set"]["sha256"],
        plan["spec"]["model_unique_id"],
        plan["model_plan_sha256"],
        raw["launch_id"],
        10,
        raw["guard_epoch"],
        2,
        raw["registration_sha256"],
        encode_native_delivery_json(executor).decode(),
        encode_native_delivery_json(plan).decode(),
    )
    request = dict(
        zip(
            attach_parameters(),
            (
                raw["registration_id"],
                raw["generation_id"],
                raw["executor_invocation_id"],
                raw["plan_set"]["locator"],
                raw["plan_set"]["sha256"],
                plan["spec"]["model_unique_id"],
            ),
            strict=True,
        )
    )
    cursor = Cursor(row)
    handle = Handle(cursor)
    transport = StrictPhysicalProtocolTransport(
        delivery=PhysicalTransportDelivery(encode_native_delivery_json(raw)),
        cancellation=cancellation or Event(),
        clock=clock,
    )
    return transport, handle, cursor, request, row


def test_exact_singleton_only_then_terminal_and_no_retry():
    transport, handle, cursor, request, row = case()
    assert transport.execute(handle, "attach", request) == (row,)
    assert handle.calls == 1 and cursor.closed
    assert cursor.calls[0][0].startswith("EXEC [dpone_physical].[physical_attach_session_v1]")
    assert cursor.calls[0][1] == tuple(request.values())
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert handle.calls == 1


@pytest.mark.parametrize("extra", [True, False, 0, 1, "", object()])
def test_any_non_none_nextset_poisons_attempt(extra):
    transport, handle, cursor, request, _ = case()
    cursor.extra = extra
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert cursor.closed
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert handle.calls == 1


@pytest.mark.parametrize("drift", ["empty", "extra", "null", "bool", "description", "leading"])
def test_raw_shape_types_and_limits_fail_closed(drift):
    transport, handle, cursor, request, row = case()
    if drift == "empty":
        cursor.rows = []
    elif drift == "extra":
        cursor.rows *= 2
    elif drift in {"null", "bool"}:
        cursor.rows = [(None if drift == "null" else True, *row[1:])]
    else:
        cursor.description = None if drift == "leading" else [("wrong",)] * 15
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert cursor.closed


@pytest.mark.parametrize(
    "operation", ["bind_transaction", "require_transaction", "observe_catalog", "receipt", "SELECT 1"]
)
def test_unimplemented_or_arbitrary_operation_never_opens_cursor(operation):
    transport, handle, _, request, _ = case()
    with pytest.raises(ValueError):
        transport.execute(handle, operation, request)
    assert handle.calls == 0


@pytest.mark.parametrize("failure", ["execute", "fetch", "nextset", "close"])
def test_driver_failures_close_poison_and_redact(failure):
    transport, handle, cursor, request, _ = case()
    cursor.failure = failure
    with pytest.raises(ValueError) as caught:
        transport.execute(handle, "attach", request)
    assert "private driver text" not in str(caught.value)
    assert cursor.closed
    with pytest.raises(ValueError):
        transport.execute(Handle(Cursor(cursor.rows)), "attach", request)


@pytest.mark.parametrize("expiry_call", range(1, 11))
def test_absolute_deadline_is_checked_through_completion(expiry_call):
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        return 100000000000000000 if calls >= expiry_call else 2

    transport, handle, cursor, request, _ = case(clock=clock)
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    if handle.calls:
        assert cursor.closed


def test_owner_cancellation_before_cursor_and_exact_aggregate_boundary():
    event = Event()
    event.set()
    transport, handle, _, request, _ = case(cancellation=event)
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert handle.calls == 0
    _, _, _, _, row = case()
    size = 8 * len(row) + sum(len(value.encode("utf-16-le")) for value in row if type(value) is str)
    for ceiling in (size - 1, size, size + 1):
        transport, handle, _, request, _ = case(generation_bytes=ceiling)
        if ceiling < size:
            with pytest.raises(ValueError):
                transport.execute(handle, "attach", request)
        else:
            assert transport.execute(handle, "attach", request) == (row,)


@pytest.mark.parametrize(
    "field", ["registration_id", "generation", "expected_invocation", "plan_set_locator", "plan_set_sha256"]
)
def test_substituted_command_scope_never_opens_cursor(field):
    transport, handle, _, request, _ = case()
    request[field] = "substituted"
    with pytest.raises(ValueError):
        transport.execute(handle, "attach", request)
    assert handle.calls == 0
