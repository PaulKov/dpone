"""One-way bootstrap admission; injected preparation is not live enrollment proof."""

from types import SimpleNamespace

import pytest

from dpone.app.composition_dispatcher_startup import DispatcherStartup
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.composition_execution_budget import ExecutionStopSignal
from tests.test_composition_dispatcher_service_policy import bootstrap_document, decode_policy, digest


def fixture(*, original=True):
    policy = decode_policy()
    body = bootstrap_document()
    raw = canonical_json_bytes(body)
    events = []
    clock = [10.0]
    stop = ExecutionStopSignal(clock=lambda: clock[0])
    held = SimpleNamespace(
        document=raw,
        sha256=digest(raw),
        require_current=lambda: events.append("recheck"),
        close=lambda: events.append("close"),
    )
    state = [held if original else None]

    def read(path, **kwargs):
        events.append("read")
        assert path == policy.bootstrap_file
        assert kwargs == {"dispatcher_gid": policy.dispatcher_gid, "deadline": 130.0}
        return state[0]

    handler = object()

    def prepare(config, deadline):
        events.append("verify")
        assert config.service_policy == policy and deadline == 130.0
        return handler

    startup = DispatcherStartup(policy, stop=stop, open_original=read, prepare=prepare, clock=lambda: clock[0])
    return SimpleNamespace(
        startup=startup, state=state, held=held, events=events, clock=clock, stop=stop, handler=handler
    )


def test_absence_keeps_admission_closed_without_renewing_deadline():
    case = fixture(original=False)
    assert not case.startup.poll()
    with pytest.raises(CompositionAdmissionError):
        _ = case.startup.handler
    case.clock[0] = 129.0
    assert not case.startup.poll()
    case.clock[0] = 130.0
    with pytest.raises(CompositionAdmissionError):
        case.startup.poll()
    assert case.events == ["read", "read"]
    assert case.startup.state == "FAILED"


def test_activate_once_only_after_verification_and_retained_path_check():
    case = fixture(original=False)
    assert not case.startup.poll()
    case.state[0] = case.held
    assert case.startup.poll()
    assert case.startup.state == "ACTIVE" and case.startup.handler is case.handler
    events = list(case.events)
    assert events.index("verify") < len(events) - 1 and events[-1] == "recheck"
    assert case.startup.poll()
    assert case.events == events
    case.startup.close()
    case.startup.close()
    assert case.events.count("close") == 1
    with pytest.raises(CompositionAdmissionError):
        _ = case.startup.handler


@pytest.mark.parametrize("fault", ["policy", "parse", "verify", "rotation", "late", "stop"])
def test_invalid_bootstrap_or_late_verification_closes_terminally(fault):
    case = fixture()
    if fault == "policy":
        body = bootstrap_document()
        body["policy"]["startup_timeout_seconds"] += 1
        case.held.document = canonical_json_bytes(body)
        case.held.sha256 = digest(case.held.document)
    elif fault == "parse":
        case.held.document = b"partial-secret"
        case.held.sha256 = digest(case.held.document)
    elif fault == "rotation":
        case.held.require_current = lambda: (_ for _ in ()).throw(OSError("private path"))
    else:

        def prepare(config, deadline):
            if fault == "verify":
                raise RuntimeError("private driver data")
            if fault == "late":
                case.clock[0] = deadline
            else:
                case.stop.notify()
            return case.handler

        case.startup._prepare = prepare
    with pytest.raises(CompositionAdmissionError, match="dispatcher_bootstrap_unavailable") as error:
        case.startup.poll()
    assert "private" not in str(error.value) and "secret" not in str(error.value)
    assert case.startup.state == "FAILED"
    assert case.events.count("close") == 1
    with pytest.raises(CompositionAdmissionError):
        case.startup.poll()
    with pytest.raises(CompositionAdmissionError):
        _ = case.startup.handler


def test_shutdown_before_read_is_irreversible():
    case = fixture()
    case.stop.notify()
    with pytest.raises(CompositionAdmissionError):
        case.startup.poll()
    assert not case.events
    assert case.stop.is_set()
