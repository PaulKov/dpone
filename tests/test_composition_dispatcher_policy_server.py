"""Concrete policy composition with explicit host/TLS boundary doubles, not live proof."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_dispatch_http_server import DispatchRequestContext
from dpone.app import composition_dispatcher_policy_server as module
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_dispatcher_service_policy import bootstrap_document, decode_policy, digest

POLICY_PATH = Path("/etc/dpone/startup/policy.json")


def harness(monkeypatch):
    policy = decode_policy()
    events, captured, clock = [], {}, [100.0]
    monkeypatch.setattr(module.sys, "platform", "linux")
    monkeypatch.setattr(module.os, "getresuid", lambda: (policy.dispatcher_uid,) * 3, raising=False)
    monkeypatch.setattr(module.os, "getresgid", lambda: (policy.dispatcher_gid,) * 3, raising=False)
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        module, "_protected_read", lambda path, gid, maximum: events.append("policy-read") or policy.document
    )
    monkeypatch.setattr(module, "_credentials", lambda value: events.append("credentials") or (object(), "x" * 43))
    raw = canonical_json_bytes(bootstrap_document())
    held = SimpleNamespace(
        document=raw,
        sha256=digest(raw),
        require_current=lambda: events.append("recheck"),
        close=lambda: events.append("bootstrap-close"),
    )
    available = [None]

    def opened(path, **kwargs):
        events.append("bootstrap-read")
        assert path == policy.bootstrap_file and kwargs["deadline"] == 220.0
        return available[0]

    monkeypatch.setattr(module, "open_dispatcher_bootstrap", opened)
    monkeypatch.setattr(
        module, "StagedDispatcherContextLoader", lambda **kwargs: captured.setdefault("loader", kwargs) or object()
    )
    monkeypatch.setattr(
        module, "RuntimeConnectionContextLoader", lambda **kwargs: captured.setdefault("runtime", kwargs)
    )

    def verify(config, loader, path, deadline):
        events.append("verify")
        captured["verify"] = (config, loader, path, deadline)

    monkeypatch.setattr(module, "_verify_bootstrap", verify)

    def handler(config, loader):
        events.append("handler-created")
        return lambda request, budget: events.append("execute") or "response"

    monkeypatch.setattr(module, "DispatcherTransferHandler", handler)
    server = SimpleNamespace(server_close=lambda: events.append("server-close"))

    def bind(address, **kwargs):
        events.append("bind")
        captured["server"] = kwargs
        captured["address"] = address
        return server

    monkeypatch.setattr(module, "create_dispatch_http_server", bind)
    return SimpleNamespace(
        policy=policy, events=events, captured=captured, clock=clock, held=held, available=available, server=server
    )


def build(case):
    return module.build_dispatcher_policy_server(
        POLICY_PATH,
        expected_policy_sha256=case.policy.sha256,
        dispatcher_uid=case.policy.dispatcher_uid,
        dispatcher_gid=case.policy.dispatcher_gid,
    )


def test_listener_binds_once_closed_then_uses_verified_admitted_catalog_and_shared_stop(monkeypatch):
    case = harness(monkeypatch)
    result = build(case)
    callbacks = case.captured["server"]
    peer = DispatchRequestContext(("127.0.0.1", 12345))
    assert result.server is case.server and result.startup.state == "WAITING"
    assert case.events == ["policy-read", "credentials", "bind"]
    with pytest.raises(RuntimeError):
        callbacks["authenticator"](peer, "Bearer " + "x" * 43)
    with pytest.raises(CompositionAdmissionError):
        callbacks["v2_handler"](object(), object())
    assert not result.startup.poll()
    case.available[0] = case.held
    assert result.startup.poll()
    assert case.events.index("verify") < case.events.index("handler-created") < case.events.index("recheck")
    loader = case.captured["loader"]
    config, _, path, deadline = case.captured["verify"]
    assert (loader["identity_kind"], loader["configuration_sha256"]) == ("policy", case.policy.sha256)
    assert loader["staged_authorities"] == {key: value.context_sha256 for key, value in config.authorities.items()}
    assert path == POLICY_PATH and deadline == 220.0
    assert callbacks["authenticator"](peer, "Bearer " + "x" * 43) is None
    assert callbacks["v2_handler"](object(), object()) == "response"
    budget = callbacks["budget_factory"](1)
    assert budget._stop is callbacks["stop_signal"] is result.startup._stop
    callbacks["stop_signal"].notify()
    with pytest.raises(CompositionAdmissionError):
        callbacks["v2_handler"](object(), budget)
    assert case.events.count("bind") == 1
    result.startup.close()


@pytest.mark.parametrize("failure", ["platform", "uid", "gid", "digest"])
def test_policy_load_rejects_wrong_process_or_hash_before_credentials(monkeypatch, failure):
    case = harness(monkeypatch)
    if failure == "platform":
        monkeypatch.setattr(module.sys, "platform", "darwin")
    elif failure == "uid":
        monkeypatch.setattr(module.os, "getresuid", lambda: (0, 0, 0))
    elif failure == "gid":
        monkeypatch.setattr(module.os, "getresgid", lambda: (0, 0, 0))
    else:
        monkeypatch.setattr(module, "_protected_read", lambda *_: case.policy.document + b"\n")
    with pytest.raises(CompositionAdmissionError):
        build(case)
    assert "credentials" not in case.events and "bind" not in case.events


def test_failed_preparation_never_exposes_handler_and_releases_original(monkeypatch):
    case = harness(monkeypatch)
    result = build(case)
    case.available[0] = case.held
    monkeypatch.setattr(module, "_verify_bootstrap", lambda *args: (_ for _ in ()).throw(RuntimeError("secret")))
    with pytest.raises(CompositionAdmissionError):
        result.startup.poll()
    assert "handler-created" not in case.events and case.events[-1] == "bootstrap-close"
    assert result.startup.state == "FAILED"


def test_fixed_startup_deadline_starts_before_listener_construction(monkeypatch):
    case = harness(monkeypatch)
    original_bind = module.create_dispatch_http_server

    def slow_bind(*args, **kwargs):
        case.clock[0] = 221.0
        return original_bind(*args, **kwargs)

    monkeypatch.setattr(module, "create_dispatch_http_server", slow_bind)
    result = build(case)
    with pytest.raises(CompositionAdmissionError):
        result.startup.poll()
    assert "bootstrap-read" not in case.events


@pytest.mark.parametrize("failure", ["credentials", "bind"])
def test_construction_failure_stops_and_closes_startup(monkeypatch, failure):
    case = harness(monkeypatch)
    original = module.DispatcherStartup
    retained = []

    def startup(*args, **kwargs):
        result = original(*args, **kwargs)
        retained.append(result)
        return result

    def fail(*args, **kwargs):
        raise RuntimeError("private-token")

    monkeypatch.setattr(module, "DispatcherStartup", startup)
    monkeypatch.setattr(module, "_credentials" if failure == "credentials" else "create_dispatch_http_server", fail)
    with pytest.raises(CompositionAdmissionError) as error:
        build(case)
    assert "private-token" not in str(error.value)
    assert len(retained) == 1 and retained[0].state == "CLOSED" and retained[0]._stop.is_set()
