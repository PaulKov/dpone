"""Offline startup boundaries and real local FD reads; no Linux/TLS deployment."""

import builtins
import io
import os
import ssl
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.app import composition_dispatcher_service as service
from dpone.app import composition_dispatcher_service_factory as factory
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.runtime.composition_execution_budget import ExecutionStopSignal
from tests.test_composition_supervisor_host_service import Signals

DIGEST = "sha256:" + "a" * 64
IDENTIFIER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
ARGS = [
    "--config",
    "/etc/dpone/startup/service.json",
    "--configuration-sha256",
    DIGEST,
    "--dispatcher-uid",
    "1200",
    "--dispatcher-gid",
    "1201",
]


class Server:
    def __init__(self, signals, failure=None):
        self.signals, self.failure = signals, failure
        self.admission_stop = ExecutionStopSignal()
        self.events = []

    def stop_admission(self):
        self.events.append("notify")
        self.admission_stop.notify()

    def __enter__(self):
        self.events.append("enter")
        return self

    def __exit__(self, *args):
        self.events.append("close-and-join")

    def handle_request(self):
        assert self.timeout == 0.25
        self.events.append("request")
        if self.failure:
            raise self.failure
        self.signals.stop(self.signals.SIGTERM)


def test_strict_cli_builds_once_and_stops_without_shutdown_deadlock():
    signals = Signals()
    server = Server(signals)
    calls, output = [], io.StringIO()

    def build(path, **kwargs):
        calls.append((path, kwargs))
        return server

    assert service.main(ARGS, build_server=build, signals=signals, stderr=output) == 0
    assert calls == [
        (
            Path("/etc/dpone/startup/service.json"),
            {"expected_configuration_sha256": DIGEST, "dispatcher_uid": 1200, "dispatcher_gid": 1201},
        )
    ]
    assert server.events == ["enter", "request", "notify", "close-and-join"]
    assert signals.handlers == signals.original
    assert not output.getvalue()


def test_service_failure_closes_restores_signals_and_redacts():
    signals = Signals()
    server = Server(signals, RuntimeError("bearer-secret"))
    output = io.StringIO()
    assert service.main(ARGS, build_server=lambda *a, **k: server, signals=signals, stderr=output) == 1
    assert server.events[-1] == "close-and-join"
    assert signals.handlers == signals.original
    assert "service unavailable" in output.getvalue() and "bearer-secret" not in output.getvalue()


def test_help_does_not_import_factory_or_adapters(monkeypatch, capsys):
    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name.startswith("dpone.adapters") or name == "dpone.app.composition_dispatcher_service_factory":
            pytest.fail("help crossed startup boundary")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    with pytest.raises(SystemExit) as caught:
        service.main(["--help"])
    assert caught.value.code == 0
    assert "--dispatcher-uid" in capsys.readouterr().out


@pytest.mark.parametrize(
    "flag,value",
    [
        ("--dispatcher-uid", "0"),
        ("--dispatcher-gid", "01"),
        ("--dispatcher-uid", "+1"),
        ("--dispatcher-uid", "2147483648"),
        ("--config", "/a/../b"),
        ("--config", "relative"),
        ("--config", "//a/b"),
        ("--config", "/one"),
        ("--configuration-sha256", DIGEST.upper()),
    ],
)
def test_bad_arguments_fail_before_io(flag, value):
    args = list(ARGS)
    args[args.index(flag) + 1] = value
    with pytest.raises(SystemExit) as caught:
        service.main(args, build_server=lambda *a, **k: pytest.fail("invalid CLI reached I/O"))
    assert caught.value.code == 2


@pytest.mark.parametrize("extra", [["--dispatcher-uid", "1200"], ["--conf", "/etc/x/y"]])
def test_duplicate_or_abbreviated_options_rejected(extra):
    with pytest.raises(SystemExit) as caught:
        service.main(ARGS + extra, build_server=lambda *a, **k: pytest.fail("invalid CLI reached I/O"))
    assert caught.value.code == 2


@pytest.fixture
def startup(monkeypatch):
    config = SimpleNamespace(
        dispatcher_id=IDENTIFIER,
        dispatcher_uid=1200,
        dispatcher_gid=1201,
        configuration_sha256=DIGEST,
        service_policy=None,
        context_root=Path("/etc/dpone/context"),
        authorities={DIGEST: SimpleNamespace(context_sha256="sha256:" + "b" * 64)},
        tls=SimpleNamespace(certificate_file=Path("/etc/dpone/tls/cert"), private_key_file=Path("/etc/dpone/tls/key")),
        bearer_file=Path("/etc/dpone/tls/bearer"),
        listen=SimpleNamespace(address="127.0.0.1", port=8443),
        accept_timeout_seconds=10,
        execution_timeout_seconds=30,
        max_concurrency=2,
    )
    events, server_args, loader_args = [], {}, {}

    def load(root, relative, **kwargs):
        assert root == Path("/etc/dpone") and relative == "startup/service.json"
        assert kwargs == {"expected_sha256": DIGEST, "bootstrap_uid": 1200, "bootstrap_gid": 1201}
        events.append("config")
        return config

    def tls(value):
        assert value is config
        events.append("credentials")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        return context, "a" * 43

    def loader(**kwargs):
        loader_args.update(kwargs)
        return "loader"

    def handler(value, loaded):
        assert value is config and loaded == "loader"
        return lambda *args: None

    def server(address, **kwargs):
        events.append("bind")
        assert address == ("127.0.0.1", 8443)
        server_args.update(kwargs)
        return "server"

    monkeypatch.setattr(factory, "load_dispatcher_service_config", load)
    monkeypatch.setattr(factory, "_credentials", tls)
    monkeypatch.setattr(factory, "StagedDispatcherContextLoader", loader)
    monkeypatch.setattr(factory, "DispatcherTransferHandler", handler)
    monkeypatch.setattr(factory, "create_dispatch_http_server", server)
    return config, events, server_args, loader_args


def test_startup_uses_exact_catalog_and_shared_stop_budget(startup):
    config, events, arguments, loaders = startup
    assert (
        factory.build_dispatcher_server(
            Path(ARGS[1]), expected_configuration_sha256=DIGEST, dispatcher_uid=1200, dispatcher_gid=1201
        )
        == "server"
    )
    assert events == ["config", "credentials", "bind"]
    assert loaders["staged_authorities"] == {DIGEST: config.authorities[DIGEST].context_sha256}
    assert loaders["configuration_sha256"] == DIGEST
    assert arguments["execution_timeout_seconds"] == 30
    budget = arguments["budget_factory"](30)
    arguments["stop_signal"].notify()
    with pytest.raises(Exception):
        budget.require_effect()
    with pytest.raises(Exception):
        arguments["handler"](None, b"", 0)


def test_authenticator_checks_peer_then_configured_metadata(startup, monkeypatch):
    from dpone.adapters.composition_dispatch_http_server import DispatchRequestContext
    from dpone.contracts.composition_dispatch_v2 import DispatchV2Request

    _, _, arguments, _ = startup
    factory.build_dispatcher_server(
        Path(ARGS[1]), expected_configuration_sha256=DIGEST, dispatcher_uid=1200, dispatcher_gid=1201
    )
    authenticate = arguments["authenticator"]
    authenticate(DispatchRequestContext(("127.0.0.1", 1234)), "Bearer " + "a" * 43)
    with pytest.raises(Exception):
        authenticate(DispatchRequestContext(("not-an-ip", 1234)), "Bearer " + "a" * 43)
    request = object.__new__(DispatchV2Request)
    for name, value in {"dispatcher_id": IDENTIFIER, "runtime_authority_sha256": DIGEST}.items():
        object.__setattr__(request, name, value)
    authenticate(DispatchRequestContext(("127.0.0.1", 1234), request), "Bearer " + "a" * 43)
    object.__setattr__(request, "runtime_authority_sha256", "sha256:" + "c" * 64)
    with pytest.raises(Exception):
        authenticate(DispatchRequestContext(("127.0.0.1", 1234), request), "Bearer " + "a" * 43)


def test_held_tls_descriptor_survives_path_substitution_but_rejects_rotation(tmp_path, monkeypatch):
    path = tmp_path / "cert"
    path.write_bytes(b"original")
    monkeypatch.setattr(factory, "open_protected", lambda path, **kwargs: os.open(path, os.O_RDONLY | os.O_DIRECTORY))
    monkeypatch.setattr(factory, "_require_file", lambda info, gid: None)
    monkeypatch.setattr(factory, "_protected_read", lambda path, gid, maximum: path.read_bytes())
    with pytest.raises(CompositionAdmissionError):
        with factory._held_original(path, 1201, b"original") as held:
            descriptor = int(held.rsplit("/", 1)[1])
            replacement = tmp_path / "replacement"
            replacement.write_bytes(b"other")
            replacement.replace(path)
            assert os.pread(descriptor, 8, 0) == b"original"
    with pytest.raises(OSError):
        os.fstat(descriptor)


def test_held_tls_rejects_leaf_symlink(tmp_path, monkeypatch):
    path = tmp_path / "cert"
    original = tmp_path / "original"
    original.write_bytes(b"original")
    path.symlink_to(original)
    monkeypatch.setattr(factory, "open_protected", lambda path, **kwargs: os.open(path, os.O_RDONLY | os.O_DIRECTORY))
    with pytest.raises(Exception):
        with factory._held_original(path, 1201, b"original"):
            pytest.fail("symlink accepted")


def test_tls_loads_held_originals_without_temporary_key_or_password_prompt(tmp_path, monkeypatch):
    cert, key, bearer = (tmp_path / name for name in ("cert", "key", "bearer"))
    cert.write_bytes(b"certificate")
    key.write_bytes(b"private-key")
    bearer.write_bytes(b"b" * 43)
    config = SimpleNamespace(
        tls=SimpleNamespace(certificate_file=cert, private_key_file=key), bearer_file=bearer, dispatcher_gid=1201
    )
    monkeypatch.setattr(factory, "open_protected", lambda path, **kwargs: os.open(path, os.O_RDONLY | os.O_DIRECTORY))
    monkeypatch.setattr(factory, "_require_file", lambda info, gid: None)
    monkeypatch.setattr(factory, "_protected_read", lambda path, gid, maximum: path.read_bytes())
    loaded = []

    class Context:
        def __init__(self, protocol):
            assert protocol == ssl.PROTOCOL_TLS_SERVER

        def load_cert_chain(self, certfile, keyfile, password):
            loaded.extend([os.pread(int(path.rsplit("/", 1)[1]), 256, 0) for path in (certfile, keyfile)])
            with pytest.raises(CompositionAdmissionError, match="encrypted_key"):
                password()

    monkeypatch.setattr(factory.ssl, "SSLContext", Context)
    context, token = factory._credentials(config)
    assert loaded == [b"certificate", b"private-key"]
    assert context.minimum_version == ssl.TLSVersion.TLSv1_2
    assert token == "b" * 43
    assert sorted(path.name for path in tmp_path.iterdir()) == ["bearer", "cert", "key"]


@pytest.mark.parametrize(
    "field,value", [("st_uid", 1200), ("st_gid", 1202), ("st_nlink", 2), ("st_mode", 0o100660), ("st_size", 1048577)]
)
def test_tls_file_metadata_fails_closed(field, value):
    info = {"st_mode": 0o100640, "st_uid": 0, "st_gid": 1201, "st_nlink": 1, "st_size": 4}
    info[field] = value
    with pytest.raises(CompositionAdmissionError):
        factory._require_file(SimpleNamespace(**info), 1201)


def test_credential_failure_never_binds_listener(startup, monkeypatch):
    _, events, _, _ = startup

    def fail(config):
        raise CompositionAdmissionError("credential rotation")

    monkeypatch.setattr(factory, "_credentials", fail)
    with pytest.raises(CompositionAdmissionError):
        factory.build_dispatcher_server(
            Path(ARGS[1]), expected_configuration_sha256=DIGEST, dispatcher_uid=1200, dispatcher_gid=1201
        )
    assert events == ["config"]


def test_partial_signal_installation_failure_closes_and_restores():
    signals = Signals()
    original = signals.signal

    def fail(signum, handler):
        if signum == signals.SIGINT:
            raise RuntimeError("signal unavailable")
        return original(signum, handler)

    signals.signal = fail
    server = Server(signals)
    assert service.main(ARGS, build_server=lambda *a, **k: server, signals=signals, stderr=io.StringIO()) == 1
    assert server.events == ["enter", "close-and-join"]
    assert signals.handlers == signals.original


def test_legacy_launch_does_not_bypass_policy_bootstrap_lifecycle(startup):
    config, events, _, _ = startup
    config.service_policy = object()
    with pytest.raises(CompositionAdmissionError, match="dispatcher_policy_startup_required"):
        factory.build_dispatcher_server(
            Path(ARGS[1]),
            expected_configuration_sha256=DIGEST,
            dispatcher_uid=1200,
            dispatcher_gid=1201,
        )
    assert events == ["config"]
