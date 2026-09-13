"""Offline entry-point/lifecycle tests; no Linux host service is deployed here."""

import io
from pathlib import Path

import pytest

from dpone.app import composition_supervisor_host_service as service

DIGEST = "sha256:" + "a" * 64
ARGS = ["--config", "/etc/dpone/host-probe.json", "--configuration-sha256", DIGEST]


class Signals:
    SIGTERM, SIGINT = 15, 2

    def __init__(self):
        self.handlers = {self.SIGTERM: "previous-term", self.SIGINT: "previous-int"}
        self.original = dict(self.handlers)

    def signal(self, signum, handler):
        previous = self.handlers[signum]
        self.handlers[signum] = handler
        return previous

    def stop(self, signum):
        self.handlers[signum](signum, None)


class Server:
    def __init__(self, signals, *, stop_signal=15, failure=None):
        self.signals, self.stop_signal, self.failure = signals, stop_signal, failure
        self.calls = self.entered = self.closed = 0

    def __enter__(self):
        self.entered += 1
        return self

    def __exit__(self, *args):
        self.closed += 1

    def serve_once(self):
        self.calls += 1
        if self.failure is not None:
            raise self.failure
        if self.calls == 1:
            return False
        self.signals.stop(self.stop_signal)
        return True


@pytest.mark.parametrize("signum", [Signals.SIGTERM, Signals.SIGINT])
def test_idle_continues_and_signal_stops_cleanly_restoring_handlers(signum):
    signals = Signals()
    server = Server(signals, stop_signal=signum)
    configured, output = [], io.StringIO()

    def build(path, *, expected_configuration_sha256):
        configured.append((path, expected_configuration_sha256))
        return server

    assert service.main(ARGS, build_server=build, signals=signals, stderr=output) == 0
    assert configured == [(Path("/etc/dpone/host-probe.json"), DIGEST)]
    assert server.calls == 2 and server.entered == server.closed == 1
    assert signals.handlers == signals.original and output.getvalue() == ""


def test_accepted_request_failure_is_not_idle_or_retried_and_details_are_redacted():
    signals, output = Signals(), io.StringIO()
    server = Server(signals, failure=RuntimeError("password=secret host private facts"))
    assert service.main(ARGS, build_server=lambda *args, **kwargs: server, signals=signals, stderr=output) == 1
    assert server.calls == 1 and server.closed == 1
    assert signals.handlers == signals.original
    assert "service unavailable" in output.getvalue()
    assert "secret" not in output.getvalue() and "private" not in output.getvalue()


def test_configuration_failure_never_installs_signal_handlers_or_starts_service():
    signals, output = Signals(), io.StringIO()

    def unavailable(*args, **kwargs):
        raise RuntimeError("sensitive configuration")

    assert service.main(ARGS, build_server=unavailable, signals=signals, stderr=output) == 1
    assert signals.handlers == signals.original
    assert "sensitive" not in output.getvalue()


def test_bind_failure_restores_signal_handlers():
    signals = Signals()

    class Refused(Server):
        def __enter__(self):
            raise RuntimeError("host root unavailable")

    server = Refused(signals)
    assert service.main(ARGS, build_server=lambda *args, **kwargs: server, signals=signals, stderr=io.StringIO()) == 1
    assert server.calls == 0 and signals.handlers == signals.original


def test_help_exits_zero_before_loading_concrete_adapter(monkeypatch, capsys):
    import builtins

    original = builtins.__import__

    def guarded(name, *args, **kwargs):
        if name == "dpone.app.composition_supervisor_host" or name.startswith("dpone.adapters"):
            raise AssertionError("help loaded concrete host authority")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    with pytest.raises(SystemExit) as result:
        service.main(["--help"])
    assert result.value.code == 0
    output = capsys.readouterr()
    assert "--configuration-sha256" in output.out and output.err == ""


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["--config", "relative.json", "--configuration-sha256", DIGEST],
        ["--config", "/etc/../host.json", "--configuration-sha256", DIGEST],
        ["--config", "/etc//host.json", "--configuration-sha256", DIGEST],
        ["--config", "/etc/host.json", "--configuration-sha256", "wrong"],
        ["--conf", "/etc/host.json", "--configuration-sha256", DIGEST],
        [*ARGS, "--policy", "forbidden"],
    ],
)
def test_invalid_arguments_exit_two_before_any_host_io(args, capsys):
    def forbidden(*args, **kwargs):
        raise AssertionError("invalid arguments reached host")

    with pytest.raises(SystemExit) as result:
        service.main(args, build_server=forbidden)
    assert result.value.code == 2
    assert "error:" in capsys.readouterr().err


def test_unexpected_serve_result_is_not_treated_as_success():
    signals = Signals()
    server = Server(signals)
    server.serve_once = lambda: None
    assert service.main(ARGS, build_server=lambda *args, **kwargs: server, signals=signals, stderr=io.StringIO()) == 1
    assert server.closed == 1 and signals.handlers == signals.original


def test_systemd_template_preserves_real_host_visibility_and_no_automatic_restart():
    root = Path(__file__).resolve().parents[1]
    text = (root / "examples/composition-supervisor/dpone-host-probe.service").read_text()
    directives = {
        line.split("=", 1)[0]: line.split("=", 1)[1]
        for line in text.splitlines()
        if "=" in line and not line.startswith("#")
    }
    assert directives["User"] == directives["Group"] == "root"
    assert directives["RuntimeDirectoryMode"] == "0750"
    assert "<DISPATCHER_GROUP>" in directives["ExecStartPre"]
    assert directives["Restart"] == "no" and int(directives["TimeoutStopSec"]) > 60
    assert "-m dpone.app.composition_supervisor_host_service" in directives["ExecStart"]
    assert "<CONFIGURATION_SHA256>" in directives["ExecStart"]
    assert not {"PrivateNetwork", "PrivateUsers", "PrivateMounts", "ProtectProc"}.intersection(directives)
    assert "TEMPLATE ONLY" in text
