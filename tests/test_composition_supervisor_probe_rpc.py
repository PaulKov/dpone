"""Real Unix streams with explicit host-authority doubles; not live host proof."""

import os
import socket
import struct
import sys
import threading
import time
from pathlib import Path

import pytest

from dpone.adapters import composition_supervisor_probe_rpc as rpc
from dpone.contracts.strict_json import canonical_json_bytes

DIGEST = "sha256:" + "a" * 64
FACTS = {"docker": {"synthetic": True}, "linux": {"synthetic": True}}


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), -1.0])
def test_client_rejects_invalid_or_expired_outer_deadline_before_endpoint(monkeypatch, deadline):
    monkeypatch.setattr(rpc, "_require_endpoint", lambda *args: pytest.fail("endpoint accessed"))
    client = rpc.CaptureSupervisorFactsClient(Path("/run/probe.sock"), dispatcher_gid=100001)
    with pytest.raises(rpc.SupervisorProbeError, match="deadline"):
        client.capture(DIGEST, deadline=deadline)


@pytest.mark.parametrize("outer,expected", [(102.0, 102.0), (200.0, 105.0), (None, 105.0)])
def test_client_outer_deadline_never_extends_own_timeout(monkeypatch, outer, expected):
    observed = []
    monkeypatch.setattr(rpc.time, "monotonic", lambda: 100.0)

    def remaining(deadline):
        observed.append(deadline)
        raise rpc.SupervisorProbeError("test_stop_before_io")

    monkeypatch.setattr(rpc, "_remaining", remaining)
    client = rpc.CaptureSupervisorFactsClient(Path("/run/probe.sock"), dispatcher_gid=100001, timeout_seconds=5)
    with pytest.raises(rpc.SupervisorProbeError, match="test_stop_before_io"):
        client.capture(DIGEST, deadline=outer)
    assert observed == [expected]


@pytest.fixture
def socket_authority(monkeypatch, tmp_path):
    # These doubles permit unprivileged/macOS transport tests. They are not
    # evidence of root ownership, Linux peer credentials or Docker isolation.
    monkeypatch.setattr(rpc, "_require_host", lambda: None)
    monkeypatch.setattr(rpc, "_protected_parent", lambda path: None)
    monkeypatch.setattr(rpc, "_require_endpoint", lambda path, gid: None)
    monkeypatch.setattr(
        rpc, "_peer_uid", lambda connection: 0 if threading.current_thread() is threading.main_thread() else 100001
    )
    monkeypatch.setattr(rpc.os, "chown", lambda *args: None)
    # AF_UNIX path limits are small on macOS; pytest temp paths can exceed them.
    import tempfile

    with tempfile.TemporaryDirectory(prefix="probe-") as root:
        yield Path(root) / "probe.sock"


def run_once(server, errors):
    try:
        assert server.serve_once() is True
    except Exception as error:
        errors.append(error)


@pytest.mark.parametrize("outer_budget", [None, 0.8])
def test_real_unix_roundtrip_has_exact_nonce_bound_facts(socket_authority, outer_budget):
    deadlines, errors = [], []

    def capture(deadline):
        deadlines.append(deadline)
        return FACTS

    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=capture,
        timeout_seconds=1,
    ) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        assert (
            rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001, timeout_seconds=1).capture(
                DIGEST, deadline=None if outer_budget is None else time.monotonic() + outer_budget
            )
            == FACTS
        )
        thread.join(2)
        assert not thread.is_alive() and errors == []
    assert len(deadlines) == 1 and deadlines[0] > time.monotonic() - 1
    assert not socket_authority.exists()


def test_existing_socket_path_is_never_deleted(socket_authority):
    socket_authority.write_text("protected original")
    with pytest.raises(rpc.SupervisorProbeError):
        with rpc.SupervisorFactsServer(
            socket_authority,
            enrollment_sha256=DIGEST,
            dispatcher_uid=100001,
            dispatcher_gid=100001,
            capture=lambda deadline: FACTS,
        ):
            pass
    assert socket_authority.read_text() == "protected original"


def exchange_raw(path, document, *, declared=None, suffix=b""):
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
        connection.settimeout(2)
        connection.connect(str(path))
        connection.sendall(struct.pack("!I", len(document) if declared is None else declared) + document + suffix)
        connection.shutdown(socket.SHUT_WR)
        try:
            return connection.recv(1024)
        except ConnectionResetError:
            return b""


@pytest.mark.parametrize(
    "mutation",
    ["policy", "enrollment", "schema", "nonce", "noncanonical", "duplicate", "trailing", "truncated", "oversize"],
)
def test_server_rejects_closed_request_violations_before_capture(socket_authority, mutation):
    body = {"schema": rpc.REQUEST_SCHEMA, "enrollment_sha256": DIGEST, "nonce": "b" * 64}
    if mutation == "policy":
        body["policy"] = {"path": "/var/run/docker.sock"}
    elif mutation == "enrollment":
        body["enrollment_sha256"] = "sha256:" + "f" * 64
    elif mutation == "schema":
        body["schema"] = "arbitrary-command"
    elif mutation == "nonce":
        body["nonce"] = "short"
    document = canonical_json_bytes(body)
    if mutation == "noncanonical":
        document += b" "
    elif mutation == "duplicate":
        document = b'{"nonce":"b","nonce":"b"}'
    captured, errors = [], []
    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: captured.append(deadline),
        timeout_seconds=1,
    ) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        size = (
            len(document) + 1
            if mutation == "truncated"
            else rpc.MAX_REQUEST_BYTES + 1
            if mutation == "oversize"
            else None
        )
        assert (
            exchange_raw(socket_authority, document, declared=size, suffix=b"x" if mutation == "trailing" else b"")
            == b""
        )
        thread.join(2)
        assert not thread.is_alive() and len(errors) == 1
        assert isinstance(errors[0], rpc.SupervisorProbeError)
    assert captured == []


def test_server_rejects_wrong_peer_before_capture(socket_authority, monkeypatch):
    monkeypatch.setattr(rpc, "_peer_uid", lambda connection: 999)
    errors, captured = [], []
    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: captured.append(deadline),
        timeout_seconds=1,
    ) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(socket_authority))
            assert connection.recv(1) == b""
        thread.join(2)
    assert captured == [] and len(errors) == 1 and "dispatcher_identity" in str(errors[0])


@pytest.mark.parametrize(
    "mutation", ["nonce", "enrollment", "schema", "extra", "facts", "truncated", "trailing", "noncanonical", "oversize"]
)
def test_client_rejects_response_substitution_and_bad_framing(socket_authority, mutation):
    failures = []

    def responder(listener):
        try:
            connection, _ = listener.accept()
            with connection:
                request = rpc._read_frame(connection, rpc.MAX_REQUEST_BYTES, time.monotonic() + 2)
                response = {
                    "schema": rpc.RESPONSE_SCHEMA,
                    "enrollment_sha256": DIGEST,
                    "nonce": request["nonce"],
                    "facts": FACTS,
                }
                if mutation == "nonce":
                    response["nonce"] = "f" * 64
                elif mutation == "enrollment":
                    response["enrollment_sha256"] = "sha256:" + "f" * 64
                elif mutation == "schema":
                    response["schema"] = rpc.REQUEST_SCHEMA
                elif mutation == "extra":
                    response["command"] = "forbidden"
                elif mutation == "facts":
                    response["facts"] = {"linux": {}}
                document = canonical_json_bytes(response)
                if mutation == "noncanonical":
                    document += b" "
                size = (
                    len(document) + 1
                    if mutation == "truncated"
                    else rpc.MAX_RESPONSE_BYTES + 1
                    if mutation == "oversize"
                    else len(document)
                )
                connection.sendall(struct.pack("!I", size) + document + (b"x" if mutation == "trailing" else b""))
                connection.shutdown(socket.SHUT_WR)
        except Exception as error:
            failures.append(error)

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(socket_authority))
        listener.listen(1)
        thread = threading.Thread(target=responder, args=(listener,))
        thread.start()
        with pytest.raises(rpc.SupervisorProbeError):
            rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001, timeout_seconds=1).capture(DIGEST)
        thread.join(2)
    assert not thread.is_alive() and failures == []


def test_unfinished_request_hits_absolute_deadline_without_capture(socket_authority):
    errors, captured = [], []
    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: captured.append(deadline),
        timeout_seconds=0.1,
    ) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
            connection.connect(str(socket_authority))
            connection.sendall(b"\x00")
            thread.join(1)
            assert not thread.is_alive()
    assert len(errors) == 1 and captured == []


def test_client_rejects_nonroot_server_without_sending_request(socket_authority, monkeypatch):
    monkeypatch.setattr(rpc, "_peer_uid", lambda connection: 100001)
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
        listener.bind(str(socket_authority))
        listener.listen(1)
        with pytest.raises(rpc.SupervisorProbeError, match="server_identity"):
            rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001).capture(DIGEST)
        connection, _ = listener.accept()
        with connection:
            assert connection.recv(1) == b""


def test_close_never_unlinks_replacement_inode(socket_authority):
    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: FACTS,
    ):
        socket_authority.unlink()
        socket_authority.write_text("replacement")
    assert socket_authority.read_text() == "replacement"


@pytest.mark.skipif(
    sys.platform != "linux", reason="SO_PEERCRED is Linux-only; Unix framing tests use explicit doubles"
)
def test_linux_socket_peer_credentials_are_observed_from_kernel():
    left, right = socket.socketpair()
    with left, right:
        assert rpc._peer_uid(left) == os.geteuid()
        assert rpc._peer_uid(right) == os.geteuid()


def test_peer_uid_does_not_require_host_pid_visibility(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(rpc.sys, "platform", "linux")
    monkeypatch.setattr(rpc.socket, "SO_PEERCRED", 17, raising=False)
    connection = SimpleNamespace(getsockopt=lambda *args: struct.pack("3i", 0, 0, 0))
    assert rpc._peer_uid(connection) == 0


def test_unprotected_directory_and_symlink_parent_are_rejected(tmp_path):
    with pytest.raises(rpc.SupervisorProbeError, match="parent"):
        rpc._protected_parent(tmp_path / "probe.sock")
    alias = tmp_path / "alias"
    alias.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(rpc.SupervisorProbeError, match="parent"):
        rpc._protected_parent(alias / "probe.sock")


def host_config(path):
    from tests.test_composition_clickhouse_supervisor_enrollment import enrolled

    enrollment = enrolled()
    return {
        "schema": "dpone.composition-host-probe-config.v1",
        "socket_path": str(path),
        "dispatcher_uid": 100001,
        "dispatcher_gid": 100001,
        "timeout_seconds": 1,
        "enrollment_sha256": enrollment.enrollment_sha256,
        "enrollment_document": enrollment.body,
    }


def test_app_loads_pinned_protected_config_and_concrete_capture(socket_authority, monkeypatch):
    from hashlib import sha256

    from dpone.app import composition_supervisor_host as host

    config = host_config(socket_authority)
    document = canonical_json_bytes(config)
    digest = "sha256:" + sha256(document).hexdigest()
    reads, captures = [], []

    def read(root, relative, *, max_bytes):
        reads.append((root, relative, max_bytes))
        return document

    def capture(docker, linux, policy, deadline):
        assert type(docker) is host.LocalDockerSupervisorClient and type(linux) is host.LinuxSupervisorProbe
        captures.append((policy, deadline))
        return FACTS

    monkeypatch.setattr(host, "read_protected_original", read)
    monkeypatch.setattr(host, "capture_supervisor_facts", capture)
    server = host.build_supervisor_facts_server(Path("/etc/dpone/host.json"), expected_configuration_sha256=digest)
    deadline = time.monotonic() + 1
    assert server.capture(deadline) == FACTS
    assert captures == [(config["enrollment_document"]["policy"], deadline)]
    assert reads == [(Path("/etc/dpone"), "host.json", host.MAX_CONFIG_BYTES)]


@pytest.mark.parametrize("mutation", ["digest", "enrollment", "extra", "noncanonical", "duplicate"])
def test_app_rejects_unpinned_or_noncanonical_configuration(socket_authority, monkeypatch, mutation):
    from hashlib import sha256

    from dpone.app import composition_supervisor_host as host

    config = host_config(socket_authority)
    if mutation == "enrollment":
        config["enrollment_sha256"] = DIGEST
    elif mutation == "extra":
        config["sql"] = "forbidden"
    document = canonical_json_bytes(config)
    if mutation == "noncanonical":
        document += b" "
    elif mutation == "duplicate":
        document = b'{"schema":"a","schema":"a"}'
    monkeypatch.setattr(host, "read_protected_original", lambda *args, **kwargs: document)
    expected = DIGEST if mutation == "digest" else "sha256:" + sha256(document).hexdigest()
    with pytest.raises(rpc.SupervisorProbeError, match="configuration"):
        host.build_supervisor_facts_server(Path("/etc/dpone/host.json"), expected_configuration_sha256=expected)


@pytest.mark.parametrize("field,value", [("st_uid", 1), ("st_gid", 2), ("st_mode", 0o140666), ("st_mode", 0o100660)])
def test_endpoint_requires_root_socket_exact_group_and_private_mode(monkeypatch, field, value):
    from types import SimpleNamespace

    monkeypatch.setattr(rpc, "_protected_parent", lambda path: None)
    facts = {"st_uid": 0, "st_gid": 100001, "st_mode": 0o140660}
    facts[field] = value
    path = SimpleNamespace(lstat=lambda: SimpleNamespace(**facts))
    with pytest.raises(rpc.SupervisorProbeError, match="endpoint"):
        rpc._require_endpoint(path, 100001)


def test_root_dispatcher_identity_and_unsupported_host_are_rejected(monkeypatch):
    with pytest.raises(rpc.SupervisorProbeError, match="dispatcher_identity"):
        rpc.SupervisorFactsServer(
            Path("/run/dpone/probe.sock"),
            enrollment_sha256=DIGEST,
            dispatcher_uid=0,
            dispatcher_gid=100001,
            capture=lambda deadline: FACTS,
        )
    monkeypatch.setattr(rpc.sys, "platform", "darwin")
    with pytest.raises(rpc.SupervisorProbeError, match="host"):
        rpc._require_host()


def test_capture_over_deadline_cannot_return_success(socket_authority):
    errors = []

    def slow(deadline):
        time.sleep(max(0, deadline - time.monotonic()) + 0.01)
        return FACTS

    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=slow,
        timeout_seconds=0.05,
    ) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        with pytest.raises(rpc.SupervisorProbeError):
            rpc.CaptureSupervisorFactsClient(
                socket_authority,
                dispatcher_gid=100001,
                timeout_seconds=0.2,
            ).capture(DIGEST)
        thread.join(1)
    assert not thread.is_alive() and len(errors) == 1


def test_idle_accept_timeout_is_distinct_from_accepted_request_failure(socket_authority):
    captured = []
    with rpc.SupervisorFactsServer(
        socket_authority,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: captured.append(deadline),
        timeout_seconds=0.01,
    ) as server:
        assert server.serve_once() is False
    assert captured == []
