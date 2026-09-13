"""Opt-in Linux policy lifecycle, with ONLY bootstrap enrollment verification doubled.

Run in the approved existing isolated runner as root with
DPONE_TEST_POLICY_BOOTSTRAP_LINUX=1. Real protected files, UID/GID drop, TLS,
factory/controller and serving loop are exercised. This is not SQL enrollment,
host isolation, business execution or deployed-route certification.
"""

import importlib.util
import json
import os
import select
import shutil
import signal
import socket
import ssl
import subprocess
import sys
import tempfile
import time
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.contracts.strict_json import canonical_json_bytes

UID, GID = 1200, 1201
DIGEST = "sha256:" + "a" * 64
IDENTIFIER = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def _emit(descriptor, **body):
    os.write(descriptor, canonical_json_bytes(body) + b"\n")


def _child(path, expected, descriptor, mode):
    """One nonroot process; no fake listener, controller, TLS or file reader."""
    from dpone.app import composition_dispatcher_policy_server as factory
    from dpone.app.composition_dispatcher_service import _serve
    from dpone.contracts.composition_identity import CompositionAdmissionError

    service = None
    failed = False

    def verify(config, loader, policy_path, deadline):
        assert policy_path == Path(path) and deadline > time.monotonic()
        _emit(descriptor, phase="verified", pid=os.getpid(), inode=os.fstat(service.server.socket.fileno()).st_ino)
        if mode == "verification":
            raise CompositionAdmissionError("explicit_test_enrollment_verifier_failure")

    factory._verify_bootstrap = verify  # The sole substituted production capability.
    try:
        service = factory.build_dispatcher_policy_server(
            Path(path), expected_policy_sha256=expected, dispatcher_uid=UID, dispatcher_gid=GID
        )
        original_close = service.server.server_close

        def record_close():
            # Transparent telemetry, not a lifecycle capability substitute.
            # Observe our own descriptor without granting parent SYS_PTRACE.
            try:
                if service.server.socket.fileno() >= 0:
                    _emit(
                        descriptor,
                        phase="closing",
                        pid=os.getpid(),
                        inode=os.fstat(service.server.socket.fileno()).st_ino,
                    )
            finally:
                original_close()

        service.server.server_close = record_close
        _emit(
            descriptor,
            phase="bound",
            pid=os.getpid(),
            uid=os.getuid(),
            gid=os.getgid(),
            inode=os.fstat(service.server.socket.fileno()).st_ino,
            source=factory.__file__,
        )
        _serve(service.server, signal, startup=service.startup)
    except Exception:
        failed = True
    finally:
        if service is not None:
            service.server.server_close()
            service.startup.close()
        _emit(descriptor, phase="closed", failed=failed)
        os.close(descriptor)


def _receive(descriptor, timeout=8):
    deadline, original = time.monotonic() + timeout, bytearray()
    while len(original) <= 4096:
        remaining = deadline - time.monotonic()
        assert remaining > 0 and select.select([descriptor], [], [], remaining)[0], "child status timeout"
        part = os.read(descriptor, 1)
        assert part, "child status channel closed"
        if part == b"\n":
            return json.loads(original)
        original.extend(part)
    pytest.fail("child status exceeded fixed budget")


def _protect(path, *, directory=False):
    os.chown(path, 0, GID)
    path.chmod(0o750 if directory else 0o440)


def _policy(root, port, timeout):
    capture = (root / "capture").stat()
    return {
        "schema": "dpone.composition-dispatcher-service-policy.v1",
        "dispatcher_id": IDENTIFIER,
        "dispatcher_uid": UID,
        "dispatcher_gid": GID,
        "capture_custody": "dispatcher_owned_v1",
        "context_root": str(root / "context"),
        "host_probe_socket": str(root / "probe.sock"),
        "capture_root": str(root / "capture"),
        "capture_root_identity": {
            "device": capture.st_dev,
            "inode": capture.st_ino,
            "uid": UID,
            "gid": GID,
            "mode": 448,
        },
        "listen": {"address": "127.0.0.1", "port": port},
        "tls": {
            "certificate_file": str(root / "immutable/cert.pem"),
            "private_key_file": str(root / "immutable/key.pem"),
        },
        "bearer_file": str(root / "immutable/bearer"),
        "accept_timeout_seconds": 1,
        "execution_timeout_seconds": 5,
        "max_concurrency": 2,
        "bootstrap_file": str(root / "bootstrap/bootstrap.json"),
        "startup_timeout_seconds": timeout,
    }


def _install(path, policy, *, malformed=False):
    body = {
        "schema": "dpone.composition-dispatcher-service.v3",
        "policy": policy,
        "supervisor_enrollment_sha256": DIGEST,
        "authorities": {
            DIGEST: {
                "context_sha256": DIGEST,
                "control_connection_ref": "control",
                "expected_control_service_id": IDENTIFIER,
                "control_schema": "dpone_control",
            }
        },
    }
    temporary = path.with_name("pending.json")
    with temporary.open("xb") as stream:
        stream.write(b"{incomplete" if malformed else canonical_json_bytes(body))
        stream.flush()
        os.fsync(stream.fileno())
    _protect(temporary)
    os.replace(temporary, path)
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


@pytest.fixture
def linux_policy_root():
    if os.environ.get("DPONE_TEST_POLICY_BOOTSTRAP_LINUX") != "1":
        pytest.skip("requires explicit DPONE_TEST_POLICY_BOOTSTRAP_LINUX=1 in the approved existing runner")
    if sys.platform != "linux" or os.geteuid() != 0:
        pytest.skip("requires actual Linux root harness; only the child drops to dispatcher UID/GID")
    assert shutil.which("openssl"), "approved Linux harness requires OpenSSL"
    root = Path(tempfile.mkdtemp(prefix="dpone-policy-test-", dir="/run"))
    try:
        _protect(root, directory=True)
        for relative in ("immutable", "bootstrap", "context"):
            (root / relative).mkdir()
            _protect(root / relative, directory=True)
        (root / "capture").mkdir(mode=0o700)
        os.chown(root / "capture", UID, GID)
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(root / "immutable/key.pem"),
                "-out",
                str(root / "immutable/cert.pem"),
                "-days",
                "1",
                "-subj",
                "/CN=127.0.0.1",
                "-addext",
                "subjectAltName=IP:127.0.0.1",
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
        (root / "immutable/bearer").write_bytes(b"fixture-only-token-" + b"x" * 40)
        for path in (root / "immutable").iterdir():
            _protect(path)
        yield root
    finally:
        shutil.rmtree(root)


@pytest.mark.parametrize("mode", ["activate", "malformed", "verification", "deadline", "shutdown"])
def test_actual_linux_closed_tls_atomic_bootstrap_same_process_socket(linux_policy_root, mode):
    root = linux_policy_root
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    policy = _policy(root, port, 2 if mode == "deadline" else 12)
    policy_path = root / "immutable/policy.json"
    original = canonical_json_bytes(policy)
    policy_path.write_bytes(original)
    _protect(policy_path)
    expected = "sha256:" + sha256(original).hexdigest()
    read_fd, write_fd = os.pipe()
    child = None
    try:
        child = subprocess.Popen(
            [sys.executable, str(Path(__file__).resolve()), "--child", str(policy_path), expected, str(write_fd), mode],
            pass_fds=(write_fd,),
            user=UID,
            group=GID,
            extra_groups=[],
            start_new_session=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        os.close(write_fd)
        write_fd = None
        bound = _receive(read_fd)
        assert bound["phase"] == "bound" and bound["pid"] == child.pid
        assert (bound["uid"], bound["gid"]) == (UID, GID)
        spec = importlib.util.find_spec("dpone.app.composition_dispatcher_policy_server")
        assert Path(bound["source"]).resolve() == Path(spec.origin).resolve()
        assert type(bound["inode"]) is int and bound["inode"] > 0
        client = ssl.create_default_context(cafile=str(root / "immutable/cert.pem"))
        client.minimum_version = ssl.TLSVersion.TLSv1_2
        with socket.create_connection(("127.0.0.1", port), timeout=1) as raw:
            with client.wrap_socket(raw, server_hostname="127.0.0.1", do_handshake_on_connect=False) as tls:
                tls.settimeout(0.3)
                with pytest.raises(TimeoutError):
                    tls.do_handshake()
        assert child.poll() is None and not Path(policy["bootstrap_file"]).exists()
        if mode in {"activate", "malformed", "verification"}:
            _install(Path(policy["bootstrap_file"]), policy, malformed=mode == "malformed")
        if mode in {"activate", "verification"}:
            verified = _receive(read_fd)
            assert verified == {"phase": "verified", "pid": child.pid, "inode": bound["inode"]}
        if mode == "activate":
            with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
                with client.wrap_socket(raw, server_hostname="127.0.0.1") as tls:
                    tls.settimeout(3)
                    # Deliberately wrong fixture bearer and no payload: reject before decoding.
                    tls.sendall(
                        b"POST /v2/composition-dispatch HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                        b"Authorization: Bearer "
                        + b"z" * 64
                        + b"\r\nContent-Type: application/vnd.dpone.composition-dispatch.v1\r\nContent-Length: 5\r\nConnection: close\r\n\r\n"
                    )
                    response = tls.recv(4096)
                    assert response.startswith(b"HTTP/1.1 401 ")
            assert child.poll() is None
        if mode in {"activate", "shutdown"}:
            child.send_signal(signal.SIGTERM)
        closing = _receive(read_fd)
        assert closing == {"phase": "closing", "pid": child.pid, "inode": bound["inode"]}
        closed = _receive(read_fd)
        assert closed == {"phase": "closed", "failed": mode in {"malformed", "verification", "deadline"}}
        assert child.wait(timeout=4) == 0
        with pytest.raises(OSError):
            socket.create_connection(("127.0.0.1", port), timeout=0.5)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=4)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=4)
        os.close(read_fd)
        if write_fd is not None:
            os.close(write_fd)


if __name__ == "__main__":
    assert sys.argv[1] == "--child"
    _child(sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5])
