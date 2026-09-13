"""Authenticated mount sidecar unit evidence; real Unix streams, no live host claim."""

import threading
import time
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.adapters import composition_supervisor_probe_rpc as rpc
from dpone.adapters.composition_clickhouse_supervisor_linux import parse_mountinfo
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_supervisor_probe_rpc import (
    DIGEST,
    FACTS,
    exchange_raw,
    run_once,
)
from tests.test_composition_supervisor_probe_rpc import (
    socket_authority as _socket_authority,
)

socket_authority = _socket_authority

IDS = ("a" * 64, "b" * 64)
ROWS = list(parse_mountinfo(b"9 1 8:1 /sub /data rw - ext4 /dev/sda rw\n2 1 8:1 / / ro - ext4 /dev/sda ro\n"))
TABLES = {identifier: deepcopy(ROWS) for identifier in IDS}
PINS = {identifier: "sha256:" + sha256(canonical_json_bytes(rows)).hexdigest() for identifier, rows in TABLES.items()}


def server_at(path, capture_mounts):
    return rpc.SupervisorFactsServer(
        path,
        enrollment_sha256=DIGEST,
        dispatcher_uid=100001,
        dispatcher_gid=100001,
        capture=lambda deadline: FACTS,
        capture_mounts=capture_mounts,
        timeout_seconds=1,
    )


def test_mount_roundtrip_preserves_exact_row_order_and_legacy_facts(socket_authority):
    deadlines, errors = [], []

    def mounts(deadline):
        deadlines.append(deadline)
        return TABLES

    with server_at(socket_authority, mounts) as server:
        client = rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001)
        for sidecar in (True, False):
            thread = threading.Thread(target=run_once, args=(server, errors))
            thread.start()
            try:
                value = (
                    client.capture_mounts(DIGEST, expected_mountinfo_sha256=PINS, deadline=time.monotonic() + 1)
                    if sidecar
                    else client.capture(DIGEST)
                )
                assert canonical_json_bytes(value) == canonical_json_bytes(TABLES if sidecar else FACTS)
            finally:
                thread.join(2)
            assert not thread.is_alive()
    assert errors == [] and len(deadlines) == 1


@pytest.mark.parametrize("field", ["paths", "container_id", "policy"])
def test_mount_request_rejects_remote_selectors(socket_authority, field):
    captured, errors = [], []
    with server_at(socket_authority, lambda deadline: captured.append(deadline)) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        body = {"schema": rpc.MOUNTS_REQUEST_SCHEMA, "enrollment_sha256": DIGEST, "nonce": "b" * 64, field: "arbitrary"}
        assert exchange_raw(socket_authority, canonical_json_bytes(body)) == b""
        thread.join(2)
    assert not captured and len(errors) == 1


def test_unavailable_mount_callback_does_not_fall_back_to_facts(socket_authority):
    errors = []
    with server_at(socket_authority, None) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        try:
            with pytest.raises(rpc.SupervisorProbeError):
                rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001).capture_mounts(
                    DIGEST, expected_mountinfo_sha256=PINS
                )
        finally:
            thread.join(2)
    assert len(errors) == 1 and "mounts_unavailable" in str(errors[0])


@pytest.mark.parametrize(
    "mutation", ["hash", "missing", "extra", "row_field", "row_type", "row_order", "duplicate", "options"]
)
def test_changed_or_malformed_mount_tables_reject(socket_authority, mutation):
    tables, errors = deepcopy(TABLES), []
    if mutation == "hash":
        tables[IDS[0]][0]["root"] = "/changed"
    elif mutation == "missing":
        del tables[IDS[0]]
    elif mutation == "extra":
        tables["c" * 64] = ROWS
    elif mutation == "row_field":
        tables[IDS[0]][0]["unknown"] = True
    elif mutation == "row_type":
        tables[IDS[0]][0]["id"] = True
    elif mutation == "row_order":
        tables[IDS[0]].reverse()
    elif mutation == "duplicate":
        tables[IDS[0]][1]["id"] = tables[IDS[0]][0]["id"]
    else:
        tables[IDS[0]][0]["options"] = ["rw", "ro"]
    with server_at(socket_authority, lambda deadline: tables) as server:
        thread = threading.Thread(target=run_once, args=(server, errors))
        thread.start()
        try:
            with pytest.raises(rpc.SupervisorProbeError):
                rpc.CaptureSupervisorFactsClient(socket_authority, dispatcher_gid=100001).capture_mounts(
                    DIGEST, expected_mountinfo_sha256=PINS
                )
        finally:
            thread.join(2)
        assert not thread.is_alive()


@pytest.mark.parametrize("drift", [None, "before", "table", "after"])
def test_concrete_host_producer_brackets_exact_protected_pid_tables(monkeypatch, tmp_path, drift):
    from dpone.app import composition_supervisor_host as host
    from tests.test_composition_clickhouse_supervisor_enrollment import enrolled, enrollment_body
    from tests.test_composition_supervisor_probe_rpc import host_config

    facts = {
        "docker": {"containers": {key: {"pid": 101 + i} for i, key in enumerate(IDS)}},
        "linux": {"containers": {key: {"mounts": {"mountinfo_sha256": PINS[key]}} for key in IDS}},
    }
    # An enrolled control container must never become a caller-selectable table.
    facts["docker"]["containers"]["c" * 64] = {"pid": 103}
    body = enrollment_body(facts)
    body["policy"]["roles"]["c" * 64] = "control"
    enrollment = enrolled(body)
    config = host_config(Path("/run/probe.sock"))
    config.update(enrollment_sha256=enrollment.enrollment_sha256, enrollment_document=enrollment.body)
    calls = []
    deadline = time.monotonic() + 1

    def capture(docker, linux, policy, observed_deadline):
        calls.append(("capture", observed_deadline))
        value = deepcopy(facts)
        if drift == "before" or (drift == "after" and len(calls) > 1):
            value["docker"]["containers"][IDS[0]]["pid"] = 999
        return value

    def mounts(self, pid, observed_deadline):
        calls.append((pid, observed_deadline))
        return tuple(reversed(ROWS)) if drift == "table" else tuple(ROWS)

    monkeypatch.setattr(host, "capture_supervisor_facts", capture)
    monkeypatch.setattr(host.LinuxSupervisorProbe, "mounts", mounts)
    server = host.build_supervisor_facts_server(
        tmp_path / "host.json", expected_configuration_sha256=DIGEST, load_config=lambda *args, **kwargs: config
    )
    if drift:
        with pytest.raises(rpc.SupervisorProbeError):
            server.capture_mounts(deadline)
    else:
        assert canonical_json_bytes(server.capture_mounts(deadline)) == canonical_json_bytes(TABLES)
        assert calls == [("capture", deadline), (101, deadline), (102, deadline), ("capture", deadline)]
    if drift == "before":
        assert calls == [("capture", deadline)]


@pytest.mark.parametrize(
    "pins",
    [None, {}, {IDS[0]: DIGEST}, {IDS[0]: DIGEST, IDS[1]: "secret-invalid"}, {"not-container": DIGEST, IDS[1]: DIGEST}],
)
def test_local_mount_pins_rejected_before_endpoint(monkeypatch, pins):
    monkeypatch.setattr(rpc, "_require_endpoint", lambda *args: pytest.fail("endpoint accessed"))
    with pytest.raises(rpc.SupervisorProbeError) as error:
        rpc.CaptureSupervisorFactsClient(Path("/run/probe.sock"), dispatcher_gid=100001).capture_mounts(
            DIGEST, expected_mountinfo_sha256=pins
        )
    assert "secret" not in str(error.value)
