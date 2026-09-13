"""Controlled supervisor snapshots are unit evidence, never live certification."""

import time
from copy import deepcopy

import pytest

from dpone.adapters.composition_clickhouse_supervisor import DockerClickHouseLocalSupervisor, capture_supervisor_facts
from dpone.adapters.composition_clickhouse_supervisor_docker import normalize_container
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_supervisor_enrollment import CH, DISPATCHER, NETWORK, enrolled, enrollment_body


def test_docker_unknown_privileged_container_cannot_be_accepted_as_control():
    with pytest.raises(CompositionAdmissionError):
        normalize_container(
            {"Id": "a" * 64, "HostConfig": {"Privileged": True}}, role="control", clickhouse_id="b" * 64
        )


def docker_container(identifier, role):
    return {
        "Id": identifier,
        "Image": "sha256:" + "e" * 64,
        "HostConfig": {
            "Privileged": False,
            "CapAdd": None,
            "CapDrop": ["ALL"],
            "PidMode": "",
            "IpcMode": "private",
            "CgroupnsMode": "private",
            "SecurityOpt": ["no-new-privileges:true"],
            "NetworkMode": "container:" + CH if role == "dispatcher" else NETWORK,
            "ReadonlyRootfs": True,
        },
        "Config": {"User": "101", "Env": ["not-a-secret=fake"]},
        "State": {
            "Running": True,
            "Paused": False,
            "Restarting": False,
            "Dead": False,
            "Pid": 102 if role == "dispatcher" else 101,
            "StartedAt": "2026-09-11T01:00:00Z",
        },
        "Mounts": [
            {
                "Type": "tmpfs" if role == "dispatcher" else "volume",
                "Source": "" if role == "dispatcher" else "/var/lib/docker/volumes/ch/_data",
                "Destination": "/run/dpone-secrets" if role == "dispatcher" else "/var/lib/clickhouse",
                "RW": True,
                "Propagation": "",
            }
        ],
        "NetworkSettings": {
            "Ports": {},
            "Networks": {}
            if role == "dispatcher"
            else {"cell": {"NetworkID": NETWORK, "IPAddress": "172.20.0.2", "EndpointID": "f" * 64}},
        },
    }


def docker_facts():
    return {
        "containers": {
            CH: normalize_container(docker_container(CH, "clickhouse"), role="clickhouse", clickhouse_id=CH),
            DISPATCHER: normalize_container(
                docker_container(DISPATCHER, "dispatcher"), role="dispatcher", clickhouse_id=CH
            ),
        },
        "network": {},
        "daemon_id": "fixed",
    }


class OfflineDocker:
    def __init__(self):
        self.facts = docker_facts()

    def snapshot(self, policy, deadline):
        return deepcopy(self.facts)


class OfflineLinux:
    """Controlled fixture explicitly substitutes host observation for unit tests."""

    def __init__(self):
        self.extra = None
        self.listeners_value = (("127.0.0.1", 8123, 9001, 101), ("172.20.0.2", 9080, 9002, 101))
        self.config_digest = "sha256:" + "1" * 64

    def host_boot(self, deadline):
        return "10000000-0000-4000-8000-000000000005"

    def namespace(self, pid, name, deadline):
        if pid == 1:
            return {"net": "1", "pid": "2", "mnt": "3"}[name]
        return {"net": "10", "pid": str(pid + 10), "mnt": str(pid + 20)}[name]

    def process(self, pid, deadline):
        if pid == self.extra:
            identifier = "9" * 64
        else:
            identifier = CH if pid == 101 else DISPATCHER
        return {
            "pid": pid,
            "start_ticks": 200 + pid,
            "NSpid": [pid, 1],
            "Uid": [101] * 4,
            "Gid": [101] * 4,
            "NoNewPrivs": 1,
            "Seccomp": 2,
            "CapInh": 0,
            "CapPrm": 0,
            "CapEff": 0,
            "CapBnd": 0,
            "CapAmb": 0,
            "cgroup": "0::/system.slice/docker-" + identifier + ".scope",
            "namespaces": {name: self.namespace(pid, name, deadline) for name in ("net", "pid", "mnt")},
        }

    def mounts(self, pid, deadline):
        return (
            {"destination": "/", "options": ("ro",)},
            {
                "destination": "/var/lib/clickhouse" if pid == 101 else "/run/dpone-secrets",
                "options": ("rw", "nosuid", "nodev", "noexec"),
                "filesystem": "ext4" if pid == 101 else "tmpfs",
            },
        )

    def path_identity(self, path, deadline):
        return {"device": 42, "inode": 1000, "mode": 448, "uid": 101, "gid": 101}

    def config_tree(self, pid, destination, deadline):
        return ({"path": ".", "sha256": self.config_digest},)

    def processes(self, deadline):
        return (101, 102) if self.extra is None else (101, 102, self.extra)

    def socket_inodes(self, pid, deadline):
        return (9001,) if pid == 101 else (9002,)

    def listeners(self, pid, deadline):
        return self.listeners_value


def capture(docker=None, linux=None):
    return capture_supervisor_facts(
        docker or OfflineDocker(), linux or OfflineLinux(), enrollment_body()["policy"], time.monotonic() + 3
    )


def test_complete_controlled_observations_are_stable_and_pin_real_inputs():
    first = capture()
    assert first == capture()
    assert first["linux"]["network_namespace_id"] == "10"
    assert first["linux"]["listeners"][0]["inode"] == 9001
    assert first["docker"]["containers"][CH]["image"] == "sha256:" + "e" * 64
    assert b"not-a-secret" not in canonical_json_bytes(first)


@pytest.mark.parametrize(
    "field,value",
    [
        ("Privileged", True),
        ("CapAdd", ["NET_ADMIN"]),
        ("NetworkMode", "host"),
        ("PidMode", "host"),
        ("PortBindings", {"8123/tcp": [{"HostPort": "8123"}]}),
        ("SecurityOpt", []),
        ("ReadonlyRootfs", False),
    ],
)
def test_container_escape_configuration_is_rejected(field, value):
    raw = docker_container(CH, "clickhouse")
    raw["HostConfig"][field] = value
    with pytest.raises(CompositionAdmissionError):
        normalize_container(raw, role="clickhouse", clickhouse_id=CH)


@pytest.mark.parametrize(
    "listener",
    [
        ("0.0.0.0", 8123, 9001, 101),
        ("127.0.0.1", 9000, 9001, 101),
        ("127.0.0.1", 8123, 9999, 101),
        ("127.0.0.1", 8123, 9001, 0),
    ],
)
def test_wildcard_additional_protocol_or_unattributed_listener_blocks(listener):
    linux = OfflineLinux()
    linux.listeners_value = (listener, linux.listeners_value[1])
    with pytest.raises(CompositionAdmissionError):
        capture(linux=linux)


def test_unenrolled_process_in_protected_namespace_blocks():
    linux = OfflineLinux()
    linux.extra = 103
    with pytest.raises(CompositionAdmissionError, match="unattributed_process"):
        capture(linux=linux)


def test_second_capture_config_drift_blocks_original(monkeypatch):
    import dpone.adapters.composition_clickhouse_supervisor as module

    facts = capture()
    value = enrolled(enrollment_body(facts))

    class Reader:
        def __init__(self, *args):
            pass

        def check(self):
            pass

        def read(self):
            return value

    monkeypatch.setattr(module, "SupervisorEnrollmentReader", Reader)
    supervisor = DockerClickHouseLocalSupervisor(
        enrollment_sha256=value.enrollment_sha256, docker=OfflineDocker(), linux=OfflineLinux()
    )
    observed = supervisor.observe(None, attempt=None, target=None)
    assert observed.evidence_document == supervisor.observe(None, attempt=None, target=None).evidence_document
    count = 0

    def drift(enrollment, deadline):
        nonlocal count
        count += 1
        result = deepcopy(facts)
        if count == 2:
            result["linux"]["containers"][CH]["configs"]["/etc/clickhouse-server"][0]["sha256"] = "sha256:" + "2" * 64
        return result

    monkeypatch.setattr(supervisor, "_capture", drift)
    with pytest.raises(CompositionAdmissionError, match="unverified"):
        supervisor.observe(None, attempt=None, target=None)


@pytest.mark.parametrize("failure", [None, "facts", "enrollment", "context", "transport", "deadline"])
def test_remote_observer_rechecks_original_and_context_around_two_fresh_captures(monkeypatch, failure):
    from pathlib import Path

    import dpone.adapters.composition_clickhouse_supervisor as module

    facts = capture()
    value = enrolled(enrollment_body(facts))
    events, deadlines = [], []
    clock = [100.0]
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])

    class Reader:
        def __init__(self, context, reference, attempt, target):
            assert (context, reference, attempt, target) == ("context", value.enrollment_sha256, "attempt", "target")

        def check(self):
            events.append("check")
            if failure == "context" and len(deadlines) == 1:
                raise RuntimeError("context changed")

        def read(self):
            events.append("read")
            if failure == "enrollment" and deadlines:
                from types import SimpleNamespace

                return SimpleNamespace(document=b"changed")
            return value

    class Client:
        def __init__(self, path, *, dispatcher_gid, timeout_seconds):
            assert (path, dispatcher_gid, timeout_seconds) == (Path("/run/probe.sock"), 100001, 10)

        def capture(self, reference, *, deadline):
            assert reference == value.enrollment_sha256
            events.append("capture")
            deadlines.append(deadline)
            if failure == "transport":
                raise RuntimeError("private transport detail")
            result = deepcopy(facts)
            if len(deadlines) == 2 and failure == "facts":
                result["linux"]["network_namespace_id"] = "changed"
            if failure == "deadline":
                clock[0] = deadline
            return result

    def forbidden(*args, **kwargs):
        pytest.fail("remote observer invoked host API")

    monkeypatch.setattr(module, "SupervisorEnrollmentReader", Reader)
    monkeypatch.setattr(module, "CaptureSupervisorFactsClient", Client)
    monkeypatch.setattr(module, "LocalDockerSupervisorClient", forbidden)
    monkeypatch.setattr(module, "LinuxSupervisorProbe", forbidden)
    monkeypatch.setattr(module, "capture_supervisor_facts", forbidden)
    supervisor = module.RemoteClickHouseLocalSupervisor(
        enrollment_sha256=value.enrollment_sha256, socket_path=Path("/run/probe.sock"), dispatcher_gid=100001
    )
    if failure is not None:
        with pytest.raises(
            CompositionAdmissionError,
            match="^DPONE_COMPOSITION_ADMISSION_UNAVAILABLE: clickhouse_supervisor_unverified$",
        ):
            supervisor.observe("context", attempt="attempt", target="target")
    else:
        observed = supervisor.observe("context", attempt="attempt", target="target")
        assert observed.enrollment_sha256 == value.enrollment_sha256
        assert events == ["read", "check", "capture", "check", "read", "check", "capture", "check", "read"]
        assert deadlines == [110.0, 110.0]
