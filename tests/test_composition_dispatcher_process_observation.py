"""Local process proof with explicit /proc doubles; not Linux deployment evidence."""

from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_dispatcher_process_observation as observation
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_supervisor_enrollment import DISPATCHER, enrolled, enrollment_body


@pytest.fixture
def process_case(monkeypatch):
    policy = SimpleNamespace(
        dispatcher_uid=100001,
        dispatcher_gid=100002,
        sha256="sha256:" + sha256(b"canonical-policy").hexdigest(),
        listen=SimpleNamespace(address="172.20.0.2", port=9080),
    )
    identity = {"device": 8, "inode": 42, "mode": 0o440, "uid": 0, "gid": 100002}
    init = {
        "pid": 246,
        "parent_pid": 240,
        "process_group": 246,
        "session": 246,
        "start_ticks": 999,
        "Uid": [100001] * 4,
        "Gid": [100002] * 4,
        "NSpid": [246, 1],
        "namespaces": {"pid": "11", "net": "12", "mnt": "13"},
        "executable": {"device": 8, "inode": 17},
        "cgroup": "0::/host/container",
        "CapInh": 0,
        "CapPrm": 0,
        "CapEff": 0,
        "CapBnd": 0,
        "CapAmb": 0,
        "NoNewPrivs": 1,
        "Seccomp": 2,
    }
    local = deepcopy(init)
    local.update(pid=1, parent_pid=0, process_group=1, session=1, NSpid=[1], cgroup="0::/")
    trees = {
        "/app": [
            {"path": ".", **{**identity, "inode": 40, "mode": 0o550}, "sha256": None},
            {"path": "policy.json", **identity, "sha256": policy.sha256},
        ],
        "/code": [{"path": ".", **{**identity, "inode": 50, "mode": 0o550}, "sha256": None}],
    }
    mounts = [{"fixed": "parsed-table"}]
    listener = {
        "address": policy.listen.address,
        "port": policy.listen.port,
        "inode": 9002,
        "uid": policy.dispatcher_uid,
        "container_id": DISPATCHER,
    }
    body = enrollment_body(
        {
            "docker": {"containers": {DISPATCHER: {"pid": 246}}},
            "linux": {
                "containers": {
                    DISPATCHER: {
                        "init": init,
                        "configs": deepcopy(trees),
                        "mounts": {"mountinfo_sha256": "sha256:" + sha256(canonical_json_bytes(mounts)).hexdigest()},
                    }
                },
                "listeners": [listener],
            },
        }
    )
    body["policy"]["config_roots"][DISPATCHER] = ["/app", "/code"]
    calls = []
    state = SimpleNamespace(
        local=local,
        trees=trees,
        mounts=mounts,
        inodes=(9002,),
        identity=identity,
        listeners=((policy.listen.address, policy.listen.port, 9002, policy.dispatcher_uid),),
        change_after=None,
        after_tree=None,
        clock=100.0,
    )

    class Probe:
        def process(self, pid, deadline):
            calls.append(("process", pid, deadline))
            value = deepcopy(state.local)
            if state.change_after and sum(call[0] == "process" for call in calls) > 1:
                state.change_after(value)
            return value

        def mounts(self, pid, deadline):
            calls.append(("mounts", pid, deadline))
            return state.mounts

        def listeners(self, pid, deadline):
            calls.append(("listeners", pid, deadline))
            return state.listeners

        def socket_inodes(self, pid, deadline):
            calls.append(("fds", pid, deadline))
            return state.inodes

        def config_tree(self, pid, root, deadline):
            calls.append(("tree", root, deadline))
            if state.after_tree is not None:
                state.after_tree(root)
            return state.trees[root]

        def path_identity(self, path, deadline):
            calls.append(("path", path, deadline))
            return state.identity

    monkeypatch.setattr(observation, "LinuxSupervisorProbe", Probe)
    monkeypatch.setattr(observation.sys, "platform", "linux")
    monkeypatch.setattr(observation.os, "getpid", lambda: 1)
    monkeypatch.setattr(observation.os, "getresuid", lambda: (100001,) * 3, raising=False)
    monkeypatch.setattr(observation.os, "getresgid", lambda: (100002,) * 3, raising=False)
    monkeypatch.setattr(observation.time, "monotonic", lambda: state.clock)
    return enrolled(body), policy, Path("/app/policy.json"), state, calls


def invoke(case):
    enrollment, policy, path, _, _ = case
    return observation.require_dispatcher_process(enrollment, policy, path, 110.0)


def test_same_namespace_process_and_exact_owned_listener_policy_and_mounts(process_case):
    assert invoke(process_case) is None
    calls = process_case[-1]
    assert {entry[1] for entry in calls if entry[0] == "tree"} == {"/app", "/code"}
    assert all(entry[-1] == 110.0 for entry in calls)
    assert sum(entry[0] == "process" for entry in calls) >= 2


@pytest.mark.parametrize(
    "field,value",
    [
        ("pid", 2),
        ("start_ticks", 1000),
        ("NSpid", [2]),
        ("NSpid", [999, 1]),
        ("Uid", [100003] * 4),
        ("Gid", [100003] * 4),
        ("namespaces", {"pid": "other", "net": "12", "mnt": "13"}),
        ("executable", {"device": 8, "inode": 99}),
    ],
)
def test_another_process_or_identity_never_matches_enrollment(process_case, field, value):
    process_case[3].local[field] = value
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


@pytest.mark.parametrize("mutation", ["fd", "address", "port", "uid", "inode", "duplicate"])
def test_listener_requires_actual_exact_socket_owned_by_self(process_case, mutation):
    state = process_case[3]
    if mutation == "fd":
        state.inodes = (9003,)
    elif mutation == "duplicate":
        state.listeners *= 2
    else:
        row = list(state.listeners[0])
        index = {"address": 0, "port": 1, "inode": 2, "uid": 3}[mutation]
        row[index] = "127.0.0.1" if mutation == "address" else 999
        state.listeners = (tuple(row),)
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


@pytest.mark.parametrize("mutation", ["policy_bytes", "policy_inode", "policy_owner", "other_root", "mounts"])
def test_all_immutable_observations_and_policy_identity_are_required(process_case, mutation):
    state = process_case[3]
    if mutation == "policy_bytes":
        state.trees["/app"][1]["sha256"] = "sha256:" + "f" * 64
    elif mutation == "policy_inode":
        state.identity = {**state.identity, "inode": 999}
    elif mutation == "policy_owner":
        state.identity = {**state.identity, "uid": 999}
    elif mutation == "other_root":
        state.trees["/code"][0]["inode"] = 999
    else:
        state.mounts = [{"changed": True}]
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


def test_identity_drift_after_reads_rejects(process_case):
    process_case[3].change_after = lambda value: value.update(start_ticks=1000)
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), 100.0])
def test_invalid_or_expired_deadline_performs_no_observation(process_case, deadline):
    with pytest.raises(CompositionAdmissionError):
        observation.require_dispatcher_process(*process_case[:3], deadline)
    assert process_case[-1] == []


def test_policy_outside_enrolled_tree_rejects(process_case):
    with pytest.raises(CompositionAdmissionError):
        observation.require_dispatcher_process(*process_case[:2], Path("/outside/policy.json"), 110.0)


@pytest.mark.parametrize("method", ["getresuid", "getresgid"])
def test_actual_kernel_resident_identity_must_match_policy(process_case, monkeypatch, method):
    monkeypatch.setattr(observation.os, method, lambda: (999, 999, 999))
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


def test_non_linux_never_claims_observation(process_case, monkeypatch):
    monkeypatch.setattr(observation.sys, "platform", "darwin")
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)
    assert process_case[-1] == []


def test_unavailable_policy_observation_has_fixed_safe_error(process_case):
    del process_case[3].trees["/code"]
    with pytest.raises(CompositionAdmissionError) as error:
        invoke(process_case)
    assert "/code" not in str(error.value)


def test_listener_loss_during_final_policy_scan_rejects(process_case):
    state, calls = process_case[3:]

    def lose_listener(root):
        if sum(call[0] == "tree" for call in calls) == 4:
            state.inodes = ()

    state.after_tree = lose_listener
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)


def test_deadline_expiring_during_policy_scan_never_returns_proof(process_case):
    state = process_case[3]
    state.after_tree = lambda root: setattr(state, "clock", 111.0)
    with pytest.raises(CompositionAdmissionError):
        invoke(process_case)
