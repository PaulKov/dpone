"""Explicit v2 custody boundary regressions; synthetic facts are not live proof."""

import time

import pytest

from dpone.adapters.composition_clickhouse_supervisor import capture_supervisor_facts
from dpone.adapters.composition_clickhouse_supervisor_docker import LocalDockerSupervisorClient, normalize_container
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe
from dpone.contracts.composition_identity import CompositionAdmissionError
from tests.test_composition_clickhouse_supervisor import OfflineDocker, OfflineLinux, docker_container
from tests.test_composition_clickhouse_supervisor_enrollment import CH, DISPATCHER, enrolled, enrollment_body


def custody():
    return {
        "profile": "dispatcher_owned_v1",
        "uid": 101,
        "gid": 101,
        "volume_name": "capture",
        "source": "/var/lib/docker/volumes/capture/_data",
        "destination": "/capture",
    }


def v2_body():
    body = enrollment_body()
    body["schema"] = "dpone.composition-clickhouse-supervisor-enrollment.v2"
    body["policy"]["capture_custody"] = custody()
    return body


def dispatcher():
    raw = docker_container(DISPATCHER, "dispatcher")
    raw["Config"]["User"] = "101:101"
    raw["Mounts"].append(
        {
            "Type": "volume",
            "Name": "capture",
            "Source": custody()["source"],
            "Destination": "/capture",
            "RW": True,
            "Propagation": "rprivate",
        }
    )
    return raw


def test_v1_bytes_and_default_mount_policy_remain_strict():
    original = enrolled(enrollment_body())
    assert enrolled(original.body).document == original.document
    with pytest.raises(CompositionAdmissionError):
        normalize_container(dispatcher(), role="dispatcher", clickhouse_id=CH)
    body = v2_body()
    assert enrolled(body).policy["capture_custody"] == custody()
    body["schema"] = "dpone.composition-clickhouse-supervisor-enrollment.v1"
    with pytest.raises(CompositionAdmissionError):
        enrolled(body)


@pytest.mark.parametrize(
    "field,value",
    [
        ("uid", True),
        ("gid", 0),
        ("profile", "auto"),
        ("volume_name", ""),
        ("destination", "/run"),
        ("source", "/a/../b"),
        ("extra", 1),
    ],
)
def test_v2_rejects_ambiguous_policy(field, value):
    body = v2_body()
    body["policy"]["capture_custody"][field] = value
    with pytest.raises(CompositionAdmissionError):
        enrolled(body)


@pytest.mark.parametrize("change", ["uid", "name", "source", "type", "third", "userns"])
def test_v2_dispatcher_requires_exact_identity_and_only_enrolled_volume(change):
    raw = dispatcher()
    assert len(normalize_container(raw, role="dispatcher", clickhouse_id=CH, capture_custody=custody())["mounts"]) == 2
    if change == "uid":
        raw["Config"]["User"] = "101"
    elif change == "userns":
        raw["HostConfig"]["UsernsMode"] = "private"
    elif change == "third":
        raw["Mounts"].append({**raw["Mounts"][-1], "Destination": "/extra"})
    else:
        raw["Mounts"][-1][{"name": "Name", "source": "Source", "type": "Type"}[change]] = {
            "name": "other",
            "source": "/other",
            "type": "bind",
        }[change]
    with pytest.raises(CompositionAdmissionError):
        normalize_container(raw, role="dispatcher", clickhouse_id=CH, capture_custody=custody())


@pytest.mark.parametrize("raw", [b"0 100000 65536\n", b"0 0 65536\n", b"0 0 4294967295\n1 1 1\n", b"", b"x"])
def test_identity_map_requires_full_identity_mapping(monkeypatch, raw):
    probe = LinuxSupervisorProbe()
    monkeypatch.setattr(probe, "_read", lambda *args: raw)
    with pytest.raises(CompositionAdmissionError):
        probe.identity_maps(102, time.monotonic() + 1)


def test_identity_maps_reject_separate_user_namespace(monkeypatch):
    probe = LinuxSupervisorProbe()
    monkeypatch.setattr(probe, "_read", lambda *args: b"         0          0 4294967295\n")
    monkeypatch.setattr(probe, "namespace", lambda pid, *args: str(pid))
    with pytest.raises(CompositionAdmissionError):
        probe.identity_maps(102, time.monotonic() + 1)
    monkeypatch.setattr(probe, "namespace", lambda *args: "shared")
    assert probe.identity_maps(102, time.monotonic() + 1) == {
        "uid_map": [[0, 0, 4294967295]],
        "gid_map": [[0, 0, 4294967295]],
    }


@pytest.mark.parametrize("security", [None, ["name=rootless"], ["name=userns"], [True]])
def test_v2_rejects_rootless_remapped_or_unknown_daemon_before_inventory(monkeypatch, security):
    docker = LocalDockerSupervisorClient()
    calls = []

    def get(path, deadline):
        calls.append(path)
        return {"SecurityOptions": security}

    monkeypatch.setattr(docker, "_get", get)
    with pytest.raises(CompositionAdmissionError, match="custody_daemon"):
        docker.snapshot(v2_body()["policy"], time.monotonic() + 1)
    assert calls == ["/v1.41/info"]


def test_directory_observer_requires_real_directory_and_rejects_symlink(monkeypatch, tmp_path):
    probe = LinuxSupervisorProbe()
    monkeypatch.setattr(probe, "_check", lambda _: None)
    folder = tmp_path / "capture"
    folder.mkdir(mode=0o700)
    actual = probe.directory_identity(str(folder.resolve()), 10)
    assert actual["inode"] == folder.stat().st_ino and actual["mode"] == 0o700
    alias = tmp_path / "alias"
    alias.symlink_to(folder)
    with pytest.raises(CompositionAdmissionError):
        probe.directory_identity(str(alias), 10)


@pytest.mark.parametrize("damage", [None, "mode", "identity", "alias", "gid"])
def test_capture_records_fresh_volume_identity_and_rejects_drift(damage):
    policy = v2_body()["policy"]
    docker = OfflineDocker()
    docker.facts["containers"][DISPATCHER] = normalize_container(
        dispatcher(), role="dispatcher", clickhouse_id=CH, capture_custody=custody()
    )

    class Linux(OfflineLinux):
        def mounts(self, pid, deadline):
            rows = super().mounts(pid, deadline)
            return rows + (({"destination": "/capture", "options": ("rw",)},) if pid == 102 else ())

        def path_identity(self, path, deadline):
            value = super().path_identity(path, deadline)
            if "capture" in path:
                value["inode"] = 2000 if damage != "alias" else 1000
            return value

        def directory_identity(self, path, deadline):
            value = self.path_identity(path, deadline)
            if damage == "mode":
                value["mode"] = 0o755
            if damage == "identity" and path.startswith("/proc"):
                value["inode"] += 1
            return value

        def identity_maps(self, pid, deadline):
            return {"uid_map": [[0, 0, 4294967295]], "gid_map": [[0, 0, 4294967295]]}

        def process(self, pid, deadline):
            value = super().process(pid, deadline)
            if damage == "gid" and pid == 102:
                value["Gid"] = [102] * 4
            return value

    if damage is not None:
        with pytest.raises(CompositionAdmissionError):
            capture_supervisor_facts(docker, Linux(), policy, time.monotonic() + 1)
    else:
        result = capture_supervisor_facts(docker, Linux(), policy, time.monotonic() + 1)
        assert result["linux"]["capture_custody"]["root_identity"]["inode"] == 2000


@pytest.mark.parametrize("field", ["uid", "gid"])
def test_custody_identity_range_matches_host_rpc_and_storage(field):
    body = v2_body()
    body["policy"]["capture_custody"][field] = 2**31 - 1
    assert enrolled(body).policy["capture_custody"][field] == 2**31 - 1
    body["policy"]["capture_custody"][field] = 2**31
    with pytest.raises(CompositionAdmissionError, match="custody_identity"):
        enrolled(body)
