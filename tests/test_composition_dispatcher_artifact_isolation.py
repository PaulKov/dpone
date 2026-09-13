"""Local tree tests with explicit ownership/mount observation doubles, not live proof."""

import os
from copy import deepcopy
from types import SimpleNamespace

import pytest

from dpone.adapters import composition_dispatcher_artifact_isolation as isolation
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_composition_clickhouse_supervisor_enrollment import CH, DISPATCHER, enrolled, enrollment_body


def mount(device, root="/", destination="/", identifier=1):
    return dict(
        id=identifier,
        parent=0 if identifier == 1 else 1,
        device=device,
        root=root,
        destination=destination,
        options=["rw"],
        propagation=[],
        filesystem="ext4",
        source="irrelevant-volume-name",
        super_options=["rw"],
    )


def repin(body, tables):
    for container, rows in tables.items():
        body["facts"]["linux"]["containers"][container]["mounts"]["mountinfo_sha256"] = isolation._digest(
            canonical_json_bytes(rows)
        )
    return enrolled(body)


@pytest.fixture
def configured(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    root.mkdir(mode=0o750)
    bootstrap = root / "startup"
    context = root / "contexts"
    bootstrap.mkdir(mode=0o750)
    context.mkdir(mode=0o750)
    (bootstrap / "bootstrap.json").write_bytes(b"no content read required")
    (bootstrap / "bootstrap.json").chmod(0o640)
    (context / "metadata").write_bytes(b"protected context")
    (context / "metadata").chmod(0o640)
    device = f"{os.major(root.stat().st_dev)}:{os.minor(root.stat().st_dev)}"
    body = enrollment_body()
    body["policy"]["config_roots"] = {CH: ["/etc/clickhouse-server"], DISPATCHER: ["/immutable"]}
    tables = {CH: [mount(device)], DISPATCHER: [mount(device)]}
    body["facts"]["linux"]["containers"] = {}
    for container, roots in body["policy"]["config_roots"].items():
        body["facts"]["linux"]["containers"][container] = {
            "mounts": {},
            "configs": {
                path: [
                    dict(
                        path=".",
                        device=root.stat().st_dev,
                        inode=987654321 + index,
                        uid=0,
                        gid=1200,
                        mode=0o750,
                        sha256=None,
                    )
                ]
                for index, path in enumerate(roots)
            },
        }
    policy = SimpleNamespace(bootstrap_file=bootstrap / "bootstrap.json", context_root=context, dispatcher_gid=1200)
    original = isolation._require_artifact

    def ownership_double(info, gid):
        fields = {key: getattr(info, key) for key in isolation._STABLE}
        fields.update(st_uid=0, st_gid=gid)
        return original(SimpleNamespace(**fields), gid)

    monkeypatch.setattr(isolation, "_require_artifact", ownership_double)
    monkeypatch.setattr(
        isolation, "open_protected", lambda path, **kwargs: os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    )
    monkeypatch.setattr(isolation.time, "monotonic", lambda: 1.0)
    return body, policy, tables


def check(configured):
    body, policy, tables = configured
    isolation.require_dispatcher_artifact_isolation(repin(body, tables), policy, tables, 20.0)


def test_same_filesystem_disjoint_trees_supported(configured):
    check(configured)


@pytest.mark.parametrize("kind", ["namespace", "backing", "inode"])
def test_alias_against_other_container_or_own_config(configured, kind):
    body, policy, tables = configured
    if kind == "namespace":
        previous = body["facts"]["linux"]["containers"][DISPATCHER]["configs"].pop("/immutable")
        body["policy"]["config_roots"][DISPATCHER] = [str(policy.context_root)]
        body["facts"]["linux"]["containers"][DISPATCHER]["configs"][str(policy.context_root)] = previous
    elif kind == "backing":
        tables[CH].append(mount(tables[CH][0]["device"], str(policy.context_root), "/etc/clickhouse-server", 2))
    else:
        item = body["facts"]["linux"]["containers"][CH]["configs"]["/etc/clickhouse-server"][0]
        item["inode"] = (policy.context_root / "metadata").stat().st_ino
    with pytest.raises(CompositionAdmissionError):
        check(configured)


@pytest.mark.parametrize("kind", ["hardlink", "symlink", "fifo"])
def test_unsafe_artifact_entries(configured, kind):
    _, policy, _ = configured
    source = policy.context_root / "metadata"
    target = policy.context_root / "unsafe"
    if kind == "hardlink":
        os.link(source, target)
    elif kind == "symlink":
        target.symlink_to(source)
    else:
        os.mkfifo(target)
    with pytest.raises(CompositionAdmissionError):
        check(configured)


@pytest.mark.parametrize("kind", ["stacked", "internal", "unknown", "bad_path", "bad_device"])
def test_unsupported_mapping(configured, kind):
    _, policy, tables = configured
    rows = tables[DISPATCHER]
    if kind == "stacked":
        rows.append(mount(rows[0]["device"], identifier=2))
    elif kind == "internal":
        rows.append(mount(rows[0]["device"], destination=str(policy.context_root / "nested"), identifier=2))
    elif kind == "unknown":
        rows[0]["filesystem"] = "overlay"
    elif kind == "bad_path":
        rows[0]["root"] = "/a/../b"
    else:
        rows[0]["device"] = "01:2"
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_mount_digest_cannot_be_replaced(configured):
    body, policy, tables = configured
    enrollment = repin(body, tables)
    altered = deepcopy(tables)
    altered[CH][0]["source"] = "changed"
    with pytest.raises(CompositionAdmissionError):
        isolation.require_dispatcher_artifact_isolation(enrollment, policy, altered, 20.0)


def test_inventory_never_reads_file_content(configured, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("artifact inventory read credential/content bytes")

    monkeypatch.setattr(isolation.os, "read", forbidden)
    monkeypatch.setattr(isolation.os, "pread", forbidden)
    check(configured)


def test_total_inventory_budget_rejects(configured, monkeypatch):
    monkeypatch.setattr(isolation, "MAX_ARTIFACT_ENTRIES", 2)
    with pytest.raises(CompositionAdmissionError):
        check(configured)


@pytest.mark.parametrize("deadline", [True, float("nan"), float("inf"), 0.0])
def test_invalid_or_expired_deadline(configured, deadline):
    body, policy, tables = configured
    with pytest.raises(CompositionAdmissionError):
        isolation.require_dispatcher_artifact_isolation(repin(body, tables), policy, tables, deadline)


def test_retained_config_root_device_must_match_mount(configured):
    body, _, _ = configured
    body["facts"]["linux"]["containers"][CH]["configs"]["/etc/clickhouse-server"][0]["device"] += 999
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_mutation_between_inventory_passes_rejected(configured, monkeypatch):
    _, policy, _ = configured
    original = isolation._inventory
    calls = []

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(1)
        if len(calls) == 2:
            (policy.context_root / "metadata").write_bytes(b"changed after first pass")
        return result

    monkeypatch.setattr(isolation, "_inventory", changed)
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_root_path_rotation_rejected(configured, monkeypatch):
    _, policy, _ = configured
    original = isolation._inventory
    calls = []

    def rotated(*args, **kwargs):
        result = original(*args, **kwargs)
        calls.append(1)
        if len(calls) == 4:
            policy.context_root.rename(policy.context_root.with_name("old-context"))
            policy.context_root.mkdir(mode=0o750)
        return result

    monkeypatch.setattr(isolation, "_inventory", rotated)
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_late_cleanup_rejects_and_closes_all_descriptors(configured, monkeypatch):
    original_open, original_close = os.open, os.close
    opened = []
    clock = [1.0]
    monkeypatch.setattr(isolation.time, "monotonic", lambda: clock[0])

    def tracked(*args, **kwargs):
        fd = original_open(*args, **kwargs)
        opened.append(fd)
        return fd

    def late_close(fd):
        original_close(fd)
        # Reopened roots are the last two opens, after all four inventories.
        if len(opened) == 8:
            clock[0] = 21.0

    monkeypatch.setattr(isolation.os, "open", tracked)
    monkeypatch.setattr(isolation.os, "close", late_close)
    with pytest.raises(CompositionAdmissionError):
        check(configured)
    for fd in opened:
        with pytest.raises(OSError):
            os.fstat(fd)


def test_depth_bound(configured, monkeypatch):
    _, policy, _ = configured
    (policy.context_root / "nested").mkdir(mode=0o750)
    monkeypatch.setattr(isolation, "MAX_ARTIFACT_DEPTH", 0)
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_actual_artifact_root_device_matches_mapping(configured):
    _, _, tables = configured
    tables[DISPATCHER][0]["device"] = "999:999"
    with pytest.raises(CompositionAdmissionError):
        check(configured)


def test_actual_metadata_policy_rejects_untrusted_owner():
    values = dict(st_mode=0o100640, st_uid=100, st_gid=1200, st_nlink=1)
    with pytest.raises(CompositionAdmissionError):
        isolation._require_artifact(SimpleNamespace(**values), 1200)
