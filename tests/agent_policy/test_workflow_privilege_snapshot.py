from __future__ import annotations

import errno
import os
import shutil
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from tests.agent_policy.workflow_privilege_fixtures import (
    POLICY_PATH,
    WORKFLOW_DIRECTORY,
    finding_codes,
    sha256,
    track_open_descriptors,
    write_repository,
)
from tests.agent_policy.workflow_privilege_fixtures import (
    assert_descriptors_closed as _assert_closed,
)
from tools.agent_policy import workflow_privilege_snapshot as snapshot_module
from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotReference, SnapshotResult
from tools.agent_policy.workflow_privilege_snapshot import SnapshotReader

_MINIMAL_WORKFLOW = b"name: Current\non: {push: null}\npermissions: {}\njobs: {}\n"
_FILE_MUTATIONS = ("replace", "mode", "mtime", "size", "content", "hardlink")
_FIXED_INPUT_MUTATIONS = (("workflow", "add"), ("workflow", "remove")) + tuple(
    (target, mutation) for target in ("policy", "workflow") for mutation in _FILE_MUTATIONS
)


def _minimal_repository(path: Path) -> Path:
    return write_repository(path, {"current.yml": _MINIMAL_WORKFLOW}, policy=b"schema_version: 1\n")


def _assert_concurrent(result: SnapshotResult) -> None:
    assert result.snapshot.complete is result.reference.complete is False
    assert result.snapshot.manifest_sha256 is result.reference.manifest_sha256 is None
    assert finding_codes(result.snapshot) == ("PRIVILEGE_CONCURRENT_MUTATION",)


def test_snapshot_acquires_fixed_inputs_in_canonical_manifest_order(tmp_path: Path) -> None:
    from tools.agent_policy.workflow_privilege_contracts import canonical_sha256

    policy = b"schema_version: 1\n"
    root = write_repository(
        tmp_path / "repo",
        {
            "z-last.yml": b"name: Z\non: {push: null}\npermissions: {}\njobs: {}\n",
            "a-first.yaml": b"name: A\non: {push: null}\npermissions: {}\njobs: {}\n",
        },
        policy=policy,
    )
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    finalized = lease.finalize(policy_schema_version=1)
    snapshot = finalized.snapshot
    assert snapshot.complete is True
    assert snapshot.policy is not None
    assert snapshot.findings == ()
    assert snapshot.overflow_dimensions == ()
    assert snapshot.policy.path == POLICY_PATH.as_posix()
    assert snapshot.policy.content == policy
    assert [entry.path for entry in snapshot.workflows] == [
        ".github/workflows/a-first.yaml",
        ".github/workflows/z-last.yml",
    ]
    entries = (snapshot.policy, *snapshot.workflows)
    assert all(entry.mode == "0644" for entry in entries)
    expected_manifest = [[entry.path, entry.mode, entry.byte_length, entry.sha256] for entry in entries]
    assert snapshot.manifest_sha256 == canonical_sha256(expected_manifest)
    assert finalized.reference == SnapshotReference(
        policy_sha256=sha256(policy),
        policy_schema_version=1,
        manifest_sha256=snapshot.manifest_sha256,
        complete=True,
    )


def test_snapshot_rejects_repository_and_workflow_symlinks(tmp_path: Path) -> None:
    actual = write_repository(
        tmp_path / "actual",
        {"safe.yml": b"name: Safe\non: {push: null}\npermissions: {}\njobs: {}\n"},
        policy=b"schema_version: 1\n",
    )
    linked_root = tmp_path / "linked-root"
    linked_root.symlink_to(actual, target_is_directory=True)
    root_result = SnapshotReader(limits=V1_LIMITS).acquire(linked_root).finalize(policy_schema_version=None).snapshot
    assert root_result.complete is False
    assert root_result.manifest_sha256 is None
    assert root_result.findings
    assert all(finding.status == "UNVERIFIED" for finding in root_result.findings)
    assert all(finding.subject == "." for finding in root_result.findings)
    external = tmp_path / "external.yml"
    external.write_text("name: External\n", encoding="utf-8")
    (actual / WORKFLOW_DIRECTORY / "linked.yml").symlink_to(external)
    workflow_result = SnapshotReader(limits=V1_LIMITS).acquire(actual).finalize(policy_schema_version=None).snapshot
    assert workflow_result.complete is False
    assert workflow_result.manifest_sha256 is None
    assert all(entry.path != ".github/workflows/linked.yml" for entry in workflow_result.workflows)
    assert workflow_result.findings
    assert finding_codes(workflow_result) == ("PRIVILEGE_INVALID_WORKFLOW",)


@pytest.mark.parametrize(("target_name", "mutation"), _FIXED_INPUT_MUTATIONS)
def test_snapshot_final_revalidation_detects_every_fixed_input_mutation(
    tmp_path: Path,
    target_name: str,
    mutation: str,
) -> None:
    root = _minimal_repository(tmp_path / f"{target_name}-{mutation}")
    target = root / POLICY_PATH if target_name == "policy" else root / WORKFLOW_DIRECTORY / "current.yml"
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    initial = target.stat()
    if mutation == "add":
        (root / WORKFLOW_DIRECTORY / "added.yml").write_text("name: Added\n", encoding="utf-8")
    elif mutation == "remove":
        target.unlink()
    elif mutation == "replace":
        replacement = target.with_name(target.name + ".replacement")
        replacement.write_bytes(target.read_bytes())
        replacement.replace(target)
    elif mutation == "mode":
        target.chmod(0o600)
    elif mutation == "mtime":
        os.utime(target, ns=(initial.st_atime_ns, initial.st_mtime_ns + 1_000_000_000))
    elif mutation == "size":
        target.write_bytes(target.read_bytes() + b"#")
    elif mutation == "content":
        content = target.read_bytes()
        target.write_bytes(b"#" + content[1:])
        os.utime(target, ns=(initial.st_atime_ns, initial.st_mtime_ns))
    else:
        os.link(target, root / f"outside-{target_name}.yml")
    finalized = lease.finalize(policy_schema_version=1)
    _assert_concurrent(finalized)


@pytest.mark.parametrize(
    "component",
    ["root", ".agents", ".agents/policy", ".github", ".github/workflows"],
)
def test_snapshot_final_revalidation_detects_directory_path_rebinding(
    tmp_path: Path,
    component: str,
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    target = root if component == "root" else root / component
    displaced = target.with_name(target.name + "-displaced")
    target.rename(displaced)
    shutil.copytree(displaced, target)
    finalized = lease.finalize(policy_schema_version=1)
    _assert_concurrent(finalized)


@pytest.mark.parametrize("target_name", ["policy", "workflow"])
def test_snapshot_rejects_hardlinked_fixed_inputs(tmp_path: Path, target_name: str) -> None:
    root = _minimal_repository(tmp_path / target_name)
    target = root / POLICY_PATH if target_name == "policy" else root / WORKFLOW_DIRECTORY / "current.yml"
    os.link(target, root / f"outside-{target_name}.yml")
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None)
    assert finalized.snapshot.complete is False
    expected = "PRIVILEGE_INVALID_POLICY" if target_name == "policy" else "PRIVILEGE_INVALID_WORKFLOW"
    assert finding_codes(finalized.snapshot) == (expected,)


@pytest.mark.parametrize("target_name", ("policy", "workflow"))
@pytest.mark.parametrize(
    "error",
    (PermissionError(errno.EACCES, "injected unreadable input"), OSError(errno.EIO, "injected I/O failure")),
)
def test_snapshot_classifies_unreadable_fixed_input_as_invalid(
    target_name: str,
    error: OSError,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _minimal_repository(tmp_path / "unreadable-policy")
    target = root / POLICY_PATH if target_name == "policy" else root / WORKFLOW_DIRECTORY / "current.yml"
    real_open = snapshot_module.os.open

    def deny_policy(path: str, flags: int, mode: int = 0o777, *, dir_fd: int | None = None) -> int:
        if path == target.name:
            raise error
        return real_open(path, flags, mode, dir_fd=dir_fd)

    monkeypatch.setattr(snapshot_module.os, "open", deny_policy)
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None)
    expected = "PRIVILEGE_INVALID_POLICY" if target_name == "policy" else "PRIVILEGE_INVALID_WORKFLOW"
    assert (finalized.snapshot.policy is None) is (target_name == "policy")
    assert (finalized.reference.policy_sha256 is None) is (target_name == "policy")
    assert finding_codes(finalized.snapshot) == (expected,)


@pytest.mark.parametrize("target", ("policy", "workflow"))
@pytest.mark.parametrize("change", ("append", "truncate"))
def test_initial_read_mutation_is_concurrent(
    target: str, change: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / f"{target}-{change}")
    target_path = root / POLICY_PATH if target == "policy" else root / WORKFLOW_DIRECTORY / "current.yml"
    real_read = snapshot_module._read_fd
    target_identity = (target_path.stat().st_dev, target_path.stat().st_ino)
    injections = 0

    def mutate_initial_read(descriptor: int, expected_size: int) -> bytes:
        nonlocal injections
        content = real_read(descriptor, expected_size)
        metadata = os.fstat(descriptor)
        is_target = (metadata.st_dev, metadata.st_ino) == target_identity
        inject_now = is_target and injections == 0
        if inject_now:
            injections += 1
            changed = target_path.read_bytes() + b"#" if change == "append" else target_path.read_bytes()[:-1]
            target_path.write_bytes(changed)
        return content[:-1] if inject_now and change == "truncate" else content

    monkeypatch.setattr(snapshot_module, "_read_fd", mutate_initial_read)
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None)
    _assert_concurrent(finalized)
    assert injections == 1


def test_snapshot_policy_byte_overflow_closes_invalid_and_resource_evidence(tmp_path: Path) -> None:
    root = _minimal_repository(tmp_path / "policy-overflow")
    policy = root / POLICY_PATH
    limits = replace(V1_LIMITS, policy_bytes=policy.stat().st_size - 1)
    finalized = SnapshotReader(limits=limits).acquire(root).finalize(policy_schema_version=None)
    assert finalized.snapshot.complete is False
    assert finalized.snapshot.manifest_sha256 is None
    assert finalized.snapshot.overflow_dimensions == ("policy_bytes",)
    assert finding_codes(finalized.snapshot) == ("PRIVILEGE_INVALID_POLICY", "PRIVILEGE_RESOURCE_LIMIT")
    assert finalized.snapshot.policy is None
    assert finalized.reference == SnapshotReference(None, None, None, False)


def test_snapshot_rejects_casefold_workflow_name_collisions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = write_repository(
        tmp_path / "repo",
        {
            "Build.yml": b"name: Build\non: {push: null}\npermissions: {}\njobs: {}\n",
            "build.yml": b"name: build\non: {push: null}\npermissions: {}\njobs: {}\n",
        },
        policy=b"schema_version: 1\n",
    )
    entries = [SimpleNamespace(name="Build.yml"), SimpleNamespace(name="build.yml")]
    monkeypatch.setattr(snapshot_module.os, "scandir", lambda _fd: nullcontext(entries))
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None)
    assert finalized.snapshot.complete is False
    assert finding_codes(finalized.snapshot) == ("PRIVILEGE_INVALID_WORKFLOW",)
    assert {item.subject for item in finalized.snapshot.findings} == {WORKFLOW_DIRECTORY.as_posix()}


@pytest.mark.parametrize("unsafe_kind", ["directory", "symlink", "fifo", "unsafe-name"])
def test_snapshot_rejects_unsafe_workflow_directory_entries(tmp_path: Path, unsafe_kind: str) -> None:
    root = write_repository(
        tmp_path / unsafe_kind,
        {"safe.yml": b"name: Safe\non: {push: null}\npermissions: {}\njobs: {}\n"},
        policy=b"schema_version: 1\n",
    )
    unsafe = root / WORKFLOW_DIRECTORY / ("unsafe\n.yml" if unsafe_kind == "unsafe-name" else "unsafe")
    if unsafe_kind == "directory":
        unsafe.mkdir()
    elif unsafe_kind == "symlink":
        unsafe.symlink_to(root / WORKFLOW_DIRECTORY / "safe.yml")
    elif unsafe_kind == "fifo":
        os.mkfifo(unsafe)
    else:
        unsafe.write_bytes(_MINIMAL_WORKFLOW)
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None)
    assert finalized.snapshot.complete is False
    assert finding_codes(finalized.snapshot) == ("PRIVILEGE_INVALID_WORKFLOW",)
    assert {item.subject for item in finalized.snapshot.findings} == {WORKFLOW_DIRECTORY.as_posix()}


@pytest.mark.parametrize("mutation", ["rename", "hardlink", "inventory"])
def test_snapshot_rejects_aba_mutations(tmp_path: Path, mutation: str) -> None:
    root = _minimal_repository(tmp_path / mutation)
    workflow = root / WORKFLOW_DIRECTORY / "current.yml"
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    if mutation == "rename":
        displaced = workflow.with_suffix(".displaced")
        workflow.rename(displaced)
        displaced.rename(workflow)
    elif mutation == "hardlink":
        linked = root / "transient-link.yml"
        os.link(workflow, linked)
        linked.unlink()
    else:
        added = root / WORKFLOW_DIRECTORY / "transient.yml"
        added.write_bytes(workflow.read_bytes())
        added.unlink()
    finalized = lease.finalize(policy_schema_version=1)
    _assert_concurrent(finalized)


@pytest.mark.parametrize("timing", ["before", "during-final-inventory"])
def test_snapshot_revalidates_supplied_parent_path(
    tmp_path: Path,
    timing: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _minimal_repository(tmp_path / "container" / "repo")
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    container = root.parent

    def replace_parent() -> None:
        displaced = container.with_name(container.name + "-displaced")
        container.rename(displaced)
        shutil.copytree(displaced, container)

    if timing == "before":
        replace_parent()
    else:
        real_names = snapshot_module._workflow_names
        calls = 0

        def mutate_before_inventory(directory_fd: int, maximum: int) -> tuple[str, ...]:
            nonlocal calls
            calls += 1
            if calls == 1:
                replace_parent()
            return real_names(directory_fd, maximum)

        monkeypatch.setattr(snapshot_module, "_workflow_names", mutate_before_inventory)
    finalized = lease.finalize(policy_schema_version=1)
    _assert_concurrent(finalized)


def test_snapshot_terminal_tree_revalidation_detects_workflow_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    workflow = root / WORKFLOW_DIRECTORY / "current.yml"
    changed = _MINIMAL_WORKFLOW.replace(b"Current", b"Changed")
    real_reopen = snapshot_module._reopen_directories
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    reopens = 0

    def replace_after_terminal_reopen(state: Any) -> dict[int, int]:
        nonlocal reopens
        current = real_reopen(state)
        reopens += 1
        if reopens == 2:
            replacement = workflow.with_suffix(".replacement")
            replacement.write_bytes(changed)
            replacement.replace(workflow)
        return current

    monkeypatch.setattr(snapshot_module, "_reopen_directories", replace_after_terminal_reopen)
    finalized = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=1)
    assert (reopens, workflow.read_bytes()) == (2, changed)
    _assert_closed(opened)
    _assert_concurrent(finalized)


@pytest.mark.parametrize("failure", ("invalid-workflow", "policy-overflow"))
def test_snapshot_closes_every_descriptor_after_controlled_acquisition_failure(
    failure: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    limits = V1_LIMITS
    if failure == "invalid-workflow":
        (root / WORKFLOW_DIRECTORY / "unsafe").mkdir()
    else:
        limits = replace(V1_LIMITS, policy_bytes=(root / POLICY_PATH).stat().st_size - 1)
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    finalized = SnapshotReader(limits=limits).acquire(root).finalize(policy_schema_version=None)
    assert finalized.snapshot.complete is False
    _assert_closed(opened)


def test_snapshot_closes_descriptors_before_propagating_internal_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    failure = KeyboardInterrupt("injected manifest failure")

    def fail_manifest(_value: object) -> str:
        raise failure

    monkeypatch.setattr(snapshot_module, "canonical_sha256", fail_manifest)
    with pytest.raises(KeyboardInterrupt) as caught:
        SnapshotReader(limits=V1_LIMITS).acquire(root)
    assert caught.value is failure
    _assert_closed(opened)


def test_notification_evidence_rejects_restored_state_even_when_metadata_matches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    workflow = root / WORKFLOW_DIRECTORY / "current.yml"
    original = workflow.read_bytes()
    workflow.write_bytes(b"temporary")
    workflow.write_bytes(original)
    monkeypatch.setattr(snapshot_module, "_revalidation_phase", lambda _state: True)
    monkeypatch.setattr(snapshot_module, "_tree_revalidates", lambda _state: True)
    _assert_concurrent(lease.finalize(policy_schema_version=1))


def test_finalization_interruption_closes_observer_and_all_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    assert lease._state is not None
    observer = lease._state.observer

    def interrupt(_state: object) -> bool:
        raise KeyboardInterrupt("injected finalization interruption")

    monkeypatch.setattr(snapshot_module, "_revalidation_phase", interrupt)
    with pytest.raises(KeyboardInterrupt):
        lease.finalize(policy_schema_version=1)
    _assert_closed(opened)
    assert not observer.unchanged()


def test_observer_close_failure_still_releases_snapshot_descriptors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _minimal_repository(tmp_path / "repo")
    opened = track_open_descriptors(monkeypatch, snapshot_module)
    lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    assert lease._state is not None
    observer = lease._state.observer
    real_close = observer.close

    def fail_close() -> None:
        real_close()
        raise OSError("injected close failure")

    monkeypatch.setattr(observer, "close", fail_close)
    with pytest.raises(OSError, match="injected close failure"):
        lease.close()
    _assert_closed(opened)
    lease.close()
