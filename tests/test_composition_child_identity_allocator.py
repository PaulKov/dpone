"""Persistent child-identity invariants; no live supervisor certification.

These cases drive the real filesystem authority boundary: real advisory locks,
real spawned processes and threads, and real symlink, permission, truncation
and corruption attacks. Root ownership is simulated because developer machines
and CI runners are unprivileged.
"""

import json
from hashlib import sha256
from multiprocessing import get_context
from pathlib import Path
from threading import Barrier, Thread

import pytest

from dpone.adapters import composition_child_identity_allocator as allocation
from dpone.adapters.composition_child_identity_allocator import (
    CHILD_IDENTITY_SCHEMA,
    CompositionChildIdentity,
    CompositionChildIdentityAllocator,
    CompositionChildIdentityRecord,
)
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from tests.composition_mssql_gate_helpers import attempt
from tests.composition_supervisor_simulation import install_supervisor_simulation, supervisor_simulation

UID_START = 1_000_000_000
GID_START = 1_100_000_000
FOREIGN = "sha256:" + "e" * 64


def allocator_for(root, count=1024):
    return CompositionChildIdentityAllocator(root, uid_start=UID_START, gid_start=GID_START, count=count)


def candidate(kind, value, count):
    """Recompute the published deterministic first candidate for one axis."""

    return int.from_bytes(sha256(kind.encode() + b"\x00" + value.encode()).digest(), "big") % count


def hex_digest(value=None):
    return (value or attempt()).attempt_sha256.removeprefix("sha256:")


def occupy(root, name, record):
    path = root / "identities" / name
    path.parent.mkdir(mode=0o700, exist_ok=True)
    path.write_bytes(record.to_bytes())
    path.chmod(0o400)
    return path


@pytest.fixture
def root(tmp_path, monkeypatch):
    supervisor_simulation(monkeypatch)
    value = tmp_path / "composition"
    value.mkdir(mode=0o755)
    return value


def test_probing_never_reuses_a_foreign_identity(root):
    allocator = allocator_for(root, count=2)
    first = allocator.allocate(attempt(1))
    second = allocator.allocate(attempt(2))
    assert first != second
    assert first.uid != second.uid and first.gid != second.gid
    assert {first.uid, second.uid} == {UID_START, UID_START + 1}
    assert {first.gid, second.gid} == {GID_START, GID_START + 1}
    assert allocator.read(first.uid).attempt_sha256 == attempt(1).attempt_sha256


def test_same_attempt_replay_is_rejected(root):
    allocator = allocator_for(root)
    allocator.allocate(attempt())
    with pytest.raises(DbtCaptureError, match="child_identity_replay"):
        allocator.allocate(attempt())


def test_first_candidate_is_deterministic_from_the_full_attempt_digest(root, tmp_path):
    other = tmp_path / "second"
    other.mkdir(mode=0o755)
    identity = allocator_for(root, count=4096).allocate(attempt())
    assert identity == allocator_for(other, count=4096).allocate(attempt())
    assert identity == CompositionChildIdentity(
        UID_START + candidate("uid", attempt().attempt_sha256, 4096),
        GID_START + candidate("gid", attempt().attempt_sha256, 4096),
    )


def test_allocation_persists_canonical_read_only_tombstones(root):
    allocator = allocator_for(root)
    identity = allocator.allocate(attempt())
    document = {
        "schema": CHILD_IDENTITY_SCHEMA,
        "attempt_sha256": attempt().attempt_sha256,
        "uid": identity.uid,
        "gid": identity.gid,
    }
    names = (
        f"attempts/{hex_digest()}.json",
        f"identities/{identity.uid}-{identity.gid}.json",
        f"identities/uid-{identity.uid}.json",
        f"identities/gid-{identity.gid}.json",
    )
    for name in names:
        path = root / name
        assert path.stat().st_mode & 0o777 == 0o400, name
        assert json.loads(path.read_bytes()) == document, name
    assert allocator.read(identity.uid) == CompositionChildIdentityRecord(
        attempt().attempt_sha256, identity.uid, identity.gid
    )


def test_lost_attempt_tombstone_never_reuses_the_identity(root):
    allocator = allocator_for(root, count=8)
    identity = allocator.allocate(attempt())
    (root / "attempts" / f"{hex_digest()}.json").unlink()
    with pytest.raises(DbtCaptureError, match="child_identity_replay"):
        allocator.allocate(attempt())
    other = allocator.allocate(attempt(2))
    assert other.uid != identity.uid and other.gid != identity.gid


def test_exhausted_range_rejects_without_partial_records(root):
    allocator = allocator_for(root, count=1)
    allocator.allocate(attempt(1))
    with pytest.raises(DbtCaptureError, match="child_identity_exhausted"):
        allocator.allocate(attempt(2))
    assert [path.name for path in (root / "attempts").iterdir()] == [f"{hex_digest(attempt(1))}.json"]
    assert len(list((root / "identities").iterdir())) == 3


def test_probing_is_bounded_below_the_configured_range(root):
    count = 5000
    allocator = allocator_for(root, count=count)
    base = candidate("uid", attempt().attempt_sha256, count)
    for step in range(4096):
        uid = UID_START + (base + step) % count
        occupy(root, f"uid-{uid}.json", CompositionChildIdentityRecord(FOREIGN, uid, GID_START))
    with pytest.raises(DbtCaptureError, match="child_identity_exhausted"):
        allocator.allocate(attempt())


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"{",
        b"[]",
        b"null",
        b'{"schema":"dpone.composition-child-identity.v1","attempt_sha256":"sha256:'
        + b"e" * 64
        + b'","uid":1000000000,"gid":1100000000}\n\n',
        b'{"gid":1100000000,"uid":1000000000,"uid":1000000000,"attempt_sha256":"sha256:'
        + b"e" * 64
        + b'","schema":"dpone.composition-child-identity.v1"}\n',
        b'{"schema":"other","attempt_sha256":"sha256:' + b"e" * 64 + b'","uid":1000000000,"gid":1100000000}\n',
    ],
)
def test_corrupt_candidate_record_rejects_allocation(root, raw):
    allocator = allocator_for(root)
    uid = UID_START + candidate("uid", attempt().attempt_sha256, 1024)
    path = occupy(root, f"uid-{uid}.json", CompositionChildIdentityRecord(FOREIGN, uid, GID_START))
    path.chmod(0o600)
    path.write_bytes(raw)
    path.chmod(0o400)
    with pytest.raises(DbtCaptureError, match="child_identity_record"):
        allocator.allocate(attempt())


def test_candidate_record_contradicting_its_own_name_rejects(root):
    allocator = allocator_for(root)
    uid = UID_START + candidate("uid", attempt().attempt_sha256, 1024)
    occupy(root, f"uid-{uid}.json", CompositionChildIdentityRecord(FOREIGN, uid + 1, GID_START))
    with pytest.raises(DbtCaptureError, match="child_identity_conflict"):
        allocator.allocate(attempt())


def test_writable_candidate_record_rejects_allocation(root):
    allocator = allocator_for(root)
    uid = UID_START + candidate("uid", attempt().attempt_sha256, 1024)
    occupy(root, f"uid-{uid}.json", CompositionChildIdentityRecord(FOREIGN, uid, GID_START)).chmod(0o600)
    with pytest.raises(DbtCaptureError, match="child_identity_record"):
        allocator.allocate(attempt())


def test_symlinked_candidate_record_is_never_followed(root, tmp_path):
    allocator = allocator_for(root)
    uid = UID_START + candidate("uid", attempt().attempt_sha256, 1024)
    hostile = tmp_path / "hostile.json"
    hostile.write_bytes(CompositionChildIdentityRecord(FOREIGN, uid, GID_START).to_bytes())
    (root / "identities").mkdir(mode=0o700)
    (root / "identities" / f"uid-{uid}.json").symlink_to(hostile)
    with pytest.raises(DbtCaptureError, match="child_identity_record"):
        allocator.allocate(attempt())


def test_symlinked_identity_directory_is_never_followed(root, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir(mode=0o700)
    (root / "identities").symlink_to(elsewhere, target_is_directory=True)
    with pytest.raises(DbtCaptureError, match="child_identity_unavailable"):
        allocator_for(root).allocate(attempt())
    assert list(elsewhere.iterdir()) == []


def test_symlinked_root_is_never_followed(root, tmp_path):
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    with pytest.raises(DbtCaptureError, match="child_identity_unavailable"):
        allocator_for(link).allocate(attempt())
    assert list(root.iterdir()) == []


def test_group_writable_root_rejects_before_any_record(root):
    root.chmod(0o777)
    with pytest.raises(DbtCaptureError, match="allocation_ancestry"):
        allocator_for(root).allocate(attempt())
    assert list(root.iterdir()) == []


@pytest.mark.parametrize("value", ["relative/root", "/absolute/../escape"])
def test_untrusted_root_paths_reject_at_construction(value):
    with pytest.raises(DbtCaptureError, match="allocation_path"):
        allocator_for(Path(value))


@pytest.mark.parametrize(
    "uid_start,gid_start,count",
    [
        (0, GID_START, 1024),
        (-1, GID_START, 1024),
        (UID_START, 0, 1024),
        (UID_START, GID_START, 0),
        (UID_START, GID_START, -1),
        (True, GID_START, 1024),
        (UID_START, GID_START, 2**31),
        (2**31 - 1, GID_START, 1024),
        (1.0, GID_START, 1024),
    ],
)
def test_unusable_identity_range_rejects_at_construction(tmp_path, uid_start, gid_start, count):
    with pytest.raises(DbtCaptureError, match="child_identity_range"):
        CompositionChildIdentityAllocator(tmp_path, uid_start=uid_start, gid_start=gid_start, count=count)


@pytest.mark.parametrize("uid", [UID_START - 1, UID_START + 1024, 0, True, "1000000000"])
def test_reading_outside_the_reserved_range_rejects(root, uid):
    with pytest.raises(DbtCaptureError, match="child_identity_range"):
        allocator_for(root).read(uid)


def test_reading_an_unallocated_identity_rejects(root):
    with pytest.raises(DbtCaptureError, match="child_identity_unknown"):
        allocator_for(root).read(UID_START + 7)


def test_untrusted_attempt_digest_rejects_before_any_record(root):
    class Untrusted:
        attempt_sha256 = "sha256:../../etc/passwd"

        def __post_init__(self):
            return None

    with pytest.raises(DbtCaptureError, match="child_identity_digest"):
        allocator_for(root).allocate(Untrusted())
    assert list(root.iterdir()) == []


def test_projection_binds_the_approved_reserved_range(root):
    projection = CompositionSupervisorProjection("composition-supervisor", UID_START, GID_START, 1_000_000)
    allocator = CompositionChildIdentityAllocator.from_projection(root, projection)
    assert (allocator.uid_start, allocator.gid_start, allocator.count) == (UID_START, GID_START, 1_000_000)
    identity = allocator.allocate(attempt())
    assert UID_START <= identity.uid < UID_START + 1_000_000
    assert GID_START <= identity.gid < GID_START + 1_000_000


def test_hostile_attempt_tombstone_owned_by_another_attempt_rejects(root):
    allocator = allocator_for(root)
    allocator.allocate(attempt(2))
    hostile = root / "attempts" / f"{hex_digest()}.json"
    hostile.write_bytes(CompositionChildIdentityRecord(FOREIGN, UID_START, GID_START).to_bytes())
    hostile.chmod(0o400)
    with pytest.raises(DbtCaptureError, match="child_identity_conflict"):
        allocator.allocate(attempt())


def test_allocation_requires_the_root_supervisor(root, monkeypatch):
    def reject():
        raise DbtCaptureError("capture_supervisor_boundary")

    monkeypatch.setattr(allocation, "require_supervisor", reject)
    with pytest.raises(DbtCaptureError, match="supervisor_boundary"):
        allocator_for(root).allocate(attempt())
    assert list(root.iterdir()) == []


def test_concurrent_threads_never_share_a_reserved_slot(root):
    allocator = allocator_for(root, count=32)
    barrier = Barrier(4)
    results: list[object] = []

    def work(numbers):
        barrier.wait(60)
        for number in numbers:
            try:
                results.append(allocator.allocate(attempt(number)))
            except DbtCaptureError as error:  # pragma: no cover - reported by assertions
                results.append(str(error))

    workers = [Thread(target=work, args=(range(start, start + 5),)) for start in (1, 6, 11, 16)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(120)
    identities = [value for value in results if isinstance(value, CompositionChildIdentity)]
    assert identities == results, results
    assert len({value.uid for value in identities}) == 20
    assert len({value.gid for value in identities}) == 20


def allocate_in_worker(directory, count, numbers, barrier, results):
    """Allocate one batch of attempts inside a real spawned supervisor process."""

    install_supervisor_simulation()
    allocator = allocator_for(Path(directory), count)
    barrier.wait(60)
    for number in numbers:
        try:
            identity = allocator.allocate(attempt(number))
            results.put({"uid": identity.uid, "gid": identity.gid})
        except DbtCaptureError as error:
            results.put({"error": str(error)})


def run_workers(root, count, batches):
    context = get_context("spawn")
    barrier = context.Barrier(len(batches))
    results = context.Queue()
    workers = [
        context.Process(target=allocate_in_worker, args=(str(root), count, batch, barrier, results))
        for batch in batches
    ]
    for worker in workers:
        worker.start()
    values = [results.get(timeout=120) for _ in range(sum(len(batch) for batch in batches))]
    for worker in workers:
        worker.join(120)
        assert worker.exitcode == 0
    return values


def test_concurrent_processes_allocate_distinct_identities(root):
    values = run_workers(root, 32, [range(1, 11), range(11, 21), range(21, 31)])
    assert [value for value in values if "error" in value] == []
    assert len({value["uid"] for value in values}) == 30
    assert len({value["gid"] for value in values}) == 30
    assert len(list((root / "attempts").iterdir())) == 30


def test_concurrent_processes_never_duplicate_one_attempt(root):
    values = run_workers(root, 32, [range(1, 6)] * 3)
    allocated = [value for value in values if "error" not in value]
    assert len(allocated) == 5, values
    assert [value["error"] for value in values if "error" in value] == ["capture_child_identity_replay"] * 10
    assert len({value["uid"] for value in allocated}) == 5
    assert len(list((root / "attempts").iterdir())) == 5
