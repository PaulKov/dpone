"""Historical evidence is closed by version, never by the installed package."""

import hashlib
import json

import pytest
from tools.dbt_self_service.starter_resource_inventory import LEGACY_RESOURCE_PATHS, RESOURCE_PATHS
from tools.dbt_self_service.starter_resource_journal import ResourceJournal
from tools.dbt_self_service.starter_resource_journal_schema import manifest_log_limit, read_manifest_events
from tools.dbt_self_service.starter_resource_recovery import recovery_report

OPERATION = "12345678-1234-1234-1234-123456789abc"
NEW_RESOURCE = "src/dpone/_assets/dbt_dpone/control/sqlserver/physical-v1/catalog-v2.sql"


def test_legacy_inventory_matches_pre_v2_ordered_snapshot():
    # Captured from a21dfaa5's sixteen-entry writer inventory, not current assets.
    encoded = json.dumps(LEGACY_RESOURCE_PATHS, separators=(",", ":")).encode()
    assert len(LEGACY_RESOURCE_PATHS) == 16
    assert hashlib.sha256(encoded).hexdigest() == "1e4dcd26fef5ad01b834d80e6d59afddd02bc1fae7e09b540861794eacdfbc00"


def retained_journal(root, version, paths, events):
    directory = root / ".dpone-starter-resource-transactions" / OPERATION
    directory.mkdir(parents=True)
    identity = root.stat()
    manifest = {
        "schema": f"dpone.starter-resource-transaction.v{version}",
        "operation": OPERATION,
        "revision": "a" * 40,
        "root": {"device": identity.st_dev, "inode": identity.st_ino},
        "entries": [
            {"path": path, "old": None, "desired_sha256": "sha256:" + hashlib.sha256(path.encode()).hexdigest()}
            for path in paths
        ],
    }
    (directory / "manifest.json").write_bytes(json.dumps(manifest, indent=2).encode())
    (directory / "events.jsonl").write_bytes(
        b"".join(json.dumps(dict(event, sequence=index)).encode() + b"\n" for index, event in enumerate(events))
    )
    return directory


@pytest.mark.parametrize("version,paths", [(1, LEGACY_RESOURCE_PATHS), (2, RESOURCE_PATHS)])
def test_retained_order_and_bytes_survive_repeated_read_only_recovery(tmp_path, version, paths):
    paths = tuple(reversed(paths))
    event = {
        "phase": "PREPARING",
        "path": paths[0],
        "artifact": "staging",
        "identity": dict(device=1, inode=2, mode=0, size=3, modified_ns=4, changed_ns=5),
    }
    directory = retained_journal(tmp_path, version, paths, [event])
    (directory / "new").mkdir()
    (directory / "new/000.bin").write_bytes(paths[0].encode())
    before = {
        str(path): (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.rglob("*") if path.is_file()
    }
    for _ in range(2):
        report = recovery_report(tmp_path)
        assert report.status == "PREPARING" and report.pending and report.discovery_required
        assert report.paths[1 : 1 + len(paths)] == paths
        assert str(directory.relative_to(tmp_path) / "new/000.bin") in report.paths
        assert (NEW_RESOURCE in report.paths) == (version == 2)
    with pytest.raises(ValueError):
        ResourceJournal.start(tmp_path, "b" * 40, [])
    assert {
        str(path): (path.read_bytes(), path.stat().st_mtime_ns) for path in directory.rglob("*") if path.is_file()
    } == before


@pytest.mark.parametrize(
    "version,paths",
    [
        (0, LEGACY_RESOURCE_PATHS),
        (3, RESOURCE_PATHS),
        (1, RESOURCE_PATHS),
        (2, LEGACY_RESOURCE_PATHS),
        (1, LEGACY_RESOURCE_PATHS[:-1]),
        (2, (*RESOURCE_PATHS[:-1], RESOURCE_PATHS[0])),
    ],
)
def test_unknown_version_wrong_inventory_subset_and_duplicate_fail_closed(tmp_path, version, paths):
    retained_journal(tmp_path, version, paths, [])
    assert recovery_report(tmp_path).status == "INVALID"


@pytest.mark.parametrize("identity", [False, True])
@pytest.mark.parametrize("leaf_sidecar", [False, True])
def test_v1_rejects_new_only_event_without_crash_or_status_masking(tmp_path, identity, leaf_sidecar):
    event = {"phase": "APPLYING", "path": NEW_RESOURCE}
    if identity:
        event["identity"] = dict(device=1, inode=2, mode=0, size=3, modified_ns=4, changed_ns=5)
    retained_journal(tmp_path, 1, LEGACY_RESOURCE_PATHS, [event])
    if leaf_sidecar:
        target = tmp_path / NEW_RESOURCE
        target.parent.mkdir(parents=True)
        target.with_name(f".{target.name}.dpone-transaction.json").write_bytes(b"untouched")
    assert recovery_report(tmp_path).status == "INVALID"


@pytest.mark.parametrize("version,count", [(1, 152), (2, 161)])
def test_event_count_and_byte_bounds_remain_version_specific(version, count):
    schema = f"dpone.starter-resource-transaction.v{version}"
    events = b"".join(json.dumps(dict(sequence=index, phase="PREPARING")).encode() + b"\n" for index in range(count))
    assert len(read_manifest_events(events, OPERATION, schema)) == count
    assert manifest_log_limit(schema) == count * 4096
    with pytest.raises(ValueError):
        read_manifest_events(
            events + json.dumps(dict(sequence=count, phase="PREPARING")).encode() + b"\n", OPERATION, schema
        )


@pytest.mark.parametrize("version", [1, 2, 3])
@pytest.mark.parametrize("path_kind", ["legacy", "new_resource", "index015", "index016", "index017"])
def test_orphan_sidecar_scope_and_indices_are_version_bound(tmp_path, version, path_kind):
    directory = tmp_path / ".dpone-starter-resource-transactions" / OPERATION
    directory.mkdir(parents=True)
    path = LEGACY_RESOURCE_PATHS[0]
    if path_kind == "new_resource":
        path = NEW_RESOURCE
    elif path_kind.startswith("index"):
        path = str(directory.relative_to(tmp_path) / "new" / (path_kind[5:] + ".bin"))
    payload = {
        "schema": f"dpone.starter-resource-recovery.v{version}",
        "operation": OPERATION,
        "root": dict(device=tmp_path.stat().st_dev, inode=tmp_path.stat().st_ino),
        "directory": dict(device=directory.stat().st_dev, inode=directory.stat().st_ino),
        "rollback": dict(
            path=path,
            device=1,
            inode=2,
            directories=[],
            removed=False,
            preserved=True,
            recovery_path=None,
            directory_recovery_paths=[],
        ),
    }
    content = json.dumps(payload).encode()
    sidecar = directory / "recovery.json"
    sidecar.write_bytes(content)
    valid = (
        version in {1, 2}
        and path_kind != "index017"
        and (version == 2 or path_kind not in {"new_resource", "index016"})
    )
    assert recovery_report(tmp_path).status == ("RECOVERY_REQUIRED" if valid else "INVALID")
    assert sidecar.read_bytes() == content


@pytest.mark.parametrize("version", [1, 2])
def test_rollback_event_cannot_expand_manifest_backup_indices(tmp_path, version):
    path = f".dpone-starter-resource-transactions/{OPERATION}/new/016.bin"
    rollback = dict(
        path=path,
        device=1,
        inode=2,
        directories=[],
        removed=False,
        preserved=True,
        recovery_path=None,
        directory_recovery_paths=[],
    )
    paths = LEGACY_RESOURCE_PATHS if version == 1 else RESOURCE_PATHS
    retained_journal(tmp_path, version, paths, [dict(phase="RECOVERY_REQUIRED", rollback=rollback)])
    assert recovery_report(tmp_path).status == ("INVALID" if version == 1 else "RECOVERY_REQUIRED")
