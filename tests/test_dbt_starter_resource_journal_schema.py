"""Closed codec rejects malformed evidence independently of journal I/O."""

import pytest
from tools.dbt_self_service.starter_resource_journal_schema import RESOURCE_PATHS, read_events, validate_event


@pytest.mark.parametrize("path", ["../foreign", "/foreign", "src/../foreign", "src\\foreign", "src/foreign"])
def test_event_rejects_paths_outside_inventory(path):
    with pytest.raises(ValueError):
        validate_event({"phase": "APPLYING", "path": path})


@pytest.mark.parametrize(
    "content",
    [
        b'{"phase":"PREPARED","sequence":0}',
        b'{"phase":"PREPARED","sequence":1}\n',
        b'{"phase":"PREPARED","sequence":0,"sequence":0}\n',
        b'{"phase":"PREPARED","sequence":true}\n',
    ],
)
def test_event_decoder_rejects_truncation_or_invalid_sequence(content):
    with pytest.raises(ValueError):
        read_events(content)


def test_fixed_target_pending_record_is_valid():
    validate_event({"phase": "APPLYING", "path": RESOURCE_PATHS[0]})
    assert read_events(b'{"phase":"PREPARING","sequence":0}\n') == [{"phase": "PREPARING"}]


@pytest.mark.parametrize(
    "path",
    [
        "src/dpone/_assets/dbt_dpone/unknown",
        "src/dpone/_assets/dbt_dpone/.dpone-recovery-nope",
        "src/dpone/_assets/dbt_dpone/macros/.dpone-recovery-" + "a" * 32,
    ],
)
def test_native_recovery_name_is_bound_to_target_parent(path):
    with pytest.raises(ValueError):
        validate_event({"phase": "APPLYING", "path": RESOURCE_PATHS[0], "recovery_paths": [path]})


@pytest.mark.parametrize("suffix", ["new", "restore"])
@pytest.mark.parametrize("current", [False, True])
def test_operation_sibling_recovery_path_requires_exact_operation(suffix, current):
    from pathlib import PurePosixPath

    operation = "12345678-1234-1234-1234-123456789abc"
    other = "00000000-0000-0000-0000-000000000000"
    target = PurePosixPath(RESOURCE_PATHS[0])
    path = target.with_name(f".{target.name}.{operation if current else other}.{suffix}")
    event = {"phase": "APPLYING", "path": RESOURCE_PATHS[0], "recovery_paths": [str(path)]}
    if current:
        validate_event(event, operation)
    else:
        with pytest.raises(ValueError):
            validate_event(event, operation)


def rollback_record():
    return {
        "path": RESOURCE_PATHS[0],
        "device": 1,
        "inode": 2,
        "directories": [{"path": "src/dpone/_assets/dbt_dpone", "device": 1, "inode": 3}],
        "removed": True,
        "preserved": False,
        "recovery_path": "src/dpone/_assets/dbt_dpone/.dpone-rollback-" + "a" * 32,
        "directory_recovery_paths": ["src/dpone/_assets/dbt_dpone"],
    }


@pytest.mark.parametrize("metadata", [False, True])
def test_rollback_codec_accepts_receipt_bound_artifacts(metadata):
    from tools.dbt_self_service.starter_resource_journal_schema import validate_rollback

    operation = "12345678-1234-1234-1234-123456789abc"
    record = rollback_record()
    if metadata:
        directory = ".dpone-starter-resource-transactions/" + operation
        record.update(
            path=directory + "/events.jsonl",
            directories=[{"path": directory, "device": 1, "inode": 3}],
            recovery_path=directory + "/.dpone-rollback-" + "a" * 32,
            directory_recovery_paths=[directory],
        )
    validate_rollback(record, operation)


@pytest.mark.parametrize(
    "change",
    ["foreign_parent", "unknown_sibling", "unowned_directory", "wrong_operation", "traversal", "identity", "flag"],
)
def test_rollback_codec_rejects_unbound_artifacts(change):
    from tools.dbt_self_service.starter_resource_journal_schema import validate_rollback

    record = rollback_record()
    if change == "foreign_parent":
        record["recovery_path"] = "foreign/.dpone-rollback-" + "a" * 32
    elif change == "unknown_sibling":
        record["recovery_path"] = "src/dpone/_assets/dbt_dpone/unknown"
    elif change == "unowned_directory":
        record["directory_recovery_paths"] = ["src"]
    elif change == "wrong_operation":
        record["path"] = ".dpone-starter-resource-transactions/00000000-0000-0000-0000-000000000000/events.jsonl"
    elif change == "traversal":
        record["path"] = "src/../foreign"
    elif change == "identity":
        record["inode"] = True
    else:
        record["removed"] = 1
    with pytest.raises(ValueError):
        validate_rollback(record, "12345678-1234-1234-1234-123456789abc")
