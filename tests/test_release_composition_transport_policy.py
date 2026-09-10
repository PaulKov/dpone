"""Transport binding preserves byte inventories, read order and first failures."""

import json
from copy import deepcopy
from dataclasses import FrozenInstanceError
from pathlib import PurePosixPath

import pytest

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import artifact_json_bytes, sha256_bytes
from dpone.contracts.release_composition import MAX_COMPOSITION_FILE_BYTES, MAX_COMPOSITION_TOTAL_BYTES
from dpone.contracts.release_composition_subject import composition_subject_bytes
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.release_composition_files import (
    composition_auxiliary_artifacts,
    composition_file_descriptors,
    verify_composition_transport_files,
)
from tests.test_release_composition_policy import composition, descriptor

PARENT = "release-set.json"
SUBJECT = "release-subjects.sha256"
NATIVE = "_composition/native/release-set.json"
METADATA_LIMIT = 8 * 1024 * 1024


def transport_files():
    """Create synthetic transport bytes; these never certify source admission."""
    release = composition()
    files = {path: b"synthetic" for path in composition_file_descriptors(release)}
    files[NATIVE] = artifact_json_bytes(release["constituents"][0]["release"])
    seal_transport(release, files)
    return release, files


def seal_transport(release, files):
    row = next(row for row in release["artifacts"]["composition_sources"] if row["path"] == NATIVE)
    row.update(bytes=len(files[NATIVE]), sha256=sha256_bytes(files[NATIVE]))
    release["release_id"] = release_id(release)
    files[PARENT] = artifact_json_bytes(release)
    files[SUBJECT] = composition_subject_bytes(release, files[PARENT])


def write_transport(root, files):
    for path, body in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)


def recording_reader(calls):
    def read(root, path, *, max_bytes):
        calls.append((path, max_bytes))
        return read_confined_file(root, path, max_bytes=max_bytes)

    return read


@pytest.mark.parametrize("with_attestation", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_transport_preserves_exact_bytes_and_descriptor_read_order(tmp_path, with_attestation, reverse):
    release, files = transport_files()
    if reverse:
        release["artifacts"] = dict(reversed(list(release["artifacts"].items())))
        for rows in release["artifacts"].values():
            rows.reverse()
        release["constituents"].reverse()
    # Noncanonical whitespace is authorized by the original descriptor bytes.
    files[PARENT] = json.dumps(release, indent=4).encode() + b"\n\n"
    files[SUBJECT] = composition_subject_bytes(release, files[PARENT])
    extras = {"_SUCCESS": b"", "release-attestation.json": b"opaque attestation bytes"} if with_attestation else {}
    write_transport(tmp_path, {**files, **extras})
    calls: list[tuple[str, int]] = []
    captured = verify_composition_transport_files(tmp_path, release, read_file=recording_reader(calls))
    expected = {path: files[path] for path in composition_file_descriptors(release)}
    assert list(captured.items()) == list(expected.items())
    assert calls == [(PARENT, METADATA_LIMIT), (SUBJECT, METADATA_LIMIT)] + [
        (path, row["bytes"]) for path, row in composition_file_descriptors(release).items()
    ]
    assert composition_auxiliary_artifacts(tmp_path, release) == (
        (PurePosixPath(SUBJECT), sha256_bytes(files[SUBJECT])),
    )
    assert {p.relative_to(tmp_path).as_posix(): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == {
        **files,
        **extras,
    }


@pytest.mark.parametrize(
    "fault, message, read_count",
    [
        ("metadata", "composition release identity differs from its content", 0),
        ("orphan", "composition contains an orphan file", 0),
        ("descriptor_json", "duplicate JSON key", 1),
        ("descriptor_replaced", "composition descriptor changed after metadata validation", 1),
        ("artifact", "composition artifact differs from its exact descriptor", 3),
        ("native", "composition native source descriptor differs from embedded authority", None),
        ("subject", "composition checksum subject differs from the parent-bound inventory", None),
    ],
)
def test_transport_first_failure_remains_ordered(tmp_path, fault, message, read_count):
    release, files = transport_files()
    if fault == "native":
        files[NATIVE] = b"{}"
        seal_transport(release, files)
    if fault in {"metadata", "orphan", "descriptor_json", "descriptor_replaced", "artifact"}:
        first = next(iter(composition_file_descriptors(release)))
        files[first] = b"different"  # Same size: failure is the digest comparison.
    if fault == "metadata":
        release["release_id"] = "sha256:" + "0" * 64
    if fault in {"metadata", "orphan"}:
        files["orphan"] = b"orphan"
    if fault in {"metadata", "orphan", "descriptor_json"}:
        files[PARENT] = b'{"schema": 1, "schema": 2}'
    if fault == "descriptor_replaced":
        changed = deepcopy(release)
        changed["producer"]["version"] = "99.0.0"
        files[PARENT] = artifact_json_bytes(changed)
    files[SUBJECT] = b"incorrect subject"
    write_transport(tmp_path, files)
    calls: list[tuple[str, int]] = []
    with pytest.raises(ValueError) as caught:
        verify_composition_transport_files(tmp_path, release, read_file=recording_reader(calls))
    assert str(caught.value) == message
    assert len(calls) == (len(composition_file_descriptors(release)) + 2 if read_count is None else read_count)


def test_subject_read_failure_precedes_artifact_validation(tmp_path):
    release, files = transport_files()
    files.pop(SUBJECT)
    files[next(iter(composition_file_descriptors(release)))] = b"different"
    write_transport(tmp_path, files)
    calls: list[tuple[str, int]] = []
    with pytest.raises(ConfinedFileError) as caught:
        verify_composition_transport_files(tmp_path, release, read_file=recording_reader(calls))
    assert caught.value.code == "file_not_found"
    assert calls == [(PARENT, METADATA_LIMIT), (SUBJECT, METADATA_LIMIT)]


def test_complete_budget_rejects_before_any_artifact_read(tmp_path):
    release, files = transport_files()
    native = release["constituents"][0]["release"]
    native["artifacts"]["canonical_schemas"] = [
        {**descriptor(f"large_{i}", f"schemas/dbt/large_{i}.schema.json"), "bytes": MAX_COMPOSITION_FILE_BYTES}
        for i in range(8)
    ]
    # Only metadata claims approach the real bound; no large payload is allocated.
    for _ in range(4):
        release["artifacts"]["canonical_schemas"] = deepcopy(native["artifacts"]["canonical_schemas"])
        native["release_id"] = release_id(native)
        files[NATIVE] = artifact_json_bytes(native)
        seal_transport(release, files)
        excess = (
            sum(row["bytes"] for row in composition_file_descriptors(release).values()) - MAX_COMPOSITION_TOTAL_BYTES
        )
        if excess == 0:
            break
        native["artifacts"]["canonical_schemas"][-1]["bytes"] -= excess
    assert excess == 0
    files = {path: files.get(path, b"wrong") for path in [*composition_file_descriptors(release), PARENT, SUBJECT]}
    write_transport(tmp_path, files)
    calls: list[tuple[str, int]] = []
    with pytest.raises(ValueError, match="^composition aggregate bytes including metadata exceed the bound$"):
        verify_composition_transport_files(tmp_path, release, read_file=recording_reader(calls))
    assert calls == [(PARENT, METADATA_LIMIT), (SUBJECT, METADATA_LIMIT)]


def test_descriptor_projection_preserves_partial_input_order_and_row_identity():
    first = {"path": "z", "bytes": "not validated here"}
    second = {"path": "a"}
    release = {"artifacts": {"unknown_section": [first], "other": [second]}}
    projected = composition_file_descriptors(release)
    assert list(projected) == ["z", "a"]
    assert projected["z"] is first and projected["a"] is second
    with pytest.raises(KeyError, match="artifacts"):
        composition_file_descriptors({})
    with pytest.raises(ValueError, match="^composition contains duplicate artifact paths$"):
        composition_file_descriptors({"artifacts": {"a": [first, first, {}]}})


@pytest.mark.parametrize("schema", [None, "dpone.release-set.v1", "dpone.release-set.v2"])
def test_auxiliary_native_and_unknown_schemas_need_no_source_read(tmp_path, schema):
    assert composition_auxiliary_artifacts(tmp_path / "absent", {"schema": schema}) == ()


def test_auxiliary_partial_composition_keeps_its_existing_admission_boundary(tmp_path):
    release = {"schema": "dpone.release-set.v3", "artifacts": {}}
    payload = artifact_json_bytes(release)
    (tmp_path / PARENT).write_bytes(payload)
    assert composition_auxiliary_artifacts(tmp_path, release) == (
        (PurePosixPath(SUBJECT), sha256_bytes(composition_subject_bytes(release, payload))),
    )
    release.pop("artifacts")
    with pytest.raises(ValueError, match="descriptor changed after metadata validation"):
        composition_auxiliary_artifacts(tmp_path, release)
    (tmp_path / PARENT).write_bytes(artifact_json_bytes(release))
    with pytest.raises(KeyError, match="artifacts"):
        composition_auxiliary_artifacts(tmp_path, release)


def test_symlink_root_is_rejected_before_any_payload_read(tmp_path):
    release, files = transport_files()
    root = tmp_path / "source"
    write_transport(root, files)
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    calls: list[tuple[str, int]] = []
    with pytest.raises(OSError):
        verify_composition_transport_files(link, release, read_file=recording_reader(calls))
    assert calls == []


def test_pure_transport_plan_detaches_pins_and_has_an_immutable_ordered_inventory():
    from dpone.contracts.release_composition_transport import CompositionTransportPlan

    release, files = transport_files()
    plan = CompositionTransportPlan.from_release(release)
    first = plan.descriptors[0]
    assert [pin.path for pin in plan.descriptors] == list(composition_file_descriptors(release))
    release["artifacts"]["dag_specs"][0]["sha256"] = "sha256:" + "0" * 64
    first.verify_bytes(files[first.path])
    with pytest.raises(ValueError, match="artifact differs from its exact descriptor"):
        first.verify_bytes(b"different")
    with pytest.raises(FrozenInstanceError):
        setattr(first, "size", 0)
    with pytest.raises(FrozenInstanceError):
        setattr(plan, "descriptors", ())
    size = sum(pin.size for pin in plan.descriptors)
    plan.require_complete_byte_budget(MAX_COMPOSITION_TOTAL_BYTES - size - 1, 1)
    with pytest.raises(ValueError, match="aggregate bytes including metadata"):
        plan.require_complete_byte_budget(MAX_COMPOSITION_TOTAL_BYTES - size, 1)
