from __future__ import annotations

import json
import stat
import zipfile
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    COMMIT_SHA,
    RELEASE,
    REPOSITORY,
    ROOT,
    canonical,
    load_module,
    policy,
    write_valid_sources,
    zip_bytes,
)

archive = load_module(
    "dpone_release_candidate_evidence_archive_test",
    "tools/agent_policy/release_candidate_evidence_archive.py",
)
builder = load_module(
    "dpone_release_candidate_evidence_archive_builder_test",
    "tools/agent_policy/release_candidate_evidence_builder.py",
)


def _bundle(tmp_path: Path) -> Path:
    input_root = tmp_path / "inputs"
    output = tmp_path / "bundle"
    write_valid_sources(input_root)
    builder.build_bundle(
        root=ROOT,
        input_root=input_root,
        output_dir=output,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )
    return output


def _files(directory: Path) -> dict[str, bytes]:
    return {
        path.relative_to(directory).as_posix(): path.read_bytes() for path in directory.rglob("*") if path.is_file()
    }


def _archive_bytes(directory: Path) -> bytes:
    return zip_bytes(sorted(_files(directory).items()))


def _verify_files(files: dict[str, bytes]) -> Any:
    return archive.verify_files(
        files,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )


def test_verifies_exact_directory_and_archive_bytes(tmp_path: Path) -> None:
    directory = _bundle(tmp_path)

    local = archive.verify_directory(
        directory,
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )
    downloaded = archive.verify_archive(
        _archive_bytes(directory),
        repository=REPOSITORY,
        commit_sha=COMMIT_SHA,
        release=RELEASE,
        run_id=101,
        run_attempt=2,
    )

    assert local == downloaded
    assert local.repository == REPOSITORY
    assert local.commit_sha == COMMIT_SHA
    assert local.release == RELEASE
    assert local.profile == policy.PROFILE
    assert local.run_id == 101
    assert local.run_attempt == 2
    assert local.source_count == len(policy.SOURCE_PATHS)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_closed_file_set_rejects_missing_or_extra_member(tmp_path: Path, mutation: str) -> None:
    files = _files(_bundle(tmp_path))
    if mutation == "missing":
        files.pop(f"{builder.SOURCE_PREFIX}/{policy.SOURCE_PATHS['cdc_state_junit']}")
    else:
        files["operator-override.json"] = b"{}\n"

    with pytest.raises(ValueError, match="file set is invalid"):
        _verify_files(files)


@pytest.mark.parametrize(
    ("repository", "commit_sha", "release", "run_id", "run_attempt", "message"),
    [
        ("attacker/fork", COMMIT_SHA, RELEASE, 101, 2, "manifest identity"),
        (REPOSITORY, "b" * 40, RELEASE, 101, 2, "manifest identity"),
        (REPOSITORY, COMMIT_SHA, f"{RELEASE}.tampered", 101, 2, "manifest identity"),
        (REPOSITORY, COMMIT_SHA, RELEASE, 102, 2, "run ID"),
        (REPOSITORY, COMMIT_SHA, RELEASE, 101, 3, "attempt"),
    ],
)
def test_rejects_outer_identity_mismatch(
    tmp_path: Path,
    repository: str,
    commit_sha: str,
    release: str,
    run_id: int,
    run_attempt: int,
    message: str,
) -> None:
    raw = _archive_bytes(_bundle(tmp_path))

    with pytest.raises(ValueError, match=message):
        archive.verify_archive(
            raw,
            repository=repository,
            commit_sha=commit_sha,
            release=release,
            run_id=run_id,
            run_attempt=run_attempt,
        )


@pytest.mark.parametrize("status", ["UNVERIFIED", "SKIP", "FAIL"])
def test_pack_cannot_self_declare_non_pass_or_non_go_status(tmp_path: Path, status: str) -> None:
    files = _files(_bundle(tmp_path))
    pack = json.loads(files[builder.PACK_NAME])
    pack["status"] = status
    pack["decision"] = "NO-GO"
    files[builder.PACK_NAME] = canonical(pack)

    with pytest.raises(ValueError, match="pack is not the exact derived PASS projection"):
        _verify_files(files)


def test_required_roles_cannot_be_shrunk(tmp_path: Path) -> None:
    files = _files(_bundle(tmp_path))
    pack = json.loads(files[builder.PACK_NAME])
    pack["required_roles"] = ["stress_benchmark"]
    files[builder.PACK_NAME] = canonical(pack)

    with pytest.raises(ValueError, match="exact derived PASS projection"):
        _verify_files(files)


def test_source_byte_tampering_breaks_manifest_binding(tmp_path: Path) -> None:
    files = _files(_bundle(tmp_path))
    name = f"{builder.SOURCE_PREFIX}/{policy.SOURCE_PATHS['cdc_state_junit']}"
    files[name] += b" "

    with pytest.raises(ValueError, match="source digest or size"):
        _verify_files(files)


def test_receipt_binding_cannot_be_relabelled(tmp_path: Path) -> None:
    files = _files(_bundle(tmp_path))
    receipt = json.loads(files[builder.RECEIPT_NAME])
    receipt["binding_id"] = "sha256:" + "0" * 64
    files[builder.RECEIPT_NAME] = canonical(receipt)

    with pytest.raises(ValueError, match="binding ID"):
        _verify_files(files)


@pytest.mark.parametrize(
    "raw",
    [
        b'{"schema":"dpone.release_candidate_evidence_manifest.v1","schema":"attacker"}\n',
        b'{"schema":NaN}\n',
    ],
)
def test_authority_json_is_duplicate_free_and_finite(tmp_path: Path, raw: bytes) -> None:
    files = _files(_bundle(tmp_path))
    files[builder.MANIFEST_NAME] = raw

    with pytest.raises(ValueError, match="duplicate|non-finite"):
        _verify_files(files)


def test_authority_json_must_be_canonical_and_lf_terminated(tmp_path: Path) -> None:
    files = _files(_bundle(tmp_path))
    manifest = json.loads(files[builder.MANIFEST_NAME])
    files[builder.MANIFEST_NAME] = json.dumps(manifest, indent=2, sort_keys=True).encode()

    with pytest.raises(ValueError, match="not canonical JSON plus LF"):
        _verify_files(files)


@pytest.mark.parametrize("unsafe", ["parent", "absolute", "backslash", "symlink", "duplicate"])
def test_zip_rejects_unsafe_or_ambiguous_members(tmp_path: Path, unsafe: str) -> None:
    members: list[tuple[str | zipfile.ZipInfo, bytes]] = sorted(_files(_bundle(tmp_path)).items())
    if unsafe == "parent":
        members.append(("../receipt.json", b"{}"))
    elif unsafe == "absolute":
        members.append(("/receipt.json", b"{}"))
    elif unsafe == "backslash":
        members.append((r"..\receipt.json", b"{}"))
    elif unsafe == "duplicate":
        members.append((builder.RECEIPT_NAME, b"{}"))
    else:
        info = zipfile.ZipInfo("sources/link.json")
        info.create_system = 3
        info.external_attr = (stat.S_IFLNK | 0o777) << 16
        members.append((info, b"target"))

    with pytest.raises(ValueError, match="unsafe|symlink|duplicate"):
        archive.verify_archive(
            zip_bytes(members),
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_zip_rejects_unexpected_directory_even_without_files(tmp_path: Path) -> None:
    members: list[tuple[str | zipfile.ZipInfo, bytes]] = sorted(_files(_bundle(tmp_path)).items())
    directory = zipfile.ZipInfo("manual-override/")
    directory.external_attr = (stat.S_IFDIR | 0o755) << 16
    members.append((directory, b""))

    with pytest.raises(ValueError, match="unexpected directory"):
        archive.verify_archive(
            zip_bytes(members),
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )


def test_directory_rejects_symlinked_authority_file(tmp_path: Path) -> None:
    directory = _bundle(tmp_path)
    target = directory / builder.RECEIPT_NAME
    original = target.read_bytes()
    target.unlink()
    outside = tmp_path / "outside.json"
    outside.write_bytes(original)
    try:
        target.symlink_to(outside)
    except OSError:
        pytest.skip("symlinks are unavailable on this platform")

    with pytest.raises(ValueError, match="unsafe file type"):
        archive.verify_directory(
            directory,
            repository=REPOSITORY,
            commit_sha=COMMIT_SHA,
            release=RELEASE,
            run_id=101,
            run_attempt=2,
        )
