"""Confined exact file inventories shared by composition admission and delivery."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.release_composition import MAX_COMPOSITION_TOTAL_BYTES
from dpone.contracts.release_composition_subject import composition_subject_bytes
from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.confined_files import read_confined_file
from dpone.ports.dbt_release_files import ConfinedReleaseFileReader

NATIVE_SIDECARS = {
    "release-set.json": "_composition/native/release-set.json",
    "_dbt/dbt-source-snapshot.json": "_composition/native/dbt-source-snapshot.json",
    "release-subjects.sha256": "_composition/native/release-subjects.sha256",
}
SUBJECT = "release-subjects.sha256"


def composition_file_descriptors(release: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Project the already validated complete parent file inventory."""
    result = {}
    for rows in release["artifacts"].values():
        for row in rows:
            path = row["path"]
            if path in result:
                raise ValueError("composition contains duplicate artifact paths")
            result[path] = row
    return result


def verify_composition_transport_files(
    root: Path, release: Mapping[str, Any], *, read_file: ConfinedReleaseFileReader = read_confined_file
) -> dict[str, bytes]:
    """Check exact parent-bound bytes; this is not source admission or signing.

    Generic registry/cache consumers use this boundary. The composition producer
    additionally runs complete source verifiers on detached constituent views.
    """
    from dpone.contracts.release_composition_policy import validate_composition_metadata

    validate_composition_metadata(release)
    descriptors = composition_file_descriptors(release)
    require_exact_composition_tree(root, set(descriptors) | {"release-set.json"})
    descriptor_payload = read_file(root, "release-set.json", max_bytes=8 * 1024 * 1024)
    if strict_json_object(descriptor_payload) != release:
        raise ValueError("composition descriptor changed after metadata validation")
    subject = read_file(root, SUBJECT, max_bytes=8 * 1024 * 1024)
    if (
        sum(row["bytes"] for row in descriptors.values()) + len(descriptor_payload) + len(subject)
        > MAX_COMPOSITION_TOTAL_BYTES
    ):
        raise ValueError("composition aggregate bytes including metadata exceed the bound")
    files = {}
    for path, row in descriptors.items():
        payload = read_file(root, path, max_bytes=row["bytes"])
        if len(payload) != row["bytes"] or sha256_bytes(payload) != row["sha256"]:
            raise ValueError("composition artifact differs from its exact descriptor")
        files[path] = payload
    native = next(item["release"] for item in release["constituents"] if item["id"] == "native")
    if strict_json_object(files[NATIVE_SIDECARS["release-set.json"]]) != native:
        raise ValueError("composition native source descriptor differs from embedded authority")
    if subject != composition_subject_bytes(release, descriptor_payload):
        raise ValueError("composition checksum subject differs from the parent-bound inventory")
    return files


def require_exact_composition_tree(root: Path, expected: set[str]) -> None:
    """Reject orphan directories, files, links and special entries without following them."""
    directories = {
        parent.as_posix() for path in expected for parent in PurePosixPath(path).parents if parent != PurePosixPath(".")
    }
    found: set[str] = set()
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW

    def walk(descriptor: int, prefix: str) -> None:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                path = f"{prefix}/{entry.name}" if prefix else entry.name
                metadata = entry.stat(follow_symlinks=False)
                if stat.S_ISDIR(metadata.st_mode):
                    if path not in directories:
                        raise ValueError("composition contains an orphan directory")
                    child = os.open(entry.name, flags, dir_fd=descriptor)
                    try:
                        walk(child, path)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(metadata.st_mode):
                    if path not in expected and path not in {SUBJECT, "_SUCCESS", "release-attestation.json"}:
                        raise ValueError("composition contains an orphan file")
                    found.add(path)
                else:
                    raise ValueError("composition contains a link or special file")

    descriptor = os.open(root, flags)
    try:
        walk(descriptor, "")
    finally:
        os.close(descriptor)
    if not expected <= found:
        raise ValueError("composition source tree is incomplete")


def write_private_files(root: Path, files: Mapping[str, bytes]) -> None:
    """Write a previously validated inventory into a caller-owned empty stage."""
    for path, body in files.items():
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)


def require_composition_root(root: Path) -> None:
    """Reject symlink ancestors before source capture or output publication."""
    absolute = root.absolute()
    for candidate in (*reversed(absolute.parents), absolute):
        try:
            metadata = candidate.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            raise ValueError("composition roots cannot traverse symbolic links")
