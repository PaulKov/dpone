"""Confined exact file inventories shared by composition admission and delivery."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from dpone.contracts.release_composition_transport import (
    CompositionTransportPlan,
    composition_auxiliary_subject_digest,
    project_composition_descriptors,
    verify_composition_descriptor,
    verify_composition_subject,
    verify_embedded_native_descriptor,
)
from dpone.manifest.confined_files import read_confined_file

if TYPE_CHECKING:
    from dpone.ports.dbt_release_files import ConfinedReleaseFileReader

SUBJECT = "release-subjects.sha256"


def composition_file_descriptors(release: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    """Project the already validated complete parent file inventory."""
    return project_composition_descriptors(release)


def verify_composition_transport_files(
    root: Path, release: Mapping[str, Any], *, read_file: ConfinedReleaseFileReader = read_confined_file
) -> dict[str, bytes]:
    """Check exact parent-bound bytes; this is not source admission or signing.

    Generic registry/cache consumers use this boundary. The composition producer
    additionally runs complete source verifiers on detached constituent views.
    """
    plan = CompositionTransportPlan.from_release(release)
    require_exact_composition_tree(root, {row.path for row in plan.descriptors} | {"release-set.json"})
    descriptor_payload = read_file(root, "release-set.json", max_bytes=8 * 1024 * 1024)
    verify_composition_descriptor(release, descriptor_payload)
    subject = read_file(root, SUBJECT, max_bytes=8 * 1024 * 1024)
    plan.require_complete_byte_budget(len(descriptor_payload), len(subject))
    files = {}
    for row in plan.descriptors:
        payload = read_file(root, row.path, max_bytes=row.size)
        row.verify_bytes(payload)
        files[row.path] = payload
    verify_embedded_native_descriptor(release, files)
    verify_composition_subject(release, descriptor_payload, subject)
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


def composition_auxiliary_artifacts(root: Path, release: Mapping[str, Any]) -> tuple[tuple[PurePosixPath, str], ...]:
    """Return transport pins for derived artifacts outside the parent inventory.

    The descriptor is reread through confinement and bound to the validated view.
    Native releases have no composition auxiliary transport requirements.
    """
    if release.get("schema") != "dpone.release-set.v3":
        return ()
    payload = read_confined_file(root, "release-set.json", max_bytes=8 * 1024 * 1024)
    return ((PurePosixPath(SUBJECT), composition_auxiliary_subject_digest(release, payload)),)
