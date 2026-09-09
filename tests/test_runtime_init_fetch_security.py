from __future__ import annotations

import base64
import gzip
import hashlib
import io
import os
import tarfile
from pathlib import Path
from typing import Any

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_storage import write_ready_last
from dpone.runtime.runtime_payload_archive import (
    extract_runtime_payload,
    runtime_payload_archive,
)


def test_runtime_payload_rejects_traversal_before_writing_outside_worktree(
    tmp_path: Path,
) -> None:
    archive = runtime_payload_archive(_pack(_tar_entry("../escape.txt", b"unsafe")))
    destination = tmp_path / "worktree"

    with pytest.raises(InitFetchError) as exc:
        extract_runtime_payload(archive, destination)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert list(destination.iterdir()) == []
    assert not (tmp_path / "escape.txt").exists()


def test_runtime_payload_rejects_symbolic_link_member(tmp_path: Path) -> None:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as bundle:
            member = tarfile.TarInfo("runtime/link")
            member.type = tarfile.SYMTYPE
            member.linkname = "manifest.json"
            bundle.addfile(member)
    archive = runtime_payload_archive(_pack(raw.getvalue()))
    destination = tmp_path / "worktree"

    with pytest.raises(InitFetchError) as exc:
        extract_runtime_payload(archive, destination)

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert list(destination.iterdir()) == []


def test_ready_publication_rejects_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside.json"
    outside.write_bytes(b"owner-data")
    ready = tmp_path / "ready.json"
    ready.symlink_to(outside)

    with pytest.raises(InitFetchError) as exc:
        write_ready_last(ready, b'{"ready":true}\n')

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert outside.read_bytes() == b"owner-data"
    assert ready.is_symlink()


def test_ready_publication_is_idempotent_but_never_overwrites(tmp_path: Path) -> None:
    ready = tmp_path / "ready.json"
    original = b'{"ready":true}\n'
    write_ready_last(ready, original)
    write_ready_last(ready, original)

    with pytest.raises(InitFetchError) as exc:
        write_ready_last(ready, b'{"ready":false}\n')

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert ready.read_bytes() == original


def test_ready_publication_ignores_stale_legacy_pid_temporary(tmp_path: Path) -> None:
    ready = tmp_path / "ready.json"
    stale = tmp_path / f".ready.json.tmp.{os.getpid()}"
    stale.write_bytes(b"stale")

    write_ready_last(ready, b'{"ready":true}\n')

    assert ready.read_bytes() == b'{"ready":true}\n'
    assert stale.read_bytes() == b"stale"


def test_ready_publication_rejects_parent_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    linked_parent = tmp_path / "linked"
    linked_parent.symlink_to(outside, target_is_directory=True)

    with pytest.raises(InitFetchError) as exc:
        write_ready_last(linked_parent / "ready.json", b'{"ready":true}\n')

    assert exc.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"
    assert not (outside / "ready.json").exists()
    assert linked_parent.is_symlink()


def _tar_entry(name: str, content: bytes) -> bytes:
    raw = io.BytesIO()
    with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as bundle:
            member = tarfile.TarInfo(name)
            member.size = len(content)
            member.mtime = 0
            bundle.addfile(member, io.BytesIO(content))
    return raw.getvalue()


def _pack(archive: bytes) -> dict[str, Any]:
    return {
        "runtime_payload": {
            "schema": "dpone.airflow-runtime-payload.v1",
            "archive": {
                "encoding": "base64",
                "format": "tar+gzip",
                "sha256": "sha256:" + hashlib.sha256(archive).hexdigest(),
                "bytes": len(archive),
                "data": base64.b64encode(archive).decode("ascii"),
            },
        }
    }
