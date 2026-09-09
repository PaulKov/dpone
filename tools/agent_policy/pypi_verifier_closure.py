"""Shared exact source manifest for source-free PyPI verifier jobs."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
from pathlib import Path
from typing import Final, NamedTuple

SCHEMA: Final = "dpone.pypi_verifier_closure.v1"
MANIFEST_NAME: Final = "pypi-verifier-closure.json"
MAX_FILE_BYTES: Final = 4 * 1024 * 1024


class ClosureFile(NamedTuple):
    """One repository source and its flat source-free destination."""

    source: str
    destination: str


def _tool(name: str) -> ClosureFile:
    return ClosureFile(f"tools/{name}", name)


def _agent_tool(name: str) -> ClosureFile:
    return ClosureFile(f"tools/agent_policy/{name}", name)


PYPI_PREPUBLICATION_FILES: Final = tuple(
    _tool(name)
    for name in (
        "pypi_core_metadata.py",
        "pypi_prepublication_codec.py",
        "pypi_prepublication_contract.py",
        "pypi_prepublication_gate.py",
        "pypi_prepublication_pypi.py",
        "pypi_prepublication_source.py",
    )
)

ORDINARY_RELEASE_FILES: Final = (
    tuple(
        _agent_tool(name)
        for name in (
            "artifact_resource_limits.py",
            "governance_live_ruleset.py",
            "pr_merge_binding.py",
            "pr_merge_receipt_contract.py",
            "pr_receipt_github_api.py",
            "release_authority_baseline.py",
            "release_candidate_evidence_archive.py",
            "release_candidate_evidence_builder.py",
            "release_candidate_evidence_codec.py",
            "release_candidate_evidence_contract.py",
            "release_candidate_evidence_gate.py",
            "release_candidate_evidence_github.py",
            "release_candidate_evidence_policy.py",
            "release_candidate_route_live_validation.py",
            "release_candidate_evidence_route_chunks.py",
            "release_candidate_evidence_route_validation.py",
            "release_candidate_evidence_source.py",
            "release_candidate_evidence_stress_validation.py",
            "release_candidate_evidence_validation.py",
            "release_commit_artifact_gate.py",
            "release_commit_evaluate.py",
            "release_commit_gate.py",
            "release_commit_models.py",
            "release_merge_receipt_gate.py",
            "release_merge_receipt_reconcile.py",
            "release_ruleset_snapshot.py",
        )
    )
    + PYPI_PREPUBLICATION_FILES
    + (
        ClosureFile(
            ".agents/policy/github-branch-protection.yml",
            "github-branch-protection.yml",
        ),
        ClosureFile(
            ".agents/policy/github-branch-protection.release-authority.json",
            "github-branch-protection.release-authority.json",
        ),
    )
)


def stage_ordinary_release(*, root: Path, output: Path) -> dict[str, object]:
    """Stage and read back the exact ordinary release verifier closure."""

    root = root.resolve()
    output = output.absolute()
    if output.exists():
        if output.is_symlink() or not output.is_dir() or any(output.iterdir()):
            raise ValueError("verifier output must be one empty regular directory")
    else:
        output.mkdir(parents=True)
    output = output.resolve()
    destinations = [item.destination for item in ORDINARY_RELEASE_FILES]
    if len(destinations) != len(set(destinations)):
        raise ValueError("verifier destinations must be unique")
    entries: list[dict[str, object]] = []
    for item in ORDINARY_RELEASE_FILES:
        raw = _read_regular(root / item.source, label=item.source)
        destination = output / item.destination
        with destination.open("xb") as stream:
            stream.write(raw)
        if _read_regular(destination, label=item.destination) != raw:
            raise ValueError(f"verifier readback differs: {item.destination}")
        entries.append(
            {
                "path": item.destination,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "size": len(raw),
                "source": item.source,
            }
        )
    payload: dict[str, object] = {
        "files": sorted(entries, key=lambda entry: str(entry["path"])),
        "profile": "ordinary-release",
        "schema": SCHEMA,
    }
    manifest = output / MANIFEST_NAME
    with manifest.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    _require_closed_tree(output, set(destinations) | {MANIFEST_NAME})
    return payload


def _read_regular(path: Path, *, label: str) -> bytes:
    file_fd: int | None = None
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NONBLOCK", 0)
        file_fd = os.open(path, flags)
        before = os.fstat(file_fd)
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_FILE_BYTES:
            raise ValueError(f"verifier source is not one bounded regular file: {label}")
        chunks: list[bytes] = []
        remaining = MAX_FILE_BYTES + 1
        while remaining:
            chunk = os.read(file_fd, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        raw = b"".join(chunks)
        after = os.fstat(file_fd)
    except (AttributeError, OSError) as exc:
        raise ValueError(f"verifier source is unavailable: {label}") from exc
    finally:
        if file_fd is not None:
            os.close(file_fd)
    if len(raw) != before.st_size or _identity(before) != _identity(after):
        raise ValueError(f"verifier source changed while read: {label}")
    return raw


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return value.st_dev, value.st_ino, value.st_mode, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _require_closed_tree(root: Path, expected: set[str]) -> None:
    observed: set[str] = set()
    for entry in root.iterdir():
        metadata = entry.lstat()
        if entry.is_symlink() or not stat.S_ISREG(metadata.st_mode):
            raise ValueError("verifier output contains a non-regular entry")
        observed.add(entry.name)
    if observed != expected:
        raise ValueError("verifier output is not the exact closure")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--profile", required=True, choices=("ordinary-release",))
    args = parser.parse_args(argv)
    try:
        payload = stage_ordinary_release(root=args.root, output=args.output)
    except (OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc), "schema": SCHEMA, "status": "FAIL"}, sort_keys=True))
        return 1
    files = payload["files"]
    if not isinstance(files, list):
        raise AssertionError("verifier payload must contain files")
    print(json.dumps({"file_count": len(files), "schema": SCHEMA, "status": "PASS"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["ORDINARY_RELEASE_FILES", "PYPI_PREPUBLICATION_FILES", "stage_ordinary_release"]
