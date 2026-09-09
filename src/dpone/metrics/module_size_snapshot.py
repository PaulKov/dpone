"""Immutable exact-HEAD inputs for authoritative module-size analysis."""

from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root, verify_project_root

from .module_size_baseline import (
    ModuleSizeBaselineError,
    confined_repo_path,
    decode_module_size_baseline,
    exact_sha,
    is_legacy_empty_baseline,
)

_BUDGET_PATH = "docs/benchmarks/quality_budgets.yml"
_MAX_CONTROL_BLOB_BYTES = 1 << 20
_MAX_ACCEPTED_ADRS = 64
_MAX_SOURCE_BLOB_BYTES = 8 << 20
_MAX_SOURCE_SNAPSHOT_BYTES = 128 << 20


@dataclass(frozen=True)
class ModuleSizeSourceSnapshot:
    path: str
    content: bytes


@dataclass(frozen=True)
class ModuleSizeHeadSnapshot:
    root_identity: ProjectRootIdentity
    head_sha: str
    baseline_path: str
    baseline: bytes
    quality_budgets: bytes
    sources: tuple[ModuleSizeSourceSnapshot, ...]
    accepted_adrs: tuple[tuple[str, str], ...]

    @property
    def source_bytes_by_path(self) -> dict[str, bytes]:
        return {source.path: source.content for source in self.sources}

    @property
    def accepted_adr_text_by_path(self) -> dict[str, str]:
        return dict(self.accepted_adrs)


def load_module_size_head_snapshot(
    *,
    repo_root: Path,
    package_dir: Path,
    baseline_path: Path,
    head_sha: str,
) -> ModuleSizeHeadSnapshot:
    """Load every authoritative input once from regular blobs in the exact HEAD tree."""

    try:
        root_identity = inspect_project_root(repo_root)
    except OSError as exc:
        raise ModuleSizeBaselineError("Module-size repository root could not be identified safely") from exc
    assert root_identity is not None
    head = exact_sha(head_sha, label="module-size snapshot head")
    package = _relative_to_repo(package_dir, repo_root)
    baseline_relative = _relative_to_repo(baseline_path, repo_root)
    _require_clean_worktree(repo_root, paths=(package, baseline_relative, _BUDGET_PATH))
    baseline = _head_blob(repo_root, head, baseline_relative)
    budgets = _head_blob(repo_root, head, _BUDGET_PATH)
    accepted_paths: tuple[str, ...] = ()
    if not is_legacy_empty_baseline(baseline, source=f"{head}:{baseline_relative}"):
        decoded = decode_module_size_baseline(baseline, source=f"{head}:{baseline_relative}")
        accepted_paths = tuple(
            sorted({entry.accepted_adr for entry in decoded.entries if entry.accepted_adr is not None})
        )
    if len(accepted_paths) > _MAX_ACCEPTED_ADRS:
        raise ModuleSizeBaselineError(f"Module-size baseline references more than {_MAX_ACCEPTED_ADRS} ADRs")
    if accepted_paths:
        _require_clean_worktree(repo_root, paths=accepted_paths)
    accepted = tuple((path, _decode_utf8(_head_blob(repo_root, head, path), path)) for path in accepted_paths)
    sources = _head_python_sources(repo_root, head, package)
    try:
        verify_project_root(root_identity)
    except OSError as exc:
        raise ModuleSizeBaselineError("Module-size repository root changed while inputs were captured") from exc
    return ModuleSizeHeadSnapshot(
        root_identity=root_identity,
        head_sha=head,
        baseline_path=baseline_relative,
        baseline=baseline,
        quality_budgets=budgets,
        sources=sources,
        accepted_adrs=accepted,
    )


def _head_python_sources(repo_root: Path, head_sha: str, package: str) -> tuple[ModuleSizeSourceSnapshot, ...]:
    records = _tree_records(repo_root, head_sha, package, recursive=True)
    selected: list[tuple[str, str, int]] = []
    sizes_by_oid: dict[str, int] = {}
    for mode, kind, blob_sha, size, path in records:
        if PurePosixPath(path).suffix != ".py":
            continue
        if kind != "blob" or mode not in {"100644", "100755"}:
            raise ModuleSizeBaselineError(f"Python module is not a regular tracked HEAD blob: {path}")
        if size > _MAX_SOURCE_BLOB_BYTES:
            raise ModuleSizeBaselineError(f"Python module exceeds snapshot byte limit: {path}")
        previous_size = sizes_by_oid.setdefault(blob_sha, size)
        if previous_size != size:
            raise ModuleSizeBaselineError(f"Git tree reports inconsistent blob size: {blob_sha}")
        selected.append((path, blob_sha, size))
    if sum(sizes_by_oid.values()) > _MAX_SOURCE_SNAPSHOT_BYTES:
        raise ModuleSizeBaselineError("Python source snapshot exceeds aggregate byte limit")
    content_by_oid = _batch_blobs(repo_root, tuple(sizes_by_oid.items()))
    return tuple(
        ModuleSizeSourceSnapshot(path=path, content=content_by_oid[blob_sha])
        for path, blob_sha, _size in sorted(selected)
    )


def _head_blob(repo_root: Path, head_sha: str, relative: str) -> bytes:
    records = _tree_records(repo_root, head_sha, relative, recursive=False)
    if len(records) != 1:
        raise ModuleSizeBaselineError(f"Module-size input is not one tracked HEAD blob: {relative}")
    mode, kind, blob_sha, size, recorded_path = records[0]
    if recorded_path != relative or kind != "blob" or mode not in {"100644", "100755"}:
        raise ModuleSizeBaselineError(f"Module-size input is not a regular tracked HEAD blob: {relative}")
    if size > _MAX_CONTROL_BLOB_BYTES:
        raise ModuleSizeBaselineError(f"Module-size control input exceeds byte limit: {relative}")
    return _git_bytes(repo_root, "cat-file", "blob", blob_sha)


def _tree_records(
    repo_root: Path, head_sha: str, relative: str, *, recursive: bool
) -> tuple[tuple[str, str, str, int, str], ...]:
    args = ["ls-tree", "-l"]
    if recursive:
        args.append("-r")
    raw = _git_bytes(repo_root, *args, "-z", head_sha, "--", relative)
    records: list[tuple[str, str, str, int, str]] = []
    for record in (item for item in raw.split(b"\0") if item):
        try:
            metadata, path = record.split(b"\t", 1)
            mode, kind, blob_sha, raw_size = metadata.decode("ascii").split()
            size = int(raw_size)
            if size < 0:
                raise ValueError("negative Git blob size")
            records.append((mode, kind, blob_sha, size, path.decode("utf-8")))
        except (UnicodeDecodeError, ValueError) as exc:
            raise ModuleSizeBaselineError(f"Malformed Git tree identity under {relative}") from exc
    return tuple(records)


def _batch_blobs(repo_root: Path, requests: tuple[tuple[str, int], ...]) -> dict[str, bytes]:
    if not requests:
        return {}
    query = "".join(f"{blob_sha}\n" for blob_sha, _size in requests).encode("ascii")
    try:
        result = subprocess.run(
            ["git", "-C", str(repo_root), "cat-file", "--batch"],
            input=query,
            check=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ModuleSizeBaselineError("Git source snapshot batch read failed") from exc
    output = result.stdout
    offset = 0
    blobs: dict[str, bytes] = {}
    for expected_oid, expected_size in requests:
        header_end = output.find(b"\n", offset)
        if header_end < 0:
            raise ModuleSizeBaselineError("Git source snapshot batch response is truncated")
        try:
            returned_oid, kind, raw_size = output[offset:header_end].decode("ascii").split()
            returned_size = int(raw_size)
        except (UnicodeDecodeError, ValueError) as exc:
            raise ModuleSizeBaselineError("Git source snapshot batch header is malformed") from exc
        if returned_oid != expected_oid or returned_oid in blobs or kind != "blob" or returned_size != expected_size:
            raise ModuleSizeBaselineError("Git source snapshot batch identity mismatch")
        content_start = header_end + 1
        content_end = content_start + returned_size
        if content_end >= len(output) or output[content_end : content_end + 1] != b"\n":
            raise ModuleSizeBaselineError("Git source snapshot batch framing is malformed")
        blobs[returned_oid] = output[content_start:content_end]
        offset = content_end + 1
    if offset != len(output) or len(blobs) != len(requests):
        raise ModuleSizeBaselineError("Git source snapshot batch response has unexpected data")
    return blobs


def _require_clean_worktree(repo_root: Path, *, paths: tuple[str, ...]) -> None:
    dirty = _git_bytes(
        repo_root,
        "status",
        "--porcelain=v1",
        "-z",
        "--untracked-files=all",
        "--",
        *sorted(set(paths)),
    )
    if dirty:
        raise ModuleSizeBaselineError("Module-size inputs differ from checked-out HEAD")


def _relative_to_repo(path: Path, repo_root: Path) -> str:
    return (
        confined_repo_path(repo_root, path, label="Module-size input")
        .relative_to(Path(repo_root).absolute())
        .as_posix()
    )


def _decode_utf8(raw: bytes, label: str) -> str:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ModuleSizeBaselineError(f"Module-size input must be UTF-8: {label}") from exc


def _git_bytes(repo_root: Path, *args: str) -> bytes:
    try:
        result = subprocess.run(["git", "-C", str(repo_root), *args], check=True, capture_output=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ModuleSizeBaselineError(f"Git HEAD snapshot lookup failed: {' '.join(args)}") from exc
    return result.stdout


__all__ = ["ModuleSizeHeadSnapshot", "ModuleSizeSourceSnapshot", "load_module_size_head_snapshot"]
