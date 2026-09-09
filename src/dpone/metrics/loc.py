from __future__ import annotations

import statistics
import subprocess
from collections.abc import Iterable, Sequence
from pathlib import Path

from .models import LocStats


def _iter_git_tracked_py_files(root: Path, *, repo_root: Path) -> tuple[Path, ...] | None:
    """Iterate Git-tracked Python files under root.

    Generated documentation metrics must be reproducible across developer
    machines and CI runners, so they should not include local scratch files,
    generated artifacts, or ignored worktree noise.
    """

    resolved_root = root.resolve()
    resolved_repo_root = repo_root.resolve()
    try:
        result = subprocess.run(
            ["git", "-C", str(resolved_repo_root), "ls-files", "*.py"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None

    paths: list[Path] = []
    for rel in result.stdout.splitlines():
        path = (resolved_repo_root / rel).resolve()
        if path.is_relative_to(resolved_root):
            paths.append(path)
    return tuple(sorted(paths, key=lambda path: path.relative_to(resolved_repo_root).as_posix()))


def iter_py_files(root: Path, *, tracked_only: bool = False, repo_root: Path | None = None) -> Iterable[Path]:
    """Iterate python files under root, ignoring typical build/cache dirs."""

    if tracked_only:
        tracked = _iter_git_tracked_py_files(root, repo_root=repo_root or root)
        if tracked is not None:
            yield from tracked
            return

    ignore_dirs = {
        ".git",
        ".venv",
        ".cache",
        "__pycache__",
        "dist",
        "build",
        ".pytest_cache",
        ".mypy_cache",
        ".ruff_cache",
        ".tox",
        ".eggs",
        "rendered",
        "site",
        "test",
        "test_artifacts",
    }

    for p in sorted(root.rglob("*.py"), key=lambda path: path.relative_to(root).as_posix()):
        if set(p.parts) & ignore_dirs:
            continue
        yield p


def count_lines(text: str) -> int:
    return len(text.splitlines())


def count_sloc(text: str) -> int:
    """Count simple source lines of code, excluding blank/comment-only lines."""

    return sum(1 for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#"))


def calc_loc_stats(files: Sequence[Path], *, root: Path, top_n: int) -> LocStats:
    items: list[tuple[str, int]] = []
    total = 0
    total_sloc = 0
    min_lines = 10**9
    min_path = ""
    max_lines = -1
    max_path = ""

    for f in files:
        try:
            text = f.read_text(encoding="utf-8")
        except Exception:
            continue
        n = count_lines(text)
        total_sloc += count_sloc(text)
        rel = str(f.relative_to(root)).replace("\\", "/")
        items.append((rel, n))
        total += n
        if n < min_lines or (n == min_lines and (not min_path or rel < min_path)):
            min_lines = n
            min_path = rel
        if n > max_lines or (n == max_lines and (not max_path or rel < max_path)):
            max_lines = n
            max_path = rel

    counts = sorted((n for _, n in items))
    avg = (total / len(items)) if items else 0.0
    med = float(statistics.median(counts)) if counts else 0.0
    top = sorted(items, key=lambda x: (-x[1], x[0]))[: max(0, top_n)]

    if not items:
        min_lines = 0
        max_lines = 0

    return LocStats(
        files=len(items),
        total_lines=total,
        total_sloc=total_sloc,
        min_lines=min_lines,
        min_path=min_path,
        max_lines=max_lines,
        max_path=max_path,
        avg_lines=float(avg),
        median_lines=float(med),
        top=top,
    )
