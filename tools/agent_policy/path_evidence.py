"""Parse exact Git path evidence without C-quoting or newline ambiguity."""

from __future__ import annotations

from pathlib import Path


def read_path_evidence(path: Path) -> list[str]:
    """Read newline-compatible legacy or canonical NUL-delimited path evidence."""

    return parse_path_evidence(path.read_bytes(), field=str(path))


def parse_path_evidence(content: bytes, *, field: str) -> list[str]:
    """Return sorted unique UTF-8 paths from legacy lines or `git diff -z`."""

    nul_delimited = b"\0" in content
    if nul_delimited:
        if not content.endswith(b"\0"):
            raise ValueError(f"{field} contains a truncated NUL-delimited path")
        raw_paths = content[:-1].split(b"\0")
    else:
        try:
            raw_paths = [line.encode("utf-8") for line in content.decode("utf-8").splitlines()]
        except UnicodeDecodeError as exc:
            raise ValueError(f"{field} must contain UTF-8 paths") from exc

    paths: list[str] = []
    for raw_path in raw_paths:
        if not raw_path:
            continue
        try:
            path = raw_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError(f"{field} must contain UTF-8 paths") from exc
        if not nul_delimited and path.startswith('"'):
            raise ValueError(f"{field} contains a C-quoted Git path; regenerate it with git diff -z")
        paths.append(path)
    return sorted(set(paths))
