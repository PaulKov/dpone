from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]

SKIP_PARTS = {
    ".cache",
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "site",
}

YAML_FENCE_RE = re.compile(r"(^```(?:yaml|yml)\s*$)(.*?)(^```\s*$)", re.MULTILINE | re.DOTALL)


def _is_checked_path(path: Path) -> bool:
    return not any(part in SKIP_PARTS for part in path.relative_to(ROOT).parts)


def _iter_repository_files(suffixes: set[str]) -> Iterable[Path]:
    for path in ROOT.rglob("*"):
        if path.is_file() and path.suffix.lower() in suffixes and _is_checked_path(path):
            yield path


def _line_number(text: str, offset: int) -> int:
    return text[:offset].count("\n") + 1


def _format_yaml_error(path: Path, line_number: int, exc: yaml.YAMLError) -> str:
    mark = getattr(exc, "problem_mark", None)
    if mark is None:
        return f"{path.relative_to(ROOT)}:{line_number}: {exc}"
    return f"{path.relative_to(ROOT)}:{line_number + mark.line}: {exc.problem}"


def test_repository_yaml_files_and_markdown_yaml_fences_parse() -> None:
    offenders: list[str] = []

    for path in sorted(_iter_repository_files({".yaml", ".yml"})):
        try:
            list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
        except yaml.YAMLError as exc:
            offenders.append(_format_yaml_error(path, 1, exc))

    for path in sorted(_iter_repository_files({".md"})):
        text = path.read_text(encoding="utf-8")
        for index, match in enumerate(YAML_FENCE_RE.finditer(text), start=1):
            block = match.group(2).strip("\n")
            if not block.strip():
                continue
            line_number = _line_number(text, match.start())
            try:
                list(yaml.safe_load_all(block))
            except yaml.YAMLError as exc:
                offenders.append(
                    f"{path.relative_to(ROOT)} fence #{index}: {_format_yaml_error(path, line_number, exc)}"
                )

    assert offenders == []
