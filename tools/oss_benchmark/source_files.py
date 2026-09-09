"""Source file discovery, LOC/SLOC and import parsing for OSS benchmark."""

from __future__ import annotations

import os
import re
from pathlib import Path

from tools.oss_benchmark.config import IGNORED_DIRS, LOGICAL_ANCHORS, SOURCE_SUFFIXES, TEST_DIR_NAMES


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1", errors="ignore")


def count_lines(text: str) -> int:
    return len(text.splitlines())


def count_sloc(text: str) -> int:
    count = 0
    in_block_comment = False
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if in_block_comment:
            if "*/" in line:
                in_block_comment = False
                line = line.split("*/", 1)[1].strip()
                if not line:
                    continue
            else:
                continue
        if line.startswith(("/*", "/**")):
            if "*/" not in line:
                in_block_comment = True
                continue
            line = line.split("*/", 1)[1].strip()
            if not line:
                continue
        if line.startswith(("#", "//", "*", "*/")):
            continue
        count += 1
    return count


def is_test_file(path: Path) -> bool:
    lowered_parts = {part.lower() for part in path.parts}
    if lowered_parts & TEST_DIR_NAMES:
        return True
    name = path.name
    lower_name = name.lower()
    return (
        lower_name.startswith("test_")
        or lower_name.endswith("_test.py")
        or lower_name.endswith("_tests.py")
        or name.endswith("Test.java")
        or name.endswith("Tests.java")
        or name.endswith("IT.java")
        or name.endswith("Test.kt")
        or name.endswith("Tests.kt")
        or name.endswith("IT.kt")
        or name.endswith("Test.groovy")
        or name.endswith("Tests.groovy")
        or name.endswith("IT.groovy")
        or lower_name.endswith(".spec.ts")
        or lower_name.endswith(".spec.tsx")
        or lower_name.endswith(".test.ts")
        or lower_name.endswith(".test.tsx")
        or lower_name.endswith(".spec.js")
        or lower_name.endswith(".test.js")
        or lower_name.endswith(".spec.jsx")
        or lower_name.endswith(".test.jsx")
    )


def should_skip_path(path: Path) -> bool:
    return bool(set(part.lower() for part in path.parts) & IGNORED_DIRS)


def iter_source_files(root: Path) -> tuple[Path, ...]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if path.is_file() and path.suffix.lower() in SOURCE_SUFFIXES:
            rel = path.relative_to(root)
            if not should_skip_path(rel):
                files.append(path)
    return tuple(sorted(files, key=lambda item: item.relative_to(root).as_posix()))


def module_name_for_file(path: Path, root: Path) -> str:
    text = read_text(path)
    suffix = path.suffix.lower()
    if suffix in {".java", ".kt", ".kts", ".groovy", ".scala"}:
        package_name = _declared_package(text)
        if package_name:
            return f"{package_name}.{path.stem}"
    return _dotted_from_path(path.relative_to(root))


def parse_import_targets(path: Path, text: str) -> set[str]:
    suffix = path.suffix.lower()
    if suffix == ".py":
        return _parse_python_imports(text)
    if suffix in {".java", ".kt", ".kts", ".groovy", ".scala"}:
        return _parse_jvm_imports(text)
    if suffix in {".ts", ".tsx", ".js", ".jsx"}:
        return _parse_ts_imports(path, text)
    if suffix == ".go":
        return _parse_go_imports(text)
    return set()


def logical_path_parts(path: Path) -> tuple[str, ...]:
    parts = tuple(path.with_suffix("").parts)
    lower_parts = [part.lower() for part in parts]
    for anchor in LOGICAL_ANCHORS:
        if anchor in lower_parts[:-1]:
            return parts[lower_parts.index(anchor) :]
    return parts[-min(4, len(parts)) :]


def clean_module_part(part: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", part).strip("_")


def _dotted_from_path(path: Path) -> str:
    parts = logical_path_parts(path)
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    return ".".join(clean_module_part(part) for part in parts if clean_module_part(part))


def _declared_package(text: str) -> str | None:
    match = re.search(r"^\s*package\s+([A-Za-z_][\w.]*)\s*;?", text, re.MULTILINE)
    return match.group(1) if match else None


def _parse_python_imports(text: str) -> set[str]:
    targets: set[str] = set()
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("import "):
            for item in stripped.removeprefix("import ").split(","):
                targets.add(item.strip().split(" as ", 1)[0].strip())
        elif stripped.startswith("from "):
            module = stripped.removeprefix("from ").split(" import ", 1)[0].strip()
            if module and not module.startswith("."):
                targets.add(module)
    return {target for target in targets if target}


def _parse_jvm_imports(text: str) -> set[str]:
    pattern = re.compile(r"^\s*import\s+(?:static\s+)?([A-Za-z_][\w.]*)(?:\.\*)?\s*;?", re.MULTILINE)
    return {match.group(1) for match in pattern.finditer(text)}


def _parse_ts_imports(path: Path, text: str) -> set[str]:
    targets: set[str] = set()
    for pattern in (
        re.compile(r"\bfrom\s+['\"]([^'\"]+)['\"]"),
        re.compile(r"^\s*import\s+['\"]([^'\"]+)['\"]", re.MULTILINE),
    ):
        for match in pattern.finditer(text):
            normalized = _normalize_ts_import(path, match.group(1))
            if normalized:
                targets.add(normalized)
    return targets


def _normalize_ts_import(path: Path, raw: str) -> str | None:
    if raw.startswith("."):
        target = Path(os.path.normpath(path.parent / raw)).with_suffix("")
        return ".".join(clean_module_part(part) for part in logical_path_parts(target) if clean_module_part(part))
    if raw.startswith("@"):
        raw = raw.lstrip("@").replace("/", ".")
    elif "/" in raw:
        raw = raw.replace("/", ".")
    return raw if "." in raw else None


def _parse_go_imports(text: str) -> set[str]:
    targets = {
        match.group(1).replace("/", ".") for match in re.finditer(r"^\s*import\s+\"([^\"]+)\"", text, re.MULTILINE)
    }
    block_match = re.search(r"^\s*import\s+\((.*?)^\s*\)", text, re.MULTILINE | re.DOTALL)
    if block_match:
        targets.update(match.group(1).replace("/", ".") for match in re.finditer(r"\"([^\"]+)\"", block_match.group(1)))
    return targets
