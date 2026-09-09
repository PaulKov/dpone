from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
GITLAB_CI_ROOT = ROOT / ".gitlab-ci.yml"


def _read_yaml_mapping(path: Path) -> dict[str, object]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise AssertionError(f"Expected a YAML mapping in {path}")
    return payload


def _normalize_includes(raw_includes: object, *, path: Path) -> list[dict[str, object]]:
    if raw_includes is None:
        return []
    if isinstance(raw_includes, dict):
        items = [raw_includes]
    elif isinstance(raw_includes, list):
        items = raw_includes
    else:
        raise AssertionError(f"Unsupported include block in {path}: {raw_includes!r}")

    normalized: list[dict[str, object]] = []
    for item in items:
        if not isinstance(item, dict) or "local" not in item:
            raise AssertionError(f"Only local GitLab CI includes are supported in tests: {path}: {item!r}")
        normalized.append(item)
    return normalized


@lru_cache(maxsize=1)
def iter_gitlab_ci_paths() -> tuple[Path, ...]:
    resolved: list[Path] = []
    visited: set[Path] = set()

    def visit(path: Path) -> None:
        real_path = path.resolve()
        if real_path in visited:
            return
        visited.add(real_path)

        data = _read_yaml_mapping(path)
        for include in _normalize_includes(data.get("include", []), path=path):
            include_path = ROOT / str(include["local"])
            if not include_path.exists():
                raise AssertionError(f"Missing local GitLab CI include: {include_path}")
            visit(include_path)

        resolved.append(path)

    visit(GITLAB_CI_ROOT)
    return tuple(resolved)


@lru_cache(maxsize=1)
def load_gitlab_ci_config() -> dict[str, object]:
    merged: dict[str, object] = {}
    for path in iter_gitlab_ci_paths():
        merged.update(_read_yaml_mapping(path))
    return merged


@lru_cache(maxsize=1)
def load_gitlab_ci_text() -> str:
    chunks: list[str] = []
    for path in iter_gitlab_ci_paths():
        rel_path = path.relative_to(ROOT).as_posix()
        chunks.append(f"# {rel_path}\n{path.read_text(encoding='utf-8')}")
    return "\n\n".join(chunks)
