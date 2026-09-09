from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from jinja2 import Environment, meta

from dpone.manifest.errors import ManifestConfigurationError

_CANON_RE = re.compile(r"\[\+(\d+)\]")


def canonicalize_path(path: str) -> str:
    return _CANON_RE.sub(r"[\1]", str(path))


def split_path(path: str) -> list[Any]:
    p = str(path).strip()
    if not p:
        return []

    tokens: list[Any] = []
    i = 0
    while i < len(p):
        ch = p[i]
        if ch == ".":
            i += 1
            continue
        if ch == "[":
            j = p.find("]", i)
            if j == -1:
                raise ManifestConfigurationError(f"Некорректный путь (нет закрывающей скобки ]): {path}")
            raw_idx = p[i + 1 : j].strip()
            if raw_idx.startswith("+"):
                raw_idx = raw_idx[1:]
            if not raw_idx.isdigit():
                raise ManifestConfigurationError(f"Некорректный индекс в пути: {path}")
            tokens.append(int(raw_idx))
            i = j + 1
            continue

        j = i
        while j < len(p) and p[j] not in ".[":
            j += 1
        tokens.append(p[i:j])
        i = j

    return tokens


def get_by_path(obj: Any, path: str) -> tuple[bool, Any]:
    cur: Any = obj
    for tok in split_path(path):
        if isinstance(tok, int):
            if not isinstance(cur, list) or tok < 0 or tok >= len(cur):
                return False, None
            cur = cur[tok]
        else:
            if not isinstance(cur, Mapping) or tok not in cur:
                return False, None
            cur = cur[tok]
    return True, cur


def parent_path(path: str) -> str:
    p = str(path)
    if not p:
        return ""
    if p.endswith("]"):
        lb = p.rfind("[")
        if lb != -1:
            return p[:lb]
    if "." in p:
        return p.rsplit(".", 1)[0]
    return ""


def origin_for_path(origin_map: Mapping[str, str], path: str) -> str:
    p = canonicalize_path(path)
    if p in origin_map:
        return origin_map[p]
    parent = p
    while parent:
        parent = parent_path(parent)
        if parent and parent in origin_map:
            return origin_map[parent]
    return "unknown"


def path_relevant(query: str, change_path: str) -> bool:
    q = canonicalize_path(query)
    p = canonicalize_path(change_path)
    if not q or not p:
        return False
    if p == q:
        return True
    if p.startswith(q + ".") or p.startswith(q + "["):
        return True
    if q.startswith(p + ".") or q.startswith(p + "["):
        return True
    return False


def collect_template_vars(value: Any) -> list[str]:
    env = Environment(autoescape=False)
    found: set[str] = set()

    def visit(v: Any) -> None:
        if isinstance(v, str):
            if "{{" not in v and "{%" not in v and "{#" not in v:
                return
            try:
                ast = env.parse(v)
                found.update(meta.find_undeclared_variables(ast))
            except Exception:
                return
        elif isinstance(v, list):
            for x in v:
                visit(x)
        elif isinstance(v, Mapping):
            for x in v.values():
                visit(x)

    visit(value)
    return sorted(found)


def is_mapping(obj: Any) -> bool:
    return isinstance(obj, Mapping)


def iter_leaf_paths(obj: Any, *, _prefix: str = "") -> Iterable[str]:
    if is_mapping(obj):
        for k, v in obj.items():
            p = f"{_prefix}.{k}" if _prefix else str(k)
            yield from iter_leaf_paths(v, _prefix=p)
        return

    if isinstance(obj, list):
        for i, _ in enumerate(obj):
            p = f"{_prefix}[{i}]" if _prefix else f"[{i}]"
            yield p
        return

    yield _prefix


def fill_missing_origins(config: Mapping[str, Any], origin_map: dict[str, str]) -> dict[str, str]:
    for leaf in iter_leaf_paths(config):
        if not leaf:
            continue
        if leaf in origin_map:
            continue

        parent = leaf
        while True:
            parent = parent_path(parent)
            if not parent:
                origin_map[leaf] = "unknown"
                break
            if parent in origin_map:
                origin_map[leaf] = origin_map[parent]
                break
    return origin_map


def safe_equal(a: Any, b: Any) -> bool:
    try:
        return a == b
    except Exception:
        return False


__all__ = [
    "canonicalize_path",
    "split_path",
    "get_by_path",
    "parent_path",
    "origin_for_path",
    "path_relevant",
    "collect_template_vars",
    "is_mapping",
    "iter_leaf_paths",
    "fill_missing_origins",
    "safe_equal",
]
