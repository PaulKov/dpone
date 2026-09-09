"""Post-parse provenance (Step 19).

This module bridges the gap between:
- compiled (effective) YAML config dict (after Variant C compilation), and
- runtime dataclasses built by ETLProcessConfig.from_dict() (LoadConfig, DependencyConfig, ...).

It is UX-oriented and is primarily used by the CLI:
  dpone manifest explain ... --post-parse

The goal is to answer questions like:
- "How did depends_on entries become normalized DependencyConfig objects?"
- "Which raw fields contributed to LoadConfig.target_schema / load_strategy / options?"
- "Which override layer set the raw field that later produced a runtime value?"
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.config.load_config import LoadConfig
from dpone.dag.config import ETLProcessConfig
from dpone.dag.errors import DagConfigurationError
from dpone.dag.parse_trace import ParseTrace, ParseTracer


@dataclass(frozen=True, slots=True)
class SourceRef:
    path: str
    origin: str


@dataclass(frozen=True, slots=True)
class PostParseRecord:
    kind: str
    target: str
    value: Any
    operation: str
    sources: tuple[SourceRef, ...]
    details: dict[str, Any]


@dataclass(frozen=True, slots=True)
class PostParseResult:
    base_path: str | None
    normalized: dict[str, Any]
    records: tuple[PostParseRecord, ...]
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "base_path": self.base_path,
            "normalized": self.normalized,
            "records": [
                {
                    "kind": r.kind,
                    "target": r.target,
                    "value": r.value,
                    "operation": r.operation,
                    "sources": [{"path": s.path, "origin": s.origin} for s in r.sources],
                    "details": r.details,
                }
                for r in self.records
            ],
            "warnings": list(self.warnings),
        }

    def to_json(self) -> str:
        return json.dumps(self.to_json_dict(), ensure_ascii=False, indent=2)


@dataclass(frozen=True, slots=True)
class ParsedWhyExplanation:
    query: str
    canonical_path: str
    exists: bool
    value: Any
    records: tuple[PostParseRecord, ...]
    warnings: tuple[str, ...] = ()

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "query": self.query,
            "canonical_path": self.canonical_path,
            "exists": self.exists,
            "value": self.value,
            "records": [
                {
                    "kind": r.kind,
                    "target": r.target,
                    "operation": r.operation,
                    "sources": [{"path": s.path, "origin": s.origin} for s in r.sources],
                    "details": r.details,
                }
                for r in self.records
            ],
            "warnings": list(self.warnings),
        }


def explain_post_parse(
    compiled_config: dict[str, Any],
    *,
    base_path: Path | None = None,
    origin_map: Mapping[str, str] | None = None,
    metadata_only: bool = True,
) -> PostParseResult:
    """Explain how ETLProcessConfig.from_dict normalizes the compiled config.

    Args:
        compiled_config: effective process config dict (already compiled & rendered).
        base_path: base directory for resolving depends_on paths.
        origin_map: mapping from raw config dot-path -> origin label (from manifest explain).
        metadata_only: keep True for UX/CLI (no connectors).
    """

    tracer = ParseTracer(base_path=base_path)
    cfg = ETLProcessConfig.from_dict(
        dict(compiled_config),
        base_path=base_path,
        metadata_only=metadata_only,
        parse_tracer=tracer,
    )
    trace: ParseTrace = tracer.to_trace()

    normalized = _serialize_etl_process(cfg)

    # Map trace records -> include raw origin info.
    om = origin_map or {}
    recs: list[PostParseRecord] = []
    for r in trace.records:
        sources = tuple(SourceRef(path=s, origin=_origin_for_path(om, s)) for s in r.sources)
        recs.append(
            PostParseRecord(
                kind=r.kind,
                target=r.target,
                value=r.value,
                operation=r.operation,
                sources=sources,
                details=_json_safe_dict(r.details),
            )
        )

    return PostParseResult(
        base_path=trace.base_path,
        normalized=normalized,
        records=tuple(recs),
        warnings=tuple(trace.warnings),
    )


def explain_why_parsed(result: PostParseResult, query_path: str) -> ParsedWhyExplanation:
    q = str(query_path).strip()
    if not q:
        raise DagConfigurationError("--why-parsed требует непустой путь")

    canonical = _canonicalize_path(q)
    exists, value = _get_by_path(result.normalized, canonical)

    # Collect records that match this target or its nearest ancestor.
    matched: list[PostParseRecord] = []
    for r in result.records:
        if _path_relevant(canonical, _canonicalize_path(r.target)):
            matched.append(r)

    warnings: list[str] = []
    if not exists:
        warnings.append("Путь не найден в нормализованной структуре")
    if not matched:
        warnings.append("Нет trace-записей для этого пути (возможно значение не нормализуется явно)")

    return ParsedWhyExplanation(
        query=q,
        canonical_path=canonical,
        exists=exists,
        value=value,
        records=tuple(matched),
        warnings=tuple(warnings),
    )


# ------------------------- serialization -------------------------


def _serialize_etl_process(cfg: ETLProcessConfig) -> dict[str, Any]:
    lc = cfg.load_config
    return {
        "etl": {
            "name": cfg.name,
            "task_group": cfg.task_group,
            "description": cfg.description,
            "load_strategy": getattr(cfg.load_strategy, "value", cfg.load_strategy),
            "unique_key": cfg.unique_key,
        },
        "load_config": _serialize_load_config(lc),
        "dependencies": [{"path": d.path, "alias": d.alias, "group": d.group} for d in (cfg.dependencies or [])],
        "transforms": [{"name": t.name, "params": t.params} for t in (cfg.transforms or [])],
        "options": dict(cfg.options or {}),
    }


def _serialize_load_config(lc: LoadConfig) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in lc.__dict__.items():
        if hasattr(v, "value"):
            out[k] = getattr(v, "value")
        else:
            out[k] = v
    return out


# ------------------------- path helpers -------------------------


def _canonicalize_path(p: str) -> str:
    # Normalize dots and list indices spacing.
    s = str(p).strip()
    s = s.replace("..", ".")
    # remove accidental spaces around dots/brackets
    s = s.replace(" .", ".").replace(". ", ".")
    s = s.replace("[ ", "[").replace(" ]", "]")
    return s


def _parent_path(p: str) -> str:
    s = str(p)
    if not s:
        return ""
    # list index tail
    if s.endswith("]") and "[" in s:
        before = s[: s.rfind("[")]
        return before
    if "." in s:
        return s.rsplit(".", 1)[0]
    return ""


def _get_by_path(obj: Any, path: str) -> tuple[bool, Any]:
    """Get value by dot-path with list indices (best-effort)."""
    p = str(path)
    if not p:
        return True, obj

    cur = obj
    # split by dots but keep list indices
    parts: list[str] = []
    buf = ""
    in_br = 0
    for ch in p:
        if ch == "." and in_br == 0:
            parts.append(buf)
            buf = ""
            continue
        if ch == "[":
            in_br += 1
        if ch == "]":
            in_br = max(0, in_br - 1)
        buf += ch
    if buf:
        parts.append(buf)

    for part in parts:
        if not part:
            continue

        # handle list index
        if "[" in part and part.endswith("]"):
            key, idx_s = part.split("[", 1)
            idx_s = idx_s[:-1]
            if key:
                if not isinstance(cur, Mapping) or key not in cur:
                    return False, None
                cur = cur[key]
            try:
                idx = int(idx_s)
            except Exception:
                return False, None
            if not isinstance(cur, list) or idx < 0 or idx >= len(cur):
                return False, None
            cur = cur[idx]
            continue

        # mapping key
        if isinstance(cur, Mapping):
            if part not in cur:
                return False, None
            cur = cur[part]
            continue

        return False, None

    return True, cur


def _path_relevant(query: str, candidate: str) -> bool:
    """Is `candidate` relevant for explaining `query`?"""
    if not query or not candidate:
        return False
    if candidate == query:
        return True
    # Candidate is an ancestor of query
    if query.startswith(candidate + ".") or query.startswith(candidate + "["):
        return True
    # Candidate is a descendant of query
    if candidate.startswith(query + ".") or candidate.startswith(query + "["):
        return True
    return False


def _origin_for_path(origin_map: Mapping[str, str], path: str) -> str:
    """Return best-effort origin for a raw config path."""
    p = str(path).strip()
    if not p:
        return "<unknown>"
    if p in origin_map:
        return origin_map[p]
    # climb ancestors
    parent = p
    while True:
        parent = _parent_path(parent)
        if not parent:
            return origin_map.get(p, "<unknown>")
        if parent in origin_map:
            return origin_map[parent]


def _json_safe_dict(d: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, Path):
            out[str(k)] = str(v)
        else:
            out[str(k)] = v
    return out
