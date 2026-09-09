from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

import yaml

from dpone.output import dumps_yaml
from dpone.output_table import render_table

HR = "-" * 80
HR2 = "=" * 80


def compact_yaml(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, str | int | float | bool):
        return json.dumps(value, ensure_ascii=False)
    s = yaml.safe_dump(value, sort_keys=True, allow_unicode=True).strip()
    return " ".join(line.strip() for line in s.splitlines())


def get_by_dot(obj: Any, path: str) -> Any:
    cur = obj
    for part in str(path).split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return None
        cur = cur[part]
    return cur


def render_warning_lines(warnings: Iterable[str]) -> str:
    return "\n".join(f"WARN: {w}" for w in warnings or ())


def render_post_parse_text(post_parse: Any, *, max_lines: int, max_items: int) -> str:
    try:
        base_path = getattr(post_parse, "base_path", None)
        normalized = getattr(post_parse, "normalized", {}) or {}
        records = list(getattr(post_parse, "records", ()) or [])
        warnings = list(getattr(post_parse, "warnings", ()) or [])
    except Exception:
        return "\nPost-parse: <failed to render>\n"

    lines: list[str] = ["\n" + HR2, "Post-parse normalization (ETLProcessConfig.from_dict)"]
    if base_path:
        lines.append(f"Base path: {base_path}")
    if warnings:
        lines.extend(f"WARN: {w}" for w in warnings)

    etl = normalized.get("etl") if isinstance(normalized, Mapping) else None
    if isinstance(etl, Mapping):
        lines.append("\nNormalized ETL:")
        for key in ("name", "task_group", "load_strategy", "unique_key"):
            if key in etl:
                lines.append(f"  {key}: {compact_yaml(etl.get(key))}")

    records_sorted = sorted(records, key=lambda r: (str(getattr(r, "kind", "")), str(getattr(r, "target", ""))))
    etl_rows = [r for r in records_sorted if getattr(r, "kind", "") == "etl.field"]
    load_rows = [r for r in records_sorted if getattr(r, "kind", "") == "load_config.field"]
    opt_rows = [r for r in records_sorted if getattr(r, "kind", "") == "option.key"]

    def _render_trace_table(title: str, rows: list[Any]) -> None:
        if not rows:
            return
        table_rows = []
        for record in rows[:max_lines]:
            sources = getattr(record, "sources", ()) or ()
            src_text = ", ".join(f"{s.path} ({s.origin})" for s in sources)
            table_rows.append(
                [
                    getattr(record, "target", "?"),
                    compact_yaml(getattr(record, "value", None)),
                    getattr(record, "operation", "?"),
                    src_text,
                ]
            )
        lines.append(f"\n{title}:")
        lines.append(render_table(["target", "value", "op", "from (origin)"], table_rows))
        if len(rows) > max_lines:
            lines.append(f"... (truncated after {max_lines} lines)")

    _render_trace_table("ETL fields", etl_rows)
    _render_trace_table("LoadConfig fields", load_rows)
    _render_trace_table("LoadConfig.options (merged)", opt_rows)

    deps_norm = normalized.get("dependencies") if isinstance(normalized, Mapping) else None
    if isinstance(deps_norm, list) and deps_norm:
        dep_records = {
            str(getattr(r, "target", "")): r
            for r in records_sorted
            if getattr(r, "kind", "") == "dependency"
            and str(getattr(r, "target", "")).startswith("dependencies[")
            and not str(getattr(r, "target", "")).endswith(".raw")
        }
        dep_rows = []
        for idx, dep in enumerate(deps_norm[:max_items]):
            record = dep_records.get(f"dependencies[{idx}]")
            op = getattr(record, "operation", "-") if record else "-"
            sources = getattr(record, "sources", ()) if record else ()
            src_text = ", ".join(f"{s.path} ({s.origin})" for s in (sources or ()))
            dep_rows.append([idx, compact_yaml(dep), op, src_text])
        lines.append("\nDependencies (normalized):")
        lines.append(render_table(["idx", "normalized", "trace op", "from (origin)"], dep_rows))
        if len(deps_norm) > max_items:
            lines.append(f"... (truncated after {max_items} items)")

    return "\n".join(lines).rstrip() + "\n"


def render_yaml_snippet(title: str, obj: Any) -> str:
    return f"{title}\n{dumps_yaml(obj, sort_keys=True).rstrip()}"
