from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from dpone.output import dumps_yaml
from dpone.output_table import render_table

from .common import HR, compact_yaml


def render_compilation_provenance(compiled_origins: Sequence[Any]) -> str:
    rows = [
        [getattr(o, "path", None), getattr(o, "origin", None), getattr(o, "origin_title", None) or "-"]
        for o in compiled_origins
    ]
    return "\n".join(
        [
            "\nCompilation provenance:",
            render_table(["raw_path", "origin", "origin_title"], rows),
        ]
    )


def render_normalization_trace(parse_records: Sequence[dict]) -> str:
    if not parse_records:
        return ""

    rows = []
    for r in parse_records:
        srcs = ",".join([f"{s.get('path')}@{s.get('origin')}" for s in (r.get("sources") or [])])
        rows.append([r.get("target"), r.get("operation"), srcs])

    return "\n".join(
        [
            "\nNormalization trace:",
            render_table(["target", "operation", "sources"], rows),
        ]
    )


def render_edge_match(edges: Sequence[Any]) -> str:
    """Render ProducedEdge list from DirectDependencyExplanation."""

    if not edges:
        return "\nEdge match: (no match)"

    rows = []
    for e in edges:
        rows.append(
            [
                getattr(e, "upstream", None),
                getattr(e, "upstream_ref", None) or "-",
                ",".join(getattr(e, "reason_kinds", ()) or ()) or "-",
            ]
        )

    parts: list[str] = [
        "\nEdge match:",
        render_table(["upstream", "up_ref", "reason_kinds"], rows),
    ]

    # De-duplicate reasons across edges (UX: do not spam)
    printed: set[tuple[str, str]] = set()
    for e in edges:
        for rr in getattr(e, "reasons", ()) or ():
            k = str(rr.get("kind"))
            d = str(rr.get("description"))
            key = (k, d)
            if key in printed:
                continue
            printed.add(key)
            parts.append(f"\n[{k}] {d}")
            ev = rr.get("evidence")
            if ev:
                parts.append(dumps_yaml(ev, sort_keys=True).rstrip())

    return "\n".join(parts)


def render_direct_dependency_explanations(items: Iterable[Any]) -> str:
    """Render a list of DirectDependencyExplanation objects."""

    lines: list[str] = []
    for d in items or ():
        lines.append("\n" + HR)
        lines.append(f"depends_on[{d.index}]: {compact_yaml(d.compiled_item)}")

        for w in getattr(d, "warnings", ()) or ():
            lines.append(f"WARN: {w}")

        lines.append(render_compilation_provenance(getattr(d, "compiled_origins", ()) or ()))

        if getattr(d, "parsed_dependency", None) is not None:
            lines.append("\nPost-parse normalized dependency:")
            lines.append(dumps_yaml(d.parsed_dependency, sort_keys=True).rstrip())

        nt = render_normalization_trace(getattr(d, "parse_records", ()) or ())
        if nt:
            lines.append(nt)

        lines.append(render_edge_match(getattr(d, "edges", ()) or ()))

    return "\n".join(lines).lstrip("\n")


def render_group_to_group_attributions(items: Iterable[Any]) -> str:
    """Render a list of GroupToGroupAttribution objects."""

    lines: list[str] = []

    for g in items or ():
        lines.append("\n" + HR)
        lines.append(f"Group '{g.dependent_group}' depends on '{g.upstream_group}'")

        for w in getattr(g, "warnings", ()) or ():
            lines.append(f"WARN: {w}")

        lines.append(f"Triggers in dependent group: {g.trigger_count}")

        triggers = list(getattr(g, "triggers", ()) or ())
        if not triggers:
            continue

        # Trigger summary table (first N already limited by caller)
        rows = []
        for tr in triggers:
            origin = tr.compiled_origins[0].origin if getattr(tr, "compiled_origins", None) else "unknown"
            rows.append(
                [
                    tr.trigger_task.get("name"),
                    tr.depends_on_index,
                    Path(str(tr.trigger_task.get("config_path") or "-")).name,
                    origin,
                ]
            )
        lines.append("\nTriggers (first N):")
        lines.append(render_table(["trigger_task", "dep_index", "manifest", "origin"], rows))

        # Trigger details
        for tr in triggers:
            lines.append(f"\nTrigger {tr.trigger_task.get('name')} depends_on[{tr.depends_on_index}]:")
            if tr.compiled_item is not None:
                lines.append(dumps_yaml(tr.compiled_item, sort_keys=True).rstrip())

            lines.append(render_compilation_provenance(getattr(tr, "compiled_origins", ()) or ()))

            if getattr(tr, "parsed_dependency", None) is not None:
                lines.append("\nPost-parse normalized dependency:")
                lines.append(dumps_yaml(tr.parsed_dependency, sort_keys=True).rstrip())

            nt = render_normalization_trace(getattr(tr, "parse_records", ()) or ())
            if nt:
                lines.append(nt)

            for w in getattr(tr, "warnings", ()) or ():
                lines.append(f"WARN: {w}")

    return "\n".join(lines).lstrip("\n")
