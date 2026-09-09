from __future__ import annotations

import copy
import re
from collections.abc import Mapping
from typing import Any

from dpone.manifest.errors import ManifestConfigurationError

from .explain_models import ExplainResult, WhyEvent, WhyExplanation
from .explain_utils import (
    canonicalize_path,
    collect_template_vars,
    get_by_path,
    origin_for_path,
    parent_path,
    path_relevant,
)


def explain_why(result: ExplainResult, query_path: str) -> WhyExplanation:
    q = str(query_path).strip()
    if not q:
        raise ManifestConfigurationError("--why требует непустой путь")

    canonical = canonicalize_path(q)

    exists, value = get_by_path(result.final_config, canonical)
    origin = origin_for_path(result.config_origin, canonical)

    events: list[WhyEvent] = []
    for layer in result.config_layers:
        for ch in result.config_changes.get(layer.id) or []:
            if path_relevant(canonical, canonicalize_path(ch.path)):
                events.append(WhyEvent(layer_id=layer.id, layer_title=layer.title, change=ch))

    naming_info = naming_why(result, canonical)

    list_items: tuple[dict[str, Any], ...] | None = None
    if exists and isinstance(value, list):
        items: list[dict[str, Any]] = []
        for i, item in enumerate(value):
            ip = f"{canonical}[{i}]"
            items.append({"index": i, "value": item, "origin": origin_for_path(result.config_origin, ip)})
        list_items = tuple(items)

    map_items: tuple[dict[str, Any], ...] | None = None
    if exists and isinstance(value, Mapping):
        rows: list[dict[str, Any]] = []
        for k in sorted(value.keys(), key=lambda x: str(x)):
            kp = f"{canonical}.{k}" if canonical else str(k)
            rows.append({"key": k, "value": value.get(k), "origin": origin_for_path(result.config_origin, kp)})
        map_items = tuple(rows)

    parent: dict[str, Any] | None = None
    ppath = parent_path(canonical)
    if ppath:
        p_exists, p_val = get_by_path(result.final_config, ppath)
        if p_exists:
            parent = {"path": ppath, "value": p_val, "origin": origin_for_path(result.config_origin, ppath)}

    timeline: tuple[dict[str, Any], ...] | None = None
    if result.config_snapshots:
        rows2: list[dict[str, Any]] = []
        for layer in result.config_layers:
            snap = result.config_snapshots.get(layer.id)
            if not isinstance(snap, Mapping):
                continue
            e, v = get_by_path(snap, canonical)
            rows2.append({"layer_id": layer.id, "layer_title": layer.title, "exists": e, "value": v})
        if not result.matches_compiler:
            e2, v2 = get_by_path(result.final_config, canonical)
            rows2.append(
                {
                    "layer_id": "final:compiler",
                    "layer_title": "final (compiler output)",
                    "exists": e2,
                    "value": v2,
                }
            )
        timeline = tuple(rows2)

    suggested_patches = suggest_patches(result, canonical, value) if exists else None

    warnings: list[str] = []
    if not exists:
        warnings.append("Путь не найден в финальном конфиге")

    return WhyExplanation(
        query=q,
        canonical_path=canonical,
        exists=exists,
        final_value=value,
        final_origin=origin,
        events=tuple(events),
        naming=naming_info,
        list_items=list_items,
        map_items=map_items,
        parent=parent,
        timeline=timeline,
        suggested_patches=suggested_patches,
        warnings=tuple(warnings),
    )


def suggest_patches(result: ExplainResult, canonical_path: str, value: Any) -> tuple[dict[str, Any], ...] | None:
    path = str(canonical_path)

    m = re.match(r"^(depends_on|transforms)\[(\d+)\](?:\..+)?$", path)
    if m:
        list_key = m.group(1)
        idx = int(m.group(2))
        root_list = result.final_config.get(list_key)
        if isinstance(root_list, list) and 0 <= idx < len(root_list):
            item = root_list[idx]
            return (
                {
                    "scope": f"table-spec:{list_key}",
                    "title": f"Добавить элемент в {list_key} на уровне table spec (append)",
                    "snippet": {list_key: [item]},
                },
            )

    if "[" in path:
        ppath = parent_path(path)
        if ppath:
            exists, parent_value = get_by_path(result.final_config, ppath)
            if exists:
                return (
                    {
                        "scope": "overrides",
                        "title": "Переопределить родительский объект (возможно потребуется заменить список целиком)",
                        "snippet": make_dot_patch(ppath, parent_value),
                    },
                )
        return None

    return (
        {
            "scope": "overrides",
            "title": "Переопределить значение через overrides",
            "snippet": make_dot_patch(path, value),
        },
    )


def make_dot_patch(path: str, value: Any) -> dict[str, Any]:
    if not path or "[" in path:
        return {}

    parts = [p for p in str(path).split(".") if p]
    out: dict[str, Any] = {}
    cur: dict[str, Any] = out
    for i, p in enumerate(parts):
        if i == len(parts) - 1:
            cur[p] = copy.deepcopy(value)
        else:
            cur[p] = {}
            cur = cur[p]
    return out


def naming_why(result: ExplainResult, path: str) -> dict[str, Any] | None:
    derived_path = None
    naming_key = None

    current = path
    while current:
        if current in result.derived_fields:
            derived_path = current
            naming_key = result.derived_fields[current]
            break
        next_path = parent_path(current)
        if not next_path or next_path == current:
            break
        current = next_path

    if not derived_path or not naming_key:
        return None

    tmpl = result.naming.get(naming_key)
    tmpl_origin = result.naming_origin.get(naming_key, "unknown")

    tmpl_fragment = tmpl
    if naming_key == "labels" and isinstance(tmpl, Mapping) and path.startswith("sink.options.table_labels."):
        label_key = path.split(".")[-1]
        tmpl_fragment = tmpl.get(label_key)

    used_vars = collect_template_vars(tmpl_fragment)
    vars_rows: list[dict[str, Any]] = []
    for var_name in used_vars:
        if var_name in result.vars:
            vars_rows.append(
                {"var": var_name, "value": result.vars.get(var_name), "origin": result.vars_origin.get(var_name, "?")}
            )
        elif isinstance(result.final_config, Mapping) and var_name in result.final_config:
            vars_rows.append({"var": var_name, "value": result.final_config.get(var_name), "origin": "config"})
        else:
            vars_rows.append({"var": var_name, "value": None, "origin": "unknown"})

    return {
        "derived_from_path": derived_path,
        "naming_key": naming_key,
        "template": tmpl_fragment,
        "template_origin": tmpl_origin,
        "referenced_vars": vars_rows,
    }


__all__ = ["explain_why", "suggest_patches", "make_dot_patch", "naming_why"]
