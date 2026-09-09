from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestExplainView


from collections.abc import Mapping
from typing import Any

import yaml

from dpone.manifest.errors import ManifestConfigurationError
from dpone.output_table import render_table

from .common import HR2, compact_yaml, get_by_dot, render_post_parse_text


def render_manifest_explain_text(view: ManifestExplainView, *, max_lines: int, max_items: int) -> str:
    res = view.explain
    lines: list[str] = [f"Manifest: {res.manifest_path} ({res.kind})"]
    if res.selector:
        lines.append(f"Process:  {res.selector} -> {res.process_name}")
    else:
        lines.append(f"Process:  {res.process_name}")
    if res.task_group:
        lines.append(f"Group:    {res.task_group}")
    if res.source:
        lines.append(f"Source:   {res.source}")
    if res.sink:
        lines.append(f"Sink:     {res.sink}")
    if res.conventions:
        lines.append("Conventions:")
        for convention in res.conventions:
            lines.append(f"  - {convention.name} ({convention.source})")
    if res.registry:
        registry = res.registry
        key = f"{registry.matched_key[0]}::{registry.matched_key[1]}"
        lines.append(f"Registry: {registry.entry_source} (key {key})")
    if res.warnings:
        lines.extend(f"WARN: {w}" for w in res.warnings)

    did_why = False

    if view.why:
        for why in view.why:
            lines.extend(_render_why_block(why, max_lines=max_lines, max_items=max_items))
        did_why = True

    if view.why_parsed:
        for why in view.why_parsed:
            lines.extend(_render_why_parsed_block(why, max_lines=max_lines))
        did_why = True

    if did_why:
        if view.post_parse is not None and bool(view.meta.options.get("post_parse")):
            lines.append(render_post_parse_text(view.post_parse, max_lines=max_lines, max_items=max_items).rstrip())
        return "\n".join(lines).rstrip() + "\n"

    if bool(view.meta.options.get("patches")) or bool(view.meta.options.get("patch_from")):
        lines.append("\nPatches:")
        snaps = res.config_snapshots or {}

        patch_from = view.meta.options.get("patch_from")
        if patch_from:
            patch_to = str(view.meta.options.get("patch_to") or "final:compiler")
            before_cfg = _get_snapshot(res, snaps, str(patch_from))
            after_cfg = _get_snapshot(res, snaps, patch_to)
            from dpone.manifest.explain import compute_deep_patch

            patch, removed = compute_deep_patch(before_cfg, after_cfg)
            lines.append(f"\nPatch from '{patch_from}' -> '{patch_to}':")
            if removed:
                lines.append(f"# WARN: removed paths cannot be expressed by deep-merge: {removed}")
            if patch:
                lines.append(yaml.safe_dump(patch, sort_keys=True, allow_unicode=True).rstrip())
            else:
                lines.append("# (empty patch)")

        if bool(view.meta.options.get("patches")):
            mode = str(view.meta.options.get("patches_mode") or "user")
            for layer in res.config_layers:
                if mode == "user" and not _is_user_layer(layer.id):
                    continue
                patch = res.config_patches.get(layer.id)
                if not patch:
                    continue
                removed = res.config_patch_removes.get(layer.id) or []
                lines.append(f"\n[{layer.id}] {layer.title}")
                if removed:
                    lines.append(f"# WARN: removed paths cannot be expressed by deep-merge: {removed}")
                lines.append(yaml.safe_dump(patch, sort_keys=True, allow_unicode=True).rstrip())

    if res.vars:
        rows = [[k, compact_yaml(res.vars.get(k)), res.vars_origin.get(k, "?")] for k in sorted(res.vars)]
        lines.append("\nVars:")
        lines.append(render_table(["var", "value", "origin"], rows))

    if res.naming:
        rows = [[k, compact_yaml(res.naming.get(k)), res.naming_origin.get(k, "?")] for k in sorted(res.naming)]
        lines.append("\nNaming:")
        lines.append(render_table(["naming", "template", "origin"], rows))

    if res.derived_fields:
        rows = []
        for path in sorted(res.derived_fields):
            rows.append(
                [
                    path,
                    compact_yaml(get_by_dot(res.final_config, path)),
                    res.derived_fields[path],
                    res.config_origin.get(path, "?"),
                ]
            )
        lines.append("\nDerived:")
        lines.append(render_table(["config path", "value", "derived from", "origin"], rows))

    if bool(view.meta.options.get("item_provenance")):
        for list_key in ("depends_on", "transforms"):
            value = res.final_config.get(list_key)
            if not isinstance(value, list) or not value:
                continue
            rows = []
            for idx, item in enumerate(value[:max_items]):
                rows.append([idx, res.config_origin.get(f"{list_key}[{idx}]", "?"), compact_yaml(item)])
            lines.append(f"\nItem-level provenance: {list_key}")
            lines.append(render_table(["index", "origin", list_key], rows))
            if len(value) > max_items:
                lines.append(f"... (truncated after {max_items} items)")

    only_prefixes = tuple(str(p) for p in (view.meta.options.get("only") or ()) if str(p).strip())
    lines.append("\nConfig changes by layer:")
    printed = 0
    for layer in res.config_layers:
        changes = list(res.config_changes.get(layer.id) or ())
        if only_prefixes:
            changes = [c for c in changes if any(c.path.startswith(prefix) for prefix in only_prefixes)]
        if not changes:
            continue
        lines.append(f"\n[{layer.id}] {layer.title}")
        for change in changes:
            if printed >= max_lines:
                lines.append(f"... (truncated after {max_lines} lines)")
                return "\n".join(lines).rstrip() + "\n"
            printed += 1
            lines.append(_format_change(change.kind, change.path, change.before, change.after))

    if view.post_parse is not None and bool(view.meta.options.get("post_parse")):
        lines.append(render_post_parse_text(view.post_parse, max_lines=max_lines, max_items=max_items).rstrip())

    return "\n".join(lines).rstrip() + "\n"


def _render_why_block(why: Any, *, max_lines: int, max_items: int) -> list[str]:
    lines: list[str] = ["\n" + HR2, f"WHY: {why.query}"]
    if why.canonical_path != why.query:
        lines.append(f"Path: {why.canonical_path}")
    if why.warnings:
        lines.extend(f"WARN: {warn}" for warn in why.warnings)

    lines.append(f"Origin: {why.final_origin}")
    lines.append(f"Value:  {compact_yaml(why.final_value)}")

    parent = why.parent or {}
    if parent.get("path") and parent.get("path") != why.canonical_path:
        lines.append("\nParent context:")
        lines.append(f"  {parent.get('path')} (origin {parent.get('origin')})")
        lines.append(f"  value: {compact_yaml(parent.get('value'))}")

    if why.timeline:
        rows = [[row.get("layer_id"), row.get("exists"), compact_yaml(row.get("value"))] for row in why.timeline]
        lines.append("\nTimeline:")
        lines.append(render_table(["layer", "exists", "value"], rows))

    if why.suggested_patches:
        lines.append("\nSuggested patches:")
        for patch in why.suggested_patches:
            title = patch.get("title") or "patch"
            scope = patch.get("scope") or "?"
            lines.append(f"- {title} [{scope}]")
            snippet = patch.get("snippet")
            if snippet is not None:
                lines.append(yaml.safe_dump(snippet, sort_keys=True, allow_unicode=True).rstrip())

    naming = why.naming or {}
    if naming:
        lines.append("\nDerived from naming:")
        lines.append(f"  naming.{naming.get('naming_key')} (origin {naming.get('template_origin')})")
        if naming.get("derived_from_path") and naming.get("derived_from_path") != why.canonical_path:
            lines.append(f"  (inherited from {naming.get('derived_from_path')})")
        lines.append(f"  template: {compact_yaml(naming.get('template'))}")
        refs = naming.get("referenced_vars") or []
        if refs:
            rows = [[row.get("var"), compact_yaml(row.get("value")), row.get("origin")] for row in refs]
            lines.append("\n  referenced vars:")
            lines.append(render_table(["var", "value", "origin"], rows))

    if why.list_items:
        rows = []
        for row in list(why.list_items)[:max_items]:
            rows.append([row.get("index"), row.get("origin"), compact_yaml(row.get("value"))])
        lines.append("\nList items provenance:")
        lines.append(render_table(["index", "origin", "item"], rows))
        if len(why.list_items) > max_items:
            lines.append(f"... (truncated after {max_items} items)")

    if why.map_items:
        rows = []
        for row in list(why.map_items)[:max_items]:
            rows.append([row.get("key"), row.get("origin"), compact_yaml(row.get("value"))])
        lines.append("\nMapping items provenance:")
        lines.append(render_table(["key", "origin", "value"], rows))
        if len(why.map_items) > max_items:
            lines.append(f"... (truncated after {max_items} items)")

    if why.events:
        lines.append("\nHistory (relevant config changes):")
        current = None
        printed = 0
        for event in why.events:
            if current != event.layer_id:
                current = event.layer_id
                lines.append(f"\n[{event.layer_id}] {event.layer_title}")
            if printed >= max_lines:
                lines.append(f"... (truncated after {max_lines} lines)")
                break
            printed += 1
            change = event.change
            lines.append(_format_change(change.kind, change.path, change.before, change.after))

    return lines


def _render_why_parsed_block(why: Any, *, max_lines: int) -> list[str]:
    lines: list[str] = ["\n" + HR2, f"WHY (parsed): {why.query}"]
    if why.canonical_path != why.query:
        lines.append(f"Path: {why.canonical_path}")
    if why.warnings:
        lines.extend(f"WARN: {warn}" for warn in why.warnings)
    lines.append(f"Value:  {compact_yaml(why.value)}")
    if why.records:
        rows = []
        for record in why.records[:max_lines]:
            srcs = ", ".join(f"{s.path} ({s.origin})" for s in record.sources)
            rows.append([record.target, record.operation, srcs])
        lines.append("\nTrace records:")
        lines.append(render_table(["target", "operation", "sources"], rows))
    return lines


def _get_snapshot(res: Any, snaps: Mapping[str, Any], layer_id: str) -> Mapping[str, Any]:
    if layer_id in ("final", "final:compiler"):
        return res.final_config
    if layer_id in ("initial", "empty"):
        return {}
    if layer_id in snaps:
        return snaps[layer_id]
    raise ManifestConfigurationError(
        f"Неизвестный layer id '{layer_id}'. Доступные: {list(snaps.keys())} + [initial, final:compiler]"
    )


def _is_user_layer(layer_id: str) -> bool:
    return (
        layer_id == "defaults"
        or layer_id.startswith("schema.defaults:")
        or layer_id.startswith("table.overrides:")
        or layer_id.startswith("table.depends_on:")
    )


def _format_change(kind: str, path: str, before: Any, after: Any) -> str:
    if kind in {"append", "add"}:
        return f"  + {path} = {compact_yaml(after)}"
    if kind == "remove":
        return f"  - {path}"
    return f"  ~ {path}: {compact_yaml(before)} -> {compact_yaml(after)}"
