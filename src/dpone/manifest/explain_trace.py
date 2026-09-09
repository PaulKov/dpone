from __future__ import annotations

import copy
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.conventions import ConventionLayer
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.registry import RegistryResolution

from .explain_merge import (
    deep_merge_with_origin,
    deep_patch,
    diff,
    mark_naming_origins,
    merge_with_origins,
    normalize_list,
    record_subtree_origins,
)
from .explain_models import ExplainLayer


def find_table_spec(
    effective_manifest: Mapping[str, Any],
    selector: str,
) -> tuple[str, str, dict[str, Any], Any]:
    schemas = effective_manifest.get("schemas")
    if not isinstance(schemas, Mapping) or not schemas:
        raise ManifestConfigurationError("schemas должен быть непустым объектом")

    for src_schema, schema_block_raw in schemas.items():
        if not isinstance(schema_block_raw, Mapping):
            continue
        schema_block: dict[str, Any] = dict(schema_block_raw)

        tables = schema_block.get("tables")
        if not isinstance(tables, Sequence) or isinstance(tables, str | bytes):
            continue
        for table_spec in tables:
            tbl_name = None
            tbl_id = None
            if isinstance(table_spec, str):
                tbl_name = table_spec
            elif isinstance(table_spec, Mapping):
                tbl_name = table_spec.get("table")
                tbl_id = table_spec.get("id")
            if not isinstance(tbl_name, str) or not tbl_name:
                continue
            sel = str(tbl_id) if isinstance(tbl_id, str) and tbl_id.strip() else f"{src_schema}.{tbl_name}"
            if sel == selector:
                return str(src_schema), str(tbl_name), schema_block, table_spec

    raise ManifestConfigurationError(f"Не удалось найти table spec для selector '{selector}'")


def extract_table_parts(table_spec: Any) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], list[Any]]:
    if isinstance(table_spec, str):
        return {}, {}, {}, []
    if not isinstance(table_spec, Mapping):
        return {}, {}, {}, []

    table_vars = table_spec.get("vars") or {}
    table_naming = table_spec.get("naming") or {}
    table_overrides = table_spec.get("overrides") or {}
    table_depends = table_spec.get("depends_on") or []

    if not isinstance(table_vars, Mapping):
        table_vars = {}
    if not isinstance(table_naming, Mapping):
        table_naming = {}
    if not isinstance(table_overrides, Mapping):
        table_overrides = {}
    if not isinstance(table_depends, list):
        table_depends = []

    return dict(table_vars), dict(table_naming), dict(table_overrides), list(table_depends)


def build_vars_origin(
    *,
    raw_manifest: Mapping[str, Any],
    conventions: Sequence[ConventionLayer],
    registry: RegistryResolution | None,
    schema_block: Mapping[str, Any],
    table_spec: Any,
) -> dict[str, str]:
    origins: dict[str, str] = {}
    merged: dict[str, Any] = {}

    if registry is not None:
        merged = merge_with_origins(
            merged,
            registry.vars,
            origin=f"registry:{registry.entry_source.name}",
            origins=origins,
            prefix="",
        )

    for c in conventions:
        vars_patch = c.patch.get("vars") or {}
        if isinstance(vars_patch, Mapping) and vars_patch:
            merged = merge_with_origins(
                merged,
                dict(vars_patch),
                origin=f"convention:{c.name}",
                origins=origins,
                prefix="",
            )

    manifest_vars = raw_manifest.get("vars") or {}
    if isinstance(manifest_vars, Mapping) and manifest_vars:
        merged = merge_with_origins(
            merged,
            dict(manifest_vars),
            origin="manifest:vars",
            origins=origins,
            prefix="",
        )

    schema_vars = schema_block.get("vars") or {}
    if isinstance(schema_vars, Mapping) and schema_vars:
        merged = merge_with_origins(
            merged,
            dict(schema_vars),
            origin="schema:vars",
            origins=origins,
            prefix="",
        )

    table_vars, _, _, _ = extract_table_parts(table_spec)
    if table_vars:
        merged = merge_with_origins(
            merged,
            dict(table_vars),
            origin="table:vars",
            origins=origins,
            prefix="",
        )

    top_key_origins = {k: origins.get(k, "unknown") for k in merged.keys()}
    for k in ("env_code", "manifest_path", "manifest_dir", "manifest_name", "manifest_stem"):
        top_key_origins.setdefault(k, "built-in")
    for k in ("src_schema", "src_table"):
        top_key_origins.setdefault(k, "implicit")
    return top_key_origins


def build_naming_origin(
    *,
    raw_manifest: Mapping[str, Any],
    conventions: Sequence[ConventionLayer],
    schema_block: Mapping[str, Any],
    table_spec: Any,
) -> dict[str, str]:
    origins: dict[str, str] = {}
    merged: dict[str, Any] = {}

    for c in conventions:
        naming_patch = c.patch.get("naming") or {}
        if isinstance(naming_patch, Mapping) and naming_patch:
            merged = merge_with_origins(
                merged,
                dict(naming_patch),
                origin=f"convention:{c.name}",
                origins=origins,
                prefix="",
            )

    manifest_naming = raw_manifest.get("naming") or {}
    if isinstance(manifest_naming, Mapping) and manifest_naming:
        merged = merge_with_origins(
            merged,
            dict(manifest_naming),
            origin="manifest:naming",
            origins=origins,
            prefix="",
        )

    schema_naming = schema_block.get("naming") or {}
    if isinstance(schema_naming, Mapping) and schema_naming:
        merged = merge_with_origins(
            merged,
            dict(schema_naming),
            origin="schema:naming",
            origins=origins,
            prefix="",
        )

    _, table_naming, _, _ = extract_table_parts(table_spec)
    if table_naming:
        merged = merge_with_origins(
            merged,
            dict(table_naming),
            origin="table:naming",
            origins=origins,
            prefix="",
        )

    return {k: origins.get(k, "unknown") for k in merged.keys()}


def trace_process_config(
    *,
    compiler: BatchManifestCompiler,
    manifest_path: Path,
    effective_manifest: Mapping[str, Any],
    schema_name: str,
    table_name: str,
    selector: str,
    schema_block: Mapping[str, Any],
    table_overrides: Mapping[str, Any],
    table_depends: Sequence[Any],
    naming_ctx: Mapping[str, Any],
    naming_origin: Mapping[str, str],
    vars_ctx: Mapping[str, Any],
    include_snapshots: bool,
) -> dict[str, Any]:
    layers: list[ExplainLayer] = []
    changes: dict[str, list[Any]] = {}
    patches: dict[str, Any] = {}
    patch_removes: dict[str, list[str]] = {}
    snapshots: dict[str, dict[str, Any]] = {}
    origin: dict[str, str] = {}
    derived_fields: dict[str, str] = {}

    cfg: dict[str, Any] = {}

    def apply(layer_id: str, title: str, patch: Mapping[str, Any]) -> None:
        nonlocal cfg
        layers.append(ExplainLayer(id=layer_id, title=title))
        before = copy.deepcopy(cfg)
        cfg = deep_merge_with_origin(cfg, dict(patch), origin_label=layer_id, origin_map=origin)
        changes[layer_id] = diff(before, cfg)
        p, rm = deep_patch(before, cfg)
        patches[layer_id] = p
        patch_removes[layer_id] = rm
        if include_snapshots:
            snapshots[layer_id] = copy.deepcopy(cfg)

    root_defaults = effective_manifest.get("defaults") or {}
    if not isinstance(root_defaults, Mapping):
        raise ManifestConfigurationError("defaults должен быть объектом")
    apply("defaults", "defaults (root)", dict(root_defaults))

    schema_defaults_patch = schema_block.get("defaults") or {}
    if not isinstance(schema_defaults_patch, Mapping):
        raise ManifestConfigurationError(f"schemas.{schema_name}.defaults должен быть объектом")
    if schema_defaults_patch:
        apply(f"schema.defaults:{schema_name}", f"schema defaults ({schema_name})", dict(schema_defaults_patch))

    if table_overrides:
        apply(f"table.overrides:{selector}", f"table overrides ({selector})", dict(table_overrides))

    layers.append(ExplainLayer(id=f"inject:{selector}", title="compiler inject (source/sink defaults)"))
    before = copy.deepcopy(cfg)
    compiler._inject_source_table(cfg, src_schema=schema_name, src_table=table_name)
    compiler._inject_sink_table_shell(cfg)
    inject_changes = diff(before, cfg)
    changes[f"inject:{selector}"] = inject_changes
    p, rm = deep_patch(before, cfg)
    patches[f"inject:{selector}"] = p
    patch_removes[f"inject:{selector}"] = rm
    if include_snapshots:
        snapshots[f"inject:{selector}"] = copy.deepcopy(cfg)
    for ch in inject_changes:
        origin[ch.path] = f"inject:{selector}"

    layers.append(ExplainLayer(id=f"naming:{selector}", title="apply naming templates"))
    before = copy.deepcopy(cfg)
    compiler._apply_naming(cfg, naming_ctx, vars_ctx)
    naming_changes = diff(before, cfg)
    changes[f"naming:{selector}"] = naming_changes
    p, rm = deep_patch(before, cfg)
    patches[f"naming:{selector}"] = p
    patch_removes[f"naming:{selector}"] = rm
    if include_snapshots:
        snapshots[f"naming:{selector}"] = copy.deepcopy(cfg)
    mark_naming_origins(
        before=before,
        after=cfg,
        origin_map=origin,
        derived_fields=derived_fields,
        selector=selector,
        naming_origin=naming_origin,
    )

    if table_depends:
        layers.append(ExplainLayer(id=f"table.depends_on:{selector}", title="append table depends_on"))
        before = copy.deepcopy(cfg)
        cfg_depends = normalize_list(cfg.get("depends_on"))
        start = len(cfg_depends)
        cfg["depends_on"] = [*cfg_depends, *list(table_depends)]
        dep_changes = diff(before, cfg)
        changes[f"table.depends_on:{selector}"] = dep_changes
        p, rm = deep_patch(before, cfg)
        patches[f"table.depends_on:{selector}"] = p
        patch_removes[f"table.depends_on:{selector}"] = rm
        if include_snapshots:
            snapshots[f"table.depends_on:{selector}"] = copy.deepcopy(cfg)
        if dep_changes:
            origin["depends_on"] = f"table.depends_on:{selector}"
        for i, item in enumerate(list(table_depends), start=start):
            record_subtree_origins(
                item,
                origin_label=f"table.depends_on:{selector}",
                origin_map=origin,
                prefix=f"depends_on[{i}]",
            )

    layers.append(ExplainLayer(id=f"render:{selector}", title="render templates"))
    before = copy.deepcopy(cfg)
    render_ctx = dict(vars_ctx)
    for k, v in cfg.items():
        render_ctx.setdefault(k, v)
    rendered = compiler._renderer.render(cfg, render_ctx)
    if not isinstance(rendered, Mapping):
        raise ManifestConfigurationError("rendered process config is not a mapping")
    cfg = dict(rendered)
    changes[f"render:{selector}"] = diff(before, cfg)
    p, rm = deep_patch(before, cfg)
    patches[f"render:{selector}"] = p
    patch_removes[f"render:{selector}"] = rm
    if include_snapshots:
        snapshots[f"render:{selector}"] = copy.deepcopy(cfg)

    name = cfg.get("name")
    if not isinstance(name, str) or not name.strip():
        raise ManifestConfigurationError(f"Не удалось вычислить обязательное поле 'name' для {selector}")

    return {
        "layers": layers,
        "changes": changes,
        "patches": patches,
        "patch_removes": patch_removes,
        "snapshots": snapshots,
        "config_origin": origin,
        "derived_fields": derived_fields,
        "final_config": cfg,
    }


__all__ = [
    "find_table_spec",
    "extract_table_parts",
    "build_vars_origin",
    "build_naming_origin",
    "trace_process_config",
]
