"""Manifest explain / provenance facade.

This module keeps the public API stable while delegating implementation to
smaller helpers:
- explain_models.py  -> dataclasses / JSON payloads
- explain_io.py      -> YAML/process selection helpers
- explain_trace.py   -> batch tracing + provenance construction
- explain_merge.py   -> deep diff/patch + origin-aware merge
- explain_utils.py   -> path/value utilities
- explain_why.py     -> --why helpers / patch suggestions
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.config.env import SOURCES_REGISTRY_PATHS
from dpone.manifest.batch_compiler_impl import BatchManifestCompiler
from dpone.manifest.batch_merge import deep_merge
from dpone.manifest.batch_rendering import TemplateRenderer
from dpone.manifest.conventions import resolve_convention_layers
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.registry import resolve_registry

from .explain_io import read_yaml, select_process
from .explain_merge import deep_patch
from .explain_models import ExplainChange, ExplainLayer, ExplainResult, WhyEvent, WhyExplanation
from .explain_trace import (
    build_naming_origin,
    build_vars_origin,
    extract_table_parts,
    find_table_spec,
    trace_process_config,
)
from .explain_utils import fill_missing_origins, iter_leaf_paths, safe_equal
from .explain_why import explain_why


def explain_manifest(
    path: Path,
    *,
    selector: str | None = None,
    registry_paths: Sequence[Path] = (),
    metadata_only: bool = True,
    include_snapshots: bool = False,
) -> ExplainResult:
    raw = read_yaml(path)
    kind = str(raw.get("kind") or "").strip()

    cli_registry_paths = tuple(Path(p) for p in registry_paths if str(p))
    loader = ManifestLoaderRouter(registry_paths=cli_registry_paths)
    effective_registry_paths = tuple(SOURCES_REGISTRY_PATHS) + tuple(cli_registry_paths)
    loaded = loader.load(path, metadata_only=metadata_only)

    spec = select_process(loaded.processes, selector)

    if kind != "dpone.batch.v1":
        lc = spec.config.load_config
        src = f"{lc.source_schema}.{lc.source_table}" if lc.source_schema and lc.source_table else None
        snk = f"{lc.target_schema}.{lc.target_table}" if lc.target_schema and lc.target_table else None
        origin = {p: "manifest" for p in iter_leaf_paths(spec.raw_config)}
        return ExplainResult(
            manifest_path=path,
            kind=loaded.source_kind or loaded.kind,
            selector=spec.selector,
            process_name=spec.name,
            task_group=spec.task_group,
            source=src,
            sink=snk,
            conventions=(),
            registry=None,
            vars={},
            vars_origin={},
            naming={},
            naming_origin={},
            derived_fields={},
            config_layers=(ExplainLayer(id="manifest", title="Legacy manifest"),),
            config_changes={"manifest": []},
            config_patches={"manifest": dict(spec.raw_config)},
            config_patch_removes={"manifest": []},
            config_origin=origin,
            final_config=dict(spec.raw_config),
            matches_compiler=True,
            config_snapshots={"manifest": dict(spec.raw_config)} if include_snapshots else None,
        )

    conventions = tuple(resolve_convention_layers(raw, manifest_path=path))
    effective_manifest = dict(loaded.raw)
    registry_res = resolve_registry(
        effective_manifest,
        manifest_path=path,
        extra_registry_paths=effective_registry_paths,
    )

    target_selector = spec.selector or spec.name
    schema_name, table_name, schema_block, table_spec = find_table_spec(effective_manifest, target_selector)

    compiler = BatchManifestCompiler(renderer=TemplateRenderer())

    root_vars = compiler._merge_vars({}, effective_manifest.get("vars") or {}, manifest_path=path)
    schema_vars = compiler._merge_vars(root_vars, schema_block.get("vars") or {}, manifest_path=path)

    table_vars, table_naming, table_overrides, table_depends = extract_table_parts(table_spec)

    vars_ctx = compiler._merge_vars(
        schema_vars,
        table_vars,
        manifest_path=path,
        extra={"src_schema": schema_name, "src_table": table_name},
    )

    root_naming = compiler._merge_dict_templates(effective_manifest.get("naming") or {})
    schema_naming = deep_merge(root_naming, compiler._merge_dict_templates(schema_block.get("naming") or {}))
    naming_ctx = deep_merge(schema_naming, table_naming)

    vars_origin = build_vars_origin(
        raw_manifest=raw,
        conventions=conventions,
        registry=registry_res,
        schema_block=schema_block,
        table_spec=table_spec,
    )
    naming_origin = build_naming_origin(
        raw_manifest=raw,
        conventions=conventions,
        schema_block=schema_block,
        table_spec=table_spec,
    )

    trace = trace_process_config(
        compiler=compiler,
        manifest_path=path,
        effective_manifest=effective_manifest,
        schema_name=schema_name,
        table_name=table_name,
        selector=target_selector,
        schema_block=schema_block,
        table_overrides=table_overrides,
        table_depends=table_depends,
        naming_ctx=naming_ctx,
        naming_origin=naming_origin,
        vars_ctx=vars_ctx,
        include_snapshots=include_snapshots,
    )

    final_config = dict(spec.raw_config)
    warnings: list[str] = []
    matches = safe_equal(trace["final_config"], final_config)
    if not matches:
        warnings.append(
            "Explain reconstruction differs from compiler output; showing compiler output as final_config. "
            "This can happen with advanced templating (expression-only dict/list returns)."
        )

    config_origin = fill_missing_origins(final_config, dict(trace["config_origin"]))

    lc = spec.config.load_config
    src = f"{lc.source_schema}.{lc.source_table}" if lc.source_schema and lc.source_table else None
    snk = f"{lc.target_schema}.{lc.target_table}" if lc.target_schema and lc.target_table else None

    return ExplainResult(
        manifest_path=path,
        kind=loaded.source_kind or loaded.kind,
        selector=spec.selector,
        process_name=spec.name,
        task_group=spec.task_group,
        source=src,
        sink=snk,
        conventions=conventions,
        registry=registry_res,
        vars=dict(vars_ctx),
        vars_origin=vars_origin,
        naming=dict(naming_ctx),
        naming_origin=naming_origin,
        derived_fields=dict(trace["derived_fields"]),
        config_layers=tuple(trace["layers"]),
        config_changes=dict(trace["changes"]),
        config_patches=dict(trace["patches"]),
        config_patch_removes=dict(trace["patch_removes"]),
        config_origin=config_origin,
        final_config=final_config,
        matches_compiler=matches,
        warnings=tuple(warnings),
        config_snapshots=dict(trace["snapshots"]) if include_snapshots else None,
    )


def compute_deep_patch(before: Mapping[str, Any], after: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
    patch, removed = deep_patch(dict(before), dict(after))
    if not isinstance(patch, Mapping):
        return {}, removed
    return dict(patch), removed


__all__ = [
    "ExplainChange",
    "ExplainLayer",
    "ExplainResult",
    "WhyEvent",
    "WhyExplanation",
    "explain_manifest",
    "compute_deep_patch",
    "explain_why",
]
