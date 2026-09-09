from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.migration_models import BatchPlan, LegacyProcess, ProcessRef


from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.batch_merge import deep_merge
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.migration_dependencies import normalize_depends_on, relative_posix, rewrite_depends_on
from dpone.manifest.migration_diff import (
    _deepcopy_yaml,
    _drop_path,
    _ensure_required_defaults,
    _get_path,
    _strip_empty_dicts,
    deep_common_dict,
    deep_diff,
)
from dpone.manifest.migration_naming import _infer_dataset_vars_and_naming, _ratio


def build_batch_manifest(
    batch: BatchPlan,
    mapping: Mapping[Path, ProcessRef],
    *,
    infer_naming: bool,
    naming_threshold: float,
    convention: str | None = None,
    registry_paths: tuple[Path, ...] = (),
) -> dict[str, Any]:
    """Builds a dpone.batch.v1 manifest dict for a batch plan."""
    processes = list(batch.processes)
    if not processes:
        raise ManifestConfigurationError("Пустая группа для batch manifest")

    # Prepare per-process bodies (remove depends_on from body, keep separately)
    bodies: list[dict[str, Any]] = []
    depends_by_proc: dict[Path, list[dict[str, Any]]] = {}

    for proc in processes:
        body = _deepcopy_yaml(proc.raw)
        deps = normalize_depends_on(body.pop("depends_on", None))
        depends_by_proc[proc.path.resolve()] = deps
        bodies.append(body)

    # Root defaults: intersection of all process bodies
    root_defaults = deep_common_dict(bodies)

    # Ensure required root blocks exist (safety)
    root_defaults = _ensure_required_defaults(root_defaults, bodies[0], manifest_name=str(batch.out_path))

    # Schema-level defaults (optional, diff-friendly)
    schema_defaults_map: dict[str, dict[str, Any]] = {}
    bodies_by_schema: dict[str, list[dict[str, Any]]] = {}
    proc_by_schema: dict[str, list[LegacyProcess]] = {}
    for proc, body in zip(processes, bodies):
        bodies_by_schema.setdefault(proc.src_schema, []).append(body)
        proc_by_schema.setdefault(proc.src_schema, []).append(proc)

    for schema, schema_bodies in bodies_by_schema.items():
        common = deep_common_dict(schema_bodies)
        extra = deep_diff(common, root_defaults)
        extra = _strip_empty_dicts(extra)
        if extra:
            schema_defaults_map[schema] = extra

    # Naming + vars inference
    vars_block: dict[str, Any] = {}
    naming_block: dict[str, Any] = {}

    target_dataset = processes[0].target_dataset
    if infer_naming and target_dataset:
        ds_vars, ds_naming = _infer_dataset_vars_and_naming(target_dataset)
        vars_block.update(ds_vars)
        naming_block.update(ds_naming)

    if infer_naming:
        # Infer sink_table pattern
        sink_table_matches = []
        for proc in processes:
            expected = f"{proc.src_schema}__{proc.src_table}"
            sink_table_matches.append(proc.target_table == expected)
        sink_table_ratio = _ratio(sink_table_matches)
        if sink_table_ratio >= naming_threshold:
            naming_block.setdefault("sink_table", "{{ src_schema }}__{{ src_table }}")

        # Infer process_name pattern
        name_matches = []
        for proc in processes:
            expected = f"{proc.src_schema}_{proc.src_table}__{proc.strategy_mode}"
            name_matches.append(proc.spec.config.name == expected)
        name_ratio = _ratio(name_matches)
        if name_ratio >= naming_threshold:
            naming_block.setdefault("process_name", "{{ src_schema }}_{{ src_table }}__{{ sink.strategy.mode }}")

    # If dataset vars inferred, we can drop sink.table.schema from defaults and rely on naming.sink_dataset.
    if naming_block.get("sink_dataset"):
        _drop_path(root_defaults, ["sink", "table", "schema"])

    # Build schemas section
    schemas_out: dict[str, Any] = {}

    for schema in sorted(proc_by_schema.keys()):
        schema_procs = proc_by_schema[schema]
        schema_bodies = bodies_by_schema[schema]
        schema_defaults = schema_defaults_map.get(schema)

        schema_block: dict[str, Any] = {}
        if schema_defaults:
            schema_block["defaults"] = schema_defaults

        tables_out: list[Any] = []

        # schema base for diff
        schema_base = deep_merge(root_defaults, schema_defaults or {})

        for proc, body in sorted(zip(schema_procs, schema_bodies), key=lambda x: (x[0].src_table, x[0].path.name)):
            overrides = deep_diff(body, schema_base)
            overrides = _strip_empty_dicts(overrides)

            # Trim redundant sink.table.name if naming covers it and it matches expected
            if naming_block.get("sink_table"):
                expected_name = f"{proc.src_schema}__{proc.src_table}"
                actual_name = _get_path(overrides, ["sink", "table", "name"])
                if actual_name == expected_name:
                    _drop_path(overrides, ["sink", "table", "name"])

            # Trim redundant name if naming covers it and it matches expected
            if naming_block.get("process_name"):
                expected_proc_name = f"{proc.src_schema}_{proc.src_table}__{proc.strategy_mode}"
                if overrides.get("name") == expected_proc_name:
                    overrides.pop("name", None)

            # Rewrite dependencies (table-level)
            deps = depends_by_proc.get(proc.path.resolve(), [])
            deps = rewrite_depends_on(
                deps,
                current_batch_path=batch.out_path,
                mapping=mapping,
                source_manifest_dir=proc.path.parent,
            )

            # Emit table spec:
            if not overrides and not deps:
                tables_out.append(proc.src_table)
            else:
                table_obj: dict[str, Any] = {"table": proc.src_table}
                if deps:
                    table_obj["depends_on"] = deps
                if overrides:
                    table_obj["overrides"] = overrides
                tables_out.append(table_obj)

        schema_block["tables"] = tables_out
        schemas_out[schema] = schema_block

    # Build final manifest in stable key order
    out: dict[str, Any] = {"kind": "dpone.batch.v1"}
    if convention:
        out["convention"] = str(convention)

    if registry_paths:
        rels = [relative_posix(p, start=batch.out_path.parent) for p in registry_paths]
        if len(rels) == 1:
            out["registry"] = rels[0]
        else:
            out["registries"] = rels
    if vars_block:
        out["vars"] = vars_block
    if naming_block:
        out["naming"] = naming_block
    out["defaults"] = root_defaults
    out["schemas"] = schemas_out

    return out
