from __future__ import annotations

import copy
from collections.abc import Mapping, MutableMapping, Sequence
from pathlib import Path
from typing import Any

from dpone.config import ENV_CODE
from dpone.governance.quality import QualityGatePolicy
from dpone.manifest.airflow_resources import reject_process_airflow_resources, reject_process_resource_declaration
from dpone.manifest.batch_dependencies import _normalize_depends_on, _validate_unique_names
from dpone.manifest.batch_merge import deep_merge
from dpone.manifest.batch_models import _RESERVED_VARS, CompiledProcess
from dpone.manifest.batch_rendering import TemplateRenderer
from dpone.manifest.errors import ManifestConfigurationError


class BatchManifestCompiler:
    """Compiles dpone.batch.v1 into many dpone.single-like process dicts."""

    def __init__(
        self,
        *,
        renderer: TemplateRenderer | None = None,
    ) -> None:
        self._renderer = renderer or TemplateRenderer()

    def compile(self, raw: Mapping[str, Any], *, manifest_path: Path) -> list[CompiledProcess]:
        kind = str(raw.get("kind") or "").strip()
        if kind != "dpone.batch.v1":
            raise ManifestConfigurationError(
                f"Неверный kind для batch manifest: '{kind}'. Ожидалось 'dpone.batch.v1' ({manifest_path})"
            )

        reject_process_airflow_resources(raw)
        root_vars = self._merge_vars({}, raw.get("vars") or {}, manifest_path=manifest_path)
        root_naming = self._merge_dict_templates(raw.get("naming") or {})
        root_defaults = self._ensure_dict(raw.get("defaults") or {}, "defaults", manifest_path)
        self._ensure_quality_mapping(root_defaults, "defaults.quality", manifest_path)
        root_quality: dict[str, Any] | None = None
        if "quality" in raw:
            root_quality = self._quality_mapping(raw["quality"], "quality", manifest_path)
            root_defaults = {**root_defaults, "quality": root_quality}

        schemas = raw.get("schemas")
        if not isinstance(schemas, Mapping) or not schemas:
            raise ManifestConfigurationError(f"schemas должен быть непустым объектом ({manifest_path})")

        compiled: list[CompiledProcess] = []

        for src_schema, schema_block_raw in schemas.items():
            schema_block = self._ensure_dict(schema_block_raw, f"schemas.{src_schema}", manifest_path)

            schema_vars = self._merge_vars(
                root_vars,
                schema_block.get("vars") or {},
                manifest_path=manifest_path,
            )
            schema_naming = deep_merge(root_naming, self._merge_dict_templates(schema_block.get("naming") or {}))
            schema_default_overrides = self._ensure_dict(
                schema_block.get("defaults") or {},
                f"schemas.{src_schema}.defaults",
                manifest_path,
            )
            self._ensure_quality_mapping(
                schema_default_overrides,
                f"schemas.{src_schema}.defaults.quality",
                manifest_path,
            )
            schema_defaults = deep_merge(root_defaults, schema_default_overrides)
            if root_quality is not None:
                schema_defaults["quality"] = copy.deepcopy(root_quality)
            elif "quality" in schema_default_overrides:
                schema_defaults["quality"] = copy.deepcopy(schema_default_overrides["quality"])

            tables = schema_block.get("tables")
            if not isinstance(tables, Sequence) or not tables:
                raise ManifestConfigurationError(
                    f"schemas.{src_schema}.tables должен быть непустым массивом ({manifest_path})"
                )

            for table_spec in tables:
                table_name, table_vars, table_naming, table_overrides, table_depends, table_id = self._parse_table_spec(
                    table_spec, manifest_path, src_schema=str(src_schema)
                )
                self._ensure_quality_mapping(
                    table_overrides,
                    f"schemas.{src_schema}.tables.{table_name}.overrides.quality",
                    manifest_path,
                )

                vars_ctx = self._merge_vars(
                    schema_vars,
                    table_vars,
                    manifest_path=manifest_path,
                    extra={
                        "src_schema": str(src_schema),
                        "src_table": str(table_name),
                    },
                )
                naming_ctx = deep_merge(schema_naming, table_naming)

                # 1) merge process fragments (defaults -> schema defaults -> table overrides)
                cfg = deep_merge(schema_defaults, table_overrides)
                if "quality" in table_overrides:
                    cfg["quality"] = copy.deepcopy(table_overrides["quality"])
                # deep_merge performs a shallow copy for unchanged branches. We
                # isolate each table config to avoid cross-table mutation during
                # source/sink shell injection and naming application.
                cfg = copy.deepcopy(self._ensure_dict(cfg, "process", manifest_path))

                # 2) inject source/sink table defaults
                self._inject_source_table(cfg, src_schema=str(src_schema), src_table=str(table_name))
                self._inject_sink_table_shell(cfg)

                # 3) apply naming templates to missing fields
                self._apply_naming(cfg, naming_ctx, vars_ctx)

                # 4) merge explicit depends_on from table spec (additive)
                if table_depends:
                    cfg_depends = _normalize_depends_on(cfg.get("depends_on"))
                    cfg["depends_on"] = [*cfg_depends, *table_depends]

                # 5) render templates inside the full process config
                render_ctx = dict(vars_ctx)
                # expose config blocks to templates (sink.strategy.mode etc.)
                for k, v in cfg.items():
                    if k not in render_ctx:
                        render_ctx[k] = v
                cfg = self._renderer.render(cfg, render_ctx)
                cfg = self._ensure_dict(cfg, "process_rendered", manifest_path)
                reject_process_resource_declaration(cfg, field=f"compiled_processes[{len(compiled)}]")
                if "quality" in cfg:
                    cfg["quality"] = self._validated_quality(
                        cfg["quality"],
                        f"process {src_schema}.{table_name}.quality",
                        manifest_path,
                    )

                # 6) sanity checks for required fields
                name = cfg.get("name")
                if not isinstance(name, str) or not name.strip():
                    raise ManifestConfigurationError(
                        f"Не удалось вычислить обязательное поле 'name' для {manifest_path}::{src_schema}.{table_name}"
                    )

                selector = str(table_id or f"{src_schema}.{table_name}")

                compiled.append(
                    CompiledProcess(
                        name=name,
                        selector=selector,
                        raw_config=dict(cfg),
                    )
                )

        # Validate uniqueness of process names inside a batch manifest
        _validate_unique_names(compiled, manifest_path)

        return compiled

    # ------------------------- helpers -------------------------

    def _parse_table_spec(
        self,
        table_spec: Any,
        manifest_path: Path,
        *,
        src_schema: str,
    ) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any], list[Any], str | None]:
        if isinstance(table_spec, str):
            return table_spec, {}, {}, {}, [], None

        if not isinstance(table_spec, Mapping):
            raise ManifestConfigurationError(
                f"Неверный формат table spec в schemas.{src_schema}: ожидается строка или объект ({manifest_path})"
            )

        table_name = table_spec.get("table")
        if not isinstance(table_name, str) or not table_name:
            raise ManifestConfigurationError(
                f"table spec должен содержать непустое поле 'table' в schemas.{src_schema} ({manifest_path})"
            )

        table_vars = self._ensure_dict(table_spec.get("vars") or {}, "vars", manifest_path)
        table_naming = self._merge_dict_templates(table_spec.get("naming") or {})
        table_overrides = self._ensure_dict(table_spec.get("overrides") or {}, "overrides", manifest_path)
        table_depends_raw = table_spec.get("depends_on") or []
        if not isinstance(table_depends_raw, list):
            raise ManifestConfigurationError(
                f"depends_on должен быть массивом в table spec {src_schema}.{table_name} ({manifest_path})"
            )
        table_id = table_spec.get("id")
        if table_id is not None and not isinstance(table_id, str):
            raise ManifestConfigurationError(
                f"table spec.id должен быть строкой в {src_schema}.{table_name} ({manifest_path})"
            )

        return (
            table_name,
            dict(table_vars),
            dict(table_naming),
            dict(table_overrides),
            list(table_depends_raw),
            table_id,
        )

    def _ensure_dict(self, value: Any, field: str, manifest_path: Path) -> dict[str, Any]:
        if value is None:
            return {}
        if not isinstance(value, Mapping):
            raise ManifestConfigurationError(f"{field} должен быть объектом (dict) ({manifest_path})")
        return dict(value)

    def _ensure_quality_mapping(
        self,
        config: Mapping[str, Any],
        field: str,
        manifest_path: Path,
    ) -> None:
        if "quality" in config:
            self._quality_mapping(config["quality"], field, manifest_path)

    def _quality_mapping(self, value: Any, field: str, manifest_path: Path) -> dict[str, Any]:
        if not isinstance(value, Mapping):
            raise ManifestConfigurationError(f"{field} должен быть объектом (dict) ({manifest_path})")
        return dict(value)

    def _validated_quality(self, value: Any, field: str, manifest_path: Path) -> dict[str, Any]:
        quality = self._quality_mapping(value, field, manifest_path)
        try:
            QualityGatePolicy.from_config(quality)
        except ValueError as exc:
            raise ManifestConfigurationError(f"{field}: {exc} ({manifest_path})") from exc
        return quality

    def _merge_dict_templates(self, naming: Any) -> dict[str, Any]:
        # naming is a dict of templates, but schema allows additionalProperties strings.
        if naming is None:
            return {}
        if not isinstance(naming, Mapping):
            raise ManifestConfigurationError("naming должен быть объектом")
        return dict(naming)

    def _merge_vars(
        self,
        base: Mapping[str, Any],
        override: Mapping[str, Any],
        *,
        manifest_path: Path,
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        base_dict = dict(base)

        # 1) built-ins only on the first merge (when base is empty)
        if not base_dict:
            base_dict.update(
                {
                    "env_code": ENV_CODE,
                    "manifest_path": str(manifest_path),
                    "manifest_dir": str(manifest_path.parent),
                    "manifest_name": manifest_path.name,
                    "manifest_stem": manifest_path.stem,
                }
            )

        if override:
            for k in override.keys():
                if str(k) in _RESERVED_VARS:
                    raise ManifestConfigurationError(
                        f"Переменная '{k}' зарезервирована и не может быть переопределена ({manifest_path})"
                    )
            base_dict = deep_merge(base_dict, dict(override))

        if extra:
            base_dict = deep_merge(base_dict, dict(extra))

        # 2) render variables (allow vars to reference other vars)
        return self._render_vars(base_dict)

    def _render_vars(self, vars_dict: dict[str, Any]) -> dict[str, Any]:
        # Best-effort multi-pass resolution.
        ctx: dict[str, Any] = dict(vars_dict)
        for _ in range(5):
            changed = False
            for k, v in list(vars_dict.items()):
                if isinstance(v, str) and "{{" in v:
                    rendered = self._renderer.render(v, ctx)
                    if rendered != v:
                        vars_dict[k] = rendered
                        ctx[k] = rendered
                        changed = True
            if not changed:
                return vars_dict
        # If still changing after passes, likely a cycle.
        raise ManifestConfigurationError(
            "Не удалось разрешить vars за 5 проходов (возможна циклическая ссылка между переменными)"
        )

    def _inject_source_table(self, cfg: MutableMapping[str, Any], *, src_schema: str, src_table: str) -> None:
        source = cfg.get("source")
        if not isinstance(source, MutableMapping):
            raise ManifestConfigurationError("В batch manifest обязательно должен быть задан блок source в defaults")

        source_type = str(source.get("type") or "").lower()
        table = source.get("table")
        if table is None:
            table = {}
            source["table"] = table
        if not isinstance(table, MutableMapping):
            raise ManifestConfigurationError("source.table должен быть объектом")

        # For api sources schema/table may be omitted; for convenience, we still map group/table.
        table.setdefault("schema", src_schema)
        table.setdefault("name", src_table)

        # Non-api sources must have schema+name.
        if source_type != "api":
            if not table.get("schema") or not table.get("name"):
                raise ManifestConfigurationError("source.table.schema и source.table.name обязательны")

    def _inject_sink_table_shell(self, cfg: MutableMapping[str, Any]) -> None:
        sink = cfg.get("sink")
        if not isinstance(sink, MutableMapping):
            raise ManifestConfigurationError("В batch manifest обязательно должен быть задан блок sink в defaults")

        table = sink.get("table")
        if table is None:
            table = {}
            sink["table"] = table
        if not isinstance(table, MutableMapping):
            raise ManifestConfigurationError("sink.table должен быть объектом")

    def _apply_naming(
        self, cfg: MutableMapping[str, Any], naming: Mapping[str, Any], vars_ctx: Mapping[str, Any]
    ) -> None:
        # We render naming templates against a context that already includes vars + current cfg.
        ctx: dict[str, Any] = dict(vars_ctx)
        for k, v in cfg.items():
            if k not in ctx:
                ctx[k] = v

        # sink dataset/table
        sink = cfg.get("sink")
        sink_table = sink.get("table") if isinstance(sink, Mapping) else None
        if isinstance(sink_table, MutableMapping):
            if not sink_table.get("schema") and naming.get("sink_dataset"):
                sink_table["schema"] = self._renderer.render(naming["sink_dataset"], ctx)
            if not sink_table.get("name") and naming.get("sink_table"):
                sink_table["name"] = self._renderer.render(naming["sink_table"], ctx)

        # process name
        if not cfg.get("name") and naming.get("process_name"):
            cfg["name"] = self._renderer.render(naming["process_name"], ctx)

        # task_group
        if not cfg.get("task_group") and naming.get("task_group"):
            cfg["task_group"] = self._renderer.render(naming["task_group"], ctx)

        # description
        if not cfg.get("description") and naming.get("description"):
            cfg["description"] = self._renderer.render(naming["description"], ctx)

        # sink table description (e.g. BigQuery table description)
        # Stored in sink.options.table_description so it ends up in LoadConfig.options.
        sink_table_desc_tmpl = naming.get("sink_table_description") or naming.get("table_description")
        if sink_table_desc_tmpl:
            sink = cfg.get("sink")
            if isinstance(sink, MutableMapping):
                options = sink.get("options")
                if options is None:
                    options = {}
                    sink["options"] = options
                if not isinstance(options, MutableMapping):
                    raise ManifestConfigurationError("sink.options должен быть объектом")
                options.setdefault("table_description", sink_table_desc_tmpl)

        # sink table labels (e.g. BigQuery table labels)
        # Stored in sink.options.table_labels so it ends up in LoadConfig.options.
        labels_tmpl = naming.get("labels")
        if labels_tmpl is not None:
            if not isinstance(labels_tmpl, Mapping):
                raise ManifestConfigurationError("naming.labels должен быть объектом")
            sink = cfg.get("sink")
            if isinstance(sink, MutableMapping):
                options = sink.get("options")
                if options is None:
                    options = {}
                    sink["options"] = options
                if not isinstance(options, MutableMapping):
                    raise ManifestConfigurationError("sink.options должен быть объектом")
                options.setdefault("table_labels", dict(labels_tmpl))
