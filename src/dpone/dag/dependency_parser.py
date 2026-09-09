"""Dependency parsing for :class:`dpone.dag.config_models.ETLProcessConfig`.

The parser is intentionally isolated from the ETL process model to keep
responsibilities small:
- normalize legacy string/object ``depends_on`` forms
- resolve ``base_path``
- produce ``DependencyConfig`` records
- emit post-parse trace records when requested
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.process_types import DependencyConfig
from dpone.dag.errors import DagConfigurationError

if TYPE_CHECKING:  # pragma: no cover
    from dpone.dag.parse_trace import ParseTracer


class DependencyParser:
    """Parses ``depends_on`` declarations into normalized dependencies."""

    def parse_many(
        self,
        dependencies_raw: Any,
        *,
        base_path: Path | None = None,
        parse_tracer: ParseTracer | None = None,
    ) -> list[DependencyConfig]:
        normalized_raw = dependencies_raw
        if isinstance(normalized_raw, dict | str):
            normalized_raw = [normalized_raw]
        elif normalized_raw is None:
            normalized_raw = []

        if not isinstance(normalized_raw, list):
            raise DagConfigurationError("depends_on должен быть списком, строкой или объектом")

        if parse_tracer:
            parse_tracer.record(
                kind="etl.field",
                target="etl.depends_on",
                value=normalized_raw,
                sources=("depends_on",),
                operation="normalize_list" if not isinstance(dependencies_raw, list) else "copy",
            )

        return [
            self._parse_one(dep, index=i, base_path=base_path, parse_tracer=parse_tracer)
            for i, dep in enumerate(normalized_raw)
        ]

    def _parse_one(
        self,
        dep: Any,
        *,
        index: int,
        base_path: Path | None,
        parse_tracer: ParseTracer | None,
    ) -> DependencyConfig:
        if isinstance(dep, str):
            dep_entry = {"path": dep}
            if parse_tracer:
                parse_tracer.record(
                    kind="dependency",
                    target=f"dependencies[{index}].raw",
                    value=dep,
                    sources=(f"depends_on[{index}]",),
                    operation="string_to_object",
                    details={"raw_kind": "string"},
                )
        elif isinstance(dep, dict):
            dep_entry = dep
            if parse_tracer:
                parse_tracer.record(
                    kind="dependency",
                    target=f"dependencies[{index}].raw",
                    value=dep,
                    sources=(f"depends_on[{index}]",),
                    operation="copy",
                    details={"raw_kind": "mapping"},
                )
        else:
            raise DagConfigurationError("depends_on должен быть строкой или объектом")

        if "group" in dep_entry:
            return self._parse_group_dependency(dep_entry, index=index, parse_tracer=parse_tracer)
        if "path" in dep_entry:
            return self._parse_path_dependency(
                dep_entry,
                index=index,
                base_path=base_path,
                parse_tracer=parse_tracer,
            )

        raise DagConfigurationError(
            "depends_on должен содержать 'path' (для зависимости от файла) "
            f"или 'group' (для зависимости от группы). Получено: {dep_entry}"
        )

    def _parse_group_dependency(
        self,
        dep_entry: dict[str, Any],
        *,
        index: int,
        parse_tracer: ParseTracer | None,
    ) -> DependencyConfig:
        dep_cfg = DependencyConfig(
            path="",
            alias=dep_entry.get("alias"),
            group=dep_entry["group"],
        )
        if parse_tracer:
            parse_tracer.record(
                kind="dependency",
                target=f"dependencies[{index}]",
                value={"path": dep_cfg.path, "alias": dep_cfg.alias, "group": dep_cfg.group},
                sources=(f"depends_on[{index}].group", f"depends_on[{index}].alias"),
                operation="group_dependency",
                details={"kind": "group", "group": dep_cfg.group},
            )
        return dep_cfg

    def _parse_path_dependency(
        self,
        dep_entry: dict[str, Any],
        *,
        index: int,
        base_path: Path | None,
        parse_tracer: ParseTracer | None,
    ) -> DependencyConfig:
        dep_path_raw = dep_entry["path"]
        if not isinstance(dep_path_raw, str) or not dep_path_raw.strip():
            raise DagConfigurationError("depends_on.path должен быть непустой строкой")

        dep_path_raw = dep_path_raw.strip()
        alias = dep_entry.get("alias")

        if dep_path_raw.startswith("#"):
            selector = dep_path_raw[1:].strip()
            if not selector:
                raise DagConfigurationError("depends_on '#selector' должен содержать selector после '#'")
            dep_cfg = DependencyConfig(path=f"#{selector}", alias=alias)
            if parse_tracer:
                parse_tracer.record(
                    kind="dependency",
                    target=f"dependencies[{index}]",
                    value={"path": dep_cfg.path, "alias": dep_cfg.alias, "group": dep_cfg.group},
                    sources=(f"depends_on[{index}].path", f"depends_on[{index}].alias"),
                    operation="local_selector",
                    details={"kind": "local_selector", "selector": selector},
                )
            return dep_cfg

        if "#" in dep_path_raw:
            file_part, selector = dep_path_raw.split("#", 1)
            file_part = file_part.strip()
            selector = selector.strip()
            if not file_part:
                raise DagConfigurationError("depends_on 'path#selector': отсутствует путь до файла")
            if not selector:
                raise DagConfigurationError("depends_on 'path#selector': отсутствует selector")
            dep_path = (base_path / file_part) if base_path else Path(file_part)
            dep_cfg = DependencyConfig(path=f"{dep_path}#{selector}", alias=alias)
            if parse_tracer:
                parse_tracer.record(
                    kind="dependency",
                    target=f"dependencies[{index}]",
                    value={"path": dep_cfg.path, "alias": dep_cfg.alias, "group": dep_cfg.group},
                    sources=(f"depends_on[{index}].path", f"depends_on[{index}].alias"),
                    operation="file_selector",
                    details={
                        "kind": "file_selector",
                        "file_part": file_part,
                        "selector": selector,
                        "base_path": str(base_path) if base_path else None,
                        "resolved_file": str(dep_path),
                    },
                )
            return dep_cfg

        dep_path = (base_path / dep_path_raw) if base_path else Path(dep_path_raw)
        dep_cfg = DependencyConfig(path=str(dep_path), alias=alias)
        if parse_tracer:
            parse_tracer.record(
                kind="dependency",
                target=f"dependencies[{index}]",
                value={"path": dep_cfg.path, "alias": dep_cfg.alias, "group": dep_cfg.group},
                sources=(f"depends_on[{index}].path", f"depends_on[{index}].alias"),
                operation="resolve_path" if base_path else "copy",
                details={
                    "kind": "file_all",
                    "file_part": dep_path_raw,
                    "base_path": str(base_path) if base_path else None,
                    "resolved_file": str(dep_path),
                },
            )
        return dep_cfg


__all__ = ["DependencyParser"]
