"""Parse compiled manifest dicts into :class:`ETLProcessConfig`."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.process_types import TransformConfig
from dpone.dag.dependency_parser import DependencyParser
from dpone.dag.errors import DagConfigurationError
from dpone.dag.load_config_builder import LoadConfigBuilder

if TYPE_CHECKING:
    from dpone.dag.parse_trace import ParseTracer

from dpone.dag.config_models import ETLProcessConfig


class ETLProcessConfigParser:
    """Builds :class:`ETLProcessConfig` from a compiled process mapping."""

    def __init__(
        self,
        *,
        load_config_builder: LoadConfigBuilder | None = None,
        dependency_parser: DependencyParser | None = None,
    ) -> None:
        self._load_config_builder = load_config_builder or LoadConfigBuilder()
        self._dependency_parser = dependency_parser or DependencyParser()

    def parse(
        self,
        config: dict[str, Any],
        *,
        base_path: Path | None = None,
        metadata_only: bool = False,
        parse_tracer: ParseTracer | None = None,
    ) -> ETLProcessConfig:
        try:
            name = config["name"]
        except KeyError as exc:
            raise DagConfigurationError(f"Отсутствует обязательный параметр {exc.args[0]}")

        if parse_tracer:
            parse_tracer.record(
                kind="etl.field",
                target="etl.name",
                value=name,
                sources=("name",),
                operation="copy",
            )

        load_config = self._load_config_builder.build(config, base_path=base_path, parse_tracer=parse_tracer)

        if parse_tracer:
            parse_tracer.record(
                kind="etl.field",
                target="etl.load_strategy",
                value=getattr(load_config.load_strategy, "value", load_config.load_strategy),
                sources=("sink.strategy.mode",),
                operation="derive",
            )
            parse_tracer.record(
                kind="etl.field",
                target="etl.unique_key",
                value=load_config.unique_key,
                sources=("source.options.unique_key",),
                operation="derive",
            )

        transforms = self._parse_transforms(config.get("transforms", []), parse_tracer=parse_tracer)
        dependencies = self._dependency_parser.parse_many(
            config.get("depends_on", []),
            base_path=base_path,
            parse_tracer=parse_tracer,
        )

        opts = config.get("options", {})
        unique_key = load_config.unique_key
        load_strategy = load_config.load_strategy
        task_group = config.get("task_group")
        description = config.get("description")

        if parse_tracer:
            parse_tracer.record(
                kind="etl.field",
                target="etl.task_group",
                value=task_group,
                sources=("task_group",),
                operation="copy" if task_group is not None else "default",
            )
            parse_tracer.record(
                kind="etl.field",
                target="etl.description",
                value=description,
                sources=("description",),
                operation="copy" if description is not None else "default",
            )

        from dpone.dag.config_models import ETLProcessConfig

        result = ETLProcessConfig(
            name=name,
            load_config=load_config,
            transforms=transforms,
            dependencies=dependencies,
            options=opts,
            description=description,
            unique_key=unique_key,
            load_strategy=load_strategy,
            task_group=task_group,
            raw_config=dict(config),
        )

        if not metadata_only:
            result.ensure_runtime_bindings()

        return result

    def _parse_transforms(
        self,
        transforms_raw: Any,
        *,
        parse_tracer: ParseTracer | None,
    ) -> list[TransformConfig]:
        transforms = [TransformConfig(**item) for item in transforms_raw]
        if parse_tracer and isinstance(transforms_raw, list):
            for i, item in enumerate(transforms_raw):
                parse_tracer.record(
                    kind="transform",
                    target=f"transforms[{i}]",
                    value=item,
                    sources=(f"transforms[{i}]",),
                    operation="dataclass",
                )
        return transforms


__all__ = ["ETLProcessConfigParser"]
