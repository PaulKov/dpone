"""Factory for ProcessNode instances."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.manifest.models import ProcessSpec


from pathlib import Path

from dpone.dag.yaml_types import ProcessNode


class ProcessNodeFactory:
    """Build ProcessNode objects from manifest specs."""

    def from_spec(self, spec: ProcessSpec, yaml_path: Path) -> ProcessNode:
        selector = spec.selector or spec.name
        return ProcessNode(
            config=spec.config,
            config_path=yaml_path,
            selector=selector,
            dependencies=spec.config.dependencies.copy(),
            task_group=spec.config.task_group,
        )


__all__ = ["ProcessNodeFactory"]
