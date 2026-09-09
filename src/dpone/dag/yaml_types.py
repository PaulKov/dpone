"""Типы данных для работы с YAML конфигурациями.

В legacy режиме dpone предполагал: 1 YAML == 1 процесс.

Для Variant C (batch manifests) один YAML может порождать множество процессов.
Поэтому ProcessNode теперь может иметь `selector` — идентификатор процесса внутри файла.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.contracts.process_types import DependencyConfig

if TYPE_CHECKING:
    from dpone.dag.config import ETLProcessConfig


@dataclass
class ProcessNode:
    """Узел процесса в графе зависимостей."""

    config: ETLProcessConfig
    config_path: Path
    selector: str | None = None

    dependencies: list[DependencyConfig] = field(default_factory=list)
    dependents: list[ProcessNode] = field(default_factory=list)
    upstream: ProcessNode | None = None
    siblings: list[ProcessNode] = field(default_factory=list)
    task_group: str | None = None

    @property
    def name(self) -> str:
        """Имя задачи (task_id), которое будет использоваться в Airflow."""
        return self.config.name

    @property
    def ref(self) -> str:
        """Стабильная ссылка на процесс внутри YAML файла.

        Формат: <absolute_path>#<selector>

        selector:
        - для batch manifest: обычно "{src_schema}.{src_table}" или пользовательский `id`
        - для single manifest: по умолчанию равен имени процесса
        """
        sel = self.selector or self.name
        return f"{self.config_path}#{sel}"

    @property
    def has_dependencies(self) -> bool:
        return len(self.dependencies) > 0

    def add_dependent(self, dependent: ProcessNode) -> None:
        if dependent not in self.dependents:
            self.dependents.append(dependent)

    def add_sibling(self, sibling: ProcessNode) -> None:
        if sibling not in self.siblings:
            self.siblings.append(sibling)
