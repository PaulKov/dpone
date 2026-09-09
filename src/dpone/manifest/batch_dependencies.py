from __future__ import annotations

from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.batch_models import CompiledProcess
from dpone.manifest.errors import ManifestConfigurationError


def _normalize_depends_on(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str | Mapping):
        return [value]
    raise ManifestConfigurationError("depends_on должен быть строкой, объектом или списком")


def _validate_unique_names(processes: Iterable[CompiledProcess], manifest_path: Path) -> None:
    seen: dict[str, str] = {}
    for p in processes:
        if p.name in seen:
            raise ManifestConfigurationError(
                f"Дублирующееся имя процесса '{p.name}' в batch manifest {manifest_path}. "
                f"Конфликт selectors: '{seen[p.name]}' и '{p.selector}'. "
                "Поменяйте naming.process_name или задайте overrides.name."
            )
        seen[p.name] = p.selector
