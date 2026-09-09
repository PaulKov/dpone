from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.errors import ManifestConfigurationError


def read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ManifestConfigurationError(f"YAML конфигурация не найдена: {path}")
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        raise ManifestConfigurationError(f"Ошибка парсинга YAML {path}: {exc}") from exc


def select_process(processes: Sequence[Any], selector: str | None) -> Any:
    if selector:
        for spec in processes:
            if spec.selector == selector or spec.name == selector:
                return spec
        raise ManifestConfigurationError(
            f"Selector '{selector}' не найден. Доступные selectors: {[p.selector for p in processes]}"
        )

    if len(processes) != 1:
        raise ManifestConfigurationError(
            f"Manifest содержит {len(processes)} процессов. Укажите --selector или --all (через render)."
        )
    return processes[0]


__all__ = ["read_yaml", "select_process"]
