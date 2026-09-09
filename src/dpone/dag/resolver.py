"""Резолвер путей для YAML конфигураций.

Поддерживает расширенный синтаксис ссылок на процессы внутри batch manifest:

- "some_file.yaml"                -> зависимость от *всех* процессов из файла
- "some_file.yaml#public.users"   -> зависимость от *конкретного* процесса (selector)
- "#public.users"                 -> зависимость от процесса в текущем файле
"""

from __future__ import annotations

from pathlib import Path

from dpone.config.env import MANIFEST_DIR
from dpone.dag.yaml_types import DependencyConfig


class DependencyResolver:
    """Разрешает пути зависимостей относительно текущего файла."""

    def __init__(self, base_path: Path | None = None):
        self.base_path = base_path if base_path is not None else MANIFEST_DIR

    @staticmethod
    def split_selector(dep_path: str) -> tuple[str, str | None, bool]:
        """Splits 'path#selector' and detects local refs ('#selector').

        Returns:
            (path_part, selector, is_local_ref)
        """
        s = (dep_path or "").strip()
        if not s:
            return "", None, False

        if s.startswith("#"):
            return "", s[1:].strip() or None, True

        if "#" in s:
            path_part, selector = s.split("#", 1)
            return path_part.strip(), selector.strip() or None, False

        return s, None, False

    def resolve_dependency_ref(self, dep_path: str, current_file_path: Path) -> tuple[Path, str | None]:
        """Resolves dependency reference to (file_path, selector).

        - local refs '#selector' point to current_file_path
        - relative paths resolve относительно current_file_path.parent
        - absolute paths stay as-is
        """
        path_part, selector, is_local = self.split_selector(dep_path)

        if is_local or not path_part:
            return Path(current_file_path), selector

        p = Path(path_part)
        if p.is_absolute():
            return p, selector

        return current_file_path.parent / p, selector

    def resolve_dependency_path(self, dep_config: DependencyConfig, current_file_path: Path) -> Path:
        """Legacy API: returns only the file path part, ignoring selector."""
        file_path, _ = self.resolve_dependency_ref(dep_config.path, current_file_path)
        return file_path

    def validate_dependency_path(self, path: Path) -> bool:
        return path.exists() and path.is_file()
