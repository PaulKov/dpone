"""Загрузчик YAML конфигураций с кэшированием.

Исторически dpone считал, что 1 YAML == 1 ETL процесс.

Для Variant C (batch manifests) один YAML сможет порождать множество процессов.
Чтобы подготовить кодовую базу без массового рефакторинга, на шаге 1 добавляем
уровень *manifest loader* и новый метод `get_manifest()`.

Существующий публичный API `get_config()` остаётся (обратная совместимость).
"""

import logging
from pathlib import Path

from dpone.config.env import MANIFEST_DIR
from dpone.dag.config import ETLProcessConfig
from dpone.manifest.loader import ManifestLoader, ManifestLoaderRouter
from dpone.manifest.models import LoadedManifest

logger = logging.getLogger(__name__)


class ConfigLoader:
    """Загрузчик конфигураций с кэшированием.

    По умолчанию использует директорию etl-process-manifest/ для поиска YAML.
    """

    def __init__(
        self,
        base_path: Path | None = None,
        *,
        manifest_loader: ManifestLoader | None = None,
    ):
        self.base_path = base_path if base_path is not None else MANIFEST_DIR
        self._config_cache: dict[Path, ETLProcessConfig] = {}
        self._manifest_cache: dict[tuple[Path, bool], LoadedManifest] = {}
        self._yaml_files_cache: list[Path] | None = None
        self._manifest_loader: ManifestLoader = manifest_loader or ManifestLoaderRouter()

    def get_yaml_files(self) -> list[Path]:
        """Получает список YAML файлов с кэшированием."""
        if self._yaml_files_cache is None:
            self._yaml_files_cache = list(self.base_path.glob("**/*.yaml"))
        return self._yaml_files_cache

    def get_config(self, yaml_path: Path) -> ETLProcessConfig | None:
        """Получает конфигурацию с кэшированием.

        ВАЖНО: Использует metadata-only парсинг для избежания разрешения креденшиалов
        на этапе парсинга DAG. Коннекторы будут созданы позже в ETLOperator.execute().
        """
        # На шаге 1 продолжаем возвращать ровно один конфиг (legacy-ожидание).
        # Когда появятся batch manifests, вызовы get_config() будут постепенно
        # заменяться на get_manifest() на уровне DependencyManager.
        if yaml_path not in self._config_cache:
            try:
                manifest = self.get_manifest(yaml_path, metadata_only=True)
                if not manifest or not manifest.processes:
                    return None
                if len(manifest.processes) != 1:
                    raise ValueError(
                        f"Manifest {yaml_path} содержит {len(manifest.processes)} процессов; используйте get_manifest()"
                    )
                self._config_cache[yaml_path] = manifest.processes[0].config
            except Exception as e:
                logger.debug(f"Не удалось загрузить конфигурацию {yaml_path}: {e}")
                return None
        return self._config_cache[yaml_path]

    def get_manifest(self, yaml_path: Path, *, metadata_only: bool = True) -> LoadedManifest | None:
        """Загружает YAML-манифест (один файл может содержать несколько процессов).

        На шаге 1 будет возвращаться LoadedManifest с одним ProcessSpec для всех
        существующих legacy YAML.
        """
        yaml_path = Path(yaml_path)
        cache_key = (yaml_path, metadata_only)
        if cache_key in self._manifest_cache:
            return self._manifest_cache[cache_key]

        try:
            manifest = self._manifest_loader.load(yaml_path, metadata_only=metadata_only)
        except Exception as e:
            logger.debug(f"Не удалось загрузить manifest {yaml_path}: {e}")
            return None

        self._manifest_cache[cache_key] = manifest
        return manifest

    def clear_cache(self) -> None:
        """Очищает кэш конфигураций."""
        self._config_cache.clear()
        self._manifest_cache.clear()
        self._yaml_files_cache = None
