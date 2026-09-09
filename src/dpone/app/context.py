from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ..adapters.fs_local import LocalFileSystem
from ..adapters.yaml_pyyaml import PyYamlCodec
from .settings import Settings

if TYPE_CHECKING:
    from dpone.readiness.dbt_publish_release_materializer import DbtReleaseMaterializer
    from dpone.services.dbt_publish_artifact_writer import DbtArtifactWriter
    from dpone.services.dbt_publish_compiler import DbtDponeCompiler

    from ..ports.filesystem import FileSystem
    from ..ports.yaml_codec import YamlCodec


@dataclass(frozen=True)
class AppContext:
    """Composition root (DI container) for dpone CLI.

    In dpone we intentionally avoid a heavy DI framework.
    AppContext is the single place where we build and cache dependencies.

    In next refactoring steps we will attach services here.
    """

    settings: Settings
    logger: logging.Logger
    fs: FileSystem
    yaml: YamlCodec

    @classmethod
    def from_env(cls, *, logger: logging.Logger) -> AppContext:
        settings = Settings.from_env()
        fs = LocalFileSystem()
        yaml = PyYamlCodec()
        return cls(settings=settings, logger=logger, fs=fs, yaml=yaml)

    def build_dbt_publish_compiler(
        self,
        *,
        root: Path,
        require_certified_routes: bool,
    ) -> DbtDponeCompiler:
        """Compose the dbt compiler behind the CLI application boundary."""

        from .dbt_publish_composition import build_dbt_dpone_compiler

        return build_dbt_dpone_compiler(
            root=root,
            require_certified_routes=require_certified_routes,
        )

    def build_dbt_artifact_writer(
        self,
        *,
        dbt_profiles_dir: Path | None,
    ) -> DbtArtifactWriter:
        """Compose the immutable dbt artifact writer."""

        from .dbt_publish_composition import build_dbt_artifact_writer

        return build_dbt_artifact_writer(dbt_profiles_dir=dbt_profiles_dir)

    def build_dbt_release_materializer(self) -> DbtReleaseMaterializer:
        """Compose the immutable release installer."""

        from .dbt_publish_composition import build_dbt_release_materializer

        return build_dbt_release_materializer()
