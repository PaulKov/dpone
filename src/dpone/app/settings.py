from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Typed settings for dpone.

    Goal:
      - keep defaults predictable
      - avoid module-level side effects
      - keep CLI runnable in local dev

    NOTE:
      Runtime/Airflow may still rely on DPONE_PROJECT_DIR, but Settings should
      degrade gracefully for local tooling (docs/metrics).
    """

    repo_root: Path
    project_dir: Path
    manifest_dir: Path
    sources_registry_paths: tuple[Path, ...]
    postgres_mssql_correctness_catalog_path: Path | None = None

    @staticmethod
    def _detect_repo_root(start: Path) -> Path:
        """Best-effort repo root detection.

        We search upwards for `pyproject.toml`.
        If not found, fallback to `start`.
        """

        cur = start.resolve()
        for _ in range(10):
            if (cur / "pyproject.toml").exists():
                return cur
            if cur.parent == cur:
                break
            cur = cur.parent
        return start.resolve()

    @classmethod
    def from_env(cls, *, cwd: Path | None = None) -> Settings:
        cwd = (cwd or Path.cwd()).resolve()
        repo_root = cls._detect_repo_root(cwd)

        project_dir_raw = os.getenv("DPONE_PROJECT_DIR", "").strip()
        project_dir = Path(project_dir_raw).resolve() if project_dir_raw else repo_root

        manifest_dir = project_dir / "etl-process-manifest"

        raw_registry = os.getenv("DPONE_SOURCES_REGISTRY", "").strip()
        paths: list[Path] = []
        if raw_registry:
            for part in [p.strip() for p in raw_registry.split(",") if p.strip()]:
                p = Path(part)
                if not p.is_absolute():
                    p = project_dir / p
                paths.append(p)

        correctness_catalog_raw = os.getenv("DPONE_POSTGRES_MSSQL_CORRECTNESS_CATALOG", "").strip()
        correctness_catalog_path: Path | None = None
        if correctness_catalog_raw:
            correctness_catalog_path = Path(correctness_catalog_raw)
            if not correctness_catalog_path.is_absolute():
                correctness_catalog_path = project_dir / correctness_catalog_path
            correctness_catalog_path = correctness_catalog_path.resolve()

        return cls(
            repo_root=repo_root,
            project_dir=project_dir,
            manifest_dir=manifest_dir,
            sources_registry_paths=tuple(paths),
            postgres_mssql_correctness_catalog_path=correctness_catalog_path,
        )
