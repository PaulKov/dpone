"""Environment configuration for dpone.

Historically this module was *strict* and raised at import time when
``DPONE_PROJECT_DIR`` wasn't set. That made local tooling (docs, metrics,
DAG debugging) unnecessarily hard to run.

To improve UX while keeping backwards compatibility:
- public functions like :func:`get_project_dir` remain strict (raise if unset)
- module-level constants (PROJECT_DIR, MANIFEST_DIR, SOURCES_REGISTRY_PATHS)
  are computed with a safe fallback so importing dpone doesn't crash

Runtime environments (e.g. Airflow) are still expected to set
``DPONE_PROJECT_DIR`` explicitly.
"""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency for local .env loading
    load_dotenv = None


# Load .env from the dpone package root (optional).
_dpone_root = Path(__file__).parent.parent
_env_path = _dpone_root / ".env"
if _env_path.exists() and load_dotenv is not None:
    load_dotenv(_env_path)


def get_env_code() -> str:
    """Return env code (vault mount point)."""
    return os.getenv("ENV_CODE", "dev")


def get_project_dir() -> Path:
    """Strictly return dpone project dir.

    Uses env var ``DPONE_PROJECT_DIR``.

    Raises:
        ValueError: if ``DPONE_PROJECT_DIR`` is not set.
    """

    project_dir = os.getenv("DPONE_PROJECT_DIR")
    if project_dir:
        return Path(project_dir)

    raise ValueError(
        "Переменная окружения DPONE_PROJECT_DIR не задана.\n"
        "Установите переменную окружения DPONE_PROJECT_DIR с абсолютным путем к корню dpone плагина.\n"
        "Пример: export DPONE_PROJECT_DIR=/opt/airflow/plugins/dpone"
    )


def _detect_repo_root(start: Path | None = None) -> Path:
    """Best-effort repository root detection.

    This is used only as a fallback for *import-time* constants.
    """

    cur = (start or Path.cwd()).resolve()
    for p in [cur] + list(cur.parents):
        if (p / "pyproject.toml").exists() or (p / ".git").exists():
            return p
    return cur


def get_project_dir_safe() -> Path:
    """Non-strict project dir.

    - If DPONE_PROJECT_DIR is set, uses it.
    - Otherwise falls back to a detected repo root (or current working dir).

    This exists to keep imports safe for local tooling.
    """

    project_dir = os.getenv("DPONE_PROJECT_DIR")
    if project_dir:
        return Path(project_dir)

    return _detect_repo_root()


def get_manifest_dir(*, base_dir: Path | None = None) -> Path:
    """Return directory with YAML manifests.

    Default: ``{DPONE_PROJECT_DIR}/etl-process-manifest``.

    Args:
        base_dir: override base directory (useful for tooling).
    """

    base = base_dir if base_dir is not None else get_project_dir()
    return base / "etl-process-manifest"


def get_sources_registry_paths(*, base_dir: Path | None = None) -> list[Path]:
    """Return a list of registry YAML paths with source metadata.

    Registry is an optional UX feature used mostly for naming/metadata conventions
    (e.g. landing/raw) where table labels/description require {host}, {type}, etc.

    Configure via env var:
      DPONE_SOURCES_REGISTRY=/abs/path/sources.yaml[,/abs/path/extra.yaml]

    Relative paths are resolved against DPONE_PROJECT_DIR (or base_dir).
    """

    raw = os.getenv("DPONE_SOURCES_REGISTRY", "").strip()
    if not raw:
        return []

    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        return []

    base = base_dir if base_dir is not None else get_project_dir_safe()
    out: list[Path] = []
    for p in parts:
        candidate = Path(p)
        if not candidate.is_absolute():
            candidate = base / candidate
        out.append(candidate)

    return out


ENV_CODE = get_env_code()

# Import-time constants are safe (never raise).
try:
    PROJECT_DIR = get_project_dir()
except ValueError:
    PROJECT_DIR = get_project_dir_safe()

MANIFEST_DIR = PROJECT_DIR / "etl-process-manifest"

# Optional, may be empty.
SOURCES_REGISTRY_PATHS = get_sources_registry_paths(base_dir=PROJECT_DIR)
