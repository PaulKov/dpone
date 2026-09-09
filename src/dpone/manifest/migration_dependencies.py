from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.migration_models import ProcessRef


def normalize_depends_on(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ManifestConfigurationError("depends_on должен быть массивом")
    out: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, str):
            # legacy shortcut: string path
            out.append({"path": item})
        elif isinstance(item, Mapping):
            out.append(dict(item))
        else:
            raise ManifestConfigurationError("depends_on элементы должны быть строкой или объектом")
    return out


def rewrite_depends_on(
    deps: list[dict[str, Any]],
    *,
    current_batch_path: Path,
    mapping: Mapping[Path, ProcessRef],
    source_manifest_dir: Path,
) -> list[dict[str, Any]]:
    """Rewrites dependency file paths to new batch references when possible."""
    out: list[dict[str, Any]] = []
    current_batch_abs = current_batch_path.resolve()

    for dep in deps:
        if "group" in dep and dep.get("group"):
            out.append(dep)
            continue
        if "path" not in dep:
            out.append(dep)
            continue

        path_raw = dep.get("path")
        if not isinstance(path_raw, str) or not path_raw.strip():
            out.append(dep)
            continue

        path_raw = path_raw.strip()

        # keep already-selector deps as-is
        if path_raw.startswith("#") or "#" in path_raw:
            out.append(dep)
            continue

        target_abs = (source_manifest_dir / path_raw).resolve()
        ref = mapping.get(target_abs)

        if not ref:
            # cannot rewrite
            out.append(dep)
            continue

        # Internal reference (same batch file)
        if ref.batch_path.resolve() == current_batch_abs:
            new_path = f"#{ref.selector}"
        else:
            rel = relative_posix(ref.batch_path, start=current_batch_path.parent)
            new_path = f"{rel}#{ref.selector}"

        new_dep = dict(dep)
        new_dep["path"] = new_path
        out.append(new_dep)

    return out


def relative_posix(path: Path, *, start: Path) -> str:
    try:
        relative_path = path.resolve().relative_to(start.resolve())
        return relative_path.as_posix()
    except Exception:
        import os

        relative_str = os.path.relpath(str(path), start=str(start))
        return Path(relative_str).as_posix()
