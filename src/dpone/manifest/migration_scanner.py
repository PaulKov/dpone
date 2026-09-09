from __future__ import annotations

from collections.abc import Iterable, Sequence
from pathlib import Path

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.loader import ManifestLoaderRouter
from dpone.manifest.migration_models import GroupKey, LegacyProcess


def _scan_legacy_manifests(path: Path, *, recursive: bool) -> Iterable[LegacyProcess]:
    """Yields legacy single-manifest processes from a directory or a file."""
    loader = ManifestLoaderRouter()

    if path.is_file():
        yield from _load_legacy_file(loader, path)
        return

    if not path.exists():
        raise ManifestConfigurationError(f"Путь не найден: {path}")

    if not path.is_dir():
        raise ManifestConfigurationError(f"Неподдерживаемый путь: {path}")

    pattern = "**/*.y*ml" if recursive else "*.y*ml"
    for p in sorted(path.glob(pattern)):
        if p.name.startswith("."):
            continue
        yield from _load_legacy_file(loader, p)


def _load_legacy_file(loader: ManifestLoaderRouter, path: Path) -> Iterable[LegacyProcess]:
    manifest = loader.load(path, metadata_only=True)
    # Authoring v1 sources already compile to the canonical batch IR. Their
    # syntax migration is owned by `dpone migrate authoring`, not legacy scan.
    if manifest.kind == "dpone.batch.v1":
        return []
    if len(manifest.processes) != 1:
        raise ManifestConfigurationError(
            f"Legacy manifest ожидает 1 процесс, получено {len(manifest.processes)}: {path}"
        )
    spec = manifest.processes[0]
    return [LegacyProcess(path=path.resolve(), spec=spec)]


def _group_processes(processes: Sequence[LegacyProcess], *, group_by: str) -> dict[GroupKey, list[LegacyProcess]]:
    if group_by not in {"dataset", "task_group", "dataset_task_group"}:
        raise ManifestConfigurationError(f"Неверный --group-by: {group_by}")

    groups: dict[GroupKey, list[LegacyProcess]] = {}

    for p in processes:
        dataset = p.target_dataset if group_by in {"dataset", "dataset_task_group"} else ""
        task_group = p.task_group if group_by in {"task_group", "dataset_task_group"} else ""
        key = GroupKey(dataset=dataset, task_group=task_group)
        groups.setdefault(key, []).append(p)

    return groups
