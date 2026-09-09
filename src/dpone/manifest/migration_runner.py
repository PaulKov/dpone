from __future__ import annotations

from pathlib import Path

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.migration_builder import build_batch_manifest
from dpone.manifest.migration_models import BatchPlan, MigrationConfig, MigrationPlan, ProcessRef
from dpone.manifest.migration_scanner import _group_processes, _scan_legacy_manifests
from dpone.manifest.migration_writer import _write_batch_yaml


def plan_migration(cfg: MigrationConfig) -> MigrationPlan:
    """Builds a migration plan (does not write files)."""
    legacy = list(_scan_legacy_manifests(cfg.src_path, recursive=cfg.recursive))
    if not legacy:
        raise ManifestConfigurationError(f"Не найдено legacy манифестов в {cfg.src_path}")

    groups = _group_processes(legacy, group_by=cfg.group_by)

    batches: list[BatchPlan] = []
    mapping: dict[Path, ProcessRef] = {}

    for key, procs in sorted(groups.items(), key=lambda kv: (kv[0].dataset, kv[0].task_group)):
        out_path = cfg.out_dir / key.to_filename()
        procs_sorted = tuple(sorted(procs, key=lambda p: (p.src_schema, p.src_table, p.path.name)))
        batches.append(BatchPlan(key=key, out_path=out_path, processes=procs_sorted))

    # mapping old file -> new batch ref (path+selector) for dependency rewriting
    for batch in batches:
        for proc in batch.processes:
            mapping[proc.path.resolve()] = ProcessRef(
                batch_path=batch.out_path.resolve(),
                selector=f"{proc.src_schema}.{proc.src_table}",
            )

    return MigrationPlan(batches=tuple(batches), mapping=mapping)


def run_migration(cfg: MigrationConfig) -> MigrationPlan:
    """Executes migration plan: writes batch manifests to disk."""
    plan = plan_migration(cfg)

    if cfg.dry_run:
        return plan

    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    for batch in plan.batches:
        if batch.out_path.exists() and not cfg.overwrite:
            raise ManifestConfigurationError(
                f"Файл уже существует: {batch.out_path}. Используйте --overwrite чтобы перезаписать."
            )

        manifest = build_batch_manifest(
            batch,
            plan.mapping,
            infer_naming=cfg.infer_naming,
            naming_threshold=cfg.naming_threshold,
            convention=cfg.convention,
            registry_paths=cfg.registry_paths,
        )

        _write_batch_yaml(batch.out_path, manifest)

    return plan
