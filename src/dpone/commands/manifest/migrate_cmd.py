from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_migrate_text
from dpone.commands.output_text import write_text
from dpone.manifest.migrate import MigrationConfig, run_migration
from dpone.services.manifest import build_manifest_context
from dpone.services.manifest.views import ManifestMigrateView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("migrate", help="Migrate legacy single manifests to batch manifests (Variant C)")
    p.add_argument("src", help="Source directory (legacy manifests) or a single YAML file")
    p.add_argument("--out-dir", help="Output directory for batch manifests (default: src dir)")
    p.add_argument("--recursive", action="store_true", help="Scan directories recursively")
    p.add_argument(
        "--group-by",
        default="dataset",
        choices=["dataset", "task_group", "dataset_task_group"],
        help="Grouping strategy for batch manifests",
    )
    p.add_argument("--dry-run", action="store_true", help="Plan only; do not write files")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output files if they already exist")
    p.add_argument("--no-infer-naming", action="store_true", help="Disable naming template inference")
    p.add_argument(
        "--naming-threshold",
        type=float,
        default=0.8,
        help="Min fraction of tables matching a pattern to enable naming templates (0..1)",
    )
    p.add_argument(
        "--convention",
        help=(
            "Add `convention:` to generated batch manifests (e.g. landing_raw_v1). "
            "Convention provides default naming/metadata/validation; user config wins."
        ),
    )
    p.add_argument(
        "--registry",
        action="append",
        default=[],
        help=(
            "Path to a sources registry YAML (can be repeated). "
            "Registry provides default vars like host/type for naming/metadata conventions."
        ),
    )
    return p


def cmd_manifest_migrate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    src = Path(args.src)
    out_dir = Path(args.out_dir) if args.out_dir else (src if src.is_dir() else src.parent)

    cfg = MigrationConfig(
        src_path=src,
        out_dir=out_dir,
        recursive=bool(args.recursive),
        group_by=str(args.group_by),
        overwrite=bool(args.overwrite),
        dry_run=bool(args.dry_run),
        infer_naming=not bool(args.no_infer_naming),
        naming_threshold=float(args.naming_threshold),
        convention=str(args.convention).strip() if args.convention else None,
        registry_paths=tuple(manifest_ctx.registry_paths),
    )
    plan = run_migration(cfg)
    view = ManifestMigrateView(
        meta=build_meta(
            "manifest.migrate",
            path=str(src),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={
                "recursive": bool(args.recursive),
                "group_by": str(args.group_by),
                "dry_run": bool(args.dry_run),
                "overwrite": bool(args.overwrite),
            },
        ),
        plan=plan,
    )
    write_text(render_manifest_migrate_text(view, dry_run=cfg.dry_run))
    return 0
