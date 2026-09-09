from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_stats_text
from dpone.commands.output_text import write_text
from dpone.services.manifest import build_manifest_context, iter_yaml_paths, load_manifest
from dpone.services.manifest.views import ManifestStatsView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("stats", help="Show stats for a directory of manifests")
    p.add_argument("path", help="Directory or manifest file")
    p.add_argument("--recursive", action="store_true", help="Scan directories recursively")
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


def cmd_manifest_stats(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    paths = tuple(iter_yaml_paths(Path(args.path), recursive=bool(args.recursive)))

    total_manifests = 0
    total_processes = 0
    kinds: dict[str, int] = {}
    by_dataset: dict[str, int] = {}
    by_group: dict[str, int] = {}

    for path in paths:
        total_manifests += 1
        manifest = load_manifest(path, manifest_ctx=manifest_ctx, metadata_only=True)
        displayed_kind = manifest.source_kind or manifest.kind
        kinds[displayed_kind] = kinds.get(displayed_kind, 0) + 1
        for spec in manifest.processes:
            total_processes += 1
            lc = spec.config.load_config
            dataset = lc.target_schema or "-"
            by_dataset[dataset] = by_dataset.get(dataset, 0) + 1
            group = spec.config.task_group or "-"
            by_group[group] = by_group.get(group, 0) + 1

    view = ManifestStatsView(
        meta=build_meta(
            "manifest.stats",
            path=str(Path(args.path)),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={"recursive": bool(args.recursive)},
        ),
        total_manifests=total_manifests,
        total_processes=total_processes,
        kinds=kinds,
        by_dataset=by_dataset,
        by_group=by_group,
    )
    write_text(render_manifest_stats_text(view))
    return 0
