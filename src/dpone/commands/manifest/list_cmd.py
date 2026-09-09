from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_list_text
from dpone.commands.output_text import write_text
from dpone.services.manifest import build_manifest_context, iter_yaml_paths, load_manifest
from dpone.services.manifest.views import ManifestListRow, ManifestListView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("list", help="List processes produced by a manifest")
    p.add_argument("path", help="Path to a YAML file or a directory")
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


def cmd_manifest_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    paths = tuple(iter_yaml_paths(Path(args.path), recursive=bool(args.recursive)))
    rows: list[ManifestListRow] = []
    for path in paths:
        manifest = load_manifest(path, manifest_ctx=manifest_ctx, metadata_only=True)
        for spec in manifest.processes:
            cfg = spec.config
            lc = cfg.load_config
            source = f"{lc.source_schema}.{lc.source_table}" if lc.source_schema and lc.source_table else "-"
            sink = f"{lc.target_schema}.{lc.target_table}" if lc.target_schema and lc.target_table else "-"
            rows.append(
                ManifestListRow(
                    manifest=path.name,
                    kind=manifest.source_kind or manifest.kind,
                    selector=spec.selector or "-",
                    name=cfg.name,
                    task_group=cfg.task_group or "-",
                    source=source,
                    sink=sink,
                )
            )

    view = ManifestListView(
        meta=build_meta(
            "manifest.list",
            path=str(Path(args.path)),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={"recursive": bool(args.recursive)},
        ),
        rows=tuple(rows),
        total_manifests=len(paths),
        total_processes=len(rows),
    )
    write_text(render_manifest_list_text(view))
    return 0
