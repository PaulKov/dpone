from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_render_text
from dpone.commands.output_text import write_text
from dpone.services.manifest import build_manifest_context, load_manifest, resolve_single_process
from dpone.services.manifest.views import ManifestRenderView, RenderedProcessDoc, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("render", help="Render effective process config (after batch compilation)")
    p.add_argument("path", help="Path to a YAML manifest")
    p.add_argument("--selector", help="Process selector inside a batch manifest")
    p.add_argument("--all", action="store_true", help="Render all processes (batch)")
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


def cmd_manifest_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    path = Path(args.path)
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    manifest = load_manifest(path, manifest_ctx=manifest_ctx, metadata_only=True)

    if bool(args.all):
        docs = tuple(
            RenderedProcessDoc(selector=spec.selector, name=spec.name, config=dict(spec.raw_config))
            for spec in manifest.processes
        )
    else:
        spec = resolve_single_process(manifest, selector=getattr(args, "selector", None))
        docs = (RenderedProcessDoc(selector=spec.selector, name=spec.name, config=dict(spec.raw_config)),)

    view = ManifestRenderView(
        meta=build_meta(
            "manifest.render",
            path=str(path),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={"selector": getattr(args, "selector", None), "all": bool(args.all)},
        ),
        docs=docs,
    )
    write_text(render_manifest_render_text(view))
    return 0
