from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.cli_render.manifest import render_manifest_sparse_paths_text
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.manifest.sparse_paths_service import ManifestSparsePathsContext, ManifestSparsePathsService


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "sparse-paths",
        help="Print a repo-relative sparse-checkout allowlist for a manifest",
    )
    p.add_argument("path", help="Path to a YAML manifest")
    p.add_argument("--workload-root", help="Workload root that bounds sparse path validation")
    p.add_argument("--include-global-overrides", action="store_true", help="Include overrides/global.yaml")
    p.add_argument(
        "--include-env-overrides",
        action="append",
        default=[],
        metavar="ENV",
        help="Include overrides/<env>.yaml (can be repeated)",
    )
    p.add_argument("--include-registry", action="store_true", help="Include the workload registry/ directory")
    p.add_argument(
        "--registry", action="append", default=[], help="Include an explicit registry path (can be repeated)"
    )
    p.add_argument(
        "--support-path",
        action="append",
        default=[],
        help="Include an extra manifest support file or directory (can be repeated)",
    )
    p.add_argument("--format", choices=["sparse", "json"], default="sparse", help="Output format")
    return p


def cmd_manifest_sparse_paths(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    view = ManifestSparsePathsService(ctx=cast(ManifestSparsePathsContext, ctx)).build_view(args)
    if getattr(args, "format", "sparse") == "json":
        write_json(view.to_jsonable())
        return view.exit_code
    write_text(render_manifest_sparse_paths_text(view))
    return view.exit_code


__all__ = ["cmd_manifest_sparse_paths", "register_parser"]
