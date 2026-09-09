from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_validate_text
from dpone.commands.output_text import write_text
from dpone.manifest.validation import get_profile, validate_manifest
from dpone.services.manifest import build_manifest_context, iter_yaml_paths, load_manifest
from dpone.services.manifest.views import ManifestValidateView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("validate", help="Validate manifests (naming/metadata conventions)")
    p.add_argument("path", help="Path to a YAML file or a directory")
    p.add_argument("--recursive", action="store_true", help="Scan directories recursively")
    p.add_argument(
        "--profile",
        help=(
            "Built-in validation profile (e.g. landing_raw_v1). "
            "If omitted, will use manifest.validation block if present."
        ),
    )
    p.add_argument("--warn-only", action="store_true", help="Exit 0 even if there are ERROR issues")
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


def cmd_manifest_validate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    paths = tuple(iter_yaml_paths(Path(args.path), recursive=bool(args.recursive)))
    profile = get_profile(args.profile) if args.profile else None

    issues = []
    for path in paths:
        manifest = load_manifest(path, manifest_ctx=manifest_ctx, metadata_only=True)
        issues.extend(validate_manifest(manifest, profile=profile))

    view = ManifestValidateView(
        meta=build_meta(
            "manifest.validate",
            path=str(Path(args.path)),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={"recursive": bool(args.recursive), "warn_only": bool(args.warn_only)},
        ),
        issues=tuple(issues),
        profile_name=profile.name if profile else None,
    )
    write_text(render_manifest_validate_text(view))
    return view.exit_code(warn_only=bool(args.warn_only))
