from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_registry_lint_text
from dpone.commands.output_text import write_text
from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.registry_lint import lint_registry
from dpone.services.manifest import build_manifest_context, iter_yaml_paths
from dpone.services.manifest.views import ManifestRegistryLintView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "lint-registry",
        help="Lint manifests against a source registry (host/type coverage)",
    )
    p.add_argument("path", help="Path to a YAML file or a directory")
    p.add_argument("--recursive", action="store_true", help="Scan directories recursively")
    p.add_argument(
        "--require-field",
        action="append",
        default=["host", "type"],
        help=("Registry vars that must exist for each used (src_system, src_database) pair. Can be repeated."),
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


def cmd_manifest_lint_registry(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    if not manifest_ctx.registry_paths:
        raise ManifestConfigurationError("--registry is required for lint-registry")

    paths = tuple(iter_yaml_paths(Path(args.path), recursive=bool(args.recursive)))
    issues = lint_registry(
        paths,
        registry_paths=manifest_ctx.registry_paths,
        require_fields=tuple(args.require_field or ()),
        infer_from_single_sink_dataset=True,
    )
    view = ManifestRegistryLintView(
        meta=build_meta(
            "manifest.lint_registry",
            path=str(Path(args.path)),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={"recursive": bool(args.recursive), "warn_only": bool(args.warn_only)},
        ),
        issues=tuple(issues),
    )
    write_text(render_manifest_registry_lint_text(view))
    return view.exit_code(warn_only=bool(args.warn_only))
