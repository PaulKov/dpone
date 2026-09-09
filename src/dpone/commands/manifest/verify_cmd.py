from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_verify_text
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text
from dpone.manifest.verify import VerificationConfig, verify_legacy_vs_batch
from dpone.output_files import write_text_file
from dpone.services.manifest import build_manifest_context
from dpone.services.manifest.views import ManifestVerifyView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "verify",
        help="Verify that batch manifests compile to the same effective configs as legacy single manifests",
    )
    p.add_argument("legacy", help="Legacy directory (or file) with single manifests")
    p.add_argument("--batch-dir", required=True, help="Directory containing generated batch manifests")
    p.add_argument("--recursive", action="store_true", help="Scan legacy directories recursively")
    p.add_argument(
        "--group-by",
        default="dataset",
        choices=["dataset", "task_group", "dataset_task_group"],
        help="Grouping strategy used during migration (must match)",
    )
    p.add_argument(
        "--ignore-depends",
        action="store_true",
        help="Do not compare depends_on (useful if you rewrite dependencies manually)",
    )
    p.add_argument(
        "--ignore",
        action="append",
        default=[],
        help="Ignore a dot-path during comparison (can be repeated), e.g. --ignore sink.options",
    )
    p.add_argument("--max-diffs", type=int, default=30, help="Max number of diffs to print per mismatched process")
    p.add_argument("--out-json", help="Write full report to a JSON file (useful for CI artifacts)")
    p.add_argument("--warn-only", action="store_true", help="Exit 0 even if verification finds mismatches")
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


def cmd_manifest_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    manifest_ctx = build_manifest_context(args, ctx=ctx)
    cfg = VerificationConfig(
        legacy_src=Path(args.legacy),
        batch_dir=Path(args.batch_dir),
        recursive=bool(args.recursive),
        group_by=str(args.group_by),
        compare_depends_on=not bool(args.ignore_depends),
        ignore_paths=tuple(args.ignore or ()),
        max_diffs=int(args.max_diffs),
        registry_paths=tuple(manifest_ctx.registry_paths),
    )
    report = verify_legacy_vs_batch(cfg)
    view = ManifestVerifyView(
        meta=build_meta(
            "manifest.verify",
            path=str(Path(args.legacy)),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={
                "recursive": bool(args.recursive),
                "group_by": str(args.group_by),
                "warn_only": bool(args.warn_only),
            },
        ),
        report=report,
    )
    if args.out_json:
        write_text_file(Path(args.out_json), dumps_json(report.to_jsonable()))
        write_text(render_manifest_verify_text(view) + f"Report written: {Path(args.out_json)}\n")
    else:
        write_text(render_manifest_verify_text(view))
    return view.exit_code(warn_only=bool(args.warn_only))
