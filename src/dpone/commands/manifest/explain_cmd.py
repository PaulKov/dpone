from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.cli_render.manifest import render_manifest_explain_text
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.dag.post_parse_explain import explain_post_parse, explain_why_parsed
from dpone.manifest.explain import explain_manifest, explain_why
from dpone.services.manifest import build_manifest_context
from dpone.services.manifest.views import ManifestExplainView, build_meta


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "explain",
        help="Explain how a process config is built (overrides/vars/naming provenance)",
    )
    p.add_argument("path", help="Path to a YAML manifest")
    p.add_argument("--selector", help="Process selector inside a batch manifest")
    p.add_argument(
        "--why",
        action="append",
        default=[],
        help="Explain why a specific config path has its value. Supports list indices, e.g. depends_on[2]. Can be repeated.",
    )
    p.add_argument(
        "--item-provenance",
        action="store_true",
        help="Show per-item provenance for list fields (depends_on, transforms).",
    )
    p.add_argument(
        "--patches", action="store_true", help="Show diff-friendly deep-merge patches per layer (copy/paste friendly)."
    )
    p.add_argument(
        "--patches-mode", default="user", choices=["user", "all"], help="Which layers to include in --patches output."
    )
    p.add_argument(
        "--patch-from", help="Compute a single patch from a given layer snapshot to another snapshot (or final)."
    )
    p.add_argument("--patch-to", help="Target layer for --patch-from (default: final compiler config).")
    p.add_argument(
        "--post-parse",
        action="store_true",
        help="Explain post-parse normalization (how compiled dict becomes ETLProcessConfig/LoadConfig).",
    )
    p.add_argument(
        "--why-parsed",
        action="append",
        default=[],
        help=(
            "Explain why a specific *normalized* path has its value (post-parse). "
            "Paths are inside a normalized tree: etl.*, load_config.*, dependencies[0].path, transforms[0].name, ... Can be repeated."
        ),
    )
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    p.add_argument(
        "--only",
        action="append",
        default=[],
        help="Show only config changes for dot-path prefix (can be repeated), e.g. --only sink.options",
    )
    p.add_argument("--max-lines", type=int, default=200, help="Max number of change lines to print (text mode)")
    p.add_argument(
        "--max-items", type=int, default=50, help="Max number of list items to print (item-level provenance)"
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


def cmd_manifest_explain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    path = Path(args.path)
    manifest_ctx = build_manifest_context(args, ctx=ctx)

    why_paths = tuple(str(p) for p in (args.why or []) if str(p).strip())
    why_parsed_paths = tuple(str(p) for p in (getattr(args, "why_parsed", []) or []) if str(p).strip())

    need_snapshots = bool(why_paths) or bool(getattr(args, "patches", False)) or bool(getattr(args, "patch_from", None))
    result = explain_manifest(
        path,
        selector=args.selector,
        registry_paths=tuple(manifest_ctx.registry_paths),
        metadata_only=True,
        include_snapshots=need_snapshots,
    )

    post_parse = None
    if bool(getattr(args, "post_parse", False)) or bool(why_parsed_paths):
        post_parse = explain_post_parse(
            dict(result.final_config),
            base_path=path.parent,
            origin_map=result.config_origin,
            metadata_only=True,
        )

    why_views = tuple(explain_why(result, query) for query in why_paths)
    why_parsed_views = tuple(
        explain_why_parsed(post_parse, query) for query in why_parsed_paths if post_parse is not None
    )

    view = ManifestExplainView(
        meta=build_meta(
            "manifest.explain",
            path=str(path),
            registry_paths=tuple(str(p) for p in manifest_ctx.registry_paths),
            options={
                "selector": getattr(args, "selector", None),
                "format": getattr(args, "format", "text"),
                "item_provenance": bool(getattr(args, "item_provenance", False)),
                "patches": bool(getattr(args, "patches", False)),
                "patches_mode": getattr(args, "patches_mode", "user"),
                "patch_from": getattr(args, "patch_from", None),
                "patch_to": getattr(args, "patch_to", None),
                "post_parse": bool(getattr(args, "post_parse", False)),
                "only": tuple(args.only or ()),
            },
        ),
        explain=result,
        why=why_views,
        post_parse=post_parse,
        why_parsed=why_parsed_views,
    )

    if getattr(args, "format", "text") == "json":
        write_json(view.to_jsonable())
        return 0

    write_text(
        render_manifest_explain_text(
            view,
            max_lines=int(getattr(args, "max_lines", 200)),
            max_items=int(getattr(args, "max_items", 50)),
        )
    )
    return 0
