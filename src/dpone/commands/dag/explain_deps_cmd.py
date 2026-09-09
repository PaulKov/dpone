from __future__ import annotations

import argparse
import logging

from dpone.cli_render.dag.deps import render_explain_deps_text
from dpone.commands.output_json import write_json
from dpone.dag.deps_end_to_end_explain import explain_end_to_end_dependencies
from dpone.dag.edge_explain import locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.views import build_explain_dependencies_view


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "explain-deps",
        help=(
            "Explain dependencies for a downstream task end-to-end: "
            "overrides provenance -> post-parse normalization -> DAG edges"
        ),
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument(
        "--for",
        dest="task",
        required=True,
        help="Downstream task token to explain: task_id, selector, #selector, file.yaml#selector",
    )
    p.add_argument(
        "--index",
        action="append",
        default=[],
        help="Explain only depends_on item with this index (0-based). Can be repeated.",
    )
    p.add_argument(
        "--no-inherited-groups",
        action="store_true",
        help="Do not include inherited TaskGroup group-to-group dependencies affecting the task.",
    )
    p.add_argument(
        "--max-upstreams",
        type=int,
        default=30,
        help="Max upstream tasks to print per dependency.",
    )
    p.add_argument(
        "--max-triggers",
        type=int,
        default=10,
        help="Max trigger tasks to inspect/print for group-to-group dependencies.",
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
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


def cmd_dag_explain_deps(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    dag = load_dag_context(args, ctx=ctx)

    try:
        dn_node = locate_node(dag.nodes, token=str(args.task), root_file=dag.root_path, base_path=dag.base_path)
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    dep_indexes: list[int] = []
    for s in args.index or []:
        if not str(s).strip():
            continue
        try:
            dep_indexes.append(int(str(s)))
        except Exception as exc:
            raise DagConfigurationError(f"--index должен быть целым числом (0-based). Получено: {s}") from exc

    include_inherited_group_deps = not bool(getattr(args, "no_inherited_groups", False))
    max_upstreams = int(getattr(args, "max_upstreams", 30) or 30)
    max_triggers = int(getattr(args, "max_triggers", 10) or 10)

    res = explain_end_to_end_dependencies(
        downstream_node=dn_node,
        ctx=dag.edge_ctx,
        registry_paths=dag.registry_paths,
        include_inherited_group_deps=include_inherited_group_deps,
        dep_indexes=dep_indexes or None,
        max_upstreams=max_upstreams,
        max_triggers=max_triggers,
    )

    view = build_explain_dependencies_view(
        dag=dag,
        result=res,
        dep_indexes=dep_indexes,
        include_inherited_group_deps=include_inherited_group_deps,
        max_upstreams=max_upstreams,
        max_triggers=max_triggers,
    )

    if args.format == "json":
        write_json(view.to_jsonable())
        return 0

    text = render_explain_deps_text(view)
    print(text, end="")
    return 0
