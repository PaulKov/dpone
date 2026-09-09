"""CLI commands for nested normalization preview and inference."""

from __future__ import annotations

import argparse
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text


def register_preview_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _base_parser(subparsers, "preview", "Preview normalized child tables from a local row sample")


def register_infer_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _base_parser(subparsers, "infer", "Infer normalized table schemas from a local row sample")


def register_lint_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("lint", help="Lint nested normalization config for production guardrails")
    parser.add_argument("--root-table", required=True, help="Root normalized table name")
    parser.add_argument("--config", required=True, help="Normalization config JSON/YAML file")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    parser.add_argument("--output", help="Optional output file path")
    parser.add_argument("--fail-on-error", action="store_true", help="Return exit code 2 when lint errors exist")
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Write nested normalization certification evidence")
    parser.add_argument("--output-dir", default="test_artifacts/nested/latest", help="Evidence output directory")
    parser.add_argument("--root-table", default="orders")
    parser.add_argument("--row-count", type=int, default=1000)
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_stress_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("stress", help="Run skewed nested normalization stress certification")
    parser.add_argument("--output-dir", default="test_artifacts/nested/stress-latest", help="Evidence output directory")
    parser.add_argument("--root-table", default="orders")
    parser.add_argument("--row-count", type=int, default=100000)
    parser.add_argument("--max-rows", type=int, default=100000)
    parser.add_argument("--spill-output-format", default="jsonl", choices=["jsonl", "ndjson", "json_each_row", "tsv"])
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_benchmark_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("benchmark", help="Run local nested normalization benchmark")
    parser.add_argument(
        "--output-dir", default="test_artifacts/nested/benchmark-latest", help="Evidence output directory"
    )
    parser.add_argument("--root-table", default="orders")
    parser.add_argument("--row-count", type=int, default=10000)
    parser.add_argument("--max-rows", type=int, default=100000)
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def cmd_normalize_preview(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(_payload(args, mode="preview"), args)
    return 0


def cmd_normalize_infer(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _emit(_payload(args, mode="infer"), args)
    return 0


def cmd_normalize_lint(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    config = _normalization_service().load_config(args.config)
    report = _lint_service().lint_config(config, root_table=args.root_table)
    _emit(report.to_dict(), args)
    return 2 if report.has_errors and args.fail_on_error else 0


def cmd_normalize_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    artifact = _certification_service().certify(
        output_dir=args.output_dir,
        row_count=args.row_count,
        root_table=args.root_table,
    )
    _emit(_artifact_payload("certification", artifact), args)
    return 0


def cmd_normalize_stress(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    artifact = _stress_service(args.max_rows).run(
        output_dir=args.output_dir,
        row_count=args.row_count,
        root_table=args.root_table,
        spill_output_format=args.spill_output_format,
    )
    _emit(_artifact_payload("stress", artifact), args)
    return 0


def cmd_normalize_benchmark(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    artifact = _benchmark_service(args.max_rows).run(
        output_dir=args.output_dir,
        row_count=args.row_count,
        root_table=args.root_table,
    )
    _emit(_artifact_payload("benchmark", artifact), args)
    return 0


def _normalization_service() -> Any:
    module = import_module("dpone.readiness.normalization_preview")
    return module.NormalizationCliService()


def _lint_service() -> Any:
    module = import_module("dpone.readiness.nested_lint")
    return module.NestedNormalizationLintService()


def _certification_service() -> Any:
    module = import_module("dpone.readiness.nested_certification")
    return module.NestedNormalizationCertificationService()


def _stress_service(max_rows: int) -> Any:
    module = import_module("dpone.readiness.nested_stress")
    return module.NestedStressCertificationService(max_rows=max_rows)


def _benchmark_service(max_rows: int) -> Any:
    module = import_module("dpone.readiness.nested_certification")
    return module.NestedNormalizationBenchmarkService(max_rows=max_rows)


def _base_parser(
    subparsers: argparse._SubParsersAction,
    name: str,
    help_text: str,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help=help_text)
    parser.add_argument("--sample", required=True, help="Local JSON, JSONL/NDJSON, CSV, or TSV row sample")
    parser.add_argument("--root-table", required=True, help="Root normalized table name")
    parser.add_argument("--config", help="Optional normalization config JSON/YAML file")
    parser.add_argument("--nested-level", type=int, help="Override normalization.nested_level")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum sample rows to read")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    parser.add_argument("--output", help="Optional output file path")
    return parser


def _payload(args: argparse.Namespace, *, mode: str) -> dict[str, Any]:
    service = _normalization_service()
    kwargs = {
        "sample": args.sample,
        "root_table": args.root_table,
        "config_path": args.config,
        "nested_level": args.nested_level,
        "limit": args.limit,
    }
    return service.infer(**kwargs) if mode == "infer" else service.preview(**kwargs)


def _emit(payload: dict[str, Any], args: argparse.Namespace) -> None:
    rendered = _render(payload, args.format)
    output_path = getattr(args, "output", None)
    if output_path:
        Path(output_path).write_text(rendered, encoding="utf-8")
        return
    if args.format == "json":
        write_json(payload)
    else:
        write_text(rendered)


def _render(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"
    if output_format == "md":
        return _render_markdown(payload)
    if "issues" in payload:
        return _render_lint_text(payload)
    if "artifact_type" in payload:
        return _render_artifact_text(payload)
    return _render_text(payload)


def _artifact_payload(artifact_type: str, artifact: Any) -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "json_path": str(artifact.json_path),
        "markdown_path": str(artifact.markdown_path),
    }


def _render_text(payload: dict[str, Any]) -> str:
    lines = [f"dpone normalize {payload.get('mode', 'preview')}", f"root_table: {payload['root_table']}"]
    for table_name, table in payload.get("tables", {}).items():
        lines.append(f"- {table_name}: rows={table.get('row_count')} columns={', '.join(table.get('columns', []))}")
    return "\n".join(lines) + "\n"


def _render_markdown(payload: dict[str, Any]) -> str:
    lines = [f"# dpone normalize {payload.get('mode', 'preview')}", "", f"Root table: `{payload['root_table']}`", ""]
    lines.append("| Table | Rows | Columns |")
    lines.append("|---|---:|---|")
    for table_name, table in payload.get("tables", {}).items():
        columns = ", ".join(f"`{column}`" for column in table.get("columns", []))
        lines.append(f"| `{table_name}` | {table.get('row_count')} | {columns} |")
    return "\n".join(lines) + "\n"


def _render_lint_text(payload: dict[str, Any]) -> str:
    lines = [f"dpone normalize lint: has_errors={payload.get('has_errors')}"]
    for issue in payload.get("issues", []):
        if isinstance(issue, dict):
            lines.append(f"- [{issue.get('severity')}] {issue.get('code')}: {issue.get('message')}")
    return "\n".join(lines) + "\n"


def _render_artifact_text(payload: dict[str, Any]) -> str:
    return (
        f"dpone normalize {payload.get('artifact_type')} artifact\n"
        f"- json: {payload.get('json_path')}\n"
        f"- markdown: {payload.get('markdown_path')}\n"
    )
