from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import yaml

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.readiness_facade import build_readiness_service
from dpone.connector_sdk.certification_rendering import ConnectorCertificationRenderer
from dpone.connector_sdk.native_transfer_certification import ConnectorCapabilityCertificationService
from dpone.connector_sdk.native_transfer_rendering import TransportEvidenceRenderer
from dpone.connector_sdk.scaffold import ConnectorSdkScaffoldService
from dpone.readiness.capability_discovery_composition import build_capability_discovery_service
from dpone.readiness.managed import ManagedRenderer

_FAIL_ON_MISSING_WARNING_EMITTED = False


def cmd_connectors_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    snapshot = build_capability_discovery_service(root=Path.cwd()).snapshot()
    items = [item.to_dict() for item in snapshot.connectors]
    connectors = [item["id"] for item in items]
    issues = [item.to_dict() for item in snapshot.issues]
    payload = {
        "passed": not issues,
        "connectors": connectors,
        "statuses": {item["id"]: item["maturity"] for item in items},
        "snapshot_id": snapshot.snapshot_id,
        "items": items,
        "issues": issues,
    }
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(ManagedRenderer.render_markdown("dpone connectors", payload))
    else:
        issue_lines = (
            [
                "Capability discovery blocked:",
                *(f"- {item['code']}: {item['message']}" for item in issues),
                "",
            ]
            if issues
            else []
        )
        write_text(
            "\n".join((*issue_lines, "dpone connectors"))
            + "\n"
            + "\n".join(f"- {item['id']}: {item['maturity']} ({', '.join(item['roles'])})" for item in items)
            + "\n"
        )
    return 1 if payload["passed"] is False else 0


def cmd_connectors_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    _warn_fail_on_missing_deprecation(bool(args.fail_on_missing))
    if args.capabilities:
        manifest = _load_certification_manifest()
        report = ConnectorCapabilityCertificationService().certify(
            manifest=manifest,
            profile=args.profile,
            requested_capabilities=tuple(args.capabilities),
        )
        renderer = TransportEvidenceRenderer()
        if args.artifact_dir:
            renderer.write(report, args.artifact_dir)
        if args.format == "json":
            write_text(renderer.render_json(report))
        else:
            write_text(renderer.render_markdown(report))
        return 0 if args.report_only else (0 if report.passed else 1)

    code, payload, markdown = build_readiness_service().certification()
    if args.artifact_dir:
        json_path, markdown_path = ConnectorCertificationRenderer().write(
            args.artifact_dir,
            payload=payload,
            markdown=markdown,
        )
        payload["artifact_paths"] = {
            "json": str(json_path),
            "markdown": str(markdown_path),
        }
    if args.format == "json":
        write_json(payload)
    else:
        write_text(markdown)
    return 0 if args.report_only else code


def cmd_connectors_scaffold(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = ConnectorSdkScaffoldService().scaffold(
        name=args.name,
        root=args.root,
        connector_type=args.connector_type,
        capabilities=tuple(args.capabilities or ("source",)),
        native_capabilities=tuple(args.native_capabilities or ()),
        include_certification=args.include_certification,
    )
    payload = result.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(ManagedRenderer.render_markdown("dpone connectors scaffold", payload))
    else:
        write_text(ManagedRenderer.render_text("dpone connectors scaffold", payload))
    return 0


def register_list_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("list", help="List connector certification status")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Render connector certification matrix")
    parser.add_argument("--format", choices=["markdown", "json"], default="markdown")
    parser.add_argument("--artifact-dir")
    strictness = parser.add_mutually_exclusive_group()
    strictness.add_argument(
        "--report-only",
        action="store_true",
        help="Always exit zero after rendering the report",
    )
    strictness.add_argument("--fail-on-missing", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--profile", choices=["static", "local_live", "vendor_live"], default="static")
    parser.add_argument("--capability", dest="capabilities", action="append")
    return parser


def register_scaffold_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("scaffold", help="Generate a community connector SDK skeleton")
    parser.add_argument("name")
    parser.add_argument("--root", default=".")
    parser.add_argument("--connector-type", choices=["api", "database", "event", "file"], default="api")
    parser.add_argument("--capability", dest="capabilities", action="append", choices=["source", "sink", "state"])
    parser.add_argument(
        "--native-capability",
        dest="native_capabilities",
        action="append",
        choices=ConnectorSdkScaffoldService.native_capability_choices(),
    )
    parser.add_argument("--no-certification", dest="include_certification", action="store_false")
    parser.set_defaults(include_certification=True)
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def _load_certification_manifest() -> dict:
    path = Path("certification") / "certification.yaml"
    if not path.exists():
        return {
            "connector": "unknown",
            "connector_type": "unknown",
            "native_transfer": {"capabilities": {}},
        }
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return payload if isinstance(payload, dict) else {}


def _warn_fail_on_missing_deprecation(enabled: bool) -> None:
    global _FAIL_ON_MISSING_WARNING_EMITTED
    if not enabled or _FAIL_ON_MISSING_WARNING_EMITTED:
        return
    sys.stderr.write(
        "warning: --fail-on-missing is deprecated because certification is strict by default; "
        "use --report-only only when a non-gating report is intentional\n"
    )
    _FAIL_ON_MISSING_WARNING_EMITTED = True
