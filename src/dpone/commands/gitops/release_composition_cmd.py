"""Thin public adapters for verified inventory and immutable release composition."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import write_json
from dpone.contracts.release_composition import ReleaseCompositionReport


def register_release_compose_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-compose",
        help="Verify a complete compact native workspace and ordinary inventory; publish one immutable release",
    )
    parser.add_argument(
        "--manifest", required=True, help="Local dpone.release-composition.v1 YAML; roots resolve relative to this file"
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Exact immutable destination outside both source roots; identical retries are verified",
    )
    parser.add_argument(
        "--format",
        choices=("json",),
        default="json",
        help="One JSON publication report; delivery does not authorize activation",
    )
    return parser


def register_release_inventory_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-inventory",
        help="Verify ordinary producer source closure and print the digest required by release-compose",
    )
    parser.add_argument(
        "--pack-root", required=True, help="Complete ordinary reconcile root with _dags and producer workload packs"
    )
    parser.add_argument(
        "--xcom-sidecar-image", required=True, help="Digest-pinned sidecar to validate supported strict transport"
    )
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def cmd_release_compose(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    from dpone.app.release_composition import build_release_composition_service
    from dpone.manifest.release_composition_request import read_release_composition_request

    try:
        request = read_release_composition_request(Path(args.manifest), output_dir=Path(args.output_dir))
    except (ValueError, OSError, TypeError):
        report = ReleaseCompositionReport(
            status="rejected",
            release_id=None,
            output_dir=Path(args.output_dir),
            source_release_id=None,
            inventory_sha256=None,
            blockers=(
                "DPONE_COMPOSITION_MANIFEST_INVALID: use the closed local composition manifest and complete pinned inputs",
            ),
        )
    else:
        report = build_release_composition_service().compose(request)
    write_json(report.to_dict())
    return 0 if report.passed else 3 if report.status == "durability_uncertain" else 2


def cmd_release_inventory(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    from dpone.app.release_composition import build_release_composition_service

    try:
        report = build_release_composition_service().inventory(
            Path(args.pack_root), xcom_sidecar_image=args.xcom_sidecar_image
        )
    except (ValueError, OSError, RuntimeError, TypeError):
        write_json(
            {
                "schema": "dpone.workload-inventory-report.v1",
                "passed": False,
                "inventory_sha256": None,
                "blockers": [
                    "DPONE_COMPOSITION_INVENTORY_INVALID: rebuild the complete supported ordinary producer root and verify its source closure"
                ],
            }
        )
        return 2
    write_json(report)
    return 0
