"""Thin platform CLI for signed catalogs and extension conformance."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.supply_chain.catalog_operations import CatalogSupplyChainOperations


def register_catalog_bundle_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("catalog-bundle-build", help="Build one immutable signed-catalog payload")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--kind", choices=["recipe_catalog", "connection_registry"], required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--bundle-root", required=True)
    parser.add_argument("--publisher-id", required=True)
    parser.add_argument("--environment")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_catalog_bundle_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("catalog-bundle-verify", help="Verify signed catalog bytes and semantics")
    parser.add_argument("--bundle-dir", required=True)
    parser.add_argument("--sigstore-bundle", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--trusted-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_extension_conformance_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("extension-conformance", help="Evaluate a closed extension evidence profile")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--request", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def cmd_catalog_bundle_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        result = CatalogSupplyChainOperations().build(
            project_root=args.project_root,
            kind=args.kind,
            source=args.source,
            bundle_root=args.bundle_root,
            publisher_id=args.publisher_id,
            environment=args.environment,
        )
    except (OSError, ValueError) as exc:
        return _emit_error(args.format, exc, stage="catalog_bundle_build")
    payload = result.to_dict()
    _emit(payload, _build_text(payload), args.format)
    return 0


def cmd_catalog_bundle_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        receipt = CatalogSupplyChainOperations().verify(
            bundle_dir=args.bundle_dir,
            sigstore_bundle=args.sigstore_bundle,
            policy=args.policy,
            trusted_root=args.trusted_root,
            output=args.output,
        )
    except (OSError, ValueError) as exc:
        return _emit_error(args.format, exc, stage="catalog_bundle_verify")
    payload = receipt.to_dict()
    _emit(payload, _verify_text(payload), args.format)
    if receipt.is_verified:
        return 0
    return 3 if receipt.code == "DPONE_CATALOG_VERIFIER_UNAVAILABLE" else 4


def cmd_extension_conformance(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    root = Path(args.project_root)
    try:
        report = CatalogSupplyChainOperations().conformance(
            request_path=Path(args.request),
            output_dir=Path(args.output_dir),
            project_root=root,
        )
    except (OSError, ValueError) as exc:
        return _emit_error(args.format, exc, stage="extension_conformance")
    payload = report.to_dict()
    _emit(payload, report.to_markdown(), args.format)
    return 0 if report.passed else 1


def _emit(payload: dict[str, object], markdown: str, output_format: str) -> None:
    write_json(payload) if output_format == "json" else write_text(markdown)


def _emit_error(output_format: str, exc: Exception, *, stage: str) -> int:
    code = str(getattr(exc, "code", "DPONE_CATALOG_INTERNAL_ERROR"))
    payload: dict[str, object] = {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": stage,
        "severity": "error",
        "message": _safe_message(exc),
        "fixes": [],
    }
    _emit(payload, f"# dpone Supply Chain\n\n- error: `{code}`\n- message: {payload['message']}\n", output_format)
    if "UNAVAILABLE" in code:
        return 3
    if any(marker in code for marker in ("INTEGRITY", "SIGNATURE", "PATH_UNSAFE")):
        return 4
    if "INTERNAL" in code:
        return 5
    if any(marker in code for marker in ("CONTENT_INVALID", "SOURCE_CHANGED", "LIMIT_EXCEEDED")):
        return 1
    return 2


def _safe_message(exc: Exception) -> str:
    return (" ".join(str(exc).split()) or exc.__class__.__name__)[:500]


def _build_text(payload: dict[str, object]) -> str:
    return (
        "# dpone Catalog Bundle Build\n\n"
        f"- status: `{payload['status']}`\n"
        f"- bundle_id: `{payload['bundle_id']}`\n"
        f"- artifacts: `{payload['artifacts']}`\n"
        f"- manifest: `{payload['manifest_path']}`\n"
        "- next: sign the exact manifest with the approved external cosign workflow\n"
    )


def _verify_text(payload: dict[str, object]) -> str:
    return (
        "# dpone Catalog Bundle Verification\n\n"
        f"- decision: `{payload['decision']}`\n"
        f"- code: `{payload['code']}`\n"
        f"- bundle_id: `{payload['bundle_id']}`\n"
        f"- verified_artifacts: `{payload['verified_artifacts']}`\n"
    )


__all__ = [
    "cmd_catalog_bundle_build",
    "cmd_catalog_bundle_verify",
    "cmd_extension_conformance",
    "register_catalog_bundle_build_parser",
    "register_catalog_bundle_verify_parser",
    "register_extension_conformance_parser",
]
