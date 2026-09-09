from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.supply_chain.attestation import SupplyChainAttestationService


def cmd_supply_chain_attest(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = SupplyChainAttestationService().build(
        project_root=Path(args.project_root),
        output_dir=Path(args.output_dir),
        release=args.release,
        subjects=[Path(item) for item in args.subject],
        repository=args.repository,
        commit_sha=args.commit_sha,
        builder_id=args.builder_id,
        signing_key=args.signing_key,
        signing_key_id=args.signing_key_id,
    )
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.passed else 1


def register_attest_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("attest", help="Build SBOM, provenance, signature, and attestation bundle")
    parser.add_argument("--project-root", default=".")
    parser.add_argument("--output-dir", default="test_artifacts/supply-chain/current")
    parser.add_argument("--release", required=True)
    parser.add_argument(
        "--subject", action="append", required=True, help="Artifact file to include as provenance subject"
    )
    parser.add_argument("--repository", required=True)
    parser.add_argument("--commit-sha", required=True)
    parser.add_argument("--builder-id", default="dpone.local")
    parser.add_argument("--signing-key", help="Local HMAC signing key; prefer CI secret or Sigstore/GitHub attestation")
    parser.add_argument("--signing-key-id", default="local-hmac")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
