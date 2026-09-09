from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.ops.facades import EvidenceChainService


def cmd_evidence_chain_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = EvidenceChainService().verify(chain_dir=args.chain_dir)
    if args.format == "json":
        write_json(report.to_dict())
    else:
        write_text(report.to_markdown())
    return 0 if report.verified else 1


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evidence-chain-verify", help="Verify a tamper-evident evidence chain")
    parser.add_argument("--chain-dir", default=".dpone/evidence-chain")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
