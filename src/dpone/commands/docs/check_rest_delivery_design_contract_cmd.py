"""CLI adapter for the governed REST delivery design-contract gate."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.docs.check_rest_delivery_design_contract_service import (
    CheckRestDeliveryDesignContractService,
)

from ..context import DocsCommandContext


def cmd_docs_check_rest_delivery_design_contract(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    """Run the read-only design-authority gate and render its report."""

    del logger
    exit_code, payload = CheckRestDeliveryDesignContractService(ctx=ctx).run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    """Register the REST delivery design-contract command."""

    parser = subparsers.add_parser(
        "check-rest-delivery-design-contract",
        help="Validate the governed REST delivery design authority",
    )
    parser.add_argument(
        "--contract",
        default="docs/rest-bulk-delivery-design-contract-v1.yaml",
        help="Design-contract YAML relative to the repository root",
    )
    parser.add_argument(
        "--schema",
        default="docs/schema/rest-bulk-delivery-design-contract-v1.schema.json",
        help="JSON Schema relative to the repository root",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


__all__ = [
    "cmd_docs_check_rest_delivery_design_contract",
    "register_parser",
]
