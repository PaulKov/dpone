"""CLI facade for the reviewed Airflow self-service public contract."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.docs.airflow_public_contract_service import (
    CheckAirflowPublicContractsService,
    UpdateAirflowPublicContractReferenceService,
)

from ..context import DocsCommandContext


def cmd_docs_check_airflow_public_contracts(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    del logger
    exit_code, payload = CheckAirflowPublicContractsService(ctx=ctx).run(args)
    write_json(payload) if isinstance(payload, dict) else write_text(payload)
    return exit_code


def register_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "check-airflow-public-contracts",
        help="Check the reviewed Airflow self-service v1 public-contract baseline",
    )
    parser.add_argument("--format", choices=("text", "json"), default="text")
    parser.add_argument(
        "--baseline",
        default="docs/airflow-self-service-public-contracts-v1.yaml",
        help="Reviewed baseline relative to repository root",
    )
    parser.add_argument(
        "--provider-root",
        default="packages/apache-airflow-providers-dpone/src/airflow/providers/dpone",
        help="Formal provider facade root relative to repository root",
    )
    return parser


def cmd_docs_update_airflow_public_contract_reference(
    args: argparse.Namespace,
    *,
    ctx: DocsCommandContext,
    logger: logging.Logger,
) -> int:
    del logger
    return UpdateAirflowPublicContractReferenceService(ctx=ctx).run(args)


def register_update_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "update-airflow-public-contract-reference",
        help="Update the generated Airflow v1 public-contract reference from its reviewed baseline",
    )
    parser.add_argument("--check", action="store_true", help="Only check that the generated reference is current")
    parser.add_argument(
        "--baseline",
        default="docs/airflow-self-service-public-contracts-v1.yaml",
        help="Reviewed baseline relative to repository root",
    )
    parser.add_argument(
        "--doc",
        default="docs/reference/airflow-public-contracts.md",
        help="Generated reference page relative to repository root",
    )
    return parser


__all__ = [
    "cmd_docs_check_airflow_public_contracts",
    "cmd_docs_update_airflow_public_contract_reference",
    "register_check_parser",
    "register_update_parser",
]
