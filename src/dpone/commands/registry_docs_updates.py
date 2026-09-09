from __future__ import annotations

from .base import Command
from .docs import (
    airflow_public_contracts_cmd,
    update_cli_reference_cmd,
    update_deprecation_roadmap_cmd,
    update_dev_metrics_cmd,
    update_gitops_schema_reference_cmd,
    update_manifest_schema_reference_cmd,
    update_shim_removal_plan_cmd,
)
from .func_command import FuncCommand


def docs_update_commands() -> list[Command]:
    return [
        FuncCommand(
            "update-airflow-public-contract-reference",
            airflow_public_contracts_cmd.register_update_parser,
            airflow_public_contracts_cmd.cmd_docs_update_airflow_public_contract_reference,
        ),
        FuncCommand(
            "update-dev-metrics",
            update_dev_metrics_cmd.register_parser,
            update_dev_metrics_cmd.cmd_docs_update_dev_metrics,
        ),
        FuncCommand(
            "update-cli-reference",
            update_cli_reference_cmd.register_parser,
            update_cli_reference_cmd.cmd_docs_update_cli_reference,
        ),
        FuncCommand(
            "update-gitops-schema-reference",
            update_gitops_schema_reference_cmd.register_parser,
            update_gitops_schema_reference_cmd.cmd_docs_update_gitops_schema_reference,
        ),
        FuncCommand(
            "update-manifest-schema-reference",
            update_manifest_schema_reference_cmd.register_parser,
            update_manifest_schema_reference_cmd.cmd_docs_update_manifest_schema_reference,
        ),
        FuncCommand(
            "update-deprecation-roadmap",
            update_deprecation_roadmap_cmd.register_parser,
            update_deprecation_roadmap_cmd.cmd_docs_update_deprecation_roadmap,
        ),
        FuncCommand(
            "update-shim-removal-plan",
            update_shim_removal_plan_cmd.register_parser,
            update_shim_removal_plan_cmd.cmd_docs_update_shim_removal_plan,
        ),
    ]
