from __future__ import annotations

from .base import Command
from .docs import (
    airflow_public_contracts_cmd,
    check_architecture_fitness_cmd,
    check_compatibility_cmd,
    check_docs_cmd,
    check_generated_references_cmd,
    check_import_rules_cmd,
    check_layer_metrics_cmd,
    check_module_size_cmd,
    check_removal_readiness_cmd,
    check_rest_delivery_design_contract_cmd,
)
from .func_command import FuncCommand


def docs_check_commands() -> list[Command]:
    return [
        FuncCommand(
            "check-airflow-public-contracts",
            airflow_public_contracts_cmd.register_check_parser,
            airflow_public_contracts_cmd.cmd_docs_check_airflow_public_contracts,
        ),
        FuncCommand("check-docs", check_docs_cmd.register_parser, check_docs_cmd.cmd_docs_check_docs),
        FuncCommand(
            "check-generated-references",
            check_generated_references_cmd.register_parser,
            check_generated_references_cmd.cmd_docs_check_generated_references,
        ),
        FuncCommand(
            "check-compatibility",
            check_compatibility_cmd.register_parser,
            check_compatibility_cmd.cmd_docs_check_compatibility,
        ),
        FuncCommand(
            "check-import-rules",
            check_import_rules_cmd.register_parser,
            check_import_rules_cmd.cmd_docs_check_import_rules,
        ),
        FuncCommand(
            "check-layer-metrics",
            check_layer_metrics_cmd.register_parser,
            check_layer_metrics_cmd.cmd_docs_check_layer_metrics,
        ),
        FuncCommand(
            "check-architecture-fitness",
            check_architecture_fitness_cmd.register_parser,
            check_architecture_fitness_cmd.cmd_docs_check_architecture_fitness,
        ),
        FuncCommand(
            "check-module-size",
            check_module_size_cmd.register_parser,
            check_module_size_cmd.cmd_docs_check_module_size,
        ),
        FuncCommand(
            "check-removal-readiness",
            check_removal_readiness_cmd.register_parser,
            check_removal_readiness_cmd.cmd_docs_check_removal_readiness,
        ),
        FuncCommand(
            "check-rest-delivery-design-contract",
            check_rest_delivery_design_contract_cmd.register_parser,
            check_rest_delivery_design_contract_cmd.cmd_docs_check_rest_delivery_design_contract,
        ),
    ]
