from .check_compatibility_cmd import (
    cmd_docs_check_compatibility,
)
from .check_compatibility_cmd import (
    register_parser as register_check_compatibility_parser,
)
from .check_docs_cmd import cmd_docs_check_docs
from .check_docs_cmd import register_parser as register_check_docs_parser
from .check_generated_references_cmd import cmd_docs_check_generated_references
from .check_generated_references_cmd import (
    register_parser as register_check_generated_references_parser,
)
from .check_import_rules_cmd import cmd_docs_check_import_rules
from .check_import_rules_cmd import register_parser as register_check_import_rules_parser
from .check_layer_metrics_cmd import cmd_docs_check_layer_metrics
from .check_layer_metrics_cmd import register_parser as register_check_layer_metrics_parser
from .check_module_size_cmd import cmd_docs_check_module_size
from .check_module_size_cmd import register_parser as register_check_module_size_parser
from .update_dev_metrics_cmd import cmd_docs_update_dev_metrics
from .update_dev_metrics_cmd import register_parser as register_update_dev_metrics_parser

__all__ = [
    "cmd_docs_check_airflow_public_contracts",
    "register_check_airflow_public_contracts_parser",
    "cmd_docs_check_compatibility",
    "register_check_compatibility_parser",
    "cmd_docs_check_docs",
    "register_check_docs_parser",
    "cmd_docs_check_generated_references",
    "register_check_generated_references_parser",
    "cmd_docs_check_import_rules",
    "register_check_import_rules_parser",
    "cmd_docs_check_layer_metrics",
    "register_check_layer_metrics_parser",
    "cmd_docs_check_module_size",
    "register_check_module_size_parser",
    "cmd_docs_update_dev_metrics",
    "register_update_dev_metrics_parser",
    "cmd_docs_update_cli_reference",
    "register_update_cli_reference_parser",
    "cmd_docs_update_gitops_schema_reference",
    "register_update_gitops_schema_reference_parser",
    "cmd_docs_update_manifest_schema_reference",
    "register_update_manifest_schema_reference_parser",
    "cmd_docs_update_deprecation_roadmap",
    "register_update_deprecation_roadmap_parser",
    "cmd_docs_update_shim_removal_plan",
    "register_update_shim_removal_plan_parser",
    "cmd_docs_check_removal_readiness",
    "register_check_removal_readiness_parser",
    "cmd_docs_check_rest_delivery_design_contract",
    "register_check_rest_delivery_design_contract_parser",
    "cmd_docs_update_airflow_public_contract_reference",
    "register_update_airflow_public_contract_reference_parser",
]

from .airflow_public_contracts_cmd import (
    cmd_docs_check_airflow_public_contracts,
    cmd_docs_update_airflow_public_contract_reference,
)
from .airflow_public_contracts_cmd import (
    register_check_parser as register_check_airflow_public_contracts_parser,
)
from .airflow_public_contracts_cmd import (
    register_update_parser as register_update_airflow_public_contract_reference_parser,
)
from .check_removal_readiness_cmd import (
    cmd_docs_check_removal_readiness,
)
from .check_removal_readiness_cmd import (
    register_parser as register_check_removal_readiness_parser,
)
from .check_rest_delivery_design_contract_cmd import (
    cmd_docs_check_rest_delivery_design_contract,
)
from .check_rest_delivery_design_contract_cmd import (
    register_parser as register_check_rest_delivery_design_contract_parser,
)
from .update_cli_reference_cmd import (
    cmd_docs_update_cli_reference,
)
from .update_cli_reference_cmd import (
    register_parser as register_update_cli_reference_parser,
)
from .update_deprecation_roadmap_cmd import (
    cmd_docs_update_deprecation_roadmap,
)
from .update_deprecation_roadmap_cmd import (
    register_parser as register_update_deprecation_roadmap_parser,
)
from .update_gitops_schema_reference_cmd import (
    cmd_docs_update_gitops_schema_reference,
)
from .update_gitops_schema_reference_cmd import (
    register_parser as register_update_gitops_schema_reference_parser,
)
from .update_manifest_schema_reference_cmd import (
    cmd_docs_update_manifest_schema_reference,
)
from .update_manifest_schema_reference_cmd import (
    register_parser as register_update_manifest_schema_reference_parser,
)
from .update_shim_removal_plan_cmd import (
    cmd_docs_update_shim_removal_plan,
)
from .update_shim_removal_plan_cmd import (
    register_parser as register_update_shim_removal_plan_parser,
)
