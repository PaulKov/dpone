from __future__ import annotations

from .airflow_self_service_cmd import airflow_group, check_command, fix_command
from .base import Command
from .data_product_cmd import data_group
from .recipe_cmd import recipe_group
from .registry_core import (
    backfill_group,
    connectors_group,
    normalize_group,
    observability_group,
    orchestration_group,
    perf_group,
    profile_group,
    runtime_group,
    state_group,
    strategy_group,
    supply_chain_group,
    top_level_commands,
)
from .registry_dag import dag_group
from .registry_dbt import dbt_group
from .registry_docs import docs_group
from .registry_gitops import gitops_group
from .registry_hooks import hooks_group
from .registry_manifest import manifest_group
from .registry_migrate import migrate_group
from .registry_ops import operations_group
from .registry_schema import cdc_group, cdc_plan_top_level, schema_group
from .registry_workload import workload_group


def get_commands() -> list[Command]:
    return [
        *top_level_commands(),
        recipe_group(),
        check_command(),
        fix_command(),
        cdc_plan_top_level(),
        backfill_group(),
        state_group(),
        normalize_group(),
        connectors_group(),
        operations_group(),
        orchestration_group(),
        observability_group(),
        supply_chain_group(),
        strategy_group(),
        perf_group(),
        profile_group(),
        runtime_group(),
        data_group(),
        schema_group(),
        cdc_group(),
        hooks_group(),
        manifest_group(),
        migrate_group(),
        gitops_group(),
        workload_group(),
        airflow_group(),
        dag_group(),
        docs_group(),
        dbt_group(),
    ]
