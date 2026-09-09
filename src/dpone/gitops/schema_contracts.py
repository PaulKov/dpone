from __future__ import annotations

from typing import Any

from dpone.gitops.schema_airflow_cache_status_contracts import airflow_cache_status_schema_contracts
from dpone.gitops.schema_airflow_contracts import airflow_schema_contracts
from dpone.gitops.schema_authoring_migration_contracts import authoring_migration_schema_contract
from dpone.gitops.schema_catalog_bundle_contracts import catalog_bundle_schema_contracts
from dpone.gitops.schema_core_contracts import core_schema_contracts
from dpone.gitops.schema_deployment_cache_contracts import deployment_cache_schema_contracts
from dpone.gitops.schema_deployment_cache_state_contracts import deployment_cache_state_contracts
from dpone.gitops.schema_environment_contracts import environment_schema_contracts
from dpone.gitops.schema_hermetic_test_contracts import hermetic_test_schema_contracts
from dpone.gitops.schema_release_deployment_contracts import release_deployment_schema_contracts
from dpone.gitops.schema_route_attestation_contracts import route_attestation_schema_contracts
from dpone.gitops.schema_route_certification_matrix_contracts import (
    route_certification_matrix_contract,
    route_matrix_claim_contract,
)
from dpone.gitops.schema_safe_sample_deployment_context_contracts import (
    safe_sample_deployment_context_schema_contracts,
)
from dpone.gitops.schema_safe_sample_planning_contracts import safe_sample_planning_schema_contracts
from dpone.gitops.schema_safe_sample_route_execution_contracts import safe_sample_route_execution_schema_contracts
from dpone.gitops.schema_safe_sample_runtime_contracts import safe_sample_runtime_schema_contracts
from dpone.gitops.schema_selected_safe_sample_contracts import selected_safe_sample_schema_contract
from dpone.gitops.schema_self_service_authoring_contracts import (
    self_service_authoring_schema_contracts,
)
from dpone.gitops.schema_self_service_certification_contracts import (
    self_service_certification_schema_contracts,
)


def gitops_schema_contracts() -> tuple[Any, ...]:
    return (
        *core_schema_contracts(),
        *release_deployment_schema_contracts(),
        *environment_schema_contracts(),
        *deployment_cache_schema_contracts(),
        *airflow_cache_status_schema_contracts(),
        *deployment_cache_state_contracts(),
        *route_attestation_schema_contracts(),
        *self_service_authoring_schema_contracts(),
        route_matrix_claim_contract(),
        route_certification_matrix_contract(),
        *self_service_certification_schema_contracts(),
        *safe_sample_planning_schema_contracts(),
        *safe_sample_deployment_context_schema_contracts(),
        *safe_sample_runtime_schema_contracts(),
        *safe_sample_route_execution_schema_contracts(),
        selected_safe_sample_schema_contract(),
        *hermetic_test_schema_contracts(),
        authoring_migration_schema_contract(),
        *airflow_schema_contracts(),
        *catalog_bundle_schema_contracts(),
    )


def get_gitops_schema_contract(raw_kind: str) -> Any | None:
    needle = str(raw_kind).strip()
    for registered_contract in gitops_schema_contracts():
        if needle in {registered_contract.name, registered_contract.kind}:
            return registered_contract
    return None


__all__ = ["get_gitops_schema_contract", "gitops_schema_contracts"]
