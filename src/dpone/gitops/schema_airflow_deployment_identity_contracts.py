from __future__ import annotations

from dpone.contracts.airflow_run_identity import airflow_deployment_identity_schema
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def airflow_deployment_identity_contract() -> GitOpsSchemaContract:
    schema = airflow_deployment_identity_schema()
    return documented_contract(
        name="airflow-deployment-identity",
        kind="dpone.airflow-deployment-identity.v1",
        title="dpone GitOps exact Airflow cache activation identity",
        required=tuple(schema["required"]),
        properties=dict(schema["properties"]),
        additional_properties=False,
    )


__all__ = ["airflow_deployment_identity_contract", "airflow_deployment_identity_schema"]
