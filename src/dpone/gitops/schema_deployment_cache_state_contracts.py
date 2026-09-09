from __future__ import annotations

from dpone.contracts.deployment_cache_retention_state_schema import (
    activation_history_schema_properties,
    retention_recovery_schema_properties,
)
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def deployment_cache_state_contracts() -> tuple[GitOpsSchemaContract, ...]:
    """Return closed schemas for destructive-retention control state."""

    return (
        deployment_cache_activation_history_contract(),
        deployment_cache_retention_recovery_contract(),
        deployment_cache_retention_recovery_v2_contract(),
    )


def deployment_cache_activation_history_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="deployment-cache-activation-history-v2",
        kind="dpone.deployment-cache-activation-history.v2",
        title="dpone GitOps verified Airflow cache activation history",
        required=("schema", "revision", "entries", "legacy_v1_diagnostics"),
        properties=activation_history_schema_properties(),
        additional_properties=False,
    )


def deployment_cache_retention_recovery_contract() -> GitOpsSchemaContract:
    return _retention_recovery_contract(
        name="deployment-cache-retention-recovery",
        kind="dpone.deployment-cache-retention-recovery.v1",
        properties=retention_recovery_schema_properties(legacy_v1=True),
    )


def deployment_cache_retention_recovery_v2_contract() -> GitOpsSchemaContract:
    return _retention_recovery_contract(
        name="deployment-cache-retention-recovery-v2",
        kind="dpone.deployment-cache-retention-recovery.v2",
        properties=retention_recovery_schema_properties(),
    )


def _retention_recovery_contract(
    *,
    name: str,
    kind: str,
    properties: dict[str, object],
) -> GitOpsSchemaContract:
    contract = documented_contract(
        name=name,
        kind=kind,
        title="dpone GitOps deployment cache retention recovery journal",
        required=(
            "schema",
            "revision",
            "status",
            "restored_deployment_ids",
            "pending_deployment_ids",
            "transactions",
        ),
        properties=properties,
        additional_properties=False,
    )
    contract.schema["allOf"] = [
        {
            "if": {"properties": {"status": {"const": "recovered"}}, "required": ["status"]},
            "then": {
                "properties": {
                    "pending_deployment_ids": {"maxItems": 0},
                    "quarantined_paths": {"maxItems": 0},
                }
            },
        },
        {
            "if": {"properties": {"status": {"const": "recovering"}}, "required": ["status"]},
            "then": {
                "properties": {
                    "pending_deployment_ids": {"minItems": 1},
                    "quarantined_paths": {"maxItems": 0},
                }
            },
        },
        {
            "if": {"properties": {"status": {"const": "blocked"}}, "required": ["status"]},
            "then": {"properties": {"quarantined_paths": {"minItems": 1}}},
        },
    ]
    return contract


__all__ = ["deployment_cache_state_contracts"]
