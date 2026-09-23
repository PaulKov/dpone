"""Closed v6 credential delivery, orthogonal to development authorization."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract
from dpone.gitops.schema_release_deployment_v3_contracts import (
    airflow_deployment_index_v3_contract,
    deployment_set_v3_contract,
)
from dpone.gitops.schema_runtime_artifact_delivery import (
    immutable_runtime_authority_source_schema,
    runtime_authority_source_schema,
)
from dpone.gitops.schema_runtime_credential_projection import credential_projection_descriptor_schema


def credential_delivery_v6_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (_v6(deployment_set_v3_contract()), _v6(airflow_deployment_index_v3_contract()))


def _v6(source: GitOpsSchemaContract) -> GitOpsSchemaContract:
    schema = deepcopy(source.schema)
    name, kind = source.name.replace("-v3", "-v6"), source.kind.replace(".v3", ".v6")
    schema["$id"] = schema["$id"].replace(source.name, name)
    schema["title"] = schema["title"].replace("v3", "v6")
    schema["properties"]["schema"] = {"const": kind}
    schema["properties"]["credential_projection"] = credential_projection_descriptor_schema()
    schema["properties"]["development_authority_required"] = {"const": True}
    schema["required"] = [field for field in schema["required"] if field != "mssql_asset_outlet_projection"] + [
        "credential_projection"
    ]
    delivery = schema["properties"]["runtime_artifact_delivery"]
    delivery["properties"]["runtime_authority"] = {
        "oneOf": [runtime_authority_source_schema(), immutable_runtime_authority_source_schema()]
    }
    schema["allOf"].append(_development_guard())
    return GitOpsSchemaContract(name=name, kind=kind, schema=schema)


def _development_guard() -> dict[str, Any]:
    return {
        "if": {"required": ["development_authority_required"]},
        "then": {
            "properties": {
                "trust_tier": {"const": "non_production"},
                "runtime_artifact_delivery": {"required": ["runtime_authority"]},
            }
        },
        "else": {"properties": {"runtime_artifact_delivery": {"not": {"required": ["runtime_authority"]}}}},
    }
