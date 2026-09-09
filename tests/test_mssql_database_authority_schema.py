from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from dpone.gitops.schema_contracts import gitops_schema_contracts


def test_committed_connection_registry_schema_matches_canonical_producer() -> None:
    committed = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    canonical = next(
        contract.schema for contract in gitops_schema_contracts() if contract.name == "connection-registry"
    )

    assert committed == canonical


@pytest.mark.parametrize(
    "mutation",
    ("empty", "uppercase_guid", "extra_identity_field", "zero_database_id"),
)
def test_connection_registry_schema_rejects_noncanonical_database_authority(
    mutation: str,
) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    payload = _registry()
    authorities = payload["connections"]["target"]["connection"]["database_authorities"]
    if mutation == "empty":
        authorities.clear()
    elif mutation == "uppercase_guid":
        authorities["DWH"]["database_guid"] = str(authorities["DWH"]["database_guid"]).upper()
    elif mutation == "extra_identity_field":
        authorities["DWH"]["server"] = "listener"
    else:
        authorities["DWH"]["database_id"] = 0

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(payload, schema)


def test_connection_registry_schema_accepts_finite_target_staging_state_pins() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    schema = json.loads(Path("docs/schemas/gitops/connection-registry.schema.json").read_text(encoding="utf-8"))
    payload = _registry()
    state = deepcopy(payload["connections"]["target"])
    state["connection"] = {
        **state["connection"],
        "database": "Example_System",
        "database_authorities": {"Example_System": _pin(9, "3")},
    }
    payload["connections"]["state"] = state

    jsonschema.validate(payload, schema)


def _registry() -> dict[str, Any]:
    return {
        "schema": "dpone.connection-registry.v1",
        "environment": "prod",
        "connections": {
            "target": {
                "type": "mssql",
                "connection": {
                    "host": "sql.internal",
                    "database": "DWH",
                    "database_authorities": {
                        "DWH": _pin(7, "1"),
                        "DWH_Stage": _pin(8, "2"),
                    },
                },
                "credentials": {
                    "resolver": "airflow_connection",
                    "connection_id": "mssql_prod",
                    "execution_mode": "operator_bridge",
                },
            }
        },
    }


def _pin(database_id: int, token: str) -> dict[str, Any]:
    guid_token = {"1": "a", "2": "b", "3": "c"}[token]
    return {
        "database_id": database_id,
        "create_token": f"2026-08-16T00:00:0{token}.0000000",
        "database_guid": (f"{guid_token * 8}-{guid_token * 4}-{guid_token * 4}-{guid_token * 4}-{guid_token * 12}"),
    }
