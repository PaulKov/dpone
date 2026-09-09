from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from dpone.readiness.airflow_mssql_database_authority_checks import (
    validate_governed_mssql_database_authorities,
)


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    (
        ("missing_target", "DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED"),
        ("missing_staging", "DPONE_MSSQL_STAGING_DATABASE_AUTHORITY_REQUIRED"),
        ("missing_state", "DPONE_MSSQL_STATE_DATABASE_AUTHORITY_REQUIRED"),
        ("malformed", "DPONE_MSSQL_DATABASE_AUTHORITY_INVALID"),
    ),
)
def test_connection_check_requires_every_governed_database_pin(
    mutation: str,
    expected_code: str,
) -> None:
    registry = _registry()
    target = registry["connections"]["mssql_target"]["connection"]
    state = registry["connections"]["mssql_state"]["connection"]
    if mutation == "missing_target":
        target["database_authorities"].pop("DWH")
    elif mutation == "missing_staging":
        target["database_authorities"].pop("DWH_Stage")
    elif mutation == "missing_state":
        state["database_authorities"].clear()
    else:
        target["database_authorities"]["DWH"]["database_guid"] = "NOT-A-GUID"

    errors = validate_governed_mssql_database_authorities(
        pipeline_source=_pipeline(),
        connection_ref_map={
            "source": "postgres_source",
            "target": "mssql_target",
            "state": "mssql_state",
        },
        registry=registry,
        path=Path("platform/connection-registries/prod.yaml"),
    )

    assert [error["code"] for error in errors] == [expected_code]
    assert errors[0]["stage"] == "check_connections"
    assert errors[0]["entity"] == {"kind": "process", "id": "orders"}
    assert errors[0]["docs_url"].endswith(f"{expected_code}.md")
    assert errors[0]["fixes"] == [
        {
            "id": "declare_mssql_database_authorities",
            "safety": "manual",
        }
    ]


def test_connection_check_accepts_exact_target_staging_state_pin_set() -> None:
    assert (
        validate_governed_mssql_database_authorities(
            pipeline_source=_pipeline(),
            connection_ref_map={
                "source": "postgres_source",
                "target": "mssql_target",
                "state": "mssql_state",
            },
            registry=_registry(),
            path=Path("platform/connection-registries/prod.yaml"),
        )
        == []
    )


@pytest.mark.parametrize(
    "endpoint_type",
    (
        "mssql",
        "MSSQL",
        "microsoft mssql",
        "microsoft_mssql",
        "odbc",
        "sqlserver",
        "sql_server",
        "sql-server",
    ),
)
def test_connection_check_aliases_cannot_bypass_database_authority(endpoint_type: str) -> None:
    pipeline = _pipeline()
    process = pipeline["processes"][0]
    process["sink"]["type"] = endpoint_type
    process["state"]["type"] = endpoint_type
    registry = _registry()
    registry["connections"]["mssql_target"]["connection"]["database_authorities"].pop("DWH")

    errors = validate_governed_mssql_database_authorities(
        pipeline_source=pipeline,
        connection_ref_map={
            "source": "postgres_source",
            "target": "mssql_target",
            "state": "mssql_state",
        },
        registry=registry,
        path=Path("platform/connection-registries/prod.yaml"),
    )

    assert [error["code"] for error in errors] == ["DPONE_MSSQL_TARGET_DATABASE_AUTHORITY_REQUIRED"]


def test_connection_check_does_not_mutate_signed_registry() -> None:
    registry = _registry()
    before = deepcopy(registry)

    validate_governed_mssql_database_authorities(
        pipeline_source=_pipeline(),
        connection_ref_map={"target": "mssql_target", "state": "mssql_state"},
        registry=registry,
        path=Path("platform/connection-registries/prod.yaml"),
    )

    assert registry == before


def _pipeline() -> dict[str, Any]:
    return {
        "kind": "dpone.flow.v1",
        "processes": [
            {
                "name": "orders",
                "source": {"type": "postgres", "connection_ref": "source"},
                "sink": {
                    "type": "mssql",
                    "connection_ref": "target",
                    "staging": {"database": "DWH_Stage", "schema": "staging"},
                },
                "state": {
                    "type": "mssql",
                    "connection_ref": "state",
                    "atomicity": "target_atomic",
                    "provisioning": "external",
                },
            }
        ],
    }


def _registry() -> dict[str, Any]:
    return {
        "schema": "dpone.connection-registry.v1",
        "connections": {
            "postgres_source": {"type": "postgres", "connection": {"database": "source"}},
            "mssql_target": {
                "type": "mssql",
                "connection": {
                    "database": "DWH",
                    "database_authorities": {
                        "DWH": _pin(7, "1"),
                        "DWH_Stage": _pin(8, "2"),
                    },
                },
            },
            "mssql_state": {
                "type": "mssql",
                "connection": {
                    "database": "Example_System",
                    "database_authorities": {"Example_System": _pin(9, "3")},
                },
            },
        },
    }


def _pin(database_id: int, token: str) -> dict[str, Any]:
    return {
        "database_id": database_id,
        "create_token": f"2026-08-16T00:00:0{token}.0000000",
        "database_guid": f"{token * 8}-{token * 4}-{token * 4}-{token * 4}-{token * 12}",
    }
