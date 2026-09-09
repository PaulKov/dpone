"""P0 binding-set integration regressions for MSSQL outlet projection."""

from __future__ import annotations

import pytest
from dpone_airflow_pack.mssql_asset_ref_codec import asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_contract import PROJECTION_INVALID

from dpone.readiness.airflow_deployment_projection_errors import (
    AirflowDeploymentProjectionError,
)
from dpone.readiness.airflow_mssql_outlet_projection import build_mssql_asset_outlet_projection

_ASSET_REF = {
    "engine": "mssql",
    "connection_ref": "mssql_marts",
    "database": "DWH",
    "schema": "dbo",
    "table": "orders",
}


def _pack_payloads() -> dict[str, dict]:
    return {
        "demo": {
            "airflow": {
                "execution": {
                    "outlets": [{"asset_ref": dict(_ASSET_REF)}],
                }
            }
        }
    }


def test_logical_ref_resolves_via_binding_to_prod_registry_uri() -> None:
    binding_set = {
        "schema": "dpone.binding-set.v1",
        "environment": "prod",
        "bindings": {"mssql_marts": {"connection_ref": "mssql_prod_writer"}},
    }
    registry = {
        "schema": "dpone.connection-registry.v1",
        "environment": "prod",
        "connections": {
            "mssql_marts": {
                "type": "mssql",
                "connection": {
                    "asset_authority": {"host": "sql-logical-must-not-win.internal", "port": 1433},
                    "database": "DWH",
                },
            },
            "mssql_prod_writer": {
                "type": "mssql",
                "connection": {
                    "asset_authority": {"host": "sql-prod.internal", "port": 1433},
                    "database": "DWH",
                },
            },
        },
    }
    projection = build_mssql_asset_outlet_projection(
        environment="prod",
        binding_set=binding_set,
        binding_set_ref="sha256:" + "b" * 64,
        connection_registry=registry,
        connection_registry_ref="sha256:" + "c" * 64,
        workload_packs=[{"id": "demo"}],
        pack_payloads=_pack_payloads(),
    )
    assert projection is not None
    digest = asset_ref_sha256(_ASSET_REF)
    assert digest is not None
    entry = next(item for item in projection["entries"] if item["asset_ref_sha256"] == digest)
    assert entry["registry_connection_ref"] == "mssql_prod_writer"
    assert entry["resolved_binding"]["registry_connection_ref"] == "mssql_prod_writer"
    assert entry["uri"] == "mssql://sql-prod.internal:1433/DWH/dbo/orders"
    assert entry["workload_ids"] == ["demo"]
    assert "sql-logical-must-not-win" not in entry["uri"]
    assert "|" not in digest


def test_missing_binding_is_blocker() -> None:
    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        build_mssql_asset_outlet_projection(
            environment="prod",
            binding_set={"schema": "dpone.binding-set.v1", "bindings": {}},
            binding_set_ref="sha256:" + "b" * 64,
            connection_registry={
                "connections": {
                    "mssql_prod_writer": {
                        "type": "mssql",
                        "connection": {"asset_authority": {"host": "sql-prod.internal", "port": 1433}},
                    }
                }
            },
            connection_registry_ref="sha256:" + "c" * 64,
            workload_packs=[{"id": "demo"}],
            pack_payloads=_pack_payloads(),
        )
    assert exc.value.code == PROJECTION_INVALID
    assert "not bound" in str(exc.value).lower()


def test_binding_to_missing_registry_entry_is_blocker() -> None:
    with pytest.raises(AirflowDeploymentProjectionError) as exc:
        build_mssql_asset_outlet_projection(
            environment="prod",
            binding_set={
                "bindings": {"mssql_marts": {"connection_ref": "mssql_prod_writer"}},
            },
            binding_set_ref="sha256:" + "b" * 64,
            connection_registry={"connections": {}},
            connection_registry_ref="sha256:" + "c" * 64,
            workload_packs=[{"id": "demo"}],
            pack_payloads=_pack_payloads(),
        )
    assert exc.value.code == PROJECTION_INVALID
    assert "mssql_prod_writer" in str(exc.value)


def test_uses_only_bound_ref_when_logical_and_bound_differ() -> None:
    projection = build_mssql_asset_outlet_projection(
        environment="prod",
        binding_set={"bindings": {"mssql_marts": {"connection_ref": "mssql_prod_writer"}}},
        binding_set_ref="sha256:" + "b" * 64,
        connection_registry={
            "connections": {
                "mssql_marts": {
                    "type": "mssql",
                    "connection": {"asset_authority": {"host": "sql-dev.internal", "port": 1433}},
                },
                "mssql_prod_writer": {
                    "type": "mssql",
                    "connection": {"asset_authority": {"host": "sql-prod.internal", "port": 1433}},
                },
            }
        },
        connection_registry_ref="sha256:" + "c" * 64,
        workload_packs=[{"id": "demo"}],
        pack_payloads=_pack_payloads(),
    )
    assert projection is not None
    assert projection["entries"][0]["uri"] == "mssql://sql-prod.internal:1433/DWH/dbo/orders"
