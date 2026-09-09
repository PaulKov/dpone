"""Build-once: same dbt release digest, env-specific MSSQL outlets at deployment."""

from __future__ import annotations

import json
from pathlib import Path

from dpone_airflow_pack.asset_outlets import outlet_uris_from_pack
from dpone_airflow_pack.mssql_asset_ref_codec import asset_ref_sha256
from dpone_airflow_pack.mssql_outlet_projection_contract import uri_by_asset_ref_sha256

from dpone.gitops.airflow_mssql_logical_asset import MssqlLogicalAssetRef
from dpone.readiness.airflow_mssql_outlet_projection import build_mssql_asset_outlet_projection


def test_compile_once_release_bytes_unchanged_prod_outlet_from_projection() -> None:
    release_pack = {
        "kind": "gitops.airflow_pack",
        "airflow": {
            "execution": {
                "outlets": [
                    {
                        "asset_ref": {
                            "engine": "mssql",
                            "connection_ref": "mssql_marts",
                            "database": "DWH",
                            "schema": "dbo",
                            "table": "orders",
                        }
                    }
                ]
            }
        },
    }
    release_bytes = json.dumps(release_pack, sort_keys=True).encode("utf-8")
    binding_set = {"bindings": {"mssql_marts": {"connection_ref": "mssql_marts"}}}

    dev_registry = {
        "schema": "dpone.connection-registry.v1",
        "environment": "dev",
        "connections": {
            "mssql_marts": {
                "type": "mssql",
                "connection": {
                    "asset_authority": {"host": "sql-dev.internal", "port": 1433},
                    "database": "DWH",
                },
            }
        },
    }
    prod_registry = {
        "schema": "dpone.connection-registry.v1",
        "environment": "prod",
        "connections": {
            "mssql_marts": {
                "type": "mssql",
                "connection": {
                    "asset_authority": {"host": "sql-prod.internal", "port": 1433},
                    "database": "DWH",
                },
            }
        },
    }
    packs = [{"id": "demo"}]
    payloads = {"demo": release_pack}

    dev_projection = build_mssql_asset_outlet_projection(
        environment="dev",
        binding_set=binding_set,
        binding_set_ref="sha256:" + "a" * 64,
        connection_registry=dev_registry,
        connection_registry_ref="sha256:" + "a" * 64,
        workload_packs=packs,
        pack_payloads=payloads,
    )
    prod_projection = build_mssql_asset_outlet_projection(
        environment="prod",
        binding_set=binding_set,
        binding_set_ref="sha256:" + "b" * 64,
        connection_registry=prod_registry,
        connection_registry_ref="sha256:" + "b" * 64,
        workload_packs=packs,
        pack_payloads=payloads,
    )
    assert dev_projection is not None and prod_projection is not None
    dev_map = uri_by_asset_ref_sha256(dev_projection)
    prod_map = uri_by_asset_ref_sha256(prod_projection)
    assert "sql-dev.internal" in next(iter(dev_map.values()))
    assert "sql-prod.internal" in next(iter(prod_map.values()))

    # Release bytes stay untouched across env projections.
    assert json.dumps(release_pack, sort_keys=True).encode("utf-8") == release_bytes
    assert "sql-dev.internal" not in release_bytes.decode("utf-8")
    assert "sql-prod.internal" not in release_bytes.decode("utf-8")

    prod_uris = outlet_uris_from_pack(
        release_pack,
        mssql_asset_uri_by_ref=prod_map,
    )
    assert prod_uris == ("mssql://sql-prod.internal:1433/DWH/dbo/orders",)

    key = MssqlLogicalAssetRef(
        connection_ref="mssql_marts",
        database="DWH",
        schema="dbo",
        table="orders",
    ).identity_key()
    assert key == asset_ref_sha256(
        {
            "engine": "mssql",
            "connection_ref": "mssql_marts",
            "database": "DWH",
            "schema": "dbo",
            "table": "orders",
        }
    )
    assert prod_map[key] == "mssql://sql-prod.internal:1433/DWH/dbo/orders"


def test_registry_resolver_reads_bytes_once(tmp_path: Path, monkeypatch) -> None:
    from dpone.gitops import airflow_mssql_registry_snapshot as snapshot_module
    from dpone.gitops.airflow_mssql_registry_path import read_mssql_registry_bytes
    from tests.test_airflow_asset_uri_mssql_aip60 import _write_registry

    _write_registry(tmp_path, host="sql-prod.internal", port=1433)
    calls = {"n": 0}
    real = read_mssql_registry_bytes

    def once(path, *, max_bytes=1024 * 1024):  # noqa: ANN001
        calls["n"] += 1
        return real(path, max_bytes=max_bytes)

    monkeypatch.setattr(snapshot_module, "read_mssql_registry_bytes", once)
    registry = snapshot_module.resolve_mssql_asset_registry(tmp_path, env="dev")
    assert registry.ok
    assert calls["n"] == 1
    assert {auth.host for auth in registry.authorities.values()} == {"sql-prod.internal"}
