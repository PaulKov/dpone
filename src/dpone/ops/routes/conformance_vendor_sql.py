"""Lazy factory for Docker/vendor-live Route Conformance adapters."""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Any

from dpone.ops.routes.conformance_vendor_live import (
    VendorLiveRouteBinding,
    VendorRouteConformanceLiveAdapter,
)
from dpone.ops.routes.conformance_vendor_sql_store import (
    ClickHouseConformanceDialect,
    MssqlConformanceDialect,
    PostgresConformanceDialect,
    SqlRouteConformanceLiveStore,
)


class DockerVendorLiveRouteConformanceAdapterFactory:
    """Build the default Docker/vendor-live adapter from integration env vars."""

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        require_opt_in: bool = True,
    ) -> VendorRouteConformanceLiveAdapter:
        del cls
        source = env or os.environ
        postgres_store = SqlRouteConformanceLiveStore(
            backend="postgres",
            connector_factory=lambda: _postgres_connector(source),
            dialect=PostgresConformanceDialect(),
        )
        mssql_store = SqlRouteConformanceLiveStore(
            backend="mssql",
            connector_factory=lambda: _mssql_connector(source),
            dialect=MssqlConformanceDialect(),
        )
        clickhouse_store = SqlRouteConformanceLiveStore(
            backend="clickhouse",
            connector_factory=lambda: _clickhouse_connector(source),
            dialect=ClickHouseConformanceDialect(),
        )
        return VendorRouteConformanceLiveAdapter(
            bindings=(
                VendorLiveRouteBinding(
                    source="postgres",
                    sink="mssql",
                    strategy="incremental_merge",
                    source_store=postgres_store,
                    sink_store=mssql_store,
                    native_path="postgres_live_store_to_mssql_live_store",
                ),
                VendorLiveRouteBinding(
                    source="mssql",
                    sink="clickhouse",
                    strategy="incremental_merge",
                    source_store=mssql_store,
                    sink_store=clickhouse_store,
                    native_path="mssql_live_store_to_clickhouse_live_store",
                ),
            ),
            require_opt_in=require_opt_in,
            environ=source,
        )


def _postgres_connector(env: Mapping[str, str]) -> Any:
    from dpone.runtime.connectors.postgres import PostgresConnector

    return PostgresConnector(
        host=env.get("DPONE_IT_PG_HOST", env.get("DPONE_IT_POSTGRES_HOST", "127.0.0.1")),
        port=int(env.get("DPONE_IT_PG_PORT", env.get("DPONE_IT_PG_PORT_FORWARD", "55432"))),
        database=env.get("DPONE_IT_PG_DATABASE", "dpone_it"),
        user=env.get("DPONE_IT_PG_USER", "dpone"),
        password=env.get("DPONE_IT_PG_PASSWORD", "dpone"),
        application_name="dpone-route-conformance-vendor-live",
    )


def _mssql_connector(env: Mapping[str, str]) -> Any:
    from dpone.runtime.connectors.mssql import MSSQLConnector

    return MSSQLConnector(
        host=env.get("DPONE_IT_MSSQL_HOST", "127.0.0.1"),
        port=int(env.get("DPONE_IT_MSSQL_PORT", env.get("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
        database=env.get("DPONE_IT_MSSQL_DATABASE", "master"),
        user=env.get("DPONE_IT_MSSQL_USER", "sa"),
        password=env.get("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!"),
        driver=env.get("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=env.get("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        bcp_path=env.get("DPONE_IT_MSSQL_BCP_PATH", "bcp"),
        application_name="dpone-route-conformance-vendor-live",
    )


def _clickhouse_connector(env: Mapping[str, str]) -> Any:
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    return ClickHouseConnector(
        host=env.get("DPONE_IT_CH_HOST", env.get("DPONE_IT_CH_CLIENT_HOST", "127.0.0.1")),
        port=int(env.get("DPONE_IT_CH_PORT", env.get("DPONE_IT_CH_CLIENT_PORT", "59000"))),
        database=env.get("DPONE_IT_CH_DATABASE", "dpone_it"),
        user=env.get("DPONE_IT_CH_USER", "default"),
        password=env.get("DPONE_IT_CH_PASSWORD", "dpone"),
        secure=str(env.get("DPONE_IT_CH_SECURE", "0")).strip().lower() in {"1", "true", "yes", "on"},
        application_name="dpone-route-conformance-vendor-live",
    )


__all__ = ["DockerVendorLiveRouteConformanceAdapterFactory"]
