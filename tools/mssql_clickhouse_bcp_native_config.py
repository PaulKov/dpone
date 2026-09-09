"""Configuration projection helpers for the BCP-native certification CLI."""

from __future__ import annotations

import argparse
import os
from typing import Any


def native_transfer_options(binary_format: str, acceleration_mode: str) -> dict[str, Any]:
    return {
        "wire": {
            "mode": "typed_binary",
            "source_native_format": "bcp_native",
            "binary_format": str(binary_format),
            "block_rows": 65_536,
            "block_bytes": "64MiB",
            "acceleration": {"mode": str(acceleration_mode)},
        },
        "execution": {
            "mode": "pipelined",
            "profile": "safe_worker",
            "cleanup_policy": "eager",
            "resume_policy": "staging_if_verified",
            "resource_policy": {
                "max_active_files": 1,
                "target_file_bytes": "64MiB",
                "max_file_bytes": "128MiB",
            },
        },
    }


def clickhouse_http_options(config: Any) -> dict[str, Any]:
    params = config.clickhouse_params
    return {
        "host": params.get("http_host") or params.get("host"),
        "port": int(params.get("http_port", 8123)),
        "database": config.target_database,
        "user": params.get("username") or params.get("user", "default"),
        "password": params.get("password", ""),
    }


def add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--mssql-host", default=os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"))
    parser.add_argument(
        "--mssql-port",
        type=int,
        default=int(os.getenv("DPONE_IT_MSSQL_PORT", os.getenv("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
    )
    parser.add_argument("--mssql-database", default=os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone_it"))
    parser.add_argument("--mssql-user", default=os.getenv("DPONE_IT_MSSQL_USER", "sa"))
    parser.add_argument("--mssql-password", default=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""))
    parser.add_argument("--mssql-driver", default=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"))
    parser.add_argument("--mssql-bcp-path", default=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"))
    parser.add_argument("--clickhouse-host", default=os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"))
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(os.getenv("DPONE_IT_CH_PORT", os.getenv("DPONE_IT_CH_PORT_FORWARD", "59000"))),
    )
    parser.add_argument(
        "--clickhouse-http-port",
        type=int,
        default=int(os.getenv("DPONE_IT_CH_HTTP_PORT", os.getenv("DPONE_IT_CH_HTTP_PORT_FORWARD", "58123"))),
    )
    parser.add_argument("--clickhouse-database", default=os.getenv("DPONE_IT_CH_DATABASE", "dpone_it"))
    parser.add_argument("--clickhouse-user", default=os.getenv("DPONE_IT_CH_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("DPONE_IT_CH_PASSWORD", ""))


__all__ = ["add_connection_args", "clickhouse_http_options", "native_transfer_options"]
