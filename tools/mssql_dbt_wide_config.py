"""CLI connection arguments for the local wide dbt proof."""

from __future__ import annotations

import argparse
import os


def add_mssql_args(parser: argparse.ArgumentParser) -> None:
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


__all__ = ["add_mssql_args"]
