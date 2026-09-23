"""Frozen production dependencies for one SQL Server OBSERVE composition."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from functools import cache
from typing import Any

from dpone.adapters.mssql_permission_preparation_capabilities import PythonSqlClientObserveLauncher
from dpone.adapters.mssql_sqlclient_observe_transport import (
    SqlClientObserveCatalog,
    read_frame,
    require_quiet,
    write_frame,
)
from dpone.adapters.mssql_tds_coordinator_connection import decode_connection_admission
from dpone.app.mssql_tds_coordinator_composition import (
    create_tds_coordinator,
    open_tds_coordinator_evidence,
)

Operation = Callable[..., Any]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientObserveDependencies:
    """Concrete process, transport and evidence effects for one OBSERVE open."""

    launcher_type: type[PythonSqlClientObserveLauncher]
    catalog_type: type[SqlClientObserveCatalog]
    read_frame: Operation
    write_frame: Operation
    require_quiet: Operation
    create_coordinator: Operation
    open_evidence: Operation
    decode_connection_admission: Operation


@cache
def sqlclient_observe_dependencies() -> SqlClientObserveDependencies:
    """Return one immutable production OBSERVE bundle for the process lifetime."""
    return SqlClientObserveDependencies(
        launcher_type=PythonSqlClientObserveLauncher,
        catalog_type=SqlClientObserveCatalog,
        read_frame=read_frame,
        write_frame=write_frame,
        require_quiet=require_quiet,
        create_coordinator=create_tds_coordinator,
        open_evidence=open_tds_coordinator_evidence,
        decode_connection_admission=decode_connection_admission,
    )


__all__ = ("SqlClientObserveDependencies", "sqlclient_observe_dependencies")
