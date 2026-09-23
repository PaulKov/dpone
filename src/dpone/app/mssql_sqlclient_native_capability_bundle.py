"""Typed dependency bundles shared by the SqlClient composition roots."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentCapabilities
    from dpone.app.mssql_sqlclient_native_runtime_composition import SqlClientNativeImportCapabilities
    from dpone.ports.mssql_native_route_capabilities import NativeActorCapacity

SqlClientNativeImportCapabilitiesFactory = Callable[[], AbstractContextManager["SqlClientNativeImportCapabilities"]]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeBindingCapabilities:
    """One coherent importer, parent, capacity and session dependency bundle."""

    imports: SqlClientNativeImportCapabilities
    parent: SqlClientNativeParentCapabilities
    capacity: NativeActorCapacity
    implementation_sha256: str
    open_import_capabilities: SqlClientNativeImportCapabilitiesFactory


__all__ = ("SqlClientNativeBindingCapabilities", "SqlClientNativeImportCapabilitiesFactory")
