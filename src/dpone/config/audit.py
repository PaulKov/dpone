"""Independent audit selection without enabling source checkpoint state.

The workload selects a logical connection and table names. Only the verified
deployment descriptor may supply database/schema coordinates. This module is
pure: selecting a binding neither opens connections nor provisions tables.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.credential_env import is_valid_connection_ref
from dpone.contracts.mssql_object_name import safe_mssql_identifier

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


class AuditConfigError(ValueError):
    """An explicit audit selection is incomplete, ambiguous, or unsafe."""


@dataclass(frozen=True, slots=True)
class AuditStorageSelection:
    """Workload-owned audit identity, independent of source state."""

    connection_ref: str
    loads_table: str
    steps_table: str
    declared_schema: str | None = None


@dataclass(frozen=True, slots=True)
class ResolvedMssqlAuditLocation:
    """External audit pair with deployment-owned database/schema coordinates."""

    database: str
    schema: str
    loads_table: str
    steps_table: str
    provisioning: str = "external"


def select_audit_storage(config: Mapping[str, Any]) -> AuditStorageSelection | None:
    """Validate a closed audit selector before credential or connector access.

    Existing manifests without ``audit.storage`` retain their old behavior.
    Explicit selection cannot coexist with the MSSQL state-owned audit pair.
    """

    if "storage" in _audit(config.get("source")):
        raise AuditConfigError("audit.storage belongs to sink.options.load_governance, not source")
    audit = _audit(config.get("sink"))
    if "storage" not in audit:
        return None
    storage = audit["storage"]
    if not isinstance(storage, Mapping) or set(storage) - {"type", "connection_ref", "provisioning"}:
        raise AuditConfigError("audit.storage must be a closed MSSQL connection selector")
    if storage.get("type") != "mssql" or storage.get("provisioning", "external") != "external":
        raise AuditConfigError("audit.storage requires type=mssql and provisioning=external")
    connection_ref = storage.get("connection_ref")
    if not isinstance(connection_ref, str) or not is_valid_connection_ref(connection_ref):
        raise AuditConfigError("audit.storage.connection_ref must be a canonical logical alias")
    state = _mapping(config.get("state"))
    sink = _mapping(config.get("sink"))
    if str(state.get("type", "")).lower() in {"mssql", "sqlserver"} or (
        state.get("reuse") == "sink" and str(sink.get("type", "")).lower() in {"mssql", "sqlserver"}
    ):
        raise AuditConfigError("audit.storage cannot also select a state-owned MSSQL audit pair")
    loads_table = _identifier(audit.get("loads_table", "dpone_load_audit"), "loads_table")
    steps_table = _identifier(audit.get("steps_table", "__dpone__load_steps"), "steps_table")
    if loads_table.casefold() == steps_table.casefold():
        raise AuditConfigError("audit loads_table and steps_table must be distinct")
    schema = _identifier(audit["state_schema"], "state_schema") if "state_schema" in audit else None
    return AuditStorageSelection(connection_ref, loads_table, steps_table, schema)


def resolve_mssql_audit_location(
    selection: AuditStorageSelection,
    connection: ResolvedBindingConnection,
) -> ResolvedMssqlAuditLocation:
    """Resolve only from the verified descriptor, never credential defaults."""

    descriptor = connection.descriptor
    if descriptor is None or descriptor.connection_type != "mssql":
        raise AuditConfigError("audit.storage requires a verified MSSQL connection descriptor")
    database = _identifier(descriptor.properties.get("database"), "registry database")
    schema = _identifier(descriptor.properties.get("schema"), "registry schema")
    if selection.declared_schema is not None and selection.declared_schema != schema:
        raise AuditConfigError("audit.state_schema must exactly match the deployment registry schema")
    return ResolvedMssqlAuditLocation(database, schema, selection.loads_table, selection.steps_table)


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or len(value) > 128 or not safe_mssql_identifier(value):
        raise AuditConfigError(f"audit {field} must be an unqualified SQL identifier")
    return value


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _audit(endpoint: object) -> Mapping[str, Any]:
    options = _mapping(_mapping(endpoint).get("options"))
    return _mapping(_mapping(options.get("load_governance")).get("audit"))
