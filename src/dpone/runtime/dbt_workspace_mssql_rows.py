"""Validate catalog rowsets without inventing SQL identifier equivalence.

Only typed, complete observations leave this boundary. Database identifiers
retain their spelling; fingerprints contain no credentials or raw SID bytes.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from hashlib import sha256
from typing import Any

from dpone.contracts.dbt_workspace_observation import (
    MssqlWorkspaceDependency,
    MssqlWorkspaceHeader,
    MssqlWorkspaceObservationRequest,
    MssqlWorkspaceSlot,
    WorkspaceObservationError,
    catalog_observation_fingerprint,
    require_observable_identifier,
)
from dpone.contracts.dbt_workspace_observation import (
    MssqlWorkspaceObservation as MssqlWorkspaceObservation,
)
from dpone.contracts.dbt_workspace_observation import (
    require_observable_database as require_observable_database,
)


def integer(value: object, *, minimum: int = 1) -> int:
    if type(value) is not int or value < minimum:
        raise WorkspaceObservationError("catalog_row")
    return value


def _token(value: object) -> str:
    value = require_observable_identifier(value)
    if len(value) > 33:
        raise WorkspaceObservationError("catalog_row")
    datetime.fromisoformat(value)
    return value


def _sid(value: object) -> str:
    if not isinstance(value, bytes) or not 1 <= len(value) <= 85:
        raise WorkspaceObservationError("principal")
    return "sha256:" + sha256(value).hexdigest()


def header_row(row: Mapping[str, Any], request: MssqlWorkspaceObservationRequest) -> MssqlWorkspaceHeader:
    """Require explicit visibility and a supported, unchanged session context."""

    if (
        integer(row.get("database_id")) != request.pin.database_id
        or row.get("database_name") != request.pin.database_name
        or integer(row.get("database_reference_mismatch_count"), minimum=0) != 0
    ):
        raise WorkspaceObservationError("database_context")
    if (
        integer(row.get("containment"), minimum=0) != 0
        or integer(row.get("engine_edition")) not in {2, 3, 4}
        or integer(row.get("compatibility_level")) < 130
    ):
        raise WorkspaceObservationError("database_capability")
    if row.get("can_view_definition") != 1 or row.get("can_select_dependencies") != 1:
        raise WorkspaceObservationError("metadata_permission")
    original = _sid(row.get("original_sid"))
    effective = _sid(row.get("effective_sid"))
    if original != effective:
        raise WorkspaceObservationError("impersonation")
    facts = {
        key: require_observable_identifier(row.get(key)) for key in ("server_name", "machine_name", "physical_name")
    }
    instance = row.get("instance_name")
    server_facts = {**facts, "instance_name": None if instance is None else require_observable_identifier(instance)}
    database_collation = require_observable_identifier(row.get("database_collation"))
    catalog_collation = require_observable_identifier(row.get("catalog_collation"))
    if database_collation != catalog_collation:
        raise WorkspaceObservationError("database_capability")
    return MssqlWorkspaceHeader(
        database_id=request.pin.database_id,
        database_name=request.pin.database_name,
        server_facts_sha256=catalog_observation_fingerprint(server_facts),
        original_login_sid_sha256=original,
        effective_login_sid_sha256=effective,
        database_principal_sid_sha256=_sid(row.get("database_sid")),
        engine_version=require_observable_identifier(row.get("engine_version")),
        server_collation=require_observable_identifier(row.get("server_collation")),
        database_collation=database_collation,
        catalog_collation=catalog_collation,
    )


def slot_rows(rows: list[dict[str, Any]], request: MssqlWorkspaceObservationRequest) -> tuple[MssqlWorkspaceSlot, ...]:
    """Return each requested slot exactly once, including known absent objects."""

    if len(rows) != len(request.writes):
        raise WorkspaceObservationError("slot_completeness")
    result = []
    seen = set()
    for row in rows:
        slot_id = integer(row.get("slot_id"), minimum=0)
        if slot_id >= len(request.writes) or slot_id in seen:
            raise WorkspaceObservationError("slot_completeness")
        seen.add(slot_id)
        if integer(row.get("database_id")) != request.pin.database_id:
            raise WorkspaceObservationError("database_context")
        if request.writes[slot_id].kind != "transfer" and row.get("literal_roundtrip") != 1:
            raise WorkspaceObservationError("macro_literal")
        schema_id, schema = row.get("schema_id"), row.get("schema_name")
        object_id = row.get("object_id")
        object_fields = ("object_name", "object_type", "create_token", "modify_token", "is_ms_shipped")
        if schema_id is None:
            if schema is not None or object_id is not None:
                raise WorkspaceObservationError("catalog_row")
        else:
            integer(schema_id)
            require_observable_identifier(schema)
        if object_id is None:
            if any(row.get(key) is not None for key in object_fields):
                raise WorkspaceObservationError("catalog_row")
        else:
            integer(object_id)
            require_observable_identifier(row.get("object_name"))
            if row.get("object_type") not in {"U", "V"} or row.get("is_ms_shipped") not in (0, False):
                raise WorkspaceObservationError("object_capability")
            _token(row.get("create_token"))
            _token(row.get("modify_token"))
        result.append(
            MssqlWorkspaceSlot(
                slot_id=slot_id,
                equivalence_class=integer(row.get("equivalence_class")),
                schema_id=schema_id,
                schema_name=schema,
                object_id=object_id,
                object_name=row.get("object_name"),
                object_type=row.get("object_type"),
                create_token=row.get("create_token"),
                modify_token=row.get("modify_token"),
            )
        )
    return tuple(sorted(result, key=lambda item: item.slot_id))


def dependency_row(row: Mapping[str, Any], parent: tuple[str, str, str]) -> MssqlWorkspaceDependency:
    """Decode a raw incoming edge, preserving even cross-server-name matches."""

    if row.get("literal_roundtrip") != 1:
        raise WorkspaceObservationError("macro_literal")
    server, referenced_id = row.get("referenced_server"), row.get("referenced_id")
    return MssqlWorkspaceDependency(
        database_arg=parent[0],
        parent_schema=parent[1],
        parent_relation=parent[2],
        object_id=integer(row.get("object_id")),
        schema_name=require_observable_identifier(row.get("schema_name"), macro_literal=True),
        object_name=require_observable_identifier(row.get("object_name"), macro_literal=True),
        create_token=_token(row.get("create_token")),
        modify_token=_token(row.get("modify_token")),
        referenced_server=None if server is None else require_observable_identifier(server),
        referenced_database=require_observable_identifier(row.get("referenced_database")),
        referenced_schema=require_observable_identifier(row.get("referenced_schema")),
        referenced_entity=require_observable_identifier(row.get("referenced_entity")),
        referenced_id=None if referenced_id is None else integer(referenced_id),
        referencing_minor_id=integer(row.get("referencing_minor_id"), minimum=0),
        referenced_minor_id=integer(row.get("referenced_minor_id"), minimum=0),
    )
