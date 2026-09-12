"""Generic exact-row observation for a verified PostgreSQL relation."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sources.postgres_source_authority_types import SelectedPostgresSourceAuthority


@dataclass(frozen=True, slots=True)
class PostgresQueryParameterSlotV1:
    name: str
    kind: str

    def to_document(self) -> dict[str, str]:
        return {"name": self.name, "kind": self.kind}


@dataclass(frozen=True, slots=True)
class PostgresQueryParameterV1:
    kind: str
    value: str

    def to_document(self) -> dict[str, str]:
        return {"kind": self.kind, "value": self.value}


@dataclass(frozen=True, slots=True)
class PostgresQueryResultFieldV1:
    name: str
    python_type: str
    nullable: bool

    def to_document(self) -> dict[str, object]:
        return {"name": self.name, "python_type": self.python_type, "nullable": self.nullable}


@dataclass(frozen=True, slots=True)
class PostgresQueryProfileItemV1:
    statement_id: str
    sql_text: str
    sql_template_text: str
    parameters: tuple[PostgresQueryParameterV1, ...]
    parameter_slots: tuple[PostgresQueryParameterSlotV1, ...]
    result_fields: tuple[PostgresQueryResultFieldV1, ...]
    cardinality: str

    def to_document(self) -> dict[str, object]:
        return {
            "statement_id": self.statement_id,
            "sql_text": self.sql_text,
            "parameters": [item.to_document() for item in self.parameters],
            "result_fields": [item.to_document() for item in self.result_fields],
            "cardinality": self.cardinality,
        }

    def to_template_document(self) -> dict[str, object]:
        return {
            "statement_id": self.statement_id,
            "sql_template_text": self.sql_template_text,
            "parameter_slots": [item.to_document() for item in self.parameter_slots],
            "result_fields": [item.to_document() for item in self.result_fields],
            "cardinality": self.cardinality,
        }

    @property
    def driver_parameters(self) -> tuple[object, ...]:
        return tuple(
            parameter.value if parameter.kind == "utf8_text" else int(parameter.value) for parameter in self.parameters
        )


@dataclass(frozen=True, slots=True)
class PostgresGenericRelationQueryProfileV1:
    selected_source_contract_version: int
    ordered_items: tuple[PostgresQueryProfileItemV1, ...]
    generic_relation_profile_sha256: bytes


class PostgresVerifiedRelationObservationError(ValueError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class InitialRelationWitnessV1:
    snapshot_token: str
    visible_horizon: int
    transaction_incarnation: str
    backend_pid: int


@dataclass(frozen=True, slots=True)
class RelationProfileObservationV1:
    relation_kind: str
    relation_persistence: str
    relation_has_subclass: bool


def execute_profile_item(connector: Any, item: PostgresQueryProfileItemV1) -> list[dict[str, Any]]:
    """Execute exactly the bound item and admit its closed result contract."""

    if type(item) is not PostgresQueryProfileItemV1:
        raise PostgresVerifiedRelationObservationError("exact_type_violation")
    try:
        if item.cardinality == "none":
            connector.execute_query(item.sql_text, item.driver_parameters)
            return []
        rows = connector.get_records(item.sql_text, item.driver_parameters, as_dict=True)
    except asyncio.CancelledError:
        raise
    except Exception as error:
        raise PostgresVerifiedRelationObservationError(_database_reason(error, item.statement_id)) from None
    if type(rows) is not list:
        raise PostgresVerifiedRelationObservationError("internal_invariant_violation")
    if item.cardinality == "exactly_one" and len(rows) != 1:
        reason = _empty_reason(item.statement_id) if not rows else "internal_invariant_violation"
        raise PostgresVerifiedRelationObservationError(reason)
    if item.cardinality == "zero_to_1025" and len(rows) > 1024:
        raise PostgresVerifiedRelationObservationError("column_count_invalid")
    for row in rows:
        _require_row(row, item)
    return rows


def observe_initial_witness(
    connector: Any,
    item: PostgresQueryProfileItemV1,
    selected: SelectedPostgresSourceAuthority,
) -> InitialRelationWitnessV1:
    row = execute_profile_item(connector, item)[0]
    if type(selected) is not SelectedPostgresSourceAuthority:
        raise PostgresVerifiedRelationObservationError("exact_type_violation")
    expected_recovery = selected.topology_role == "standby"
    identity = (
        row["database_name"] == selected.database.canonical_name,
        row["database_oid"] == selected.database.oid,
        row["effective_principal"] == selected.effective_principal.canonical_name,
        row["effective_principal_oid"] == selected.effective_principal.oid,
        row["session_principal"] == selected.session_principal.canonical_name,
        row["session_principal_oid"] == selected.session_principal.oid,
        row["in_recovery"] is expected_recovery,
        row["namespace_oid"] == selected.relation.namespace_oid,
        row["schema_name"] == selected.relation.schema,
        row["relation_oid"] == selected.relation.relation_oid,
        row["relation_name"] == selected.relation.relation,
    )
    if not all(identity):
        raise PostgresVerifiedRelationObservationError("source_authority_mismatch")
    if row["isolation_level"] != "repeatable read" or row["read_only"] != "on":
        raise PostgresVerifiedRelationObservationError("snapshot_lease_mismatch")
    incarnation = row["transaction_incarnation"]
    count = row["lock_witness_count"]
    if type(count) is not int or count < 0 or count > 1:
        raise PostgresVerifiedRelationObservationError("internal_invariant_violation")
    if type(incarnation) is not str or not incarnation or count != 1:
        raise PostgresVerifiedRelationObservationError("relation_lock_not_proven")
    return InitialRelationWitnessV1(row["snapshot_token"], row["visible_horizon"], incarnation, row["backend_pid"])


def verify_physical_identity(
    connector: Any,
    item: PostgresQueryProfileItemV1,
    selected: SelectedPostgresSourceAuthority,
) -> None:
    row = execute_profile_item(connector, item)[0]
    if row["system_identifier"] != selected.system_identifier or row["timeline_id"] != selected.timeline_id:
        raise PostgresVerifiedRelationObservationError("source_authority_mismatch")


def observe_relation_profile(
    connector: Any,
    item: PostgresQueryProfileItemV1,
    selected: SelectedPostgresSourceAuthority,
) -> RelationProfileObservationV1:
    row = execute_profile_item(connector, item)[0]
    if (
        row["relation_oid"] != selected.relation.relation_oid
        or row["namespace_oid"] != selected.relation.namespace_oid
        or row["schema_name"] != selected.relation.schema
        or row["relation_name"] != selected.relation.relation
    ):
        raise PostgresVerifiedRelationObservationError("source_authority_mismatch")
    if row["relkind"] != "r" or row["relpersistence"] != "p" or row["relhassubclass"] is not False:
        raise PostgresVerifiedRelationObservationError("relation_profile_unsupported")
    return RelationProfileObservationV1(row["relkind"], row["relpersistence"], row["relhassubclass"])


def require_active_witness(
    connector: Any,
    item: PostgresQueryProfileItemV1,
    *,
    snapshot_token: str,
    transaction_incarnation: str,
) -> None:
    row = execute_profile_item(connector, item)[0]
    if (
        row["snapshot_token"] != snapshot_token
        or row["transaction_incarnation"] != transaction_incarnation
        or row["lock_witness_count"] != 1
    ):
        raise PostgresVerifiedRelationObservationError("snapshot_lease_mismatch")


def _empty_reason(statement_id: str) -> str:
    if statement_id in {"initial_snapshot_witness", "v1_physical_identity", "relation_profile"}:
        return "source_authority_mismatch"
    if statement_id == "active_scope_revalidation":
        return "snapshot_lease_mismatch"
    return "internal_invariant_violation"


def _require_row(row: object, item: PostgresQueryProfileItemV1) -> None:
    if type(row) is not dict or set(row) != {field.name for field in item.result_fields}:
        raise PostgresVerifiedRelationObservationError(_shape_reason(item.statement_id))
    for field in item.result_fields:
        value = row[field.name]
        if value is None and field.nullable:
            continue
        expected = {"str": str, "int": int, "bool": bool}[field.python_type]
        if type(value) is not expected:
            raise PostgresVerifiedRelationObservationError(_field_shape_reason(item.statement_id, field.name))


def _shape_reason(statement_id: str) -> str:
    del statement_id
    return "internal_invariant_violation"


def _field_shape_reason(statement_id: str, field_name: str) -> str:
    if statement_id == "initial_snapshot_witness" and field_name in {
        "database_name",
        "database_oid",
        "effective_principal",
        "effective_principal_oid",
        "session_principal",
        "session_principal_oid",
        "namespace_oid",
        "schema_name",
        "relation_oid",
        "relation_name",
    }:
        return "source_authority_mismatch"
    return "internal_invariant_violation"


def _database_reason(error: Exception, statement_id: str) -> str:
    sqlstate = getattr(error, "sqlstate", None)
    if type(sqlstate) is not str:
        return "internal_invariant_violation"
    if sqlstate == "42501":
        return "metadata_permission_denied"
    if sqlstate[:2] in {"08", "40", "53"} or sqlstate in {
        "55P03",
        "57014",
        "57P01",
        "57P02",
        "57P03",
    }:
        return "catalog_observation_failed"
    if sqlstate in {"42P01", "42704"} and statement_id in {
        "lock_relation",
        "initial_snapshot_witness",
        "active_scope_revalidation",
        "relation_profile",
        "column_catalog",
    }:
        return "source_authority_mismatch"
    return "internal_invariant_violation"


__all__ = [
    "InitialRelationWitnessV1",
    "PostgresGenericRelationQueryProfileV1",
    "PostgresQueryParameterSlotV1",
    "PostgresQueryParameterV1",
    "PostgresQueryProfileItemV1",
    "PostgresQueryResultFieldV1",
    "PostgresVerifiedRelationObservationError",
    "RelationProfileObservationV1",
    "execute_profile_item",
    "observe_initial_witness",
    "observe_relation_profile",
    "require_active_witness",
    "verify_physical_identity",
]
