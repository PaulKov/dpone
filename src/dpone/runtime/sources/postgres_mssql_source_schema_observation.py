"""Closed query-profile contracts for PostgreSQL R1 schema observation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from dpone.runtime.sources.postgres_mssql_source_schema_queries import (
    ACTIVE_SCOPE_REVALIDATION_SQL,
    COLUMN_CATALOG_SQL,
    INITIAL_SNAPSHOT_WITNESS_SQL,
    LOCK_RELATION_TEMPLATE,
    RELATION_PROFILE_SQL,
    SET_LOCK_TIMEOUT_SQL,
    SET_TRANSACTION_SQL,
    V1_PHYSICAL_IDENTITY_SQL,
)
from dpone.runtime.sources.postgres_verified_relation_observation import (
    PostgresGenericRelationQueryProfileV1,
    PostgresQueryParameterSlotV1,
    PostgresQueryParameterV1,
    PostgresQueryProfileItemV1,
    PostgresQueryResultFieldV1,
)

_TEMPLATE_DOMAIN = b"dpone-postgres-mssql-source-schema-query-template-profile-v1\0"
_EXECUTION_DOMAIN = b"dpone-postgres-mssql-source-schema-query-profile-v1\0"
_GENERIC_DOMAIN = b"dpone-postgres-generic-relation-query-profile-v1\0"


@dataclass(frozen=True, slots=True)
class PostgresMssqlRelationQueryProfileV1:
    contract_version: str
    ordered_items: tuple[PostgresQueryProfileItemV1, ...]
    generic_relation_profile: PostgresGenericRelationQueryProfileV1
    query_execution_profile_sha256: bytes
    query_template_profile_sha256: bytes


def _field(name: str, kind: str, nullable: bool = False) -> PostgresQueryResultFieldV1:
    return PostgresQueryResultFieldV1(name, kind, nullable)


def _item(
    statement_id: str,
    sql: str,
    *,
    slots: tuple[tuple[str, str], ...] = (),
    values: tuple[str, ...] = (),
    fields: tuple[PostgresQueryResultFieldV1, ...] = (),
    cardinality: str = "none",
    template: str | None = None,
) -> PostgresQueryProfileItemV1:
    parameter_slots = tuple(PostgresQueryParameterSlotV1(*slot) for slot in slots)
    parameters = tuple(PostgresQueryParameterV1(slot[1], value) for slot, value in zip(slots, values, strict=True))
    return PostgresQueryProfileItemV1(
        statement_id,
        sql,
        template or sql,
        parameters,
        parameter_slots,
        fields,
        cardinality,
    )


_INITIAL_FIELDS = (
    _field("snapshot_token", "str"),
    _field("visible_horizon", "int"),
    _field("transaction_incarnation", "str", True),
    _field("isolation_level", "str"),
    _field("read_only", "str"),
    _field("backend_pid", "int"),
    _field("database_name", "str"),
    _field("database_oid", "int"),
    _field("effective_principal", "str"),
    _field("effective_principal_oid", "int"),
    _field("session_principal", "str"),
    _field("session_principal_oid", "int"),
    _field("in_recovery", "bool"),
    _field("namespace_oid", "int", True),
    _field("schema_name", "str", True),
    _field("relation_oid", "int", True),
    _field("relation_name", "str", True),
    _field("lock_witness_count", "int"),
)
_ACTIVE_FIELDS = (
    _field("snapshot_token", "str"),
    _field("transaction_incarnation", "str", True),
    _field("lock_witness_count", "int"),
)
_RELATION_FIELDS = (
    _field("relation_oid", "int"),
    _field("namespace_oid", "int"),
    _field("schema_name", "str"),
    _field("relation_name", "str"),
    _field("relkind", "str"),
    _field("relpersistence", "str"),
    _field("relhassubclass", "bool"),
)
_COLUMN_FIELDS = (
    _field("relation_oid", "int"),
    _field("namespace_oid", "int"),
    _field("relkind", "str"),
    _field("relpersistence", "str"),
    _field("relhassubclass", "bool"),
    _field("attribute_number", "int"),
    _field("column_name", "str"),
    _field("type_oid", "int"),
    _field("type_namespace_oid", "int"),
    _field("type_namespace_name", "str"),
    _field("type_name", "str"),
    _field("type_kind", "str"),
    _field("type_modifier", "int"),
    _field("nullable", "bool"),
    _field("collation_oid", "int"),
    _field("generated_kind", "str"),
    _field("identity_kind", "str"),
)


def build_relation_query_profile(selected_source: object) -> PostgresMssqlRelationQueryProfileV1:
    """Bind the exact query bytes and parameters to one selected relation."""

    from psycopg import sql

    version = getattr(selected_source, "version", None)
    relation = getattr(selected_source, "relation", None)
    if type(version) is not int or version not in {1, 2} or relation is None:
        raise ValueError("exact selected source required")
    schema = relation.schema
    name = relation.relation
    lock = (
        sql.SQL("LOCK TABLE ONLY {}.{} IN ACCESS SHARE MODE")
        .format(sql.Identifier(schema), sql.Identifier(name))
        .as_string(None)
    )
    items = [
        _item("set_transaction", SET_TRANSACTION_SQL),
        _item("set_lock_timeout", SET_LOCK_TIMEOUT_SQL),
        _item("lock_relation", lock, template=LOCK_RELATION_TEMPLATE),
        _item(
            "initial_snapshot_witness",
            INITIAL_SNAPSHOT_WITNESS_SQL,
            slots=(("selected_schema", "utf8_text"), ("selected_relation", "utf8_text")),
            values=(schema, name),
            fields=_INITIAL_FIELDS,
            cardinality="exactly_one",
        ),
    ]
    if version == 1:
        items.append(
            _item(
                "v1_physical_identity",
                V1_PHYSICAL_IDENTITY_SQL,
                fields=(_field("system_identifier", "str"), _field("timeline_id", "int")),
                cardinality="exactly_one",
            )
        )
    items.extend(
        (
            _item(
                "active_scope_revalidation",
                ACTIVE_SCOPE_REVALIDATION_SQL,
                slots=(("selected_relation_oid", "uint32_decimal"),),
                values=(str(relation.relation_oid),),
                fields=_ACTIVE_FIELDS,
                cardinality="exactly_one",
            ),
            _item(
                "relation_profile",
                RELATION_PROFILE_SQL,
                slots=(("selected_relation_oid", "uint32_decimal"), ("selected_namespace_oid", "uint32_decimal")),
                values=(str(relation.relation_oid), str(relation.namespace_oid)),
                fields=_RELATION_FIELDS,
                cardinality="exactly_one",
            ),
            _item(
                "column_catalog",
                COLUMN_CATALOG_SQL,
                slots=(("selected_relation_oid", "uint32_decimal"), ("selected_namespace_oid", "uint32_decimal")),
                values=(str(relation.relation_oid), str(relation.namespace_oid)),
                fields=_COLUMN_FIELDS,
                cardinality="zero_to_1025",
            ),
        )
    )
    ordered = tuple(items)
    generic_items = tuple(item for item in ordered if item.statement_id != "column_catalog")
    generic_doc = {
        "contract_version": "dpone-postgres-generic-relation-query-profile-1",
        "selected_source_contract_version": version,
        "items": [item.to_document() for item in generic_items],
    }
    generic_digest = _digest(_GENERIC_DOMAIN, generic_doc)
    generic = PostgresGenericRelationQueryProfileV1(version, generic_items, generic_digest)
    execution_doc = {
        "contract_version": "dpone-postgres-mssql-source-schema-query-profile-1",
        "items": [item.to_document() for item in ordered],
    }
    template_doc = {
        "contract_version": "dpone-postgres-mssql-source-schema-query-template-profile-1",
        "items": [item.to_template_document() for item in ordered],
    }
    return PostgresMssqlRelationQueryProfileV1(
        "dpone-postgres-mssql-source-schema-query-profile-1",
        ordered,
        generic,
        _digest(_EXECUTION_DOMAIN, execution_doc),
        _digest(_TEMPLATE_DOMAIN, template_doc),
    )


def _digest(domain: bytes, document: object) -> bytes:
    payload = json.dumps(document, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    return hashlib.sha256(domain + payload).digest()


POSTGRES_MSSQL_QUERY_TEMPLATE_PROFILE_SHA256_V1 = bytes.fromhex(
    "4b4af3caa7213e174c85610b5bdad930bc994a10000031cc190d2e125157568b"
)
POSTGRES_MSSQL_QUERY_TEMPLATE_PROFILE_SHA256_V2 = bytes.fromhex(
    "7c12e711cc6a48f460892769ecb5fdb71b765b587cc9798339690bd9fb47c9f3"
)


__all__ = [
    "POSTGRES_MSSQL_QUERY_TEMPLATE_PROFILE_SHA256_V1",
    "POSTGRES_MSSQL_QUERY_TEMPLATE_PROFILE_SHA256_V2",
    "PostgresGenericRelationQueryProfileV1",
    "PostgresMssqlRelationQueryProfileV1",
    "PostgresQueryProfileItemV1",
    "PostgresQueryResultFieldV1",
    "build_relation_query_profile",
]
