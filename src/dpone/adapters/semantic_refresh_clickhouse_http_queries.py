"""Bounded SQL construction and protected-plan parsing for ClickHouse HTTP."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from dpone.ports.semantic_refresh_clickhouse_authority import (
    ClickHousePublicationAuthority,
    SemanticRefreshClickHousePublicationAuthorityPort,
    clickhouse_authority_rows_sha256,
    clickhouse_operation_table_names,
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RELATION_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$")
_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SQL_FIELDS = frozenset(
    {
        "populate_shadow_retained",
        "append_staging",
        "forward_multiset_difference",
        "reverse_multiset_difference",
    }
)


class ClickHouseHttpGatewayError(RuntimeError):
    """Fail-closed HTTP/query/projection error without endpoint or credentials."""


@dataclass(frozen=True)
class PlanNames:
    """Validated relation names used by one protected publication plan."""

    database: str
    target: str
    staging: str
    shadow: str


def validate_plan(
    plan: Mapping[str, object],
    authority: SemanticRefreshClickHousePublicationAuthorityPort,
) -> tuple[PlanNames, ClickHousePublicationAuthority]:
    """Authenticate the complete mutation-relevant plan before any command."""

    protected = load_authority(plan, authority)
    staging_table, shadow_table = clickhouse_operation_table_names(protected.target_table, protected.operation_id)
    expected: dict[str, object] = {
        "workflow_execution_id": protected.workflow_execution_id,
        "operation_id": protected.operation_id,
        "operation_plan_sha256": protected.operation_plan_sha256,
        "workflow_plan_sha256": protected.workflow_plan_sha256,
        "workflow_execution_binding_sha256": protected.workflow_execution_binding_sha256,
        "attempt_binding_sha256": protected.attempt_binding_sha256,
        "fence_epoch": protected.fencing_epoch,
        "target_resource_id": protected.target_resource_id,
        "target_authority_id": protected.target_authority_id,
        "clickhouse_cluster_authority_id": protected.clickhouse_cluster_authority_id,
        "database": protected.database,
        "target_table": protected.target_table,
        "staging_table": staging_table,
        "shadow_table": shadow_table,
        "scope_id": protected.scope_id,
        "scope_start": protected.scope_start,
        "scope_end": protected.scope_end,
        "scope_revision": protected.scope_revision,
        "expected_target_uuid": protected.expected_target_uuid,
        "expected_schema_sha256": protected.expected_schema_sha256,
        "expected_physical_sha256": protected.expected_physical_sha256,
        "business_columns": protected.business_columns,
        "effective_key_columns": protected.effective_key_columns,
        "event_time_column": protected.event_time_column,
        "max_staging_rows": protected.max_staging_rows,
        "max_target_scope_rows": protected.max_target_scope_rows,
        "max_staging_bytes": protected.max_staging_bytes,
        "max_shadow_bytes": protected.max_shadow_bytes,
        "max_retained_backup_bytes": protected.max_retained_backup_bytes,
        "max_total_transient_bytes": protected.max_total_transient_bytes,
    }
    if any(plan.get(field) != value for field, value in expected.items()):
        raise ClickHouseHttpGatewayError("ClickHouse HTTP protected publication authority differs")
    return PlanNames(
        database=identifier_value(plan.get("database"), "database"),
        target=relation_identifier_value(plan.get("target_table"), "target_table"),
        staging=relation_identifier_value(plan.get("staging_table"), "staging_table"),
        shadow=relation_identifier_value(plan.get("shadow_table"), "shadow_table"),
    ), protected


def load_authority(
    request: Mapping[str, object],
    authority: SemanticRefreshClickHousePublicationAuthorityPort,
) -> ClickHousePublicationAuthority:
    """Resolve and compare the protected run/attempt/fence/target identity."""

    operation_id = request.get("operation_id")
    binding = request.get("workflow_execution_binding_sha256")
    if not isinstance(operation_id, str) or not isinstance(binding, str):
        raise ClickHouseHttpGatewayError("ClickHouse HTTP authority lookup identity is invalid")
    try:
        protected = authority.load(binding, operation_id)
    except Exception as exc:
        raise ClickHouseHttpGatewayError("ClickHouse HTTP protected authority is unavailable") from exc
    staging_table, shadow_table = clickhouse_operation_table_names(protected.target_table, protected.operation_id)
    if (
        request.get("workflow_execution_id") != protected.workflow_execution_id
        or request.get("attempt_binding_sha256") != protected.attempt_binding_sha256
        or request.get("fence_epoch") != protected.fencing_epoch
        or request.get("clickhouse_cluster_authority_id") != protected.clickhouse_cluster_authority_id
        or request.get("database") != protected.database
        or request.get("target_table") != protected.target_table
        or ("staging_table" in request and request.get("staging_table") != staging_table)
        or ("shadow_table" in request and request.get("shadow_table") != shadow_table)
    ):
        raise ClickHouseHttpGatewayError("ClickHouse HTTP guard/target authority differs")
    return protected


def validate_sql(sql: Mapping[str, str]) -> dict[str, str]:
    """Accept only the closed SQL statement set emitted by the runtime builder."""

    if set(sql) != _SQL_FIELDS or any(not isinstance(value, str) or not value.strip() for value in sql.values()):
        raise ClickHouseHttpGatewayError("ClickHouse PREPARE SQL projection is not closed")
    return dict(sql)


def desired_count_query(plan: Mapping[str, object], names: PlanNames) -> str:
    columns = identifier_tuple(plan.get("business_columns"), "business_columns")
    keys = identifier_tuple(plan.get("effective_key_columns"), "effective_key_columns")
    target_columns = ", ".join(f"target.{identifier(column)}" for column in columns)
    staged_columns = ", ".join(f"staged.{identifier(column)}" for column in columns)
    join = " AND ".join(f"target.{identifier(column)} = staged.{identifier(column)}" for column in keys)
    return (
        "SELECT count() FROM ("
        f"SELECT {target_columns} FROM {table(names.database, names.target)} AS target "
        f"LEFT ANTI JOIN {table(names.database, names.staging)} AS staged ON {join} "
        f"UNION ALL SELECT {staged_columns} FROM {table(names.database, names.staging)} AS staged)"
    )


def count_query(database: str, relation: str) -> str:
    return f"SELECT count() FROM {table(database, relation)}"


def scope_count_query(
    database: str,
    relation: str,
    event_time_column: str,
    scope_start: str,
    scope_end: str,
) -> str:
    """Count the exact half-open protected event-time scope."""

    column = identifier(identifier_value(event_time_column, "event_time_column"))
    start_literal = _utc_datetime64_literal(scope_start, "scope_start")
    end_literal = _utc_datetime64_literal(scope_end, "scope_end")
    return (
        f"SELECT count() FROM {table(database, relation)} WHERE "
        f"{column} >= toDateTime64({literal(start_literal)}, 6, 'UTC') AND "
        f"{column} < toDateTime64({literal(end_literal)}, 6, 'UTC')"
    )


def _utc_datetime64_literal(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, ValueError) as exc:
        raise ClickHouseHttpGatewayError(f"ClickHouse {field} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ClickHouseHttpGatewayError(f"ClickHouse {field} must use UTC semantics")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")  # noqa: UP017


def uuid_query(database: str, relation: str) -> str:
    return (
        f"SELECT toString(uuid) FROM system.tables WHERE database = {literal(database)} AND name = {literal(relation)}"
    )


def duplicate_groups_query(database: str, relation: str, keys: str) -> str:
    return f"SELECT count() FROM (SELECT {keys} FROM {table(database, relation)} GROUP BY {keys} HAVING count() > 1)"


def relation_bytes_query(database: str, relation: str) -> str:
    """Observe current stored bytes for one exact relation, or zero if absent."""

    return (
        "SELECT toInt64(coalesce(sum(total_bytes), 0)) FROM system.tables "
        f"WHERE database = {literal(database)} AND name = {literal(relation)}"
    )


def retained_backup_bytes_query(names: PlanNames) -> str:
    """Observe every prior deterministic scratch relation outside this operation."""

    shadow_prefix = f"{names.target}__dpone_shadow__"
    staging_prefix = f"{names.target}__dpone_stage__"
    return (
        "SELECT toInt64(coalesce(sum(total_bytes), 0)) FROM system.tables "
        f"WHERE database = {literal(names.database)} AND "
        f"(startsWith(name, {literal(shadow_prefix)}) OR startsWith(name, {literal(staging_prefix)})) "
        f"AND name NOT IN ({literal(names.shadow)}, {literal(names.staging)})"
    )


def cluster_topology_query(cluster_name: str) -> str:
    """Read the ordered physical members of one protected ClickHouse cluster."""

    return (
        "SELECT host_name, port, shard_num, replica_num FROM system.clusters "
        f"WHERE cluster = {literal(identifier_value(cluster_name, 'cluster_name'))} "
        "ORDER BY shard_num, replica_num, host_name, port FORMAT TabSeparatedRaw"
    )


def relation_comment_query(database: str, relation: str) -> str:
    return f"SELECT comment FROM system.tables WHERE database = {literal(database)} AND name = {literal(relation)}"


def schema_query(database: str, relation: str) -> str:
    return (
        "SELECT name, type, position, default_kind, default_expression, compression_codec FROM system.columns "
        f"WHERE database = {literal(database)} AND table = {literal(relation)} ORDER BY position "
        "FORMAT TabSeparatedRaw"
    )


def physical_query(database: str, relation: str) -> str:
    return (
        "SELECT engine, partition_key, sorting_key, primary_key, sampling_key, storage_policy, engine_full, "
        "toUInt64(match(create_table_query, '(?i)\\\\bTTL\\\\b')), "
        "toUInt64(match(create_table_query, '(?i)\\\\bCODEC\\\\s*\\\\(')), "
        "toUInt64(match(create_table_query, '(?i)\\\\bPROJECTION\\\\b')), "
        "toUInt64(match(create_table_query, '(?i)\\\\bINDEX\\\\b')), "
        "toUInt64(match(create_table_query, '(?i)\\\\bCONSTRAINT\\\\b')), "
        "(SELECT toUInt64(count()) FROM system.tables AS mv "
        "WHERE mv.engine = 'MaterializedView' "
        "AND replaceRegexpAll("
        "extract(mv.create_table_query, '(?i)\\\\sTO\\\\s+([^\\\\s(]+)'), '[`\"]', ''"
        f") = {literal(f'{database}.{relation}')}), "
        "(SELECT toUInt64(count()) FROM system.data_skipping_indices AS indices "
        f"WHERE indices.database = {literal(database)} AND indices.table = {literal(relation)}), "
        "(SELECT toUInt64(count()) FROM system.mutations AS mutations "
        f"WHERE mutations.database = {literal(database)} AND mutations.table = {literal(relation)} "
        "AND mutations.is_done = 0) "
        "FROM system.tables "
        f"WHERE database = {literal(database)} AND name = {literal(relation)} FORMAT TabSeparatedRaw"
    )


def operation_query_id_prefix(
    workflow_execution_binding_sha256: str,
    operation_id: str,
) -> str:
    """Return the bounded query-id namespace for one protected operation."""

    if _DIGEST_RE.fullmatch(workflow_execution_binding_sha256) is None:
        raise ClickHouseHttpGatewayError("ClickHouse workflow execution binding digest is invalid")
    if _DIGEST_RE.fullmatch(operation_id) is None:
        raise ClickHouseHttpGatewayError("ClickHouse operation digest is invalid")
    return (
        "dpone-semref-"
        f"{workflow_execution_binding_sha256.removeprefix('sha256:')[:20]}-"
        f"{operation_id.removeprefix('sha256:')[:20]}-"
    )


def operation_query_id(
    workflow_execution_binding_sha256: str,
    operation_id: str,
    phase: str,
) -> str:
    """Bind one mutation request to the operation query-id namespace."""

    normalized = identifier_value(phase, "query phase").lower()
    return operation_query_id_prefix(workflow_execution_binding_sha256, operation_id) + normalized


def active_operation_query_ids_query(prefix: str) -> str:
    """Read every active query in one exact operation namespace."""

    if not isinstance(prefix, str) or not prefix.startswith("dpone-semref-") or len(prefix) > 96:
        raise ClickHouseHttpGatewayError("ClickHouse operation query-id prefix is invalid")
    return (
        "SELECT query_id FROM system.processes "
        f"WHERE startsWith(query_id, {literal(prefix)}) ORDER BY query_id FORMAT TabSeparatedRaw"
    )


def canonical_rows_sha256(rows: tuple[tuple[object, ...], ...]) -> str:
    try:
        return clickhouse_authority_rows_sha256(rows)
    except (TypeError, ValueError) as exc:
        raise ClickHouseHttpGatewayError("ClickHouse authority rows are not canonical JSON") from exc


def columns(values: tuple[str, ...]) -> str:
    return ", ".join(identifier(value) for value in values)


def input_type(value: str) -> str:
    if not value or any(character in value for character in (";", "\x00", "\n", "\r")):
        raise ClickHouseHttpGatewayError("ClickHouse target type is outside the bounded input subset")
    return value


def identifier_tuple(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, tuple) or not value:
        raise ClickHouseHttpGatewayError(f"ClickHouse {field} is invalid")
    return tuple(identifier_value(item, field) for item in value)


def identifier_value(value: object, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER_RE.fullmatch(value) is None:
        raise ClickHouseHttpGatewayError(f"ClickHouse {field} is outside the closed identifier subset")
    return value


def relation_identifier_value(value: object, field: str) -> str:
    """Accept a quoted table name made only of safe dot-separated segments."""

    if not isinstance(value, str) or _RELATION_IDENTIFIER_RE.fullmatch(value) is None:
        raise ClickHouseHttpGatewayError(f"ClickHouse {field} is outside the closed relation identifier subset")
    return value


def identifier(value: str) -> str:
    return f"`{value}`"


def table(database: str, relation: str) -> str:
    return f"{identifier(database)}.{identifier(relation)}"


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = [
    "ClickHouseHttpGatewayError",
    "PlanNames",
    "active_operation_query_ids_query",
    "canonical_rows_sha256",
    "columns",
    "count_query",
    "desired_count_query",
    "duplicate_groups_query",
    "identifier",
    "identifier_tuple",
    "identifier_value",
    "input_type",
    "literal",
    "load_authority",
    "operation_query_id",
    "operation_query_id_prefix",
    "physical_query",
    "relation_bytes_query",
    "relation_comment_query",
    "relation_identifier_value",
    "retained_backup_bytes_query",
    "schema_query",
    "scope_count_query",
    "table",
    "uuid_query",
    "validate_plan",
    "validate_sql",
]
