"""Direct-member ClickHouse staging with bounded, content-addressed observation."""

from __future__ import annotations

import copy
import hashlib
import math
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any
from uuid import UUID

from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityRecord,
    ExternalContractError,
    MemberGenerationObservation,
    PhysicalGeneration,
    canonical_json,
    digest_payload,
)
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.clickhouse_nullability_policy import ClickHouseNullInsertPolicy
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDdlRenderer, ClickHouseTableDesign
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.support.type_mapping.mssql_clickhouse import (
    MssqlClickHouseTypeMapper,
    MssqlClickHouseTypePolicy,
)

_CONTENT_DIGEST_VERSION = "dpone.clickhouse.canonical-rows.v1"
_SCHEMA_DIGEST_VERSION = "dpone.clickhouse.canonical-schema.v1"


def insert_external_rows(
    sink: Any,
    load_config: Any,
    payload: LoadPayload,
    *,
    query_id: str,
    deduplication_token: str,
    map_schema: Callable[[Any, Sequence[tuple[str, str]]], Sequence[tuple[str, str]]],
    coerce_row: Callable[[tuple[Any, ...], Sequence[str]], tuple[Any, ...]],
) -> int:
    """Insert one member generation with synchronous, replay-safe settings."""

    artifact = payload.artifact
    if not isinstance(artifact, InMemoryRowsArtifact):
        raise ValueError("clickhouse_external_artifact.requires_in_memory_rows")
    mapped_schema = map_schema(load_config, payload.schema)
    columns = [column for column, _ in mapped_schema]
    column_types = [column_type for _, column_type in mapped_schema]
    rows = [coerce_row(tuple(row.get(column) for column in columns), column_types) for row in artifact._rows]
    if not rows:
        return 0
    policy = ClickHouseNullInsertPolicy.from_load_config(load_config)
    policy.validate_rows(columns, rows)
    settings = {
        **policy.driver_settings(),
        "async_insert": 0,
        "wait_for_async_insert": 1,
        "insert_deduplication_token": deduplication_token,
    }
    column_sql = ", ".join(f"`{column}`" for column in columns)
    connection = sink.connector.connection
    insert_rows = getattr(connection, "insert_rows", None)
    if callable(insert_rows):
        insert_rows(
            sink._table(load_config),
            rows,
            column_names=columns,
            settings=settings,
            query_id=query_id,
        )
    else:
        connection.execute(
            f"INSERT INTO {sink._table(load_config)} ({column_sql}) VALUES",
            rows,
            settings=settings,
            query_id=query_id,
        )
    return len(rows)


def _insert_with_sink_ingestion(
    sink: Any,
    load_config: Any,
    payload: LoadPayload,
    *,
    query_id: str,
    deduplication_token: str,
) -> int:
    ingestion = sink._payload_ingestion
    return insert_external_rows(
        sink,
        load_config,
        payload,
        query_id=query_id,
        deduplication_token=deduplication_token,
        map_schema=ingestion._clickhouse_schema,
        coerce_row=ingestion._row_value_coercer.coerce_row,
    )


class ClickHouseExternalReplicationMemberDriver:
    """Execute one-shot local candidate operations on a direct member connector.

    The caller owns topology and Keeper fencing. This driver owns only local
    table mutation and bounded physical observation; endpoint values never
    leave the injected connection boundary.
    """

    def __init__(
        self,
        *,
        load_config: Any,
        payload_schema: Sequence[tuple[str, str]],
        sink_factory: Callable[[Any], Any],
        member_identity: Callable[[Any], str],
        max_content_rows: int,
        insert_rows: Callable[..., int] = _insert_with_sink_ingestion,
    ) -> None:
        if isinstance(max_content_rows, bool) or max_content_rows <= 0:
            raise ValueError("clickhouse_external_member_driver.max_content_rows_must_be_positive")
        self._load_config = load_config
        self._payload_schema = tuple((str(name), str(dtype)) for name, dtype in payload_schema)
        self._sink_factory = sink_factory
        self._member_identity = member_identity
        self._max_content_rows = max_content_rows
        self._insert_rows = insert_rows

    def observe(self, connector: Any, record: ExternalAuthorityRecord) -> MemberGenerationObservation:
        record.validate()
        member_id = self._require_member(connector, record)
        return MemberGenerationObservation(
            member_id=member_id,
            target=self._observe_table(connector, record.database, record.target),
            candidate=self._observe_table(connector, record.database, record.candidate),
        )

    def create_candidate(
        self, connector: Any, record: ExternalAuthorityRecord, *, expected_uuid: str
    ) -> PhysicalGeneration:
        record.validate()
        self._require_payload_schema()
        self._require_member(connector, record)
        if self._observe_table(connector, record.database, record.candidate) is not None:
            raise ExternalContractError("GENERATION_DIVERGED", "owned candidate already exists")
        config = self._candidate_config(record)
        sink = self._sink(connector)
        _create_candidate_with_uuid(sink, config, self._payload_schema, expected_uuid)
        created = self._observe_table(connector, record.database, record.candidate)
        if created is None:
            raise ExternalContractError("GENERATION_UNKNOWN", "created candidate is not observable")
        if created.uuid != expected_uuid:
            raise ExternalContractError("GENERATION_DIVERGED", "created candidate UUID differs from intent")
        return created

    def load_candidate(self, connector: Any, record: ExternalAuthorityRecord, sealed_payload: Any) -> None:
        record.validate()
        self._require_payload_schema()
        member_id = self._require_member(connector, record)
        if not isinstance(sealed_payload, LoadPayload):
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "sealed replay must be a LoadPayload")
        if tuple(sealed_payload.schema) != self._payload_schema:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "sealed replay schema differs")
        expected = next(item.candidate for item in record.members if item.member_id == member_id)
        observed = self._observe_table(connector, record.database, record.candidate)
        if expected is None or observed is None or observed.uuid != expected.uuid:
            raise ExternalContractError("GENERATION_DIVERGED", "candidate physical identity is not bound")
        query_id = f"dpone-external-stage-{record.operation_id[:16]}-{member_id[:16]}"
        deduplication_token = digest_payload(
            {
                "operation_id": record.operation_id,
                "generation_id": record.generation_id,
                "member_id": member_id,
            }
        )
        sink = self._sink(connector)
        self._insert_rows(
            sink,
            self._candidate_config(record),
            sealed_payload,
            query_id=query_id,
            deduplication_token=deduplication_token,
        )

    def drop_candidate(
        self,
        connector: Any,
        record: ExternalAuthorityRecord,
        expected: PhysicalGeneration,
    ) -> None:
        record.validate()
        self._require_member(connector, record)
        expected.validate()
        observed = self._observe_table(connector, record.database, record.candidate)
        if observed is None:
            return
        if observed.uuid != expected.uuid:
            raise ExternalContractError("GENERATION_DIVERGED", "candidate physical identity differs")
        connector.execute_query(f"DROP TABLE {_qualified(record.database, record.candidate)}")
        if self._observe_table(connector, record.database, record.candidate) is not None:
            raise ExternalContractError("GENERATION_UNKNOWN", "candidate absence is not proven")

    def _observe_table(self, connector: Any, database: str, table: str) -> PhysicalGeneration | None:
        params = {"database": database, "table": table}
        metadata = connector.get_records(
            "SELECT toString(uuid), engine_full FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            params,
        )
        if not metadata:
            return None
        if len(metadata) != 1:
            raise ExternalContractError("GENERATION_DIVERGED", "table catalog identity is ambiguous")
        uuid, engine_full = str(metadata[0][0]), str(metadata[0][1])
        _require_non_replicated_merge_tree(engine_full)
        columns = connector.get_records(
            "SELECT name, type, default_kind, default_expression, position FROM system.columns "
            "WHERE database = %(database)s AND table = %(table)s ORDER BY position",
            params,
        )
        if not columns:
            raise ExternalContractError("GENERATION_UNKNOWN", "table schema is unavailable")
        row_count = _row_count(connector, database, table, params)
        if row_count > self._max_content_rows:
            raise ExternalContractError("CONTENT_BUDGET_EXCEEDED", "canonical observation row budget exceeded")
        names = tuple(str(column[0]) for column in columns)
        projection = ", ".join(_quote(name) for name in names)
        rows = connector.get_records(
            f"SELECT {projection} FROM {_qualified(database, table)} "
            f"ORDER BY {projection} LIMIT {self._max_content_rows + 1}",
            params,
        )
        if len(rows) != row_count:
            raise ExternalContractError("GENERATION_UNKNOWN", "canonical observation count changed")
        generation = PhysicalGeneration(
            uuid=uuid,
            engine_full=engine_full,
            schema_digest=canonical_schema_digest(columns),
            content_digest=canonical_rows_digest(rows),
            row_count=row_count,
        )
        generation.validate()
        return generation

    def _candidate_config(self, record: ExternalAuthorityRecord) -> Any:
        options = copy.deepcopy(getattr(self._load_config, "options", {}) or {})
        physical = _mapping(options.setdefault("physical_design", {}), "physical_design")
        storage = _mapping(physical.setdefault("storage", {}), "physical_design.storage")
        clickhouse = _mapping(storage.setdefault("clickhouse", {}), "physical_design.storage.clickhouse")
        design = ClickHouseTableDesign.from_options(options)
        _require_non_replicated_merge_tree(design.engine)
        clickhouse["cluster"] = {"ddl_scope": "local", "replication_mode": "external"}
        clickhouse.pop("access_table", None)
        return replace(
            self._load_config,
            target_schema=record.database,
            target_table=record.candidate,
            options=options,
        )

    def _sink(self, connector: Any) -> Any:
        sink = self._sink_factory(connector)
        if sink is None:
            raise ExternalContractError("INVENTORY_INVALID", "direct member sink is unavailable")
        return sink

    def _require_member(self, connector: Any, record: ExternalAuthorityRecord) -> str:
        member_id = str(self._member_identity(connector) or "")
        if not member_id or sum(item.member_id == member_id for item in record.members) != 1:
            raise ExternalContractError("INVENTORY_INVALID", "direct member identity is not admitted")
        return member_id

    def _require_payload_schema(self) -> None:
        if not self._payload_schema:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "payload schema is required for staging")


def canonical_schema_digest(columns: Sequence[Sequence[Any]]) -> str:
    """Digest ordered ClickHouse catalog columns without exposing their values."""

    normalized = []
    for column in columns:
        if len(column) < 5:
            raise ExternalContractError("GENERATION_UNKNOWN", "schema catalog row is incomplete")
        normalized.append(
            {
                "name": str(column[0]),
                "type": str(column[1]),
                "default_kind": str(column[2] or ""),
                "default_expression": str(column[3] or ""),
                "position": int(column[4]),
            }
        )
    payload = {"version": _SCHEMA_DIGEST_VERSION, "columns": normalized}
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def canonical_rows_digest(rows: Iterable[Sequence[Any]]) -> str:
    """Digest a typed row multiset in deterministic canonical order."""

    digest = hashlib.sha256()
    digest.update((_CONTENT_DIGEST_VERSION + "\n").encode("utf-8"))
    encoded_rows = sorted(canonical_json([_canonical_value(value) for value in row]) for row in rows)
    for encoded in encoded_rows:
        digest.update(str(len(encoded)).encode("ascii"))
        digest.update(b":")
        digest.update(encoded.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def canonical_rows_json(rows: Iterable[Sequence[Any]]) -> str:
    """Serialize typed rows canonically while preserving artifact row order."""

    return canonical_json([[_canonical_value(value) for value in row] for row in rows])


def _create_candidate_with_uuid(
    sink: Any,
    load_config: Any,
    schema: Sequence[tuple[str, str]],
    table_uuid: str,
) -> None:
    options = getattr(load_config, "options", {}) or {}
    mapper = MssqlClickHouseTypeMapper(MssqlClickHouseTypePolicy.from_config(options.get("type_fidelity")))
    columns = [f"`{column}` {sink._column_type(load_config, mapper, column, dtype)}" for column, dtype in schema]
    design = sink._ensure_database(load_config)
    statement = ClickHouseTableDdlRenderer().render_create_table(
        table=sink._table(load_config),
        columns_sql=columns,
        design=design,
        table_uuid=table_uuid,
    )
    sink.connector.execute_query(statement)


def _canonical_value(value: Any) -> Any:
    if value is None:
        return ["null", None]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, Decimal):
        return ["decimal", format(value, "f")]
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ExternalContractError("GENERATION_UNKNOWN", "non-finite values are unsupported")
        return ["float", value.hex()]
    if isinstance(value, str):
        return ["string", value]
    if isinstance(value, bytes | bytearray | memoryview):
        return ["bytes", bytes(value).hex()]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(timespec="microseconds")]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if isinstance(value, time):
        return ["time", value.isoformat(timespec="microseconds")]
    if isinstance(value, UUID):
        return ["uuid", str(value)]
    if isinstance(value, tuple | list):
        return ["sequence", [_canonical_value(item) for item in value]]
    if isinstance(value, Mapping):
        items = [(_canonical_value(key), _canonical_value(item)) for key, item in value.items()]
        items.sort(key=lambda pair: canonical_json(pair[0]))
        return ["mapping", [[key, item] for key, item in items]]
    raise ExternalContractError("GENERATION_UNKNOWN", "canonical value type is unsupported")


def _row_count(connector: Any, database: str, table: str, params: Mapping[str, str]) -> int:
    rows = connector.get_records(f"SELECT count() FROM {_qualified(database, table)}", params)
    if len(rows) != 1:
        raise ExternalContractError("GENERATION_UNKNOWN", "table count is unavailable")
    count = int(rows[0][0])
    if count < 0:
        raise ExternalContractError("GENERATION_UNKNOWN", "table count is invalid")
    return count


def _require_non_replicated_merge_tree(engine_full: str) -> None:
    normalized = engine_full.lstrip()
    if (
        normalized.startswith(("Replicated", "Shared"))
        or re.match(r"^[A-Za-z]*MergeTree(?:\s|\(|$)", normalized) is None
    ):
        raise ExternalContractError("ENGINE_UNSUPPORTED", "external mode requires non-replicated MergeTree")


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ExternalContractError("ENGINE_UNSUPPORTED", f"{field} must be an object")
    return value


def _quote(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"


__all__ = [
    "ClickHouseExternalReplicationMemberDriver",
    "canonical_rows_digest",
    "canonical_rows_json",
    "canonical_schema_digest",
    "insert_external_rows",
]
