"""Observe ClickHouse catalog over closed HTTP; never copy sealed generation hashes."""

from __future__ import annotations

import re
from collections.abc import Mapping
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

from dpone.adapters.composition_clickhouse_http import BoundedClickHouseHttp, clickhouse_http_path
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotCatalogObservation, SnapshotPublicationIntent
from dpone.contracts.strict_json import strict_json_object

_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DESIGN = re.compile(
    r"\b(TTL|PROJECTION|INDEX|CONSTRAINT|CODEC|DEFAULT|MATERIALIZED|ALIAS|ON\s+CLUSTER)\b",
    re.IGNORECASE,
)
_DATABASE_SQL = "SELECT engine FROM system.databases WHERE name={database:String} LIMIT 2"
_TABLES_SQL = (
    "SELECT name, toString(uuid), engine, total_bytes, total_rows FROM system.tables "
    "WHERE database={database:String} ORDER BY name LIMIT 65"
)
_DDL_SQL = (
    "SELECT name, create_table_query FROM system.tables "
    "WHERE database={database:String} AND name IN ({target:String}, {generation:String}) LIMIT 3"
)
_TOPOLOGY_SQL = (
    "SELECT toUInt64(uniqExact(host_name)) AS node_count, "
    "toUInt64(uniqExact(replica_num)) AS replica_count FROM system.clusters WHERE is_local"
)
_EFFECTS_SQL = (
    "SELECT (SELECT count() FROM system.mutations WHERE NOT is_done) AS mutations, "
    "(SELECT count() FROM system.row_policies) AS row_policies, "
    "(SELECT count() FROM system.columns WHERE database={database:String} AND default_kind != '') "
    "AS computed_columns, "
    "(SELECT count() FROM system.data_skipping_indices WHERE database={database:String}) "
    "AS skipping_indices, "
    "(SELECT count() FROM system.clusters WHERE NOT is_local OR shard_num != 1 OR replica_num != 1) "
    "AS distributed"
)


def _identifier(value: str) -> str:
    if type(value) is not str or not 1 <= len(value) <= 128 or _IDENTIFIER.fullmatch(value) is None:
        raise CompositionAdmissionError("snapshot_catalog_shape")
    return value


def _cell(value: object, kind: str) -> object:
    if kind.startswith("Nullable(") and kind.endswith(")"):
        return None if value is None else _cell(value, kind[9:-1])
    if kind == "String":
        if type(value) is not str or len(value) > 65536:
            raise CompositionAdmissionError("snapshot_catalog_shape")
        return value
    if kind == "UInt64" and type(value) is str:
        if re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is None:
            raise CompositionAdmissionError("snapshot_catalog_shape")
        value = int(value)
    if kind == "UInt64" and type(value) is int and 0 <= value <= 2**64 - 1:
        return value
    raise CompositionAdmissionError("snapshot_catalog_shape")


def _rows(
    body: bytes, columns: tuple[str, ...], types: tuple[str, ...], maximum: int
) -> tuple[tuple[object, ...], ...]:
    value = strict_json_object(body)
    if not {"meta", "data", "rows"} <= set(value) or not set(value) <= {
        "meta",
        "data",
        "rows",
        "statistics",
        "rows_before_limit_at_least",
    }:
        raise CompositionAdmissionError("snapshot_catalog_shape")
    if value["meta"] != [{"name": name, "type": kind} for name, kind in zip(columns, types, strict=True)]:
        raise CompositionAdmissionError("snapshot_catalog_shape")
    data = value["data"]
    if type(data) is not list or len(data) > maximum or type(value["rows"]) is not int or value["rows"] != len(data):
        raise CompositionAdmissionError("snapshot_catalog_shape")
    if not all(type(row) is list and len(row) == len(columns) for row in data):
        raise CompositionAdmissionError("snapshot_catalog_shape")
    return tuple(tuple(_cell(cell, kind) for cell, kind in zip(row, types, strict=True)) for row in data)


def _uuid(value: object) -> str:
    text = str(value)
    try:
        if str(UUID(text)) != text or UUID(text).int == 0:
            raise ValueError
    except (ValueError, TypeError, AttributeError):
        raise CompositionAdmissionError("snapshot_catalog_shape") from None
    return text


def _require_int(value: object) -> int:
    if type(value) is not int:
        raise CompositionAdmissionError("snapshot_catalog_shape")
    return value


def _counter(value: object) -> int | None:
    return None if value is None else _require_int(value)


def _findings(
    tables: tuple[tuple[object, ...], ...], ddl: Mapping[object, object], effects: tuple[object, ...]
) -> tuple[str, ...]:
    labels = ("mutations", "row_policies", "computed_columns", "skipping_indices", "distributed_topology")
    findings = [label for label, count in zip(labels, effects, strict=True) if _require_int(count)]
    for row in tables:
        engine = str(row[2])
        if "View" in engine:
            findings.append("view")
        elif engine != "MergeTree":
            findings.append("non_mergetree")
    for sql in ddl.values():
        for match in _DESIGN.finditer(str(sql)):
            findings.append(re.sub(r"\s+", "_", match.group(0).lower()))
    return tuple(sorted(set(findings)))


class ClickHouseHttpSnapshotCatalog:
    """Fresh HTTP catalog observation; missing bytes stay unknown."""

    def __init__(
        self,
        http: Any = None,
        *,
        endpoint: str | None = None,
        credentials: Any = None,
        timeout_seconds: float = 30.0,
        max_response_bytes: int = 1024 * 1024,
        ca_file: str | None = None,
    ) -> None:
        if http is None:
            if endpoint is None or credentials is None:
                raise CompositionAdmissionError("snapshot_catalog_shape")
            http = BoundedClickHouseHttp(
                endpoint=endpoint,
                credentials=credentials,
                timeout_seconds=timeout_seconds,
                max_response_bytes=max_response_bytes,
                ca_file=ca_file,
            )
        if not callable(getattr(http, "request", None)):
            raise CompositionAdmissionError("snapshot_catalog_shape")
        self._http = http

    def can_classify_publication(self) -> bool:
        """Typed B/schema/physical hashes are not invented from HTTP metadata."""

        return False

    def inspect(self, intent: SnapshotPublicationIntent) -> SnapshotCatalogObservation:
        intent.__post_init__()
        self._attempt_query_prefix = intent.attempt.attempt_sha256.removeprefix("sha256:")[:32]
        target = intent.target
        database = _identifier(target.database)
        names = {
            "database": database,
            "target": _identifier(target.target_table),
            "generation": _identifier(target.generation_table),
        }
        evidence = sha256()
        engine_rows = self._query(_DATABASE_SQL, {"database": database}, ("engine",), ("String",), 2, evidence)
        if len(engine_rows) != 1 or type(engine_rows[0][0]) is not str:
            raise CompositionAdmissionError("snapshot_catalog_shape")
        tables = self._query(
            _TABLES_SQL,
            {"database": database},
            ("name", "toString(uuid)", "engine", "total_bytes", "total_rows"),
            ("String", "String", "String", "Nullable(UInt64)", "Nullable(UInt64)"),
            64,
            evidence,
        )
        ddl = {
            row[0]: row[1]
            for row in self._query(_DDL_SQL, names, ("name", "create_table_query"), ("String", "String"), 2, evidence)
        }
        topology = self._query(_TOPOLOGY_SQL, {}, ("node_count", "replica_count"), ("UInt64", "UInt64"), 1, evidence)
        effects = self._query(
            _EFFECTS_SQL,
            {"database": database},
            ("mutations", "row_policies", "computed_columns", "skipping_indices", "distributed"),
            ("UInt64",) * 5,
            1,
            evidence,
        )
        if len(topology) != 1 or len(effects) != 1:
            raise CompositionAdmissionError("snapshot_catalog_shape")
        identities = tuple(_uuid(row[1]) for row in tables)
        if len(set(identities)) != len(identities):
            raise CompositionAdmissionError("snapshot_catalog_shape")
        by_name = {row[0]: row for row in tables}
        by_uuid = {identity: row for identity, row in zip(identities, tables, strict=True)}
        target_row = by_name.get(target.target_table)
        generation_row = by_name.get(target.generation_table)
        generation = by_uuid.get(intent.generation.new_generation_uuid)
        previous = by_uuid.get(intent.generation.old_target_uuid)
        retained: int | None = 0
        for row, identity in zip(tables, identities, strict=True):
            if identity == intent.generation.new_generation_uuid:
                continue
            measured = _counter(row[3])
            retained = None if measured is None or retained is None else retained + measured
        observation = SnapshotCatalogObservation(
            target,
            None if target_row is None else _uuid(target_row[1]),
            None if generation_row is None else _uuid(generation_row[1]),
            engine_rows[0][0],
            (
                None if target_row is None else str(target_row[2]),
                None if generation_row is None else str(generation_row[2]),
            ),
            _require_int(topology[0][0]),
            _require_int(topology[0][1]),
            (None, None),
            (None, None),
            _findings(tables, ddl, effects[0]),
            None,
            None if generation is None else _counter(generation[4]),
            None if generation is None else _counter(generation[3]),
            None if previous is None else _counter(previous[3]),
            retained,
            "sha256:" + evidence.hexdigest(),
        )
        observation.__post_init__()
        return observation

    def _query(
        self,
        statement: str,
        parameters: Mapping[str, str],
        columns: tuple[str, ...],
        types: tuple[str, ...],
        maximum: int,
        evidence: Any,
    ) -> tuple[tuple[object, ...], ...]:
        prefix = getattr(self, "_attempt_query_prefix", "") or uuid4().hex[:32]
        query_id = "dpone-catalog-" + prefix + "-" + uuid4().hex[:8]
        try:
            observed = self._http.request(
                path=clickhouse_http_path(query_id=query_id, parameters=parameters, result_rows=maximum + 1),
                payload=f"SELECT * FROM ({statement}) LIMIT {maximum + 1} FORMAT JSONCompact".encode(),
                query_id=query_id,
            )
        except Exception:
            raise CompositionAdmissionError("snapshot_catalog_unavailable") from None
        evidence.update(len(observed.body).to_bytes(8, "big"))
        evidence.update(observed.body)
        try:
            return _rows(observed.body, columns, types, maximum)
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("snapshot_catalog_shape") from None


__all__ = ["ClickHouseHttpSnapshotCatalog"]
