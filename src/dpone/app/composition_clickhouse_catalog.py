"""Observe ClickHouse catalog over closed HTTP; never copy sealed generation hashes."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from hashlib import sha256
from typing import Any
from uuid import uuid4

from dpone.adapters.composition_clickhouse_http import BoundedClickHouseHttp, clickhouse_http_path
from dpone.adapters.composition_clickhouse_materialization import ClickHouseSnapshotMaterializationReader
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import SnapshotCatalogObservation, SnapshotPublicationIntent, SnapshotTarget
from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject
from dpone.contracts.composition_snapshot_materialization import (
    catalog_counter,
    catalog_integer,
    catalog_response_rows,
    catalog_uuid,
    require_snapshot_columns,
)

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
    "SELECT toUInt64(greatest(1, uniqExact(host_name))) AS node_count, "
    "toUInt64(greatest(1, uniqExact(replica_num))) AS replica_count FROM system.clusters WHERE is_local"
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


def _findings(
    tables: tuple[tuple[object, ...], ...], ddl: Mapping[object, object], effects: tuple[object, ...]
) -> tuple[str, ...]:
    labels = ("mutations", "row_policies", "computed_columns", "skipping_indices", "distributed_topology")
    findings = [label for label, count in zip(labels, effects, strict=True) if catalog_integer(count)]
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
    """Fresh HTTP catalog observation; missing bytes stay unknown.

    Classification is opt-in with a closed typed schema and a protected
    ``require_visibility(intent)`` collaborator. That collaborator must freshly
    prove this HTTP principal sees the complete server catalog and dependencies,
    validate protected enrollment and writer quiescence, and return its original
    bounded evidence bytes. Mere registration or a constant acknowledgement is
    insufficient. HTTP response bytes, including metadata and all content pages,
    share ``max_content_bytes``; oversized observations never classify.
    """

    def __init__(
        self,
        http: Any = None,
        *,
        endpoint: str | None = None,
        credentials: Any = None,
        timeout_seconds: float = 30.0,
        max_response_bytes: int = 1024 * 1024,
        ca_file: str | None = None,
        columns: tuple[ClickHouseDispatchColumn, ...] | None = None,
        max_rows: int = 10000,
        max_content_bytes: int = 1024 * 1024,
        require_visibility: Callable[[Any], bytes] | None = None,
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
        self._reader = None
        self._visibility = require_visibility
        self._max_content_bytes = max_content_bytes
        if type(max_rows) is not int or max_rows < 1 or type(max_content_bytes) is not int or max_content_bytes < 1:
            raise CompositionAdmissionError("snapshot_materialization_budget")
        if columns is not None:
            require_snapshot_columns(columns)
            self._reader = ClickHouseSnapshotMaterializationReader(
                columns=columns, max_rows=max_rows, max_bytes=max_content_bytes
            )

    def can_observe_capture(self) -> bool:
        """Configured bounded read path; actual schema and proofs remain runtime checks."""
        return callable(self._visibility)

    def can_classify_publication(self) -> bool:
        """Require a supported typed schema and a protected visibility observer."""

        return self._reader is not None and callable(self._visibility)

    def inspect(self, intent: SnapshotPublicationIntent) -> SnapshotCatalogObservation:
        intent.__post_init__()
        return self._observe(
            intent.attempt,
            intent.target,
            intent.generation.new_generation_uuid,
            intent.generation.old_target_uuid,
            intent,
        )

    def observe_capture(
        self,
        subject: SnapshotCaptureSubject,
        columns: tuple[ClickHouseDispatchColumn, ...],
        *,
        old_target_uuid: str | None = None,
    ) -> SnapshotCatalogObservation:
        """Observe a real capture subject without constructing an invented intent.

        Before CREATE, missing B remains unknown. After ingest closure, the same
        observation follows the journaled UUID B, retaining the originally seen
        A UUID even when names move. The protected visibility collaborator sees
        this actual capture subject and must validate its current authority.
        """
        subject.__post_init__()
        require_snapshot_columns(columns)
        self._reader = ClickHouseSnapshotMaterializationReader(
            columns=columns, max_rows=subject.limits.max_rows, max_bytes=self._max_content_bytes
        )
        return self._observe(
            subject.attempt, subject.target, subject.generation_uuid, old_target_uuid, subject, plain_profile=True
        )

    def _observe(
        self,
        attempt: CompositionAttemptIdentity,
        target: SnapshotTarget,
        new_uuid: str,
        old_uuid: str | None,
        visibility_subject: Any,
        *,
        plain_profile: bool = False,
    ) -> SnapshotCatalogObservation:
        self._attempt_query_prefix = attempt.attempt_sha256.removeprefix("sha256:")[:32]
        database = _identifier(target.database)
        names = {
            "database": database,
            "target": _identifier(target.target_table),
            "generation": _identifier(target.generation_table),
        }
        evidence = sha256()
        self._observed_bytes = 0
        if self._reader is not None:
            if not callable(self._visibility):
                raise CompositionAdmissionError("snapshot_catalog_visibility")
            original = self._visibility(visibility_subject)
            if type(original) is not bytes or not 1 <= len(original) <= 65536:
                raise CompositionAdmissionError("snapshot_catalog_visibility")
            evidence.update(len(original).to_bytes(8, "big"))
            evidence.update(original)
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
        identities = tuple(catalog_uuid(row[1]) for row in tables)
        if len(set(identities)) != len(identities) or len({row[0] for row in tables}) != len(tables):
            raise CompositionAdmissionError("snapshot_catalog_shape")
        by_name = {row[0]: row for row in tables}
        by_uuid = {identity: row for identity, row in zip(identities, tables, strict=True)}
        target_row = by_name.get(target.target_table)
        generation_row = by_name.get(target.generation_table)
        generation = by_uuid.get(new_uuid)
        if old_uuid is None and target_row is not None:
            old_uuid = catalog_uuid(target_row[1])
        previous = None if old_uuid is None else by_uuid.get(old_uuid)
        retained: int | None = 0
        for row, identity in zip(tables, identities, strict=True):
            if identity == new_uuid:
                continue
            measured = catalog_counter(row[3])
            retained = None if measured is None or retained is None else retained + measured
        if plain_profile:
            ClickHouseSnapshotMaterializationReader.require_plain_profile(self._query, target, evidence)
        schemas, designs, content, row_count = self._materialize(
            target, new_uuid, tables, engine_rows[0][0], topology[0], evidence
        )
        observation = SnapshotCatalogObservation(
            target,
            None if target_row is None else catalog_uuid(target_row[1]),
            None if generation_row is None else catalog_uuid(generation_row[1]),
            engine_rows[0][0],
            (
                None if target_row is None else str(target_row[2]),
                None if generation_row is None else str(generation_row[2]),
            ),
            catalog_integer(topology[0][0]),
            catalog_integer(topology[0][1]),
            schemas,
            designs,
            _findings(tables, ddl, effects[0]),
            content,
            row_count if self._reader is not None else (None if generation is None else catalog_counter(generation[4])),
            None if generation is None else catalog_counter(generation[3]),
            None if previous is None else catalog_counter(previous[3]),
            retained,
            "sha256:" + evidence.hexdigest(),
        )
        observation.__post_init__()
        if self._reader is not None and callable(self._visibility) and self._visibility(visibility_subject) != original:
            raise CompositionAdmissionError("snapshot_catalog_visibility_changed")
        return observation

    def _materialize(
        self, target: SnapshotTarget, new_uuid: str, tables: Any, engine: Any, topology: Any, evidence: Any
    ) -> Any:
        if self._reader is None:
            return (None, None), (None, None), None, None
        schemas: list[str | None] = []
        designs: list[str | None] = []
        content: str | None = None
        count: int | None = None

        def query(
            statement: str,
            parameters: Mapping[str, str],
            columns: tuple[str, ...],
            types: tuple[str, ...],
            maximum: int,
        ) -> Any:
            return self._query(statement, parameters, columns, types, maximum, evidence)

        identity = query(
            "SELECT toString(serverUUID()) AS service_uuid, toString(uuid) AS uuid FROM system.databases WHERE name={database:String}",
            {"database": target.database},
            ("service_uuid", "uuid"),
            ("String", "String"),
            1,
        )
        if identity != ((target.service_id, target.database_id),):
            raise CompositionAdmissionError("snapshot_catalog_database_identity")
        exclusions = query(
            "SELECT (SELECT toUInt64(count()) FROM system.tables WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') AND "
            "(engine != 'MergeTree' OR notEmpty(dependencies_database) OR "
            "match(create_table_query, '(?i)TTL|PROJECTION|CONSTRAINT|CODEC|ON[[:space:]]+CLUSTER'))) AS excluded_tables, "
            "(SELECT toUInt64(count()) FROM system.clusters WHERE NOT is_local OR shard_num != 1 OR replica_num != 1) AS excluded_nodes",
            {},
            ("excluded_tables", "excluded_nodes"),
            ("UInt64", "UInt64"),
            1,
        )
        if exclusions != ((0, 0),):
            raise CompositionAdmissionError("snapshot_catalog_unsupported")
        for name in (target.target_table, target.generation_table):
            table = next((row for row in tables if row[0] == name), None)
            if table is None:
                schemas.append(None)
                designs.append(None)
                continue
            result = self._reader.inspect(
                query,
                database=target.database,
                table=name,
                database_engine=engine,
                nodes=topology[0],
                replicas=topology[1],
                read_content=table[1] == new_uuid,
            )
            schemas.append(result[0])
            designs.append(result[1])
            if result[2] is not None:
                content, count = result[2:]
        # UUID/name drift between metadata and content is never publication proof.
        after = self._query(
            _TABLES_SQL,
            {"database": target.database},
            ("name", "toString(uuid)", "engine", "total_bytes", "total_rows"),
            ("String", "String", "String", "Nullable(UInt64)", "Nullable(UInt64)"),
            64,
            evidence,
        )
        if after != tables:
            raise CompositionAdmissionError("snapshot_catalog_changed")
        return tuple(schemas), tuple(designs), content, count

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
        self._observed_bytes = getattr(self, "_observed_bytes", 0) + len(observed.body)
        if self._reader is not None and self._observed_bytes > self._max_content_bytes:
            raise CompositionAdmissionError("snapshot_materialization_budget")
        evidence.update(len(observed.body).to_bytes(8, "big"))
        evidence.update(observed.body)
        try:
            return catalog_response_rows(observed.body, columns, types, maximum, self._max_content_bytes)
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("snapshot_catalog_shape") from None


__all__ = ["ClickHouseHttpSnapshotCatalog"]
