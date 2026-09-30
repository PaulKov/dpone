"""Frozen query shape and query-lifetime provenance for raw Replacing snapshots."""

from __future__ import annotations

import hashlib
import sys
from collections.abc import Iterator, Mapping
from datetime import timedelta
from typing import Any

from dpone.contracts.clickhouse_raw_snapshot import ClickHouseRawSnapshotProfileV1, ClickHouseRawSourceEofV1
from dpone.manifest.clickhouse_raw_snapshot_policy import native_source_snapshot_policy
from dpone.manifest.mssql_native_policy import native_window
from dpone.runtime.sources.clickhouse_native_guard import admit_source_type, source_identifier
from dpone.runtime.sources.clickhouse_native_values import projection, restore, temporal
from dpone.runtime.sources.clickhouse_raw_catalog import MAX_PARTS, RawSnapshotCatalog, digest, engine_signature


class RawSnapshot:
    """Own profile acquisition and one stream on the same catalog/session authority."""

    def __init__(self, connector: Any, config: Any) -> None:
        self.catalog = RawSnapshotCatalog(connector, config.source_schema, config.source_table)
        self.profile: ClickHouseRawSnapshotProfileV1 | None = None
        self.eof: ClickHouseRawSourceEofV1 | None = None
        self.partition_ids: tuple[str, ...] | None = None
        self.query = ""
        self.query_params: dict[str, str] = {}

    def acquire_profile(self, config: Any, *, reject_mutations: bool = True) -> ClickHouseRawSnapshotProfileV1:
        """Acquire immutable sanitized authority before chunk/recovery identity."""
        if self.catalog.params != {"database": config.source_schema, "table": config.source_table}:
            raise ValueError("mssql_native.source_snapshot_profile_changed")
        table, schema = self.catalog.relation()
        scope = native_source_snapshot_policy(config).replica_scope
        if scope is None:
            raise ValueError("mssql_native.source_snapshot_mode_invalid")
        signature = engine_signature(table["engine"], table["engine_full"], scope)
        for name, dtype, _ in schema:
            source_identifier(name)
            admit_source_type(dtype)
        authority = self.catalog.authority(scope)
        window = native_window(config)
        self.partition_ids = _partitions(table["partition_key"], window)
        partition_identity = _partition_identity(table["partition_key"], window, schema, self.catalog.timezones)
        self.catalog.physical(self.partition_ids, reject_mutations=reject_mutations)
        projection_shape = [projection(name, dtype) for name, dtype, _ in schema]
        relation = f"{source_identifier(config.source_schema)}.{source_identifier(config.source_table)}"
        query = (
            "SELECT "
            + ", ".join(item[0] for item in projection_shape)
            + ", CAST(`_part` AS String) AS `_part`, `_part_offset` FROM "
            + relation
        )
        params: dict[str, str] = {}
        window_identity = None
        if window is not None:
            if window.column not in {name for name, _, _ in schema} or not temporal(
                dict((name, dtype) for name, dtype, _ in schema)[window.column]
            ):
                raise ValueError("mssql_native.window_column_missing")
            column = f"`__dpone_native_source`.{source_identifier(window.column)}"
            query += f" AS `__dpone_native_source` WHERE {column} >= toDateTime64(%(start)s, 6, 'UTC') AND {column} < toDateTime64(%(end)s, 6, 'UTC')"
            params = {
                "start": window.start.strftime("%Y-%m-%d %H:%M:%S.%f"),
                "end": window.end.strftime("%Y-%m-%d %H:%M:%S.%f"),
            }
            window_identity = (window.column, window.start.isoformat(), window.end.isoformat())
        profile = ClickHouseRawSnapshotProfileV1(
            engine_signature=signature,
            relation_uuid=table["uuid"],
            database_engine=table["database_engine"],
            ordered_schema=schema,
            partition_key_sha256=digest(partition_identity),
            sorting_key_sha256=digest(table["sorting_key"]),
            primary_key_sha256=digest(table["primary_key"]),
            window=window_identity,
            read_settings=tuple(sorted(self.catalog.settings.items())),
            query_shape_sha256=digest(["dpone.clickhouse-raw-select.v1", query]),
            projection_sha256=digest(projection_shape),
            typed_parameters_sha256=digest(
                [[key, "DateTime64(6,'UTC')", value] for key, value in sorted(params.items())]
            ),
            replica_scope=scope,
            **authority,
        )
        self.query, self.query_params = query, params
        self.profile = profile
        return profile

    def rows(
        self, config: Any, expected: ClickHouseRawSnapshotProfileV1, query_id: str
    ) -> Iterator[Mapping[str, object]]:
        """Stream once, reject incomplete provenance and seal only after EOF checks."""
        stream = None
        primary: BaseException | None = None
        try:
            self.catalog.assert_session()
            if self.acquire_profile(config) != expected:
                raise ValueError("mssql_native.source_snapshot_profile_changed")
            parts = self.catalog.physical(self.partition_ids, reject_mutations=True)
            before = digest(
                [expected.relation_uuid, expected.replica_identity_sha256, parts, self.catalog.mutation_sha256]
            )
            self.catalog.assert_session()
            stream = self.catalog.client.execute_iter(
                self.query,
                self.query_params,
                query_id=query_id,
                with_column_types=True,
                settings={**dict(expected.read_settings), "strings_as_bytes": True, "max_block_size": 65536},
            )
            schema = tuple((name, dtype) for name, dtype, _ in expected.ordered_schema)
            wire = tuple((name, projection(name, dtype)[1]) for name, dtype in schema) + (
                ("_part", "String"),
                ("_part_offset", "UInt64"),
            )
            if tuple(next(stream)) != wire:
                raise ValueError("mssql_native.source_schema_changed")
            self.catalog.assert_session()
            coverage: dict[str, tuple[int, int]] = {}
            count = 0
            for row in stream:
                if len(row) != len(schema) + 2:
                    raise ValueError("mssql_native.source_row_arity")
                part, offset = row[-2:]
                if isinstance(part, bytes):
                    part = part.decode("utf-8", errors="strict")
                if part not in parts or type(offset) is not int or not 0 <= offset < parts[part]["rows"]:
                    raise ValueError("mssql_native.source_provenance_incomplete")
                part_count, total = coverage.get(part, (0, 0))
                coverage[part] = (part_count + 1, (total + int(digest([part, offset]), 16)) % (1 << 256))
                count += 1
                yield {name: restore(value, dtype) for (name, dtype), value in zip(schema, row[:-2], strict=True)}
            # Catalog commands are issued only once the vendor stream has reached
            # EOF; a native session cannot issue a query while a stream is active.
            after_parts = self.catalog.physical(self.partition_ids, reject_mutations=False)
            after = digest(
                [expected.relation_uuid, expected.replica_identity_sha256, after_parts, self.catalog.mutation_sha256]
            )
            if self.acquire_profile(config, reject_mutations=False) != expected:
                raise ValueError("mssql_native.source_snapshot_profile_changed")
            self.eof = ClickHouseRawSourceEofV1(
                hashlib.sha256(query_id.encode()).hexdigest(), count, digest(coverage), before, after, before == after
            )
        except BaseException as error:
            primary = error
            if isinstance(error, ValueError) and str(error).startswith("mssql_native."):
                raise
            if isinstance(error, (GeneratorExit, KeyboardInterrupt, SystemExit)):
                raise
            raise ValueError("mssql_native.source_provenance_incomplete") from None
        finally:
            try:
                close = getattr(stream, "close", None)
                if callable(close):
                    close()
            except BaseException:
                if primary is None:
                    raise ValueError("mssql_native.source_provenance_incomplete") from None
                primary.add_note("mssql_native.source_iterator_cleanup_failed")
            finally:
                closing_error = sys.exc_info()[1]
                try:
                    self.catalog.client.disconnect()
                except BaseException:
                    if closing_error is None and primary is None:
                        raise ValueError("mssql_native.source_provenance_incomplete") from None
                    if primary is not None:
                        primary.add_note("mssql_native.source_disconnect_failed")


def _partitions(expression: str, window: Any) -> tuple[str, ...] | None:
    """Enumerate UTC date/month partition IDs under a closed bounded allowlist."""
    if window is None:
        return None
    normalized = expression.replace("`", "").replace(" ", "")
    functions = {
        f"toYYYYMM({window.column})": "%Y%m",
        f"toYYYYMMDD({window.column})": "%Y%m%d",
        f"toDate({window.column})": "%Y%m%d",
    }
    if normalized not in functions:
        raise ValueError("mssql_native.source_provenance_incomplete")
    day = window.start.date()
    last = (window.end - timedelta(microseconds=1)).date()
    partitions = set()
    while day <= last:
        partitions.add(day.strftime(functions[normalized]))
        if len(partitions) > MAX_PARTS:
            raise ValueError("mssql_native.source_provenance_incomplete")
        if functions[normalized] == "%Y%m":
            day = (day.replace(day=1) + timedelta(days=32)).replace(day=1)
        else:
            day += timedelta(days=1)
    return tuple(sorted(partitions))


def _partition_identity(
    expression: str, window: Any, schema: tuple[tuple[str, str, str], ...], timezones: tuple[Any, Any]
) -> object:
    """Bind the proven calendar semantics used for UTC partition enumeration.

    An explicit UTC column is independent of server/session calendar defaults.
    An implicit column requires both defaults to be UTC; changing query settings
    cannot repair the partition IDs of already-stored non-UTC data.
    """
    if window is None:
        return expression
    dtype = next((dtype for name, dtype, _ in schema if name == window.column), None)
    if dtype is None or not temporal(dtype):
        raise ValueError("mssql_native.window_column_missing")
    explicit = "'UTC'" in dtype
    if not explicit and any(zone not in ("UTC", "Etc/UTC") for zone in timezones):
        raise ValueError("mssql_native.source_read_profile_unsupported")
    return [
        expression,
        {
            "kind": "dpone.utc-partition-semantics.v1",
            "column_type": dtype,
            "calendar_timezone": "UTC",
            "authority": "explicit_column" if explicit else "server_and_session",
        },
    ]
