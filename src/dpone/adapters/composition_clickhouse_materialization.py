"""Bounded independent HTTP projections for typed snapshot materialization.

The injected query operation validates complete JSONCompact framing and accounts
for aggregate response bytes. The caller must hold protected writer quiescence
and prove complete catalog visibility before entering this reader.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot import SnapshotTarget
from dpone.contracts.composition_snapshot_materialization import (
    SNAPSHOT_MATERIALIZATION_PAGE_ROWS,
    snapshot_content_sha256,
)
from dpone.ports.semantic_refresh_clickhouse_authority import (
    clickhouse_authority_rows_sha256,
    clickhouse_physical_authority_rows,
)

_SCHEMA_SQL = (
    "SELECT name, type, toUInt64(position) AS position, default_kind, default_expression, compression_codec "
    "FROM system.columns WHERE database={database:String} AND table={table:String} ORDER BY position"
)
_SCHEMA_NAMES = ("name", "type", "position", "default_kind", "default_expression", "compression_codec")
_PHYSICAL_NAMES = (
    "engine",
    "partition_key",
    "sorting_key",
    "primary_key",
    "sampling_key",
    "storage_policy",
    "engine_full",
)


class ClickHouseSnapshotMaterializationReader:
    """Read actual schema/design and typed B rows without accessing claimed hashes."""

    def __init__(self, *, columns: tuple[ClickHouseDispatchColumn, ...], max_rows: int, max_bytes: int) -> None:
        self.columns, self.max_rows, self.max_bytes = columns, max_rows, max_bytes

    @staticmethod
    def require_plain_profile(query: Callable[..., Any], target: SnapshotTarget, evidence: Any) -> None:
        """Reject existing designs that the plain generation CREATE cannot preserve."""
        rows = query(
            "SELECT engine AS capture_engine, partition_key, sorting_key, primary_key, sampling_key, storage_policy, engine_full FROM system.tables WHERE database={database:String} AND name={target:String}",
            {"database": target.database, "target": target.target_table},
            (
                "capture_engine",
                "partition_key",
                "sorting_key",
                "primary_key",
                "sampling_key",
                "storage_policy",
                "engine_full",
            ),
            ("String",) * 7,
            2,
            evidence,
        )
        if (
            len(rows) != 1
            or rows[0][0] != "MergeTree"
            or rows[0][1] != ""
            or rows[0][2] not in {"", "tuple()"}
            or rows[0][3] != rows[0][2]
            or rows[0][4] != ""
            or rows[0][5] != "default"
            or rows[0][6] != "MergeTree ORDER BY tuple()"
        ):
            raise CompositionAdmissionError("snapshot_capture_physical_profile")

    def inspect(
        self,
        query: Callable[..., Any],
        *,
        database: str,
        table: str,
        database_engine: str,
        nodes: int,
        replicas: int,
        read_content: bool,
    ) -> tuple[str, str, str | None, int | None]:
        schema = query(
            _SCHEMA_SQL,
            {"database": database, "table": table},
            _SCHEMA_NAMES,
            ("String", "String", "UInt64", "String", "String", "String"),
            64,
        )
        expected = tuple((c.name, c.type_name, i) for i, c in enumerate(self.columns, 1))
        if tuple(row[:3] for row in schema) != expected or any(row[3] or row[4] for row in schema):
            raise CompositionAdmissionError("snapshot_materialization_schema")
        names = _PHYSICAL_NAMES + tuple(f"feature_{i}" for i in range(8))
        features = [
            f"toUInt64(positionCaseInsensitive(create_table_query, '{token}') > 0) AS feature_{i}"
            for i, token in enumerate(("TTL", "CODEC", "PROJECTION", "INDEX", "CONSTRAINT"))
        ]
        features += [
            "(SELECT toUInt64(count()) FROM system.tables WHERE engine = 'MaterializedView') AS feature_5",
            "(SELECT toUInt64(count()) FROM system.data_skipping_indices WHERE database={database:String} AND table={table:String}) AS feature_6",
            "(SELECT toUInt64(count()) FROM system.mutations WHERE database={database:String} AND table={table:String} AND NOT is_done) AS feature_7",
        ]
        statement = (
            "SELECT "
            + ", ".join((*_PHYSICAL_NAMES, *features))
            + " FROM system.tables WHERE database={database:String} AND name={table:String}"
        )
        physical = query(statement, {"database": database, "table": table}, names, ("String",) * 7 + ("UInt64",) * 8, 1)
        if len(physical) != 1:
            raise CompositionAdmissionError("snapshot_materialization_physical")
        physical_hash = clickhouse_authority_rows_sha256(
            clickhouse_physical_authority_rows(
                database_engine=database_engine,
                shard_count=nodes,
                replica_count=replicas,
                table_observation=physical[0],
            )
        )
        content = count = None
        if read_content:
            count_query = f"SELECT toUInt64(count()) AS observed_count FROM `{database}`.`{table}`"
            before_count = query(count_query, {}, ("observed_count",), ("UInt64",), 1)
            if len(before_count) != 1 or before_count[0][0] > self.max_rows:
                raise CompositionAdmissionError("snapshot_materialization_budget")
            rows: list[tuple[object, ...]] = []
            projection = ", ".join(f"toString(`{c.name}`) AS `{c.name}`" for c in self.columns)
            ordering = ", ".join(f"`{c.name}`" for c in self.columns)
            while True:
                maximum = min(SNAPSHOT_MATERIALIZATION_PAGE_ROWS, self.max_rows - len(rows) + 1)
                observed = query(
                    f"SELECT {projection} FROM `{database}`.`{table}` ORDER BY {ordering} LIMIT {maximum} OFFSET {len(rows)}",
                    {},
                    tuple(c.name for c in self.columns),
                    tuple(
                        "Nullable(String)" if c.type_name.startswith("Nullable(") else "String" for c in self.columns
                    ),
                    maximum,
                )
                rows.extend(
                    tuple(_typed(cell, c.type_name) for cell, c in zip(row, self.columns, strict=True))
                    for row in observed
                )
                if len(rows) > self.max_rows:
                    raise CompositionAdmissionError("snapshot_materialization_budget")
                if len(observed) < maximum:
                    break
            count = len(rows)
            after_count = query(count_query, {}, ("observed_count",), ("UInt64",), 1)
            if before_count != ((count,),) or after_count != before_count:
                raise CompositionAdmissionError("snapshot_materialization_count")
            after_schema = query(
                _SCHEMA_SQL,
                {"database": database, "table": table},
                _SCHEMA_NAMES,
                ("String", "String", "UInt64", "String", "String", "String"),
                64,
            )
            after_physical = query(
                statement, {"database": database, "table": table}, names, ("String",) * 7 + ("UInt64",) * 8, 1
            )
            if after_schema != schema or after_physical != physical:
                raise CompositionAdmissionError("snapshot_materialization_changed")
            content = snapshot_content_sha256(self.columns, rows, max_rows=self.max_rows, max_bytes=self.max_bytes)
        return clickhouse_authority_rows_sha256(schema), physical_hash, content, count


def _typed(value: object, kind: str) -> object:
    if kind.startswith("Nullable("):
        return None if value is None else _typed(value, kind[9:-1])
    if type(value) is not str:
        raise CompositionAdmissionError("snapshot_materialization_scalar")
    try:
        if kind.startswith(("Int", "UInt")):
            return int(value)
        if kind.startswith("Decimal("):
            return Decimal(value)
        return value
    except (ValueError, ArithmeticError):
        raise CompositionAdmissionError("snapshot_materialization_scalar") from None
