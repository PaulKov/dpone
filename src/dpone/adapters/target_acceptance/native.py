"""Worker-only native clients, fixed strict limits and immutable replica facts."""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any

from dpone.contracts.clickhouse_cluster_publication import ClusterInventory, ClusterReplica
from dpone.contracts.quality_replay import TargetAcceptanceError, TargetAcceptanceRequest, require_count

FORCED_SETTINGS = {
    "readonly": 1,
    "skip_unavailable_shards": 0,
    "result_overflow_mode": "throw",
    "read_overflow_mode": "throw",
    "timeout_overflow_mode": "throw",
    "max_result_rows": 4096,
    "max_result_bytes": 262144,
    "max_rows_to_read": 1000000000,
    "max_bytes_to_read": 1000000000000,
    "max_memory_usage": 536870912,
    "max_execution_time": 60,
}


class NativeSession:
    """A fresh process-local SDK client; never shared with sink or authority."""

    def __init__(
        self, descriptor: dict[str, Any], deadline: float, *, host: str | None = None, port: int | None = None
    ) -> None:
        from clickhouse_driver import Client

        self.deadline = deadline
        remaining = self.remaining()
        self.client = Client(
            host=host or descriptor["host"],
            port=port or descriptor["port"],
            database=descriptor["database"],
            user=descriptor["user"],
            password=descriptor["password"],
            secure=descriptor["secure"],
            compression=descriptor["compression"],
            ca_certs=descriptor["ca_cert"],
            connect_timeout=min(remaining, descriptor["connect_timeout"]),
            send_receive_timeout=min(remaining, descriptor["send_receive_timeout"]),
            settings={**FORCED_SETTINGS, "max_execution_time": remaining},
            sync_request_timeout=remaining,
            disable_reconnect=True,
            settings_is_important=True,
        )

    def remaining(self) -> float:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TargetAcceptanceError("INCOMPLETE")
        return remaining

    def read(self, query: str, params: dict[str, Any] | None = None, *, typed: bool = False) -> Any:
        """Only generated SELECT statements reach this private worker service."""
        remaining = self.remaining()
        connection = self.client.connection
        connection.connect_timeout = min(connection.connect_timeout, remaining)
        connection.send_receive_timeout = min(connection.send_receive_timeout, remaining)
        connection.sync_request_timeout = min(connection.sync_request_timeout, remaining)
        if connection.socket is not None:
            connection.socket.settimeout(connection.send_receive_timeout)
        return self.client.execute(
            query, params or {}, with_column_types=typed, settings={"max_execution_time": remaining}
        )

    def close(self) -> None:
        self.client.disconnect()

    def require_settings(self) -> None:
        rows = self.read(
            "SELECT name, value FROM system.settings WHERE name IN %(names)s", {"names": tuple(FORCED_SETTINGS)}
        )
        if len(rows) != len(FORCED_SETTINGS) or len({row[0] for row in rows}) != len(rows):
            raise TargetAcceptanceError("UNSUPPORTED")
        values = dict(rows)
        for name, expected in FORCED_SETTINGS.items():
            if name == "max_execution_time":
                if not 0 < float(values[name]) <= 60:
                    raise TargetAcceptanceError("UNSUPPORTED")
            elif str(values.get(name)) != str(expected):
                raise TargetAcceptanceError("UNSUPPORTED")

    def inventory(self, cluster: str) -> ClusterInventory:
        rows = self.read(
            "SELECT host_name, host_address, port, shard_num, replica_num, internal_replication "
            "FROM system.clusters WHERE cluster = %(cluster)s ORDER BY shard_num, replica_num",
            {"cluster": cluster},
        )
        inventory = ClusterInventory(
            cluster,
            tuple(
                ClusterReplica(str(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4]), bool(row[5]))
                for row in rows
            ),
        )
        inventory.validate()
        return inventory

    def generation(
        self, database: str, table: str, host: str, replica_count: int, *, allow_absent: bool = False
    ) -> dict[str, Any] | None:
        params = {"database": database, "table": table}
        tables = self.read(
            "SELECT hostName(), toString(uuid), engine_full, create_table_query "
            "FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            params,
        )
        if not tables and allow_absent:
            return None
        if len(tables) != 1 or tables[0][0] != host:
            raise TargetAcceptanceError("MISMATCH")
        _, uuid, engine, ddl = tables[0]
        engine = " ".join(engine.split())
        if not re.match(r"^ReplicatedMergeTree\s*\(", engine) or re.search(r"\bTTL\b", ddl, re.IGNORECASE):
            raise TargetAcceptanceError("UNSUPPORTED")
        columns = self.read(
            "SELECT name, type, default_kind, default_expression, position FROM system.columns "
            "WHERE database = %(database)s AND table = %(table)s ORDER BY position",
            params,
        )
        if not columns or len({row[0] for row in columns}) != len(columns):
            raise TargetAcceptanceError("MISMATCH")
        replicas = self.read(
            "SELECT ifNull(zookeeper_name, ''), zookeeper_path, is_readonly, is_session_expired, "
            "queue_size, active_replicas, total_replicas, last_queue_update_exception, zookeeper_exception "
            "FROM system.replicas WHERE database = %(database)s AND table = %(table)s",
            params,
        )
        if len(replicas) != 1:
            raise TargetAcceptanceError("MISMATCH")
        replica = replicas[0]
        if replica[2:5] != (0, 0, 0) and list(replica[2:5]) != [0, 0, 0]:
            raise TargetAcceptanceError("MISMATCH")
        if replica[5] != replica_count or replica[6] != replica_count or replica[7] or replica[8]:
            raise TargetAcceptanceError("MISMATCH")
        mutations = self.read(
            "SELECT count() FROM system.mutations WHERE database = %(database)s AND table = %(table)s AND NOT is_done",
            params,
        )
        if mutations != [(0,)]:
            raise TargetAcceptanceError("MISMATCH")
        return {
            "uuid": uuid,
            "engine_full": engine,
            "schema_digest": hashlib.sha256(repr(tuple(columns)).encode()).hexdigest(),
            "keeper_name": replica[0],
            "keeper_path": replica[1],
            "columns": [(row[0], row[1]) for row in columns],
        }


def inspect_replicas(
    descriptor: dict[str, Any], cluster: str, database: str, table: str, deadline: float, *, allow_absent: bool = False
) -> tuple[ClusterInventory, dict[str, dict[str, Any]]]:
    """Every replica must answer its own local metadata reads inside the budget."""
    coordinator = NativeSession(descriptor, deadline)
    try:
        coordinator.require_settings()
        inventory = coordinator.inventory(cluster)
    finally:
        coordinator.close()
    facts = {}
    for replica in sorted(inventory.replicas, key=lambda item: item.host):
        session = NativeSession(descriptor, deadline, host=replica.host, port=replica.native_port)
        try:
            session.require_settings()
            fact = session.generation(database, table, replica.host, len(inventory.replicas), allow_absent=allow_absent)
            if fact is not None:
                facts[replica.host] = fact
        finally:
            session.close()
    if facts and set(facts) != set(inventory.hosts):
        raise TargetAcceptanceError("MISMATCH")
    return inventory, facts


def require_binding(
    inventory: ClusterInventory,
    facts: dict[str, dict[str, Any]],
    binding: dict[str, Any],
    columns: tuple[tuple[str, str], ...],
) -> None:
    """Schema, Keeper identity, UUID and exact inventory remain inseparable."""
    if set(facts) != set(inventory.hosts) or binding.get("inventory_digest") != inventory.digest:
        raise TargetAcceptanceError("MISMATCH")
    for fact in facts.values():
        if {key: value for key, value in fact.items() if key != "columns"} != binding.get("desired"):
            raise TargetAcceptanceError("MISMATCH")
        actual = dict(fact["columns"])
        if any(actual.get(name) != kind for name, kind in columns):
            raise TargetAcceptanceError("MISMATCH")


# Conservative native scalar support; selected nested/aggregate types fail before publication.
_SCALAR = re.compile(
    r"^(?:U?Int(?:8|16|32|64|128|256)|Float(?:32|64)|String|FixedString\(\d+\)|UUID|Bool|Date|Date32|DateTime(?:\('[^']+'\))?|DateTime64\(\d+(?:, '[^']+')?\)|Decimal(?:32|64|128|256)?\([\d, ]+\)|Enum(?:8|16)\(.+\))$"
)


def quote(value: str) -> str:
    """Backtick identifiers escape both backslashes and embedded backticks."""
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def metric_plan(request: TargetAcceptanceRequest) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Ordinal aliases cannot collide with payload column names or metric kinds."""
    request.validate()
    selected = set(request.null_columns) | set(request.distinct_columns)
    for name, kind in request.columns:
        while kind.startswith(("Nullable(", "LowCardinality(")) and kind.endswith(")"):
            kind = kind[kind.index("(") + 1 : -1]
        if name in selected and not _SCALAR.fullmatch(kind):
            raise TargetAcceptanceError("UNSUPPORTED")
    metrics: list[tuple[str, str]] = []
    expressions: list[str] = []
    if request.row_count:
        metrics.append(("row_count", ""))
        expressions.append("count()")
    for field, columns in (("null_counts", request.null_columns), ("distinct_counts", request.distinct_columns)):
        for column in columns:
            metrics.append((field, column))
            function = "countIf(isNull({}))" if field == "null_counts" else "uniqExact({})"
            expressions.append(function.format(quote(column)))
    # Empty metric selections still require a real aggregate row, never table rows.
    if not expressions:
        expressions.append("count()")
        metrics.append(("_discard", ""))
    fields = ", ".join(f"{expression} AS __dpone_m{index}" for index, expression in enumerate(expressions))
    return f"SELECT {fields} FROM {quote(request.database)}.{quote(request.table)}", tuple(metrics)


def parse_metrics(rows: Any, columns: Any, plan: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    """One row, exact ordinal aliases and exact UInt64 values; no default zero."""
    if not isinstance(rows, (list, tuple)) or len(rows) != 1:
        raise TargetAcceptanceError()
    aliases = [(f"__dpone_m{index}", "UInt64") for index in range(len(plan))]
    if columns != aliases or not isinstance(rows[0], (list, tuple)) or len(rows[0]) != len(plan):
        raise TargetAcceptanceError()
    result: dict[str, Any] = {"row_count": None, "null_counts": {}, "distinct_counts": {}}
    for (field, name), value in zip(plan, rows[0], strict=True):
        require_count(value)
        if field == "row_count":
            result[field] = value
        elif field != "_discard":
            result[field][name] = value
    return result
