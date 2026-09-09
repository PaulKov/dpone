"""Physical storage checks shared by exact SQL Server catalog contracts."""

from __future__ import annotations

from typing import Any


def require_table_storage_integrity(
    connector: Any,
    *,
    prefix: str,
    schema: str,
    table: str,
    label: str,
    require_none_compression: bool,
    require_single_partition: bool,
) -> None:
    """Require the declared partition and compression shape for every index."""

    rows = connector.get_records(
        "SELECT i.index_id, p.partition_number, p.data_compression_desc "
        f"FROM {prefix}sys.indexes AS i "
        f"INNER JOIN {prefix}sys.tables AS t ON t.object_id = i.object_id "
        f"INNER JOIN {prefix}sys.schemas AS s ON s.schema_id = t.schema_id "
        f"INNER JOIN {prefix}sys.partitions AS p "
        "ON p.object_id = i.object_id AND p.index_id = i.index_id "
        "WHERE s.name = ? AND t.name = ? AND i.is_hypothetical = 0",
        (schema, table),
        as_dict=True,
    )
    grouped: dict[int, list[Any]] = {}
    for row in rows:
        grouped.setdefault(int(row["index_id"]), []).append(row)
    valid = bool(rows)
    if require_single_partition:
        valid = valid and all(
            len(partitions) == 1 and int(partitions[0].get("partition_number") or 0) == 1
            for partitions in grouped.values()
        )
    if require_none_compression:
        valid = valid and all(str(row.get("data_compression_desc") or "").upper() == "NONE" for row in rows)
    if not valid:
        raise RuntimeError(f"mssql_external_state_storage_contract:{label}")


__all__ = ["require_table_storage_integrity"]
