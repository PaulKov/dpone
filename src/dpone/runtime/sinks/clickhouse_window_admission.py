"""Read-only physical topology and schema admission for atomic window targets."""

from __future__ import annotations

import re

from dpone.contracts.bounded_window import WindowContractError
from dpone.runtime.sinks.clickhouse_window_staging import WindowIO, literal


def validate_target(io: WindowIO) -> None:
    """Reject topology or schema capabilities that cannot preserve visible rows.

    The owning target checks plan identity and writer authority first. This
    inspection performs no mutations.
    """
    with io.connection() as connector:
        database = connector.get_records(f"SELECT engine FROM system.databases WHERE name = {literal(io.database)}")
        if database != [("Atomic",)]:
            raise WindowContractError("Window publication requires a local Atomic database")
        rows = connector.get_records(
            f"SELECT engine, create_table_query, dependencies_database, dependencies_table FROM system.tables WHERE database = {literal(io.database)} AND name = {literal(io.table)}"
        )
        if len(rows) != 1 or rows[0][0] != "MergeTree" or rows[0][2] or rows[0][3]:
            raise WindowContractError("Window target requires plain MergeTree without dependencies")
        if re.search(r"\b(TTL|PROJECTION)\b", str(rows[0][1]), re.IGNORECASE):
            raise WindowContractError("TTL and projections are unsupported for window publication")
        columns = connector.get_records(
            f"SELECT name, type, default_kind FROM system.columns WHERE database = {literal(io.database)} AND table = {literal(io.table)} ORDER BY position"
        )
        if tuple((str(row[0]), str(row[1])) for row in columns) != io.schema or any(row[2] for row in columns):
            raise WindowContractError("Physical schema differs or contains computed columns")
        mutations = connector.get_records(
            f"SELECT count() FROM system.mutations WHERE database = {literal(io.database)} AND table = {literal(io.table)} AND NOT is_done"
        )
        if mutations != [(0,)]:
            raise WindowContractError("Target has active mutations")
        policies = connector.get_records(
            f"SELECT count() FROM system.row_policies WHERE database IN ({literal(io.database)}, '*') "
            f"AND table IN ({literal(io.table)}, '*')"
        )
        if policies != [(0,)]:
            raise WindowContractError("Row policies may hide target data; window publication is unsupported")
