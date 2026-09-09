"""Hermetic contracts for SQL Server partition-replace fallback finalization."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.decision_audit import RuntimeDecision, RuntimeDecisionContext
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLPartitionReplaceStrategy,
)


class _DecisionCollector:
    def __init__(self) -> None:
        self.decisions: list[RuntimeDecision] = []

    def publish(self, decision: RuntimeDecision) -> None:
        self.decisions.append(decision)


class _Connector:
    def __init__(self, partition_values: list[object]) -> None:
        self.partition_values = partition_values
        self.calls: list[tuple[str, str]] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value.replace(']', ']]')}]"

    def get_records(
        self,
        query: str,
        _params: object = None,
        *,
        as_dict: bool = False,
    ) -> list[object]:
        sql = str(query)
        self.calls.append(("query", sql))
        if "SELECT DISTINCT p." in sql:
            return [(value,) for value in self.partition_values]
        if "[__dpone__affected_rows]" in sql:
            assert as_dict
            return [{"__dpone__affected_rows": 7}]
        if "COUNT_BIG(*)" in sql:
            return [(11,)]
        raise AssertionError(f"Unexpected SQL: {sql}")


class _Strategy(MSSQLPartitionReplaceStrategy):
    def _consume_with_staging(self, load_config, payload, handler):  # noqa: ANN001
        del load_config
        return handler(payload.artifact)

    def _table_exists(self, load_config):  # noqa: ANN001
        del load_config
        return True

    def _insert_from_staging(self, load_config, staging):  # noqa: ANN001
        del load_config
        self.connector.calls.append(("insert", staging.table))
        return staging.row_count

    def _count_target(self, load_config):  # noqa: ANN001
        del load_config
        return 11


def _config(*, maximum: int = 10) -> LoadConfig:
    partition = {
        "column": "business_date",
        "native_mode": "fallback",
        "max_partitions_per_run": maximum,
    }
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="marketing",
        source_table="events",
        target_database="DWH_Dev",
        target_schema="ch",
        target_table="marketing__events",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition=partition,
        options={"sink_type": "mssql", "partition": partition},
    )


def _staging(dtype: str) -> StagingTableArtifact:
    return StagingTableArtifact(
        schema="dpone_staging",
        table="events_native",
        columns=["business_date", "event_id"],
        staging_manager=SimpleNamespace(),
        row_count=2,
        database="DWH_Dev",
        target_column_types={"business_date": dtype, "event_id": "bigint"},
    )


def _load(
    dtype: str,
    partition_values: list[object],
    *,
    maximum: int = 10,
) -> tuple[_Connector, _DecisionCollector]:
    connector = _Connector(partition_values)
    strategy = _Strategy(connector, SimpleNamespace(), SimpleNamespace())
    collector = _DecisionCollector()
    with RuntimeDecisionContext.activate(collector):
        strategy.load(
            _config(maximum=maximum),
            LoadPayload(artifact=_staging(dtype), schema=[("business_date", dtype), ("event_id", "bigint")]),
        )
    return connector, collector


def test_date_partition_uses_sargable_distinct_join_and_records_decision() -> None:
    connector, collector = _load("date", [date(2026, 8, 22), date(2026, 8, 23)])

    sql = "\n".join(statement for kind, statement in connector.calls if kind == "query")
    assert "SELECT DISTINCT p.[business_date] FROM [DWH_Dev].[dpone_staging].[events_native] AS p" in sql
    assert "INNER JOIN (SELECT DISTINCT s.[business_date]" in sql
    assert "ON p.[business_date] = t.[business_date]" in sql
    assert "CONCAT(" not in sql
    assert "varbinary(max)" not in sql
    assert [kind for kind, _statement in connector.calls][-1] == "insert"
    assert len(collector.decisions) == 1
    decision = collector.decisions[0]
    assert decision.decision_id == "mssql.partition_replace.finalizer"
    assert decision.component == "mssql_partition_replace"
    assert decision.category == "finalizer_selection"
    assert decision.requested == "auto"
    assert decision.selected == "typed_sargable_distinct_join"
    assert decision.release_gate == "green"
    assert decision.details == {"partition_type": "date"}
    assert "DWH_Dev" not in str(decision.to_jsonable())


def test_non_date_partition_retains_canonical_identity_fallback() -> None:
    connector, collector = _load("nvarchar(32)", ["2026-08-22", "2026-08-23"])

    sql = "\n".join(statement for kind, statement in connector.calls if kind == "query")
    assert "[__dpone__partition_identity]" in sql
    assert "WHERE EXISTS" in sql
    assert "CONCAT(" in sql
    assert "CONVERT(varbinary(max)" in sql
    assert [decision.selected for decision in collector.decisions] == ["canonical_identity_fallback"]


@pytest.mark.parametrize(
    ("partition_values", "maximum", "message"),
    (
        ([None], 10, "rejects NULL partition identities"),
        ([date(2026, 8, 21), date(2026, 8, 22)], 1, "above max_partitions_per_run=1"),
    ),
)
def test_partition_guards_run_before_delete_or_insert(
    partition_values: list[object],
    maximum: int,
    message: str,
) -> None:
    connector = _Connector(partition_values)
    strategy = _Strategy(connector, SimpleNamespace(), SimpleNamespace())

    with pytest.raises(ValueError, match=message):
        strategy.load(
            _config(maximum=maximum),
            LoadPayload(artifact=_staging("date"), schema=[("business_date", "date"), ("event_id", "bigint")]),
        )

    assert all("[__dpone__affected_rows]" not in statement for _kind, statement in connector.calls)
    assert all(kind != "insert" for kind, _statement in connector.calls)


def test_date_delete_sql_is_deterministic_and_scoped_only_to_staged_dates() -> None:
    first, _collector = _load("date", [date(2026, 8, 23)])
    second, _collector = _load("date", [date(2026, 8, 23)])

    def delete_sql(connector: _Connector) -> str:
        return next(sql for _kind, sql in connector.calls if "DELETE t FROM" in sql)

    assert delete_sql(first) == delete_sql(second)
    assert "SELECT DISTINCT s.[business_date]" in delete_sql(first)
    assert "ON p.[business_date] = t.[business_date]" in delete_sql(first)
