from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import normalize_mssql_load_strategy
from dpone.config.mssql_strategy_contract_error import MSSQLStrategyContractError
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.strategies.bigquery.bigquery_increment_merge import BigQueryPartitionReplaceStrategy
from dpone.runtime.sinks.strategies.mssql.mssql_strategies import MSSQLPartitionReplaceStrategy
from dpone.runtime.sinks.strategies.postgres.postgres_increment_merge import PostgresPartitionReplaceStrategy


class _Artifact:
    estimated_rows = 2

    def __init__(self, staging_schema: str = "staging", staging_table: str = "orders_stg") -> None:
        self.staging_schema = staging_schema
        self.staging_table = staging_table

    def materialize(self, staging_manager, load_config, schema):
        return SimpleNamespace(
            schema=self.staging_schema,
            table=self.staging_table,
            columns=[column for column, _ in schema],
            row_count=2,
            cleanup=lambda: None,
        )

    def cleanup(self):
        return None


def _partition_config(**partition_overrides) -> LoadConfig:
    partition = {
        "column": "business_date",
        "native_mode": "required",
        "max_partitions_per_run": 4,
    }
    partition.update(partition_overrides)
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition=partition,
        options={"partition": partition},
    )


class _NoopLogger:
    def warning(self, message: str) -> None:
        self.last_warning = message

    def log_etl_progress(self, *args, **kwargs) -> None:
        return None


def test_mssql_explicit_fallback_skips_native_metadata_and_warning() -> None:
    class Connector:
        def get_records(self, *_args, **_kwargs):
            raise AssertionError("fallback mode must not probe native partition metadata")

    cfg = _partition_config(native_mode="fallback")
    policy = normalize_mssql_load_strategy(cfg).partition_replace
    assert policy is not None
    logger = _NoopLogger()
    strategy = MSSQLPartitionReplaceStrategy(Connector(), logger, staging_manager=object())

    result = strategy._try_native_partition_switch(  # noqa: SLF001
        cfg,
        SimpleNamespace(schema="staging", table="orders_stg"),
        policy,
    )

    assert result is None
    assert policy.native is False
    assert not hasattr(logger, "last_warning")


def test_mssql_partition_replace_rejects_uncertified_native_switch_before_io() -> None:
    class Connector:
        def __init__(self) -> None:
            self.queries: list[tuple[str, object]] = []

        def quote_identifier(self, name: str) -> str:
            return "[" + name.replace("]", "]]") + "]"

        def qualified_name(self, schema: str, table: str) -> str:
            return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

        def begin(self) -> None:
            self.queries.append(("BEGIN", None))

        def commit_transaction(self) -> None:
            self.queries.append(("COMMIT", None))

        def rollback(self) -> None:
            self.queries.append(("ROLLBACK", None))

        def table_exists(self, schema: str, table: str) -> bool:
            return table in {"orders", "orders_stg", "orders__switchout__42"}

        def execute_query(self, query, params=None):
            self.queries.append((str(query), params))
            return 1

        def get_records(self, query, params=None, as_dict=False):
            text = str(query)
            self.queries.append((text, params))
            if "sys.partition_functions" in text and params == ("landing", "orders"):
                return [("pf_orders_business_date",)]
            if "sys.partition_functions" in text and params == ("staging", "orders_stg"):
                return [("pf_orders_business_date",)]
            if "i.index_id > 1" in text:
                return [(0,)]
            if "SELECT DISTINCT" in text:
                return [("2026-06-03",)]
            if "$PARTITION" in text:
                return [(42,)]
            if "COUNT_BIG" in text:
                return [(0,)] if "orders__switchout__42" in text else [(100,)]
            return []

    cfg = _partition_config(
        partition_function="pf_orders_business_date",
        switch_out_schema="landing_switch",
        switch_out_table_template="orders__switchout__{partition}",
    )
    connector = Connector()
    strategy = MSSQLPartitionReplaceStrategy(connector, _NoopLogger(), staging_manager=object())

    with pytest.raises(MSSQLStrategyContractError) as caught:
        strategy.load(
            cfg,
            LoadPayload(artifact=_Artifact(), schema=[("id", "bigint"), ("business_date", "date")]),
        )

    assert caught.value.blocker == "mssql.strategy.partition_replace.native_switch"
    assert connector.queries == []


def test_postgres_partition_replace_uses_declarative_detach_attach_when_bounds_resolve() -> None:
    class Connector:
        def __init__(self) -> None:
            self.queries: list[tuple[str, object]] = []

        def execute_query(self, query, params=None):
            self.queries.append((str(query), params))
            return 2

        def get_records(self, query, params=None, as_dict=False):
            text = str(query)
            self.queries.append((text, params))
            if "c.relkind = 'p'" in text:
                return [(True,)]
            if "SELECT DISTINCT" in text:
                return [("2026-06-03",)]
            if "tableoid::regclass" in text:
                return [("landing.orders_20260603",)]
            if "pg_get_expr" in text:
                return [("FOR VALUES FROM ('2026-06-03') TO ('2026-06-04')",)]
            if "IS DISTINCT FROM" in text:
                return [(False,)]
            if "COUNT(*) FROM" in text:
                return [(100,)]
            return []

    class Strategy(PostgresPartitionReplaceStrategy):
        def _ensure_target_table(self, load_config, schema):
            return False

        def _consume_with_staging(self, load_config, payload, handler):
            return handler(payload.artifact.materialize(None, load_config, payload.schema))

    cfg = _partition_config()
    connector = Connector()
    strategy = Strategy(connector, _NoopLogger(), staging_manager=object())

    result = strategy.load(cfg, LoadPayload(artifact=_Artifact(), schema=[("id", "bigint"), ("business_date", "date")]))

    joined = "\n".join(query for query, _ in connector.queries)
    assert result.replaced_rows == result.inserted_rows == 2
    assert result.hard_deleted_rows == 100
    assert "DETACH PARTITION" in joined
    assert "landing.orders_20260603" in joined
    assert "ATTACH PARTITION" in joined
    assert "FOR VALUES FROM ('2026-06-03') TO ('2026-06-04')" in joined
    assert "DELETE FROM" not in joined


def test_bigquery_partition_replace_uses_partition_decorator_write_truncate() -> None:
    google_modules_before = set(sys.modules)

    class Job:
        def result(self):
            return None

    class Client:
        def __init__(self) -> None:
            self.jobs = []

        def get_table(self, table_id: str):
            assert table_id == "demo.landing.orders"
            return SimpleNamespace(time_partitioning=SimpleNamespace(field="business_date"))

        def query(self, query: str, job_config):
            self.jobs.append((query, job_config))
            return Job()

    class Connector:
        project_id = "demo"

        def __init__(self) -> None:
            self.connection = Client()
            self.queries = []

        def get_records(self, query, params=None, as_dict=False):
            text = str(query)
            self.queries.append((text, params))
            if "SELECT DISTINCT" in text:
                return [{"partition_value": "2026-06-03"}]
            if "COUNT(*) AS row_count" in text:
                return [{"row_count": 2}]
            return []

        def execute_query(self, query, params=None):
            raise AssertionError("native BigQuery partition_replace should not execute fallback DML")

    class Strategy(BigQueryPartitionReplaceStrategy):
        def _create_target_table_if_not_exists(self, load_config, schema):
            return False

        def _build_select_with_json_parse(self, schema, staging_table_alias=None, include_technical_columns=True):
            return "`id`, `business_date`", "`id`, `business_date`"

    cfg = _partition_config()
    connector = Connector()
    strategy = Strategy(connector, _NoopLogger())

    try:
        result = strategy.load(
            cfg, LoadPayload(artifact=_Artifact(), schema=[("id", "bigint"), ("business_date", "date")])
        )
    finally:
        for module_name in list(sys.modules):
            if module_name.startswith("google") and module_name not in google_modules_before:
                sys.modules.pop(module_name, None)

    assert result.replaced_rows == 1
    assert result.inserted_rows == 2
    query, job_config = connector.connection.jobs[0]
    assert "WHERE CAST(`business_date` AS STRING) = @partition_value" in query
    assert str(job_config.destination) == "demo.landing.orders$20260603"
    assert str(job_config.write_disposition) == "WRITE_TRUNCATE"
    for module_name in list(sys.modules):
        if module_name.startswith("google") and module_name not in google_modules_before:
            sys.modules.pop(module_name, None)


def test_bigquery_partition_replace_falls_back_for_ingestion_time_partitioning() -> None:
    """Ingestion-time tables must not use $business_date decorator overwrite."""

    class Job:
        def result(self):
            raise AssertionError("native decorator overwrite must not run")

    class Client:
        def get_table(self, table_id: str):
            return SimpleNamespace(time_partitioning=SimpleNamespace(field=None))

        def query(self, query: str, job_config):
            return Job()

    class Connector:
        project_id = "demo"

        def __init__(self) -> None:
            self.connection = Client()
            self.dml: list[str] = []

        def get_records(self, query, params=None, as_dict=False):
            text = str(query)
            if "SELECT DISTINCT" in text:
                return [{"partition_value": "2026-06-03"}]
            return []

        def execute_query(self, query, params=None):
            self.dml.append(str(query))
            return 2

    class Strategy(BigQueryPartitionReplaceStrategy):
        def _create_target_table_if_not_exists(self, load_config, schema):
            return False

        def _build_select_with_json_parse(self, schema, staging_table_alias=None, include_technical_columns=True):
            return "`id`, `business_date`", "`id`, `business_date`"

        def _execute_dml_query(self, query: str) -> int:
            return self.connector.execute_query(query)

    cfg = _partition_config(native_mode="auto")
    connector = Connector()
    result = Strategy(connector, _NoopLogger()).load(
        cfg, LoadPayload(artifact=_Artifact(), schema=[("id", "bigint"), ("business_date", "date")])
    )

    assert result.inserted_rows == 2
    assert any("DELETE FROM" in sql for sql in connector.dml)
    assert any("INSERT INTO" in sql for sql in connector.dml)
    assert not hasattr(connector.connection, "jobs") or not getattr(connector.connection, "jobs", None)
