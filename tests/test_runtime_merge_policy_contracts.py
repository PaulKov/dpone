from __future__ import annotations

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.kafka import KafkaSink
from dpone.runtime.sinks.merge_policy import (
    MergePolicy,
    resolve_merge_policy,
    validate_partition_replace,
)


def _load_config(**overrides) -> LoadConfig:
    data = {
        "source_conn_id": "src",
        "target_conn_id": "tgt",
        "source_schema": "public",
        "source_table": "orders",
        "target_schema": "landing",
        "target_table": "orders",
        "load_strategy": LoadStrategy.INCREMENTAL_MERGE,
        "unique_key": "id",
    }
    data.update(overrides)
    return LoadConfig(**data)


def test_merge_policy_auto_resolves_per_sink_defaults() -> None:
    cfg = _load_config()

    assert resolve_merge_policy(cfg, "mssql") == MergePolicy.DELETE_INSERT
    assert resolve_merge_policy(cfg, "postgres") == MergePolicy.DELETE_INSERT
    assert resolve_merge_policy(cfg, "bigquery") == MergePolicy.DELETE_INSERT
    assert resolve_merge_policy(cfg, "clickhouse") == MergePolicy.LIGHTWEIGHT_DELETE_INSERT
    assert resolve_merge_policy(cfg, "kafka") == MergePolicy.EVENT_UPSERT


def test_clickhouse_mutation_merge_policy_requires_explicit_opt_in() -> None:
    cfg = _load_config(merge_policy="mutation_delete_insert")

    with pytest.raises(ValueError, match="non-recommended"):
        resolve_merge_policy(cfg, "clickhouse")

    allowed = _load_config(merge_policy="mutation_delete_insert", allow_non_recommended_policy=True)
    assert resolve_merge_policy(allowed, "clickhouse") == MergePolicy.MUTATION_DELETE_INSERT


def test_partition_replace_validation_accepts_db_sinks_and_rejects_kafka() -> None:
    cfg = _load_config(
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={"column": "business_date", "max_partitions_per_run": 8},
    )

    assert validate_partition_replace(cfg, "mssql").column == "business_date"
    with pytest.raises(ValueError, match="not supported"):
        validate_partition_replace(cfg, "kafka")


def test_partition_replace_validation_accepts_physical_value_expression() -> None:
    cfg = _load_config(
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        partition={
            "column": "business_date",
            "value_expression": "toYYYYMM(business_date)",
            "max_partitions_per_run": 8,
        },
    )

    partition = validate_partition_replace(cfg, "clickhouse")

    assert partition.column == "business_date"
    assert partition.value_expression == "toYYYYMM(business_date)"


def test_load_config_builder_parses_merge_and_partition_strategy_fields() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"schema": "public", "name": "orders"},
            },
            "sink": {
                "type": "mssql",
                "connection_id": "sql",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {
                    "mode": "partition_replace",
                    "duplicate_policy": "fail",
                    "allow_non_recommended_policy": True,
                    "mutations_sync": 2,
                    "partition": {"column": "business_date", "values_from_staging": True},
                },
            },
        }
    )

    assert cfg.load_strategy == LoadStrategy.PARTITION_REPLACE
    assert cfg.merge_policy == "auto"
    assert cfg.duplicate_policy == "fail"
    assert cfg.allow_non_recommended_policy is True
    assert cfg.mutations_sync == 2
    assert cfg.partition == {"column": "business_date", "values_from_staging": True}
    assert cfg.options["partition"]["column"] == "business_date"


def test_load_config_builder_parses_merge_policy_for_merge_strategy() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"schema": "public", "name": "orders"},
            },
            "sink": {
                "type": "mssql",
                "connection_id": "sql",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {
                    "mode": "incremental_merge",
                    "unique_key": ["id"],
                    "merge_policy": "delete_insert",
                },
            },
        }
    )

    assert cfg.load_strategy == LoadStrategy.INCREMENTAL_MERGE
    assert cfg.merge_policy == "delete_insert"


def test_load_config_builder_parses_new_production_strategy_and_lineage_options() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "postgres",
                "connection_id": "pg",
                "table": {"schema": "public", "name": "customers"},
            },
            "sink": {
                "type": "mssql",
                "connection_id": "sql",
                "table": {"schema": "mart", "name": "dim_customers"},
                "strategy": {
                    "mode": "scd2",
                    "unique_key": ["customer_id"],
                    "scd2": {"delete_policy": "expire"},
                },
                "options": {
                    "lineage": {
                        "enabled": True,
                        "preset": "standard",
                        "features": {"operations": True, "diagnostics": True},
                    }
                },
            },
        }
    )

    assert cfg.load_strategy == LoadStrategy.SCD2
    assert cfg.unique_key == ["customer_id"]
    assert cfg.options["scd2"] == {"delete_policy": "expire"}
    assert cfg.options["lineage"]["preset"] == "standard"
    assert cfg.options["lineage"]["features"]["operations"] is True


def test_kafka_sink_rejects_partition_replace_with_clear_diagnostic() -> None:
    cfg = _load_config(load_strategy=LoadStrategy.PARTITION_REPLACE)
    sink = KafkaSink(connector=object())

    with pytest.raises(ValueError, match="does not support partition_replace"):
        sink.load(cfg, LoadPayload(artifact=InMemoryRowsArtifact([]), schema=[("id", "bigint")]))
