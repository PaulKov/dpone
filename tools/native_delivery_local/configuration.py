"""Pure synthetic load configuration, with all finite limits supplied by harness."""

from __future__ import annotations

from tools.native_delivery_live_support.profiles import Dataset

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.manifest.mssql_native_policy import validate_native_config

from .inventory import Inventory


def load_configuration(value: Inventory) -> LoadConfig:
    limits = dict(value.configuration["limits"])
    parallelism = limits.pop("parallelism")
    options = {
        "source_type": "clickhouse",
        "sink_type": "mssql",
        "lineage": {"preset": "bulk_standard"},
        "physical_design": {
            "columns": {
                column["name"]: {"target_type": {"mssql": column["target"]}}
                for column in Dataset(value.profile, value.rows, value.seed).schema()
            }
        },
        "__dpone_load_identity": {"run_id": value.invocation_id[:26], "load_id": value.invocation_id[:26]},
        "native_transfer": {
            "wire": {"mode": "typed_binary", "binary_format": "mssql_native"},
            "execution": {
                "chunking": {"mode": "bounded_stream", "checkpointing": "resumable", "parallelism": parallelism},
                "native_chunks": limits,
            },
        },
    }
    if value.strategy == "partition_replace":
        options["mssql_native_window"] = {"column": "event_at", "anchor": "data_interval_end", "lookback": "P1D"}
        options["interval"] = {"interval_end": "2026-01-02T00:00:00+00:00"}
    config = LoadConfig(
        source_conn_id="dda-local-source",
        target_conn_id="dda-local-target",
        source_schema=value.source_database,
        source_table=value.schema,
        target_database=value.target_database,
        target_schema=value.schema,
        target_table=value.table,
        staging_database=value.target_database,
        staging_schema=value.schema,
        load_strategy=LoadStrategy(value.strategy),
        options=options,
        log_sample_rows=0,
    )
    validate_native_config(config)
    return config
