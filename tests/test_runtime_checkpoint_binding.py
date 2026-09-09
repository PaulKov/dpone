from __future__ import annotations

from dpone.config import LoadConfig
from dpone.dag.config_models import ETLProcessConfig
from dpone.ports.runtime_hydrator import RuntimeBindings


def test_etl_process_config_applies_partition_checkpoint_store_binding() -> None:
    checkpoint_store = object()
    config = ETLProcessConfig(
        name="orders",
        load_config=LoadConfig(
            source_conn_id="src",
            target_conn_id="sink",
            source_schema="dbo",
            source_table="orders",
            target_schema="analytics",
            target_table="orders",
        ),
    )

    config.apply_runtime_bindings(
        RuntimeBindings(
            source_obj=object(),
            sink_obj=object(),
            etl_logger=object(),
            run_state_storage=object(),
            partition_checkpoint_store=checkpoint_store,
        )
    )

    assert config.partition_checkpoint_store is checkpoint_store
