from __future__ import annotations

from pathlib import Path

from dpone.dag.load_config_builder import LoadConfigBuilder
from dpone.dag.loader import ConfigLoader
from dpone.manifest.loader import ManifestLoaderRouter


def test_mssql_manifest_examples_load_metadata(tmp_path: Path) -> None:
    examples = [
        "landing_postgres_to_mssql.batch.yaml",
        "landing_mssql_to_mssql.batch.yaml",
        "landing_mssql_to_clickhouse.batch.yaml",
        "landing_mssql_to_postgres.batch.yaml",
        "landing_mssql_to_bigquery.batch.yaml",
        "landing_mssql_to_kafka.batch.yaml",
        "landing_rest_api_to_mssql.batch.yaml",
        "landing_postgres_xmin_state_mssql.batch.yaml",
    ]
    manifest_dir = tmp_path / "examples"
    manifest_dir.mkdir()
    for name in examples:
        source = Path("examples/batch") / name
        target = manifest_dir / name
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")

    loader = ConfigLoader(manifest_dir, manifest_loader=ManifestLoaderRouter())
    for name in examples:
        manifest = loader.get_manifest(manifest_dir / name, metadata_only=True)
        assert manifest is not None
        assert manifest.processes


def test_load_config_builder_preserves_mssql_and_rest_types_in_options() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "api",
                "api_type": "rest",
                "options": {
                    "resource": "orders",
                    "columns": [{"name": "id", "type": "bigint"}],
                },
            },
            "sink": {
                "type": "mssql",
                "connection_id": "mssql-dwh",
                "table": {"schema": "landing", "name": "orders"},
                "strategy": {
                    "mode": "incremental_append",
                    "only_new_rows": True,
                    "unique_key": ["id"],
                },
                "options": {"bulk": {"mode": "bcp", "bcp": {"batch_size": 100000}}},
            },
        }
    )

    assert cfg.source_conn_id == "api__rest"
    assert cfg.options["columns"] == [{"name": "id", "type": "bigint"}]
    assert cfg.options["bulk"]["mode"] == "bcp"


def test_load_config_builder_deep_merges_native_transfer_source_and_sink_options() -> None:
    cfg = LoadConfigBuilder().build(
        {
            "source": {
                "type": "mssql",
                "connection_id": "mssql-dwh",
                "table": {"database": "DWH", "schema": "rep", "name": "orders"},
                "options": {
                    "native_transfer": {
                        "wire": {
                            "mode": "typed_binary",
                            "source_native_format": "bcp_native",
                            "binary_format": "native",
                        }
                    },
                    "bulk": {"mode": "bcp", "bcp": {"file_format": "native"}},
                },
            },
            "sink": {
                "type": "clickhouse",
                "connection_id": "clickhouse",
                "table": {"schema": "Example_Datamarts", "name": "orders"},
                "strategy": {"mode": "full_refresh"},
                "options": {
                    "native_transfer": {"execution": {"certification": {"mode": "advisory"}}},
                    "clickhouse_bulk": {"mode": "client", "ingest_contract": "typed_binary_staging"},
                },
            },
        }
    )

    assert cfg.options["native_transfer"]["wire"]["mode"] == "typed_binary"
    assert cfg.options["native_transfer"]["wire"]["source_native_format"] == "bcp_native"
    assert cfg.options["native_transfer"]["wire"]["binary_format"] == "native"
    assert cfg.options["native_transfer"]["execution"]["certification"] == {"mode": "advisory"}
