from __future__ import annotations

from pathlib import Path

import pytest

from dpone.config import LoadStrategy
from dpone.contracts.errors import ETLConfigurationError
from dpone.dag.config import DependencyParser, ETLProcessConfig, LoadConfigBuilder, split_manifest_ref


def test_split_manifest_ref_keeps_selector() -> None:
    path, selector = split_manifest_ref("manifests/root.yaml#public.users")
    assert path == Path("manifests/root.yaml")
    assert selector == "public.users"


def test_dependency_parser_normalizes_multiple_dependency_shapes(tmp_path: Path) -> None:
    parser = DependencyParser()
    deps = parser.parse_many(
        [
            "upstream.yaml",
            {"path": "#public.users"},
            {"path": "other.yaml#public.orders", "alias": "orders"},
            {"group": "core_group"},
        ],
        base_path=tmp_path,
    )

    assert deps[0].path == str(tmp_path / "upstream.yaml")
    assert deps[1].path == "#public.users"
    assert deps[2].path == f"{tmp_path / 'other.yaml'}#public.orders"
    assert deps[2].alias == "orders"
    assert deps[3].group == "core_group"


def test_load_config_builder_supports_api_sources() -> None:
    cfg = {
        "source": {
            "type": "api",
            "connection_id": "api-conn",
            "api_type": "rest",
            "options": {"resource": "bookings", "batch_size": 123},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "bq-conn",
            "table": {"schema": "landing__sys__db", "name": "default__bookings"},
            "strategy": {"mode": "full_refresh"},
        },
    }

    load_cfg = LoadConfigBuilder().build(cfg)
    assert load_cfg.source_schema == "api__rest"
    assert load_cfg.source_table == "bookings"
    assert load_cfg.batch_size == 123
    assert load_cfg.load_strategy == LoadStrategy.FULL_REFRESH


def test_load_config_builder_preserves_source_and_sink_option_namespaces() -> None:
    cfg = {
        "source": {
            "type": "mssql",
            "connection_id": "src",
            "table": {"schema": "dbo", "name": "orders"},
            "options": {
                "native_transfer": {
                    "snapshot": {
                        "columnar_fast_path": {
                            "mode": "required",
                            "object_storage": {"runtime_access": {"connection_id": "s3_writer"}},
                        }
                    }
                }
            },
        },
        "sink": {
            "type": "clickhouse",
            "connection_id": "sink",
            "table": {"schema": "raw", "name": "orders"},
            "strategy": {"mode": "full_refresh"},
            "options": {
                "clickhouse_bulk": {
                    "columnar_pull": {"cluster": "dwh"},
                }
            },
        },
    }

    load_cfg = LoadConfigBuilder().build(cfg)

    assert load_cfg.options["source_options"]["native_transfer"]["snapshot"]["columnar_fast_path"]["mode"] == "required"
    assert load_cfg.options["sink_options"]["clickhouse_bulk"]["columnar_pull"]["cluster"] == "dwh"
    assert load_cfg.options["native_transfer"]["snapshot"]["columnar_fast_path"]["mode"] == "required"
    assert load_cfg.options["clickhouse_bulk"]["columnar_pull"]["cluster"] == "dwh"


def test_etl_process_config_from_dict_uses_split_parser(tmp_path: Path) -> None:
    cfg = {
        "name": "public_users__full_refresh",
        "description": "demo",
        "task_group": "demo_group",
        "source": {
            "type": "postgres",
            "connection_id": "src",
            "table": {"schema": "public", "name": "users"},
            "options": {"batch_size": 1000, "unique_key": ["id"]},
        },
        "sink": {
            "type": "bigquery",
            "connection_id": "sink",
            "table": {"schema": "landing__demo__db", "name": "public__users"},
            "strategy": {"mode": "full_refresh"},
            "options": {"log_sample_rows": 3},
        },
        "depends_on": [
            "upstream.yaml",
            {"path": "other.yaml#public.accounts"},
        ],
        "transforms": [{"name": "noop", "params": {}}],
    }

    parsed = ETLProcessConfig.from_dict(cfg, base_path=tmp_path, metadata_only=True)

    assert parsed.name == cfg["name"]
    assert parsed.description == "demo"
    assert parsed.task_group == "demo_group"
    assert parsed.load_config.source_table == "users"
    assert parsed.load_config.target_table == "public__users"
    assert parsed.load_config.unique_key == ["id"]
    assert parsed.dependencies[0].path == str(tmp_path / "upstream.yaml")
    assert parsed.dependencies[1].path == f"{tmp_path / 'other.yaml'}#public.accounts"
    assert len(parsed.transforms) == 1
    assert parsed.raw_config == cfg


def test_etl_process_config_from_dict_accepts_single_process_batch_manifest(tmp_path: Path) -> None:
    cfg = {
        "kind": "dpone.batch.v1",
        "vars": {
            "layer": "landing",
            "src_system": "sogu",
            "src_database": "sogu",
        },
        "naming": {
            "sink_dataset": "{{ layer }}__{{ src_system }}__{{ src_database }}",
            "sink_table": "{{ src_schema }}__{{ src_table }}",
            "process_name": "{{ src_schema }}_{{ src_table }}__{{ sink.strategy.mode }}",
        },
        "defaults": {
            "source": {
                "type": "clickhouse",
                "connection_id": "src",
            },
            "sink": {
                "type": "bigquery",
                "connection_id": "sink",
                "strategy": {"mode": "incremental_append"},
            },
        },
        "schemas": {
            "sogu": {
                "tables": [
                    "events",
                ]
            }
        },
    }

    parsed = ETLProcessConfig.from_dict(cfg, base_path=tmp_path, metadata_only=True)

    assert parsed.name == "sogu_events__incremental_append"
    assert parsed.load_config.source_schema == "sogu"
    assert parsed.load_config.source_table == "events"
    assert parsed.load_config.target_schema == "landing__sogu__sogu"
    assert parsed.load_config.target_table == "sogu__events"
    assert parsed.raw_config["name"] == "sogu_events__incremental_append"


def test_etl_process_config_from_dict_rejects_multi_process_batch_manifest(tmp_path: Path) -> None:
    cfg = {
        "kind": "dpone.batch.v1",
        "vars": {
            "layer": "landing",
            "src_system": "demo",
            "src_database": "db1",
        },
        "naming": {
            "sink_dataset": "{{ layer }}__{{ src_system }}__{{ src_database }}",
            "sink_table": "{{ src_schema }}__{{ src_table }}",
            "process_name": "{{ src_schema }}__{{ src_table }}",
        },
        "defaults": {
            "source": {
                "type": "postgres",
                "connection_id": "src",
            },
            "sink": {
                "type": "bigquery",
                "connection_id": "sink",
                "strategy": {"mode": "full_refresh"},
            },
        },
        "schemas": {
            "public": {
                "tables": [
                    "users",
                    "orders",
                ]
            }
        },
    }

    with pytest.raises(ETLConfigurationError) as exc_info:
        ETLProcessConfig.from_dict(cfg, base_path=tmp_path, metadata_only=True)

    message = str(exc_info.value)
    assert "Batch manifest нельзя однозначно распарсить" in message
    assert "public.users" in message
    assert "public.orders" in message
    assert "#<selector>" in message


def test_legacy_shim_from_yaml_metadata_supports_single_process_batch_manifest(tmp_path: Path) -> None:
    from dpone.yaml_config_handler.config import ETLProcessConfig as LegacyETLProcessConfig

    manifest = tmp_path / "landing_sogu.batch.yaml"
    manifest.write_text(
        """
kind: dpone.batch.v1
defaults:
  name: default_events__incremental_append
  source:
    type: clickhouse
    connection_id: clickhouse_sogu
    table:
      schema: "{{ src_schema }}"
      name: "{{ src_table }}"
  sink:
    type: bigquery
    connection_id: bigquery-dwh
    strategy:
      mode: incremental_append
    table:
      schema: landing__sogu__sogu
      name: default__events
schemas:
  sogu:
    tables:
      - events
""".lstrip(),
        encoding="utf-8",
    )

    parsed = LegacyETLProcessConfig.from_yaml_metadata(manifest)

    assert parsed.name == "default_events__incremental_append"
    assert parsed.load_config.source_schema == "sogu"
    assert parsed.load_config.source_table == "events"
