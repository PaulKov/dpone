from __future__ import annotations

import importlib.util
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_parquet_s3_type_certification.py")
    spec = importlib.util.spec_from_file_location(
        "dpone_tools_mssql_clickhouse_parquet_s3_type_certification",
        path,
    )
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parquet_s3_load_config_selects_required_named_collection_pull() -> None:
    module = _load_tool_module()
    config = module.ParquetS3CertificationConfig(
        rows=10_000,
        release_id="0.74.0",
        column_count=202,
        source_schema="dbt_calc",
        source_table="wide_dbt_result",
        target_database="analytics",
        target_table="wide_dbt_parquet",
        output_dir=Path("out"),
        mssql_params={"host": "127.0.0.1", "password": "secret"},
        clickhouse_params={"host": "127.0.0.1", "password": "secret"},
        s3_params={"endpoint_url": "http://127.0.0.1:9000", "access_key": "key", "secret_key": "secret"},
        object_prefix="s3://dpone-stage/wide-dbt/{run_id}/",
        named_collection="dpone_stage",
        run_id="run-1",
        batch_size=5_000,
        target_chunk_bytes=4 * 1024 * 1024,
        max_chunk_bytes=16 * 1024 * 1024,
    )

    load_config = module.build_load_config(config)

    columnar = load_config.options["native_transfer"]["snapshot"]["columnar_fast_path"]
    assert columnar["mode"] == "required"
    assert columnar["provider"] == "object_storage_pull"
    assert columnar["object_storage"]["format"] == "parquet"
    assert columnar["object_storage"]["compression"] == "zstd"
    assert columnar["object_storage"]["clickhouse_read_access"] == {
        "mode": "named_collection",
        "named_collection": "dpone_stage",
    }
    assert load_config.options["clickhouse_bulk"]["columnar_pull"]["use_cluster_function"] == "s3"


def test_parquet_s3_cli_can_reuse_a_prepared_dbt_relation() -> None:
    module = _load_tool_module()

    config = module.build_config(
        module.parse_args(
            [
                "--release-id",
                "0.74.0",
                "--source-schema",
                "dbt_calc",
                "--source-table",
                "wide_dbt_result",
                "--target-table",
                "wide_dbt_parquet",
                "--rows",
                "10000",
                "--column-count",
                "202",
                "--run-id",
                "dbt-wide",
                "--upstream-evidence",
                "dbt_materialization.json",
            ]
        )
    )

    assert config.source_schema == "dbt_calc"
    assert config.source_table == "wide_dbt_result"
    assert config.rows == 10_000
    assert config.column_count == 202
    assert config.run_id == "dbt-wide"
    assert config.upstream_evidence == Path("dbt_materialization.json")


def test_parquet_cli_defaults_match_local_compose_object_storage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_tool_module()
    for name in (
        "DPONE_IT_S3_ENDPOINT",
        "DPONE_IT_MINIO_PORT_FORWARD",
        "DPONE_IT_S3_ACCESS_KEY",
        "DPONE_IT_S3_SECRET_KEY",
        "DPONE_IT_MINIO_ACCESS_KEY",
        "DPONE_IT_MINIO_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    args = module.parse_args(
        [
            "--release-id",
            "0.74.0",
            "--source-schema",
            "dbt_calc",
            "--source-table",
            "wide_dbt_result",
            "--target-table",
            "wide_dbt_parquet",
            "--rows",
            "10000",
            "--column-count",
            "202",
            "--run-id",
            "run-1",
            "--upstream-evidence",
            "dbt.json",
        ]
    )

    assert args.s3_endpoint == "http://127.0.0.1:59090"
    assert args.s3_access_key == ""
    assert args.s3_secret_key == ""


def test_local_compose_prepares_bucket_and_clickhouse_named_collection() -> None:
    compose = yaml.safe_load(Path("docker/docker-compose.integration.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    assert services["minio-init"]["depends_on"] == ["minio"]
    init_command = services["minio-init"]["entrypoint"][-1]
    assert "mc mb --ignore-existing local/dpone-stage" in init_command
    clickhouse = services["clickhouse"]
    assert any("local-object-storage.xml" in volume for volume in clickhouse["volumes"])
    assert any("users.d/local-object-storage.xml" in volume for volume in clickhouse["volumes"])
    root = ET.parse("docker/clickhouse/config.d/local-object-storage.xml").getroot()
    collection = root.find("./named_collections/dpone_stage")
    assert collection is not None
    assert collection.findtext("url") == "http://minio:9000/dpone-stage/"
    assert collection.find("access_key_id").attrib == {"from_env": "DPONE_IT_MINIO_ACCESS_KEY"}
    assert collection.find("secret_access_key").attrib == {"from_env": "DPONE_IT_MINIO_SECRET_KEY"}
    users = ET.parse("docker/clickhouse/users.d/local-object-storage.xml").getroot()
    grants = [node.text for node in users.findall("./users/default/grants/query")]
    assert grants == [
        "GRANT ALL ON *.*",
        "GRANT NAMED COLLECTION ON dpone_stage",
    ]


def test_local_compose_prepares_mssql_database_after_healthcheck() -> None:
    compose = yaml.safe_load(Path("docker/docker-compose.integration.yml").read_text(encoding="utf-8"))
    service = compose["services"]["mssql-init"]

    assert service["depends_on"] == {"mssql": {"condition": "service_healthy"}}
    command = service["entrypoint"][-1]
    assert "^[A-Za-z][A-Za-z0-9_]{0,127}$" in command
    assert "IF DB_ID" in command
    assert "CREATE DATABASE" in command


def test_parquet_release_requires_upstream_and_full_hash_before_connectors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_tool_module()
    monkeypatch.setattr(module, "capture_local_source_snapshot", lambda: object())
    config = module.ParquetS3CertificationConfig(
        rows=10_000,
        release_id="0.74.0",
        column_count=202,
        source_schema="dbt_calc",
        source_table="wide_dbt_result",
        target_database="analytics",
        target_table="wide_dbt_parquet",
        output_dir=Path("out"),
        mssql_params={"password": "secret"},
        clickhouse_params={"password": "secret"},
        s3_params={"access_key": "key", "secret_key": "secret"},
        object_prefix="s3://dpone-stage/wide/{run_id}/",
        named_collection="dpone_stage",
        run_id="run-1",
        batch_size=5_000,
        target_chunk_bytes=1024,
        max_chunk_bytes=2048,
        upstream_evidence=None,
    )

    with pytest.raises(ValueError, match="requires_upstream_evidence"):
        module.run_live_certification(config)
