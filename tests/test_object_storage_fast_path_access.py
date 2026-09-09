from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest

from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.object_storage_access import (
    ClickHouseObjectStorageReadinessProbe,
    ObjectStorageAccessPreflightService,
    ObjectStorageAccessRequest,
    ObjectStorageConnectionRef,
    ObjectStorageReadContract,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.object_storage_connection_resolver import ObjectStorageConnectionResolver
from dpone.storage import LocalObjectStorageClient, ObjectStorageUri


def test_preflight_requires_runtime_connection_id() -> None:
    with pytest.raises(ValueError, match="runtime_access.connection_id"):
        ObjectStorageAccessRequest(
            uri_prefix="s3://dpone-stage/msql/{run_id}/",
            runtime_access=ObjectStorageRuntimeAccess(
                connection=ObjectStorageConnectionRef(connection_type="airflow", connection_id=""),
                required_permissions=("put_object",),
            ),
            clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        )


def test_access_request_builds_from_columnar_fast_path_options() -> None:
    request = ObjectStorageAccessRequest.from_options(
        {
            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
            "runtime_access": {
                "connection_type": "airflow",
                "connection_id": "s3_dpone_stage_writer",
                "required_permissions": ["put_object", "get_object"],
            },
            "clickhouse_read_access": {
                "mode": "named_collection",
                "named_collection": "dpone_stage",
            },
            "preflight": {
                "require_cluster_read": True,
                "sentinel_format": "parquet",
            },
        },
        columnar_pull={"use_cluster_function": "s3Cluster", "cluster": "dwh"},
    )

    assert request.runtime_access.connection_id == "s3_dpone_stage_writer"
    assert request.clickhouse_read_access.named_collection == "dpone_stage"
    assert request.require_cluster_read is True
    assert request.use_cluster_function == "s3Cluster"
    assert request.cluster == "dwh"


def test_access_request_accepts_connection_ref_alias() -> None:
    request = ObjectStorageAccessRequest.from_options(
        {
            "uri_prefix": "s3://dpone-stage/msql/{run_id}/",
            "runtime_access": {
                "connection_type": "airflow",
                "connection_ref": "s3_dpone_stage_writer",
            },
            "clickhouse_read_access": {
                "mode": "named_collection",
                "named_collection": "dpone_stage",
            },
        }
    )

    assert request.runtime_access.connection_id == "s3_dpone_stage_writer"


def test_preflight_rejects_bucket_root_prefix(tmp_path: Path) -> None:
    request = ObjectStorageAccessRequest(
        uri_prefix="s3://dpone-stage/",
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer"),
            required_permissions=("put_object", "get_object", "list_prefix"),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
    )

    evidence = ObjectStorageAccessPreflightService(
        object_client=LocalObjectStorageClient(root_dir=tmp_path / "unused"),
        clickhouse_probe=_NoopClickHouseProbe(),
    ).run(request, run_id="run-1")

    assert evidence.passed is False
    assert "object_storage_prefix_not_run_scoped" in evidence.blockers


def test_preflight_checks_runtime_and_clickhouse_access(tmp_path: Path) -> None:
    probe = _RecordingClickHouseProbe()
    service = ObjectStorageAccessPreflightService(
        object_client=LocalObjectStorageClient(root_dir=tmp_path / "store"),
        clickhouse_probe=probe,
    )
    request = ObjectStorageAccessRequest(
        uri_prefix="s3://dpone-stage/msql/{run_id}/",
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer"),
            required_permissions=("put_object", "get_object", "list_prefix", "delete_prefix"),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        require_clickhouse_read=True,
    )

    evidence = service.run(request, run_id="run-1")

    assert evidence.passed is True
    assert evidence.schema_version == "dpone.object_storage.access_preflight.v1"
    assert evidence.runtime_access.connection_id == "s3_writer"
    assert evidence.clickhouse_read_access.mode == "named_collection"
    assert evidence.clickhouse_probe_sql == (
        "SELECT count() FROM s3(dpone_stage, filename='msql/run-1/__dpone_sentinel.parquet')"
    )
    assert probe.sql == evidence.clickhouse_probe_sql
    assert not list((tmp_path / "store").rglob("__dpone_sentinel.parquet"))


def test_preflight_writes_valid_parquet_sentinel_when_parquet_is_requested(tmp_path: Path) -> None:
    parquet = pytest.importorskip("pyarrow.parquet")
    service = ObjectStorageAccessPreflightService(
        object_client=LocalObjectStorageClient(root_dir=tmp_path / "store"),
    )
    request = ObjectStorageAccessRequest(
        uri_prefix="s3://dpone-stage/msql/{run_id}/",
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer"),
            required_permissions=("put_object",),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        require_clickhouse_read=False,
        sentinel_format="parquet",
    )

    service.run(request, run_id="run-1")

    sentinel = tmp_path / "store" / "s3" / "dpone-stage" / "msql" / "run-1" / "__dpone_sentinel.parquet"
    table = parquet.read_table(sentinel)
    assert table.num_rows == 1
    assert table.column_names == ["dpone_sentinel"]


def test_named_collection_probe_sql_never_embeds_secret() -> None:
    contract = ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage")

    sql = ClickHouseObjectStorageReadinessProbe.render_sql(
        contract=contract,
        sentinel_uri=ObjectStorageUri.parse("s3://bucket/prefix/__dpone_sentinel.parquet"),
        use_cluster_function="s3",
        cluster=None,
    )

    assert sql == "SELECT count() FROM s3(dpone_stage, filename='prefix/__dpone_sentinel.parquet')"
    assert "secret" not in sql.lower()
    assert "password" not in sql.lower()


def test_clickhouse_probe_supports_dpone_connector_get_records_port() -> None:
    connector = _RecordsConnector()

    ClickHouseObjectStorageReadinessProbe(connector=connector).check(sql="SELECT 1")

    assert connector.queries == ["SELECT 1"]


def test_manifest_schema_documents_columnar_object_storage_access_contract() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for source_options in (
        config_schema["properties"]["source"]["properties"]["options"]["properties"],
        batch_schema["definitions"]["process_fragment"]["properties"]["source"]["properties"]["options"]["properties"],
    ):
        native_transfer = source_options["native_transfer"]["properties"]
        fast_path = native_transfer["snapshot"]["properties"]["columnar_fast_path"]["properties"]
        execution = fast_path["execution"]["properties"]
        object_storage = fast_path["object_storage"]["properties"]

        assert execution["mode"]["enum"] == ["chunked", "file", "streaming"]
        assert execution["target_chunk_bytes"]["default"] == "512MiB"
        assert execution["max_chunk_bytes"]["default"] == "1GiB"
        assert execution["max_inflight_chunks"]["default"] == 1
        assert object_storage["runtime_access"]["properties"]["connection_id"]["type"] == "string"
        assert object_storage["runtime_access"]["properties"]["connection_type"]["enum"] == [
            "airflow",
            "env",
            "params",
            "vault",
        ]
        assert object_storage["clickhouse_read_access"]["properties"]["mode"]["enum"] == [
            "named_collection",
            "connection",
            "presigned_url",
        ]
        assert "preflight" in object_storage


def test_manifest_schema_documents_clickhouse_columnar_pull_contract() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))

    for sink_options in (
        config_schema["properties"]["sink"]["properties"]["options"]["properties"],
        batch_schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"],
    ):
        columnar_pull = sink_options["clickhouse_bulk"]["properties"]["columnar_pull"]["properties"]
        assert columnar_pull["use_cluster_function"]["enum"] == ["auto", "s3", "s3Cluster"]
        assert columnar_pull["auth_mode"]["enum"] == ["named_collection", "connection", "presigned_url"]
        assert columnar_pull["settings"]["properties"]["input_format_parquet_allow_missing_columns"]["default"] is False


def test_object_storage_docs_cover_connection_and_preflight_contracts() -> None:
    text = Path("docs/object-storage-staging.md").read_text(encoding="utf-8")

    assert "runtime_access" in text
    assert "clickhouse_read_access" in text
    assert "connection_id: s3_dpone_stage_writer" in text
    assert "named_collection: dpone_stage" in text
    assert "No access keys, SAS tokens, passwords" in text
    assert "runtime:" in text
    assert "capabilities:" in text
    assert "sink.cluster_pull_unsupported" in text
    assert "feature checks such as `s3(...)`, `s3cluster(...)`, named collections and parquet" in text.lower()
    assert "dpone.runtime.route_capability_decision.v1" in text
    assert "etl_state.__dpone__load_steps" in text
    assert "route_capabilities.summary" in text


def test_runtime_permission_failures_have_stable_blocker_codes(tmp_path: Path) -> None:
    request = ObjectStorageAccessRequest(
        uri_prefix="s3://dpone-stage/msql/{run_id}/",
        runtime_access=ObjectStorageRuntimeAccess(
            connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer"),
            required_permissions=("put_object", "get_object", "list_prefix"),
        ),
        clickhouse_read_access=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        require_clickhouse_read=False,
    )

    evidence = ObjectStorageAccessPreflightService(
        object_client=_DenyPutObjectClient(),
        clickhouse_probe=_NoopClickHouseProbe(),
    ).run(request, run_id="run-1")

    assert evidence.passed is False
    assert evidence.blockers == ("object_storage_runtime_put_denied",)
    assert evidence.to_dict()["blockers"] == ["object_storage_runtime_put_denied"]


def test_s3_resolver_passes_airflow_aws_credentials_to_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: dict[str, object] = {}

    def fake_boto3_client(service_name: str, **kwargs: object):
        created["service_name"] = service_name
        created.update(kwargs)
        return _ListableFakeS3Client()

    monkeypatch.setitem(
        sys.modules,
        "boto3",
        types.SimpleNamespace(client=fake_boto3_client),
    )
    resolver = ObjectStorageConnectionResolver(
        credentials_manager=_StaticCredentialsManager(
            CredentialsConfig(
                endpoint="https://storage.example.com",
                username="access-key",
                password="secret-key",
                token="session-token",
                additional_params={"region_name": "ru-central1"},
            )
        )
    )

    client = resolver.build_client(
        ref=ObjectStorageConnectionRef(connection_type="airflow", connection_id="s3_writer"),
        uri=ObjectStorageUri.parse("s3://bucket-a/prefix/"),
    )

    assert created == {
        "service_name": "s3",
        "endpoint_url": "https://storage.example.com",
        "region_name": "ru-central1",
        "aws_access_key_id": "access-key",
        "aws_secret_access_key": "secret-key",
        "aws_session_token": "session-token",
    }
    assert client.list_prefix(ObjectStorageUri.parse("s3://bucket-a/prefix/")) == (
        "s3://bucket-a/prefix/chunk-000.parquet",
    )


def test_connection_mode_redacts_credentials_in_evidence(tmp_path: Path) -> None:
    contract = ObjectStorageReadContract(
        mode="connection",
        connection=ObjectStorageConnectionRef(
            connection_type="params",
            connection_id='{"aws_access_key_id":"AKIA","aws_secret_access_key":"secret-value"}',
        ),
    )

    evidence = ObjectStorageAccessPreflightService(
        object_client=LocalObjectStorageClient(root_dir=tmp_path / "unused"),
        clickhouse_probe=_NoopClickHouseProbe(),
    ).run(
        ObjectStorageAccessRequest(
            uri_prefix="s3://dpone-stage/msql/{run_id}/",
            runtime_access=ObjectStorageRuntimeAccess(
                connection=ObjectStorageConnectionRef(connection_type="env", connection_id="s3_writer")
            ),
            clickhouse_read_access=contract,
            require_runtime_write=False,
            require_clickhouse_read=False,
        ),
        run_id="run-1",
    )

    serialized = str(evidence.to_dict())
    assert "secret-value" not in serialized
    assert "<redacted>" in serialized


class _NoopClickHouseProbe:
    def check(self, *, sql: str) -> None:
        del sql


class _RecordingClickHouseProbe:
    sql: str | None = None

    def check(self, *, sql: str) -> None:
        self.sql = sql


class _RecordsConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str):
        self.queries.append(query)
        return [(1,)]


class _DenyPutObjectClient:
    def put_file(self, *args, **kwargs):
        del args, kwargs
        raise PermissionError("put denied")

    def get_file(self, *args, **kwargs) -> None:
        del args, kwargs

    def delete_prefix(self, *args, **kwargs) -> int:
        del args, kwargs
        return 0

    def exists(self, *args, **kwargs) -> bool:
        del args, kwargs
        return False

    def list_prefix(self, *args, **kwargs) -> tuple[str, ...]:
        del args, kwargs
        return ()


class _StaticCredentialsManager:
    def __init__(self, credentials: CredentialsConfig) -> None:
        self._credentials = credentials

    def get_credentials(self, *args, **kwargs) -> CredentialsConfig:
        del args, kwargs
        return self._credentials


class _ListableFakeS3Client:
    def get_paginator(self, name: str):
        assert name == "list_objects_v2"
        return self

    def paginate(self, *, Bucket: str, Prefix: str):
        assert Bucket == "bucket-a"
        assert Prefix == "prefix/"
        return [{"Contents": [{"Key": "prefix/chunk-000.parquet"}]}]
