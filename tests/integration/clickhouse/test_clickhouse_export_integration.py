from __future__ import annotations

import os
import uuid

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_clickhouse,
    pytest.mark.integration_extended,
    pytest.mark.nightly,
]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)
if str(os.getenv("DPONE_RUN_INTEGRATION_EXTENDED", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Extended integration tests are disabled", allow_module_level=True)
pytest.importorskip("clickhouse_driver")
pytest.importorskip("minio")


def _table_name(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def _remove_prefix(minio_client, bucket: str, prefix: str) -> None:
    objects = [obj.object_name for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True)]
    for object_name in objects:
        minio_client.remove_object(bucket, object_name)


def _list_prefix(minio_client, bucket: str, prefix: str) -> list[str]:
    return sorted(obj.object_name for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True))


def _patch_gcs_helpers(monkeypatch: pytest.MonkeyPatch, minio_settings) -> None:
    import dpone.runtime.support.gcs as gcs_mod

    internal_root = minio_settings.internal_url.rstrip("/")

    def fake_gs_to_https(uri: str) -> str:
        assert uri.startswith("gs://")
        bucket_and_path = uri[len("gs://") :].strip("/")
        return f"{internal_root}/{bucket_and_path}"

    monkeypatch.setattr(gcs_mod, "gs_to_https", fake_gs_to_https)
    monkeypatch.setattr(gcs_mod, "normalize_gcs_path", lambda path: path.rstrip("/"))
    monkeypatch.setattr(
        gcs_mod,
        "get_gcs_hmac_credentials",
        lambda **kwargs: (minio_settings.access_key, minio_settings.secret_key),
    )


def test_clickhouse_export_to_minio_and_read_back(
    clickhouse_connector,
    clickhouse_settings,
    minio_client,
    minio_settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table_name("it_export")
    db = clickhouse_settings.database
    prefix = f"it-exports/{uuid.uuid4().hex[:10]}"
    gs_uri = f"gs://{minio_settings.bucket}/{prefix}"
    _remove_prefix(minio_client, minio_settings.bucket, prefix)
    _patch_gcs_helpers(monkeypatch, minio_settings)

    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (id Int32, city String) ENGINE = MergeTree ORDER BY id"
    )
    try:
        clickhouse_connector.execute_query(
            f"INSERT INTO `{db}`.`{table}` (id, city) VALUES (1, 'Paris'), (2, 'Berlin')"
        )

        clickhouse_connector.export_to_gcs(
            f"SELECT id, city FROM `{db}`.`{table}` ORDER BY id",
            gs_uri,
            format="csv",
            file_prefix="data",
        )

        exported = _list_prefix(minio_client, minio_settings.bucket, prefix)
        assert exported, "expected exported objects in MinIO"

        internal_urls = [
            f"{minio_settings.internal_url.rstrip('/')}/{minio_settings.bucket}/{name}" for name in exported
        ]
        rows = clickhouse_connector.get_records(
            f"SELECT id, city FROM s3('{internal_urls[0]}', '{minio_settings.access_key}', '{minio_settings.secret_key}', 'CSVWithNames') ORDER BY id",
            as_dict=True,
        )
        assert rows == [{"id": 1, "city": "Paris"}, {"id": 2, "city": "Berlin"}]
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        _remove_prefix(minio_client, minio_settings.bucket, prefix)


def test_clickhouse_partition_export_to_minio(
    clickhouse_connector,
    clickhouse_settings,
    minio_client,
    minio_settings,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    table = _table_name("it_export_parts")
    db = clickhouse_settings.database
    prefix = f"it-partitions/{uuid.uuid4().hex[:10]}"
    gs_uri = f"gs://{minio_settings.bucket}/{prefix}"
    _remove_prefix(minio_client, minio_settings.bucket, prefix)
    _patch_gcs_helpers(monkeypatch, minio_settings)

    clickhouse_connector.execute_query(
        f"CREATE TABLE `{db}`.`{table}` (id Int32, event_date Date, city String) ENGINE = MergeTree ORDER BY (event_date, id)"
    )
    try:
        clickhouse_connector.execute_query(
            f"INSERT INTO `{db}`.`{table}` (id, event_date, city) VALUES "
            "(1, '2026-03-01', 'Paris'),"
            "(2, '2026-03-01', 'Berlin'),"
            "(3, '2026-03-02', 'Rome')"
        )

        partitions = [
            ("2026-03-01", "toDate(`event_date`) = '2026-03-01'"),
            ("2026-03-02", "toDate(`event_date`) = '2026-03-02'"),
        ]
        exported = clickhouse_connector.export_partitions(
            query=f"SELECT id, event_date, city FROM `{db}`.`{table}`",
            partitions=partitions,
            gcs_base_uri=gs_uri,
            format="csv",
            file_prefix="part",
        )
        assert exported == ["2026-03-01", "2026-03-02"]

        objects = _list_prefix(minio_client, minio_settings.bucket, prefix)
        assert any("dt_date=2026-03-01" in name for name in objects)
        assert any("dt_date=2026-03-02" in name for name in objects)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{db}`.`{table}`")
        _remove_prefix(minio_client, minio_settings.bucket, prefix)
