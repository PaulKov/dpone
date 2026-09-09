"""ClickHouse object storage read preflight."""

from __future__ import annotations

from typing import Any

from dpone.runtime.object_storage_access_models import ObjectStorageReadContract
from dpone.storage import ObjectStorageUri


class ClickHouseObjectStorageReadinessProbe:
    def __init__(self, *, connector: Any | None = None) -> None:
        self._connector = connector

    def check(self, *, sql: str) -> None:
        if self._connector is None:
            return
        if hasattr(self._connector, "execute"):
            self._connector.execute(sql)
            return
        if hasattr(self._connector, "query"):
            self._connector.query(sql)
            return
        if hasattr(self._connector, "get_records"):
            self._connector.get_records(sql)
            return
        if hasattr(self._connector, "execute_query"):
            self._connector.execute_query(sql)
            return
        raise TypeError("ClickHouse connector must expose execute/query/get_records/execute_query")

    @staticmethod
    def render_sql(
        *,
        contract: ObjectStorageReadContract,
        sentinel_uri: ObjectStorageUri,
        use_cluster_function: str,
        cluster: str | None,
    ) -> str:
        function = _resolve_table_function(use_cluster_function, cluster)
        if contract.mode == "named_collection":
            return _render_named_collection(contract, sentinel_uri, function, cluster)
        uri = str(sentinel_uri).replace("'", "''")
        fmt = "Parquet" if sentinel_uri.key.lower().endswith(".parquet") else "RawBLOB"
        if contract.mode == "presigned_url":
            return f"SELECT count() FROM url('{uri}', '{fmt}')"
        if function == "s3Cluster":
            return f"SELECT count() FROM s3Cluster('{cluster}', '{uri}', '<redacted>', '<redacted>', '{fmt}')"
        return f"SELECT count() FROM s3('{uri}', '<redacted>', '<redacted>', '{fmt}')"


def clickhouse_blocker(contract: ObjectStorageReadContract) -> str:
    if contract.mode == "named_collection":
        return "clickhouse_named_collection_missing"
    return "clickhouse_object_storage_read_denied"


def _render_named_collection(
    contract: ObjectStorageReadContract,
    sentinel_uri: ObjectStorageUri,
    function: str,
    cluster: str | None,
) -> str:
    key = sentinel_uri.key.replace("'", "''")
    if function == "s3Cluster":
        return f"SELECT count() FROM s3Cluster('{cluster}', {contract.named_collection}, filename='{key}')"
    return f"SELECT count() FROM s3({contract.named_collection}, filename='{key}')"


def _resolve_table_function(use_cluster_function: str, cluster: str | None) -> str:
    mode = use_cluster_function.strip()
    if mode == "s3Cluster" or (mode == "auto" and cluster):
        return "s3Cluster"
    return "s3"
