"""Runtime and ClickHouse object storage access preflight."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Protocol

from dpone.runtime.object_storage_access_models import (
    ObjectStorageAccessEvidence,
    ObjectStorageAccessRequest,
    ObjectStorageRuntimeAccess,
)
from dpone.runtime.object_storage_clickhouse_probe import (
    ClickHouseObjectStorageReadinessProbe,
    clickhouse_blocker,
)
from dpone.runtime.object_storage_retention import ObjectStorageBudgetProbe, ObjectStorageBudgetResult
from dpone.storage import ObjectStorageUri
from dpone.storage.protocols import ObjectStorageClient

SENTINEL_NAME = "__dpone_sentinel.parquet"


class ClickHouseProbe(Protocol):
    def check(self, *, sql: str) -> None:
        """Execute a readiness SQL statement or raise on failure."""


class ObjectStorageAccessPreflightService:
    def __init__(self, *, object_client: ObjectStorageClient, clickhouse_probe: ClickHouseProbe | None = None) -> None:
        self._object_client = object_client
        self._clickhouse_probe = clickhouse_probe or ClickHouseObjectStorageReadinessProbe()

    def run(self, request: ObjectStorageAccessRequest, *, run_id: str) -> ObjectStorageAccessEvidence:
        prefix = request.resolved_prefix(run_id)
        sentinel_uri = prefix.child(SENTINEL_NAME)
        checks: list[str] = []
        warnings: list[str] = []
        blockers: list[str] = []
        probe_sql: str | None = None
        budget_result: ObjectStorageBudgetResult | None = None

        if _is_unsafe_prefix(request.uri_prefix, prefix):
            blockers.append("object_storage_prefix_not_run_scoped")
            return _evidence(request, sentinel_uri, probe_sql, checks, warnings, blockers, budget_result)

        if request.retention_policy.enabled:
            budget = ObjectStorageBudgetProbe(self._object_client).check(
                _budget_prefix(request.uri_prefix, prefix),
                policy=request.retention_policy,
            )
            budget_result = budget
            checks.append("object_storage_budget_guard")
            warnings.extend(budget.warnings)
            blockers.extend(budget.blockers)
            if blockers:
                return _evidence(request, sentinel_uri, probe_sql, checks, warnings, blockers, budget_result)

        if request.require_runtime_write:
            _run_runtime_probe(
                self._object_client,
                request.runtime_access,
                sentinel_uri,
                request.sentinel_format,
                checks,
                blockers,
            )

        if request.require_clickhouse_read and not blockers:
            probe_sql = ClickHouseObjectStorageReadinessProbe.render_sql(
                contract=request.clickhouse_read_access,
                sentinel_uri=sentinel_uri,
                use_cluster_function=request.use_cluster_function,
                cluster=request.cluster if request.require_cluster_read else None,
            )
            try:
                self._clickhouse_probe.check(sql=probe_sql)
                checks.append("clickhouse_read")
            except Exception:
                blockers.append(clickhouse_blocker(request.clickhouse_read_access))

        if request.runtime_access.requires("delete_prefix"):
            try:
                self._object_client.delete_prefix(prefix)
                checks.append("runtime_delete_prefix")
            except Exception:
                blockers.append("object_storage_runtime_delete_denied")

        return _evidence(request, sentinel_uri, probe_sql, checks, warnings, blockers, budget_result)


def _run_runtime_probe(
    client: ObjectStorageClient,
    access: ObjectStorageRuntimeAccess,
    sentinel_uri: ObjectStorageUri,
    sentinel_format: str,
    checks: list[str],
    blockers: list[str],
) -> None:
    with tempfile.TemporaryDirectory(prefix="dpone-object-storage-preflight-") as tmp:
        source = Path(tmp) / SENTINEL_NAME
        downloaded = Path(tmp) / "downloaded.parquet"
        if not _write_sentinel(source, sentinel_format, blockers):
            return
        try:
            client.put_file(source, sentinel_uri, content_type="application/octet-stream")
            checks.append("runtime_put_object")
        except Exception:
            blockers.append("object_storage_runtime_put_denied")
            return
        _check_get(client, access, sentinel_uri, downloaded, checks, blockers)
        _check_list(client, access, sentinel_uri, checks, blockers)


def _write_sentinel(path: Path, sentinel_format: str, blockers: list[str]) -> bool:
    if sentinel_format.strip().lower() != "parquet":
        path.write_bytes(b"dpone sentinel\n")
        return True
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except Exception:
        blockers.append("object_storage_sentinel_parquet_writer_missing")
        return False
    pq.write_table(pa.table({"dpone_sentinel": pa.array([1], type=pa.int8())}), path)
    return True


def _check_get(
    client: ObjectStorageClient,
    access: ObjectStorageRuntimeAccess,
    sentinel_uri: ObjectStorageUri,
    downloaded: Path,
    checks: list[str],
    blockers: list[str],
) -> None:
    if not access.requires("get_object"):
        return
    try:
        client.get_file(sentinel_uri, downloaded)
        checks.append("runtime_get_object")
    except Exception:
        blockers.append("object_storage_runtime_get_denied")


def _check_list(
    client: ObjectStorageClient,
    access: ObjectStorageRuntimeAccess,
    sentinel_uri: ObjectStorageUri,
    checks: list[str],
    blockers: list[str],
) -> None:
    if not access.requires("list_prefix"):
        return
    try:
        if str(sentinel_uri) not in client.list_prefix(_parent_prefix(sentinel_uri)):
            blockers.append("object_storage_runtime_list_missing_sentinel")
        else:
            checks.append("runtime_list_prefix")
    except Exception:
        blockers.append("object_storage_runtime_list_denied")


def _evidence(
    request: ObjectStorageAccessRequest,
    sentinel_uri: ObjectStorageUri | None,
    probe_sql: str | None,
    checks: list[str],
    warnings: list[str],
    blockers: list[str],
    budget_result: ObjectStorageBudgetResult | None = None,
) -> ObjectStorageAccessEvidence:
    return ObjectStorageAccessEvidence(
        passed=not blockers,
        runtime_access=request.runtime_access,
        clickhouse_read_access=request.clickhouse_read_access,
        uri_prefix=request.uri_prefix,
        sentinel_uri=str(sentinel_uri) if sentinel_uri else None,
        clickhouse_probe_sql=probe_sql,
        checks=tuple(checks),
        warnings=tuple(warnings),
        blockers=tuple(dict.fromkeys(blockers)),
        retention_policy=request.retention_policy if request.retention_policy.enabled else None,
        budget_result=budget_result,
    )


def _is_unsafe_prefix(raw_prefix: str, prefix: ObjectStorageUri) -> bool:
    key_parts = [part for part in prefix.key.split("/") if part]
    return not key_parts or ("{run_id}" not in raw_prefix and len(key_parts) < 2)


def _parent_prefix(uri: ObjectStorageUri) -> ObjectStorageUri:
    parent, _, _ = uri.key.rstrip("/").rpartition("/")
    return ObjectStorageUri(uri.provider, uri.bucket, parent, account=uri.account).prefix()


def _budget_prefix(raw_prefix: str, resolved_prefix: ObjectStorageUri) -> ObjectStorageUri:
    if "{run_id}" not in raw_prefix:
        return resolved_prefix
    before_run, _, _ = raw_prefix.partition("{run_id}")
    return ObjectStorageUri.parse(before_run).prefix()
