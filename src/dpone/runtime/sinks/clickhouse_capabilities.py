"""ClickHouse route capability probes for columnar object-storage pull."""

from __future__ import annotations

from typing import Any

from dpone.runtime.object_storage_access_models import ObjectStorageReadContract
from dpone.runtime.object_storage_clickhouse_probe import ClickHouseObjectStorageReadinessProbe
from dpone.runtime.route_capabilities import CapabilityEvidence, RuntimeCapability
from dpone.storage import ObjectStorageUri

REQ_S3 = "sink.object_storage_pull.s3"
REQ_S3_CLUSTER = "sink.object_storage_pull.s3_cluster"
REQ_NAMED_COLLECTION = "sink.auth.named_collection"
REQ_PARQUET = "format.parquet.read"
REQ_PARQUET_MISSING_COLUMNS = "sink.setting.input_format_parquet_allow_missing_columns"


class ClickHouseColumnarCapabilityProbe:
    def __init__(self, *, connector: Any) -> None:
        self._connector = connector

    def probe_columnar_pull(
        self,
        *,
        read_contract: ObjectStorageReadContract,
        sentinel_uri: ObjectStorageUri,
        use_cluster_function: str,
        cluster: str | None,
        settings: dict[str, object] | None = None,
    ) -> dict[str, CapabilityEvidence]:
        version = self._server_version()
        s3_sql = _render_read_sql(read_contract, sentinel_uri, "s3", None)
        cluster_sql = _render_read_sql(read_contract, sentinel_uri, "s3Cluster", cluster) if cluster else None
        s3 = self._probe_sql(
            REQ_S3, RuntimeCapability.TRANSPORT, s3_sql, version, "sink.object_storage_pull.s3_missing"
        )
        s3_cluster = (
            self._probe_sql(
                REQ_S3_CLUSTER,
                RuntimeCapability.CLUSTER,
                cluster_sql,
                version,
                "sink.cluster_pull_unsupported",
                alternatives=("Use single-node object-storage pull or a streaming fallback.",),
            )
            if cluster_sql
            else CapabilityEvidence.failure(
                requirement_id=REQ_S3_CLUSTER,
                domain=RuntimeCapability.CLUSTER,
                server_version=version,
                blockers=("sink.cluster_pull_unsupported",),
                alternatives=("Configure clickhouse_bulk.columnar_pull.cluster or use single-node s3 pull.",),
            )
        )
        named = self._named_collection_evidence(read_contract, s3_sql, version)
        parquet = _derived_parquet_evidence(version, s3, named)
        setting = self._setting_evidence(settings or {}, version)
        return {
            REQ_S3: s3,
            REQ_S3_CLUSTER: s3_cluster,
            REQ_NAMED_COLLECTION: named,
            REQ_PARQUET: parquet,
            REQ_PARQUET_MISSING_COLUMNS: setting,
        }

    def probe_direct_push(self) -> dict[str, CapabilityEvidence]:
        version = self._server_version()
        return {
            REQ_PARQUET: self._probe_sql(
                REQ_PARQUET,
                RuntimeCapability.FILE_FORMAT,
                "SELECT 1 FORMAT Parquet",
                version,
                "format.parquet_read_unsupported",
                alternatives=("Use typed binary streaming or object-storage pull after fixing Parquet support.",),
            )
        }

    def probe_columnar_pull_from_preflight(
        self,
        *,
        read_contract: ObjectStorageReadContract,
        access_evidence: Any,
        cluster: str | None,
        require_cluster_read: bool,
        settings: dict[str, object] | None = None,
    ) -> dict[str, CapabilityEvidence]:
        version = self._server_version()
        if not bool(getattr(access_evidence, "passed", False)):
            return {}
        checks = tuple(getattr(access_evidence, "checks", ()))
        named = _preflight_success(REQ_NAMED_COLLECTION, RuntimeCapability.AUTH, version, checks)
        s3 = _preflight_success(REQ_S3, RuntimeCapability.TRANSPORT, version, checks)
        s3_cluster = (
            _preflight_success(REQ_S3_CLUSTER, RuntimeCapability.CLUSTER, version, checks)
            if cluster and require_cluster_read
            else CapabilityEvidence.failure(
                requirement_id=REQ_S3_CLUSTER,
                domain=RuntimeCapability.CLUSTER,
                server_version=version,
                blockers=("sink.cluster_pull_unsupported",),
                alternatives=("Configure clickhouse_bulk.columnar_pull.cluster or use single-node s3 pull.",),
            )
        )
        parquet = CapabilityEvidence.success(
            requirement_id=REQ_PARQUET,
            domain=RuntimeCapability.FILE_FORMAT,
            server_version=version,
            checks=("object_storage_preflight_parquet_read",),
        )
        setting = self._setting_evidence(settings or {}, version)
        if read_contract.mode != "named_collection":
            named = CapabilityEvidence.failure(
                requirement_id=REQ_NAMED_COLLECTION,
                domain=RuntimeCapability.AUTH,
                server_version=version,
                blockers=("sink.auth.named_collection_missing",),
            )
        return {
            REQ_S3: s3,
            REQ_S3_CLUSTER: s3_cluster,
            REQ_NAMED_COLLECTION: named,
            REQ_PARQUET: parquet,
            REQ_PARQUET_MISSING_COLUMNS: setting,
        }

    def _server_version(self) -> str | None:
        if not hasattr(self._connector, "get_records"):
            return None
        try:
            rows = self._connector.get_records("SELECT version()")
        except Exception:
            return None
        if not rows:
            return None
        first = rows[0]
        return str(first[0] if isinstance(first, (list, tuple)) else first)

    def _named_collection_evidence(
        self,
        contract: ObjectStorageReadContract,
        sql: str,
        version: str | None,
    ) -> CapabilityEvidence:
        if contract.mode != "named_collection":
            return CapabilityEvidence.failure(
                requirement_id=REQ_NAMED_COLLECTION,
                domain=RuntimeCapability.AUTH,
                server_version=version,
                blockers=("sink.auth.named_collection_missing",),
                alternatives=(
                    "Create the required named collection or allow a non-production credential mode explicitly.",
                ),
            )
        return self._probe_sql(
            REQ_NAMED_COLLECTION,
            RuntimeCapability.AUTH,
            sql,
            version,
            "sink.auth.named_collection_missing",
            alternatives=(
                "Create the required named collection or allow a non-production credential mode explicitly.",
            ),
        )

    def _setting_evidence(self, settings: dict[str, object], version: str | None) -> CapabilityEvidence:
        if "input_format_parquet_allow_missing_columns" not in settings:
            return CapabilityEvidence.success(
                requirement_id=REQ_PARQUET_MISSING_COLUMNS,
                domain=RuntimeCapability.SINK_STAGE,
                server_version=version,
                checks=("setting_not_requested",),
            )
        value = 1 if bool(settings["input_format_parquet_allow_missing_columns"]) else 0
        return self._probe_sql(
            REQ_PARQUET_MISSING_COLUMNS,
            RuntimeCapability.SINK_STAGE,
            f"SELECT 1 SETTINGS input_format_parquet_allow_missing_columns = {value}",
            version,
            "sink.setting.input_format_parquet_allow_missing_columns_unsupported",
        )

    def _probe_sql(
        self,
        requirement_id: str,
        domain: RuntimeCapability,
        sql: str,
        version: str | None,
        blocker: str,
        alternatives: tuple[str, ...] = (),
    ) -> CapabilityEvidence:
        try:
            _execute(self._connector, sql)
        except Exception:
            return CapabilityEvidence.failure(
                requirement_id=requirement_id,
                domain=domain,
                server_version=version,
                blockers=(blocker,),
                alternatives=alternatives,
                details={"sql": _redact_sql(sql)},
            )
        return CapabilityEvidence.success(
            requirement_id=requirement_id,
            domain=domain,
            server_version=version,
            checks=("sql_probe",),
            details={"sql": _redact_sql(sql)},
        )


def _derived_parquet_evidence(
    version: str | None,
    s3: CapabilityEvidence,
    named: CapabilityEvidence,
) -> CapabilityEvidence:
    if s3.passed and named.passed:
        return CapabilityEvidence.success(
            requirement_id=REQ_PARQUET,
            domain=RuntimeCapability.FILE_FORMAT,
            server_version=version,
            checks=("parquet_sentinel_read",),
        )
    blockers = tuple(dict.fromkeys((*s3.blockers, *named.blockers, "format.parquet_read_unsupported")))
    return CapabilityEvidence.failure(
        requirement_id=REQ_PARQUET,
        domain=RuntimeCapability.FILE_FORMAT,
        server_version=version,
        blockers=blockers,
    )


def _preflight_success(
    requirement_id: str,
    domain: RuntimeCapability,
    version: str | None,
    checks: tuple[str, ...],
) -> CapabilityEvidence:
    return CapabilityEvidence.success(
        requirement_id=requirement_id,
        domain=domain,
        server_version=version,
        checks=("object_storage_preflight_clickhouse_read", *checks),
    )


def _render_read_sql(
    contract: ObjectStorageReadContract,
    sentinel_uri: ObjectStorageUri,
    use_cluster_function: str,
    cluster: str | None,
) -> str:
    return ClickHouseObjectStorageReadinessProbe.render_sql(
        contract=contract,
        sentinel_uri=sentinel_uri,
        use_cluster_function=use_cluster_function,
        cluster=cluster,
    )


def _execute(connector: Any, sql: str) -> None:
    if hasattr(connector, "execute_query"):
        connector.execute_query(sql)
        return
    if hasattr(connector, "execute"):
        connector.execute(sql)
        return
    if hasattr(connector, "query"):
        connector.query(sql)
        return
    raise TypeError("ClickHouse connector must expose execute_query(sql), execute(sql), or query(sql)")


def _redact_sql(sql: str) -> str:
    return sql.replace("password", "<redacted>").replace("secret", "<redacted>")


__all__ = [
    "ClickHouseColumnarCapabilityProbe",
    "REQ_NAMED_COLLECTION",
    "REQ_PARQUET",
    "REQ_PARQUET_MISSING_COLUMNS",
    "REQ_S3",
    "REQ_S3_CLUSTER",
]
