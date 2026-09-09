"""Evidence writer and verifier for the Parquet/S3 ClickHouse route."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any

from dpone.storage import ObjectStorageUri
from local_route_certification_receipt import (
    LocalRouteExpectedSubject,
    LocalRouteIdentity,
    LocalSourceSnapshot,
    capture_local_source_snapshot,
    verify_local_route_certification_receipt,
    write_local_route_certification_receipt,
)
from mssql_clickhouse_target_schema import ClickHouseTargetSchemaMetrics
from mssql_dbt_wide_evidence import (
    clickhouse_connection_sha256,
    mssql_connection_sha256,
    object_storage_authority_sha256,
)


def write_evidence(output_dir: Path, result: Any) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "mssql_clickhouse_parquet_s3_type_certification.json"
    with json_path.open("x", encoding="utf-8") as stream:
        json.dump(asdict(result), stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    with (output_dir / "mssql_clickhouse_parquet_s3_type_certification.md").open("x", encoding="utf-8") as stream:
        stream.write(
            "# MSSQL → Parquet/S3 → ClickHouse certification\n\n"
            f"- Passed: `{str(result.passed).lower()}`\n"
            f"- Rows: `{result.source_count}` → `{result.target_count}`\n"
            f"- Columns: `{result.column_count}`\n"
            f"- Parquet chunks/bytes: `{result.parquet_chunks}` / `{result.parquet_bytes}`\n"
            f"- Duplicate keys: `{result.duplicate_count}`\n"
            f"- Typed hash equal: `{result.typed_hash_source == result.typed_hash_target}`\n"
            f"- Cleanup verified: `{result.cleanup_verified}`\n"
            f"- Failed phase: `{result.failed_phase or ''}`\n"
        )
    return json_path


def write_route_receipt(
    config: Any,
    result: Any,
    result_path: Path,
    source_snapshot: LocalSourceSnapshot,
    target_schema: ClickHouseTargetSchemaMetrics | None,
) -> None:
    prefix = _resolved_prefix(config)
    s3_authority = object_storage_authority_sha256(
        endpoint_url=str(config.s3_params.get("endpoint_url") or ""),
        region_name=str(config.s3_params.get("region_name") or ""),
        bucket=prefix.bucket,
        named_collection=config.named_collection,
    )
    written = write_local_route_certification_receipt(
        output_dir=config.output_dir,
        source_snapshot=source_snapshot,
        final_source_snapshot=capture_local_source_snapshot(),
        result_path=result_path,
        release_id=config.release_id,
        route=LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
        source_relation=f"{config.source_schema}.{config.source_table}",
        target_relation=f"{config.target_database}.{config.target_table}",
        transport="parquet_s3_pull",
        transport_details={
            "hashed_rows": config.typed_hash_rows,
            "s3_endpoint_sha256": s3_authority,
            "bucket": prefix.bucket,
            "object_prefix": str(prefix),
            "named_collection": config.named_collection,
            "parquet_chunks": result.parquet_chunks,
            "parquet_bytes": result.parquet_bytes,
        },
        binary_format="parquet",
        requested_acceleration_mode=None,
        runtime_decisions=(),
        mssql_connection_sha256=mssql_connection_sha256(config.mssql_params),
        clickhouse_connection_sha256=clickhouse_connection_sha256(config.clickhouse_params),
        target_schema_sha256=target_schema.observed_sha256 if target_schema is not None else None,
        upstream_evidence_path=config.upstream_evidence,
        cleanup_verified=result.cleanup_verified,
        remaining_artifacts=() if result.cleanup_verified else (str(prefix),),
        passed=result.passed,
    )
    payload = json.loads(written.path.read_text(encoding="utf-8"))
    if payload.get("evidence_status") != "PASS":
        return
    if target_schema is None:
        raise ValueError("local_route_certification_target_schema_missing")
    verify_local_route_certification_receipt(
        written.path,
        source_snapshot=source_snapshot,
        expected=LocalRouteExpectedSubject(
            release_id=config.release_id,
            route=LocalRouteIdentity(source="mssql", sink="clickhouse", strategy="full_refresh"),
            source_relation=f"{config.source_schema}.{config.source_table}",
            target_relation=f"{config.target_database}.{config.target_table}",
            transport="parquet_s3_pull",
            binary_format="parquet",
            requested_acceleration_mode=None,
            expected_rows=config.rows,
            expected_column_count=config.column_count,
            mssql_connection_sha256=mssql_connection_sha256(config.mssql_params),
            clickhouse_connection_sha256=clickhouse_connection_sha256(config.clickhouse_params),
            target_schema_sha256=target_schema.expected_sha256,
            s3_endpoint_sha256=s3_authority,
            s3_bucket=prefix.bucket,
            s3_named_collection=config.named_collection,
        ),
    )


def _resolved_prefix(config: Any) -> ObjectStorageUri:
    return ObjectStorageUri.parse(config.object_prefix.format(run_id=config.run_id)).prefix()


__all__ = ["write_evidence", "write_route_receipt"]
