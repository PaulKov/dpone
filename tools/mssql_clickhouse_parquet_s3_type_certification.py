#!/usr/bin/env python3
"""Certify an existing wide MSSQL relation through Parquet/S3 pull into ClickHouse."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy
from dpone.runtime.sources.strategies.mssql.mssql_columnar_provider import MssqlColumnarSnapshotProvider
from dpone.runtime.sources.strategies.mssql.mssql_queryout_artifacts import MSSQLQueryoutArtifactFactory
from dpone.storage import ObjectStorageUri, S3ObjectStorageClient

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as base  # noqa: E402
from local_route_certification_receipt import (  # noqa: E402
    capture_local_source_snapshot,
    require_exact_upstream_evidence,
)
from local_route_certification_validation import prepare_create_once_directory  # noqa: E402
from mssql_clickhouse_parquet_s3_evidence import write_evidence, write_route_receipt  # noqa: E402
from mssql_clickhouse_target_schema import (  # noqa: E402
    clickhouse_target_schema_metrics,
)
from mssql_dbt_wide_evidence import (  # noqa: E402
    mssql_connection_sha256,
)


@dataclass(frozen=True, slots=True)
class ParquetS3CertificationConfig:
    """Inputs for one real Parquet object-storage pull certification."""

    rows: int
    release_id: str
    column_count: int
    source_schema: str
    source_table: str
    target_database: str
    target_table: str
    output_dir: Path
    mssql_params: dict[str, Any]
    clickhouse_params: dict[str, Any]
    s3_params: dict[str, Any]
    object_prefix: str
    named_collection: str
    run_id: str
    batch_size: int
    target_chunk_bytes: int
    max_chunk_bytes: int
    typed_hash_rows: int = 10_000
    target_binary_representation: str = "raw"
    upstream_evidence: Path | None = None


@dataclass(frozen=True, slots=True)
class ParquetS3CertificationResult:
    """Closed evidence for one dbt/MSSQL -> Parquet/S3 -> ClickHouse run."""

    rows: int
    column_count: int
    source_count: int
    target_count: int
    duplicate_count: int
    typed_hash_source: str | None
    typed_hash_target: str | None
    parquet_chunks: int
    parquet_bytes: int
    object_prefix: str
    cleanup_verified: bool
    elapsed_seconds: float
    passed: bool
    failed_phase: str | None = None
    error: str | None = None
    schema_version: str = "dpone.mssql_clickhouse.parquet_s3_type_certification.v1"


def build_load_config(config: ParquetS3CertificationConfig) -> LoadConfig:
    """Build the runtime config emitted by the advanced self-service manifest."""

    columnar = {
        "mode": "required",
        "provider": "object_storage_pull",
        "execution": {
            "mode": "chunked",
            "target_chunk_bytes": config.target_chunk_bytes,
            "max_chunk_bytes": config.max_chunk_bytes,
            "max_inflight_chunks": 1,
            "cleanup_policy": "on_success",
        },
        "object_storage": {
            "uri_prefix": config.object_prefix,
            "format": "parquet",
            "compression": "zstd",
            "clickhouse_read_access": {
                "mode": "named_collection",
                "named_collection": config.named_collection,
            },
        },
    }
    return LoadConfig(
        source_conn_id="mssql-parquet-source",
        target_conn_id="clickhouse-parquet-sink",
        source_schema=config.source_schema,
        source_table=config.source_table,
        target_schema=config.target_database,
        target_table=config.target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        unique_key="order_id",
        batch_size=config.batch_size,
        options={
            "run_id": config.run_id,
            "source_type": "mssql",
            "sink_type": "clickhouse",
            "native_transfer": {"snapshot": {"columnar_fast_path": columnar}},
            "source_options": {"native_transfer": {"snapshot": {"columnar_fast_path": columnar}}},
            "clickhouse_bulk": {
                "ingest_contract": "columnar_staging",
                "columnar_pull": {
                    "use_cluster_function": "s3",
                    "auth_mode": "named_collection",
                    "settings": {"input_format_parquet_allow_missing_columns": False},
                },
            },
        },
    )


def run_live_certification(config: ParquetS3CertificationConfig) -> ParquetS3CertificationResult:
    """Run the production provider and sink, exact-reconcile, then clean the run prefix."""

    source_snapshot = capture_local_source_snapshot()
    if (
        not config.mssql_params.get("password")
        or not config.clickhouse_params.get("password")
        or not config.s3_params.get("access_key")
        or not config.s3_params.get("secret_key")
    ):
        raise ValueError("local_certification_connection_secrets_required")
    if config.upstream_evidence is None:
        raise ValueError("parquet_s3_certification_requires_upstream_evidence")
    if config.typed_hash_rows != config.rows:
        raise ValueError("wide_release_requires_full_typed_hash")
    require_exact_upstream_evidence(
        config.upstream_evidence,
        source_snapshot=source_snapshot,
        output_relation=f"{config.source_schema}.{config.source_table}",
        connection_sha256=mssql_connection_sha256(config.mssql_params),
        release_id=config.release_id,
        expected_rows=config.rows,
        expected_source_columns=config.column_count - 1,
        expected_target_columns=config.column_count,
    )
    prepare_create_once_directory(config.output_dir)
    base_config = cast(base.WideTypeCertificationConfig, config)
    mssql = base._mssql_connector(base_config)
    clickhouse = base._clickhouse_connector(base_config)
    object_client = _object_client(config)
    started = time.perf_counter()
    phase = "prepare_target"
    artifact: Any | None = None
    result: ParquetS3CertificationResult | None = None
    try:
        base._prepare_clickhouse_target(clickhouse, base_config)
        load_config = build_load_config(config)
        provider = MssqlColumnarSnapshotProvider(connector=mssql, object_client=object_client)
        artifacts = MSSQLQueryoutArtifactFactory(
            connector=mssql,
            logger=base._Logger(),
            sink_connector=clickhouse,
            columnar_snapshot_provider=provider,
        )
        source = MSSQLFullExtractStrategy(
            mssql,
            base._Logger(),
            sink_connector=clickhouse,
            queryout_artifacts=artifacts,
        )
        phase = "parquet_upload"
        extract = source.extract(load_config, None)
        artifact = extract.artifact
        parquet_chunks = len(tuple(getattr(artifact, "chunks", ())))
        parquet_bytes = int(getattr(artifact, "size_bytes", 0) or 0)
        if parquet_chunks <= 0 or parquet_bytes <= 0:
            raise RuntimeError("parquet_s3_artifact_empty")
        phase = "clickhouse_pull"
        ClickHouseSink(clickhouse, logger=base._Logger()).load(
            load_config,
            LoadPayload(artifact=artifact, schema=extract.schema),
        )
        phase = "exact_reconciliation"
        source_count = base._count_mssql(mssql, config.source_schema, config.source_table)
        target_count = base._count_clickhouse(clickhouse, config.target_database, config.target_table)
        duplicate_count = base._count_clickhouse_duplicates(clickhouse, config.target_database, config.target_table)
        source_hash, target_hash = base._typed_hashes(mssql, clickhouse, base_config)
        target_schema = clickhouse_target_schema_metrics(
            mssql=mssql,
            clickhouse=clickhouse,
            source_schema=config.source_schema,
            source_table=config.source_table,
            target_database=config.target_database,
            target_table=config.target_table,
            type_fidelity=load_config.options.get("type_fidelity"),
        )
        phase = "cleanup"
        artifact.cleanup()
        cleanup_verified = not object_client.list_prefix(_resolved_prefix(config))
        passed = (
            source_count == target_count == config.rows
            and duplicate_count == 0
            and source_hash is not None
            and source_hash == target_hash
            and target_schema.column_count == config.column_count
            and target_schema.mismatch_count == 0
            and target_schema.observed_sha256 == target_schema.expected_sha256
            and cleanup_verified
        )
        result = ParquetS3CertificationResult(
            rows=config.rows,
            column_count=config.column_count,
            source_count=source_count,
            target_count=target_count,
            duplicate_count=duplicate_count,
            typed_hash_source=source_hash,
            typed_hash_target=target_hash,
            parquet_chunks=parquet_chunks,
            parquet_bytes=parquet_bytes,
            object_prefix=str(_resolved_prefix(config)),
            cleanup_verified=cleanup_verified,
            elapsed_seconds=time.perf_counter() - started,
            passed=passed,
        )
        result_path = write_evidence(config.output_dir, result)
        write_route_receipt(config, result, result_path, source_snapshot, target_schema)
        return result
    except Exception:
        if artifact is not None:
            try:
                artifact.cleanup()
            except Exception:
                pass
        result = ParquetS3CertificationResult(
            rows=config.rows,
            column_count=config.column_count,
            source_count=base._safe_count_mssql(mssql, config.source_schema, config.source_table),
            target_count=base._safe_count_clickhouse(clickhouse, config.target_database, config.target_table),
            duplicate_count=base._safe_count_clickhouse_duplicates(
                clickhouse,
                config.target_database,
                config.target_table,
            ),
            typed_hash_source=None,
            typed_hash_target=None,
            parquet_chunks=len(tuple(getattr(artifact, "chunks", ()))) if artifact is not None else 0,
            parquet_bytes=int(getattr(artifact, "size_bytes", 0) or 0) if artifact is not None else 0,
            object_prefix=str(_resolved_prefix(config)),
            cleanup_verified=not object_client.list_prefix(_resolved_prefix(config)),
            elapsed_seconds=time.perf_counter() - started,
            passed=False,
            failed_phase=phase,
            error=f"{phase}_failed",
        )
        result_path = write_evidence(config.output_dir, result)
        write_route_receipt(config, result, result_path, source_snapshot, None)
        raise
    finally:
        base._close_quietly(clickhouse)
        base._close_quietly(mssql)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--column-count", type=int, required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-table", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output-dir", default="test_artifacts/live_certification/parquet_s3_wide_type_latest")
    parser.add_argument("--object-prefix", default="s3://dpone-stage/wide-dbt/{run_id}/")
    parser.add_argument("--named-collection", default="dpone_stage")
    parser.add_argument("--batch-size", type=int, default=5_000)
    parser.add_argument("--target-chunk-bytes", type=int, default=4 * 1024 * 1024)
    parser.add_argument("--max-chunk-bytes", type=int, default=16 * 1024 * 1024)
    parser.add_argument("--typed-hash-rows", type=int, default=10_000)
    parser.add_argument("--upstream-evidence", type=Path, required=True)
    _add_connection_args(parser)
    return parser.parse_args(argv)


def build_config(args: argparse.Namespace) -> ParquetS3CertificationConfig:
    return ParquetS3CertificationConfig(
        rows=args.rows,
        release_id=args.release_id,
        column_count=args.column_count,
        source_schema=args.source_schema,
        source_table=args.source_table,
        target_database=args.clickhouse_database,
        target_table=args.target_table,
        output_dir=Path(args.output_dir).expanduser().resolve(),
        mssql_params=base._mssql_params(args),
        clickhouse_params=base._clickhouse_params(args),
        s3_params={
            "endpoint_url": args.s3_endpoint,
            "region_name": args.s3_region,
            "access_key": args.s3_access_key,
            "secret_key": args.s3_secret_key,
        },
        object_prefix=args.object_prefix,
        named_collection=args.named_collection,
        run_id=args.run_id,
        batch_size=args.batch_size,
        target_chunk_bytes=args.target_chunk_bytes,
        max_chunk_bytes=args.max_chunk_bytes,
        typed_hash_rows=args.typed_hash_rows,
        upstream_evidence=args.upstream_evidence,
    )


def main(argv: list[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    result = run_live_certification(config)
    print(json.dumps(asdict(result), ensure_ascii=False, indent=2))
    receipt = json.loads((config.output_dir / "local_route_certification_receipt.json").read_text(encoding="utf-8"))
    return 0 if result.passed and receipt["evidence_status"] == "PASS" else 1


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--mssql-host", default=os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"))
    parser.add_argument(
        "--mssql-port",
        type=int,
        default=int(os.getenv("DPONE_IT_MSSQL_PORT", os.getenv("DPONE_IT_MSSQL_PORT_FORWARD", "51433"))),
    )
    parser.add_argument("--mssql-database", default=os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone_it"))
    parser.add_argument("--mssql-user", default=os.getenv("DPONE_IT_MSSQL_USER", "sa"))
    parser.add_argument("--mssql-password", default=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""))
    parser.add_argument("--mssql-driver", default=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"))
    parser.add_argument("--mssql-bcp-path", default=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"))
    parser.add_argument("--clickhouse-host", default=os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"))
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(os.getenv("DPONE_IT_CH_PORT", os.getenv("DPONE_IT_CH_PORT_FORWARD", "59000"))),
    )
    parser.add_argument(
        "--clickhouse-http-port",
        type=int,
        default=int(os.getenv("DPONE_IT_CH_HTTP_PORT", os.getenv("DPONE_IT_CH_HTTP_PORT_FORWARD", "58123"))),
    )
    parser.add_argument("--clickhouse-database", default=os.getenv("DPONE_IT_CH_DATABASE", "dpone_it"))
    parser.add_argument("--clickhouse-user", default=os.getenv("DPONE_IT_CH_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("DPONE_IT_CH_PASSWORD", ""))
    parser.add_argument(
        "--s3-endpoint",
        default=os.getenv(
            "DPONE_IT_S3_ENDPOINT",
            f"http://127.0.0.1:{os.getenv('DPONE_IT_MINIO_PORT_FORWARD', '59090')}",
        ),
    )
    parser.add_argument("--s3-region", default=os.getenv("DPONE_IT_S3_REGION", "us-east-1"))
    parser.add_argument(
        "--s3-access-key",
        default=os.getenv("DPONE_IT_S3_ACCESS_KEY", os.getenv("DPONE_IT_MINIO_ACCESS_KEY", "")),
    )
    parser.add_argument(
        "--s3-secret-key",
        default=os.getenv("DPONE_IT_S3_SECRET_KEY", os.getenv("DPONE_IT_MINIO_SECRET_KEY", "")),
    )


def _object_client(config: ParquetS3CertificationConfig) -> S3ObjectStorageClient:
    params = config.s3_params
    return S3ObjectStorageClient(
        endpoint_url=str(params.get("endpoint_url") or "") or None,
        region_name=str(params.get("region_name") or "") or None,
        aws_access_key_id=str(params.get("access_key") or "") or None,
        aws_secret_access_key=str(params.get("secret_key") or "") or None,
    )


def _resolved_prefix(config: ParquetS3CertificationConfig) -> ObjectStorageUri:
    return ObjectStorageUri.parse(config.object_prefix.format(run_id=config.run_id)).prefix()


if __name__ == "__main__":
    raise SystemExit(main())
