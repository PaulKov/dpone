#!/usr/bin/env python3
"""Create one independent local authority plan for the wide release campaign."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as base  # noqa: E402
from local_route_certification_receipt import capture_local_source_snapshot  # noqa: E402
from mssql_clickhouse_bcp_native_config import add_connection_args  # noqa: E402
from mssql_clickhouse_target_schema import expected_clickhouse_schema_sha256  # noqa: E402
from mssql_dbt_wide_evidence import (  # noqa: E402
    canonical_sha256,
    clickhouse_connection_sha256,
    load_json_object,
    mssql_connection_sha256,
    object_storage_authority_sha256,
)

_SCHEMA_VERSION = "dpone.mssql-clickhouse-wide-release-authority.v1"
_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1] / "docs/schemas/dpone.mssql-clickhouse-wide-release-authority.v1.schema.json"
)
_SLOTS = ("bcp_native_required", "bcp_native_off", "parquet_s3_pull")


def build_authority(args: argparse.Namespace) -> Mapping[str, Any]:
    """Observe the trusted source schema and non-secret endpoint coordinates."""

    snapshot = capture_local_source_snapshot()
    if snapshot.worktree_dirty:
        raise ValueError("wide_release_authority_worktree_dirty")
    if not args.mssql_password:
        raise ValueError("local_certification_connection_secrets_required")
    mssql_params = base._mssql_params(args)
    clickhouse_params = base._clickhouse_params(args)
    connector = base._mssql_connector(cast(Any, SimpleNamespace(mssql_params=mssql_params)))
    try:
        source_schema = tuple(connector.fetch_schema(args.source_schema, args.source_table))
        if len(source_schema) != 202:
            raise ValueError("wide_release_authority_source_column_count_mismatch")
        bcp_schema = expected_clickhouse_schema_sha256(
            mssql=connector,
            source_schema=args.source_schema,
            source_table=args.source_table,
            type_fidelity={"binary_encoding": "hex", "time_encoding": "seconds_since_midnight"},
        )
        parquet_schema = expected_clickhouse_schema_sha256(
            mssql=connector,
            source_schema=args.source_schema,
            source_table=args.source_table,
            type_fidelity=None,
        )
    finally:
        base._close_quietly(connector)
    payload: dict[str, Any] = {
        "schema_version": _SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "release_id": args.release_id,
        "git_head_sha": snapshot.git_head_sha,
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "worktree_dirty": False,
        "environment_class": "LOCAL_DOCKER",
        "source_relation": f"{args.source_schema}.{args.source_table}",
        "expected_rows": 10_000,
        "expected_source_columns": 201,
        "expected_target_columns": 202,
        "mssql_connection_sha256": mssql_connection_sha256(mssql_params),
        "clickhouse_connection_sha256": clickhouse_connection_sha256(clickhouse_params),
        "target_schema_sha256_by_slot": {
            "bcp_native_required": bcp_schema,
            "bcp_native_off": bcp_schema,
            "parquet_s3_pull": parquet_schema,
        },
        "s3_endpoint_sha256": object_storage_authority_sha256(
            endpoint_url=args.s3_endpoint,
            region_name=args.s3_region,
            bucket=args.s3_bucket,
            named_collection=args.s3_named_collection,
        ),
        "s3_bucket": args.s3_bucket,
        "s3_named_collection": args.s3_named_collection,
        "production_certification": "UNVERIFIED",
    }
    payload["authority_sha256"] = canonical_sha256(payload)
    return payload


def write_authority(path: Path, payload: Mapping[str, Any]) -> Path:
    """Write the authority once; acknowledgement-loss replay uses the same file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as stream:
        json.dump(dict(payload), stream, ensure_ascii=False, indent=2, sort_keys=True)
        stream.write("\n")
    load_authority(
        path,
        release_id=str(payload["release_id"]),
        source_relation=str(payload["source_relation"]),
    )
    return path


def load_authority(path: Path, *, release_id: str, source_relation: str) -> Mapping[str, Any]:
    """Authenticate one authority plan against this exact clean checkout."""

    if path.is_symlink() or not path.is_file():
        raise ValueError("wide_release_authority_path_invalid")
    value = load_json_object(path)
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    if tuple(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(value)):
        raise ValueError("wide_release_authority_schema_invalid")
    canonical = dict(value)
    claimed = canonical.pop("authority_sha256", None)
    if claimed != canonical_sha256(canonical):
        raise ValueError("wide_release_authority_digest_mismatch")
    snapshot = capture_local_source_snapshot()
    expected = {
        "release_id": release_id,
        "source_relation": source_relation,
        "git_head_sha": snapshot.git_head_sha,
        "source_snapshot_sha256": snapshot.source_snapshot_sha256,
        "worktree_dirty": False,
        "environment_class": "LOCAL_DOCKER",
        "expected_rows": 10_000,
        "expected_source_columns": 201,
        "expected_target_columns": 202,
        "production_certification": "UNVERIFIED",
    }
    if snapshot.worktree_dirty or any(value.get(field) != item for field, item in expected.items()):
        raise ValueError("wide_release_authority_subject_mismatch")
    if set(value.get("target_schema_sha256_by_slot", {})) != set(_SLOTS):
        raise ValueError("wide_release_authority_slot_closure_invalid")
    return value


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--s3-endpoint", default=os.getenv("DPONE_IT_S3_ENDPOINT", "http://127.0.0.1:59090"))
    parser.add_argument("--s3-region", default=os.getenv("DPONE_IT_S3_REGION", "us-east-1"))
    parser.add_argument("--s3-bucket", default="dpone-stage")
    parser.add_argument("--s3-named-collection", default="dpone_stage")
    add_connection_args(parser)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    path = write_authority(args.output, build_authority(args))
    print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
