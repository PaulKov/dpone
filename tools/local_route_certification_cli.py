"""CLI verifier for one exact local route certification receipt."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path

from local_route_certification_receipt import (
    LocalRouteExpectedSubject,
    LocalRouteIdentity,
    capture_local_source_snapshot,
    verify_local_route_certification_receipt,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verify", type=Path, required=True)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--release-id", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--source-relation", required=True)
    parser.add_argument("--target-relation", required=True)
    parser.add_argument("--transport", required=True)
    parser.add_argument("--binary-format")
    parser.add_argument("--acceleration-mode")
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--expected-column-count", type=int, required=True)
    parser.add_argument("--mssql-connection-sha256", required=True)
    parser.add_argument("--clickhouse-connection-sha256", required=True)
    parser.add_argument("--target-schema-sha256", required=True)
    parser.add_argument("--s3-endpoint-sha256")
    parser.add_argument("--s3-bucket")
    parser.add_argument("--s3-named-collection")
    args = parser.parse_args(argv)
    value = verify_local_route_certification_receipt(
        args.verify,
        source_snapshot=capture_local_source_snapshot(args.repo_root),
        expected=LocalRouteExpectedSubject(
            release_id=args.release_id,
            route=LocalRouteIdentity(source=args.source, sink=args.sink, strategy=args.strategy),
            source_relation=args.source_relation,
            target_relation=args.target_relation,
            transport=args.transport,
            binary_format=args.binary_format,
            requested_acceleration_mode=args.acceleration_mode,
            expected_rows=args.expected_rows,
            expected_column_count=args.expected_column_count,
            mssql_connection_sha256=args.mssql_connection_sha256,
            clickhouse_connection_sha256=args.clickhouse_connection_sha256,
            target_schema_sha256=args.target_schema_sha256,
            s3_endpoint_sha256=args.s3_endpoint_sha256,
            s3_bucket=args.s3_bucket,
            s3_named_collection=args.s3_named_collection,
        ),
    )
    print(
        json.dumps(
            {
                "receipt_sha256": value["receipt_sha256"],
                "evidence_status": value["evidence_status"],
                "route": value["route"],
                "transport": value["transport"],
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
