"""CLI for inspecting dpone Airflow pack cache status."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Sequence
from pathlib import Path

from dpone_airflow_pack.airflow_metadata import AirflowVariableStatusPublisher
from dpone_airflow_pack.cache_reconcile_status import DEFAULT_MAX_RECONCILE_AGE_SECONDS
from dpone_airflow_pack.cache_status import read_airflow_pack_cache_status
from dpone_airflow_pack.cache_status_ack import attach_loader_ack_status


def _positive_integer(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Read-only inspection of a local dpone Airflow cache.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "The command auto-detects legacy_pack_index_v1 and exact_deployment_v1 layouts. "
            "The command will exit 1 when status is blocked and otherwise exits 0; it never repairs or mutates "
            "the cache root."
        ),
    )
    parser.add_argument(
        "--cache-dir",
        default=None,
        help=(
            "Cache root. Shared deployments should pass it explicitly. If omitted, use "
            "DPONE_AIRFLOW_PACK_CACHE_DIR, then the historical compatibility root. "
            "New deployments should pass /opt/airflow/.dpone-cache explicitly."
        ),
    )
    parser.add_argument(
        "--workload-id",
        action="append",
        default=[],
        help="Workload ID to verify; repeat for multiple workloads.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        dest="as_json",
        help="Write the complete machine-readable JSON status instead of the text summary.",
    )
    parser.add_argument("--ack-path", help="Optional external loader ACK file to verify")
    parser.add_argument("--ack-root", help="Dedicated ACK root; required with --ack-path")
    parser.add_argument(
        "--max-reconcile-age-seconds",
        type=_positive_integer,
        default=os.environ.get(
            "DPONE_AIRFLOW_PACK_RECONCILE_MAX_AGE_SECONDS",
            str(DEFAULT_MAX_RECONCILE_AGE_SECONDS),
        ),
        help="Maximum age of exact desired-state watcher evidence",
    )
    parser.add_argument(
        "--airflow-variable-key",
        help="Optional non-secret Airflow Variable diagnostic projection; never cache authority",
    )
    args = parser.parse_args(argv)
    if bool(args.ack_path) != bool(args.ack_root):
        parser.error("--ack-path and --ack-root must be provided together")

    status = read_airflow_pack_cache_status(
        args.cache_dir,
        workload_ids=tuple(args.workload_id),
        max_reconcile_age_seconds=args.max_reconcile_age_seconds,
    )
    if args.ack_path and args.ack_root:
        status = attach_loader_ack_status(
            status,
            cache_root=Path(status["cache_dir"]),
            ack_path=Path(args.ack_path),
            ack_root=Path(args.ack_root),
        )
    status = AirflowVariableStatusPublisher(args.airflow_variable_key).publish(status)
    if args.as_json:
        print(json.dumps(status, ensure_ascii=False, sort_keys=True))
    else:
        print(f"status={status['status']} current_generation={status.get('current_generation')}")
        for workload_id, workload in status.get("workloads", {}).items():
            print(f"workload={workload_id} exists={workload.get('exists')} sha256={workload.get('sha256')}")
    return 1 if status.get("status") == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
