"""CLI for syncing remote dpone Airflow packs into a local cache."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from dpone_airflow_pack.cache_sync import (
    AirflowPackSyncOptions,
    sync_airflow_pack_cache,
    validate_airflow_pack_sync_options,
    watch_airflow_pack_cache,
    write_sync_warning,
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sync remote dpone Airflow packs into a bounded legacy cache.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        epilog=(
            "Byte sizes must be positive. CLI values accept raw bytes; binary KiB, MiB, GiB; "
            "or decimal KB/MB/GB. Only the Python API can explicitly use None for an unbounded "
            "compatibility limit. --once writes JSON evidence to stdout and will exit 1 for a blocker "
            "or runtime command failure; invalid CLI syntax or option relationships exit 2 before I/O. "
            "Failures are redacted on stderr. Default budgets are 512 MiB total, 10 MiB per artifact, "
            "and 25 MiB per index."
        ),
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--once",
        action="store_true",
        help="Run one sync and emit one JSON evidence object.",
    )
    mode.add_argument(
        "--watch",
        action="store_true",
        help="Retry continuously; each failed cycle records redacted warning evidence.",
    )
    parser.add_argument(
        "--index-uri",
        required=True,
        help="Mutable pack-index URI (file:// or s3://); credentials are redacted from evidence.",
    )
    parser.add_argument(
        "--reader-connection-id",
        default=None,
        help="Airflow connection ID used only for s3:// reads.",
    )
    parser.add_argument(
        "--cache-dir",
        required=True,
        help=("Dedicated legacy_pack_index_v1 cache root. Never use an exact_deployment_v1 cache root."),
    )
    parser.add_argument(
        "--max-total-bytes",
        type=_byte_size,
        default=512 * 1024 * 1024,
        help="Hard total cache bound in bytes (binary suffixes are accepted).",
    )
    parser.add_argument(
        "--max-pack-bytes",
        type=_byte_size,
        default=10 * 1024 * 1024,
        help="Per-pack and per-DAG-spec read bound in bytes (binary suffixes are accepted).",
    )
    parser.add_argument(
        "--max-index-bytes",
        type=_byte_size,
        default=25 * 1024 * 1024,
        help="Pack-index read bound in bytes (binary suffixes are accepted).",
    )
    parser.add_argument("--keep-generations", type=_positive_int, default=3, help="Minimum retained generations.")
    parser.add_argument(
        "--high-watermark-pct",
        type=_positive_int,
        default=80,
        help="Start retention above this percentage of the total byte bound.",
    )
    parser.add_argument(
        "--low-watermark-pct",
        type=_positive_int,
        default=60,
        help="Retention target percentage; must be below the high watermark.",
    )
    parser.add_argument(
        "--partial-download-ttl-minutes",
        type=_positive_int,
        default=30,
        help="Minimum age in minutes before an abandoned stage can be reclaimed.",
    )
    parser.add_argument(
        "--interval-seconds",
        type=_positive_int,
        default=60,
        help="Watch-mode delay between sync attempts, in seconds.",
    )
    parser.add_argument(
        "--jitter-seconds",
        type=_non_negative_int,
        default=0,
        help="Maximum random watch-mode delay added to each interval, in seconds.",
    )
    parser.add_argument(
        "--status-path",
        default=None,
        help="Status JSON path; defaults to CACHE_DIR/status/last-sync-status.json.",
    )
    parser.add_argument(
        "--airflow-variable-key",
        default=None,
        help="Optional Airflow Variable diagnostic projection; local receipt remains authoritative.",
    )
    args = parser.parse_args(argv)
    options = AirflowPackSyncOptions(
        index_uri=args.index_uri,
        cache_dir=Path(args.cache_dir),
        reader_connection_id=args.reader_connection_id,
        max_total_bytes=args.max_total_bytes,
        max_pack_bytes=args.max_pack_bytes,
        max_index_bytes=args.max_index_bytes,
        keep_generations=args.keep_generations,
        high_watermark_pct=args.high_watermark_pct,
        low_watermark_pct=args.low_watermark_pct,
        partial_download_ttl_minutes=args.partial_download_ttl_minutes,
        status_path=Path(args.status_path) if args.status_path else None,
        airflow_variable_key=args.airflow_variable_key,
    )
    try:
        validate_airflow_pack_sync_options(options)
    except ValueError as exc:
        parser.error(str(exc))
    try:
        if args.watch:
            watch_airflow_pack_cache(
                options, interval_seconds=args.interval_seconds, jitter_seconds=args.jitter_seconds
            )
            return 0
        evidence = sync_airflow_pack_cache(options)
        print(json.dumps(evidence, ensure_ascii=False, sort_keys=True))
        return 1 if evidence.get("status") == "blocked" else 0
    except Exception as exc:  # noqa: BLE001 - fail-open wrappers inspect the status file.
        message = "Airflow pack sync failed"
        try:
            warning = write_sync_warning(options, reason="sync_command_failed", message=str(exc))
            message = str(warning.get("message") or message)
        except Exception as evidence_exc:  # noqa: BLE001 - diagnostics cannot replace the command failure.
            message = f"{message}; diagnostic publication failed: {evidence_exc.__class__.__name__}"
        print(message, file=sys.stderr)
        return 1


def _byte_size(value: str | None) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        raise argparse.ArgumentTypeError("expected a positive byte size, not an empty value")
    units = {"kib": 1024, "mib": 1024**2, "gib": 1024**3, "kb": 1000, "mb": 1000**2, "gb": 1000**3}
    lower = text.lower()
    number = lower
    multiplier: int | None = None
    for suffix, candidate_multiplier in units.items():
        if lower.endswith(suffix):
            number = lower[: -len(suffix)]
            multiplier = candidate_multiplier
            break
    try:
        parsed = int(float(number) * multiplier) if multiplier is not None else int(number)
    except (OverflowError, ValueError) as exc:
        raise argparse.ArgumentTypeError("expected a positive byte size such as 1048576, 10MiB, or 10MB") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive byte size greater than zero")
    return parsed


def _positive_int(value: str) -> int:
    parsed = _integer(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("expected a positive integer greater than zero")
    return parsed


def _non_negative_int(value: str) -> int:
    parsed = _integer(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("expected a non-negative integer")
    return parsed


def _integer(value: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an integer") from exc


if __name__ == "__main__":
    raise SystemExit(main())
