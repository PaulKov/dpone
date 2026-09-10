"""Compare retained native-delivery v1 evidence without running a live workload."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dpone.runtime.native_delivery_benchmark import BenchmarkInputError, compare


def build_parser() -> argparse.ArgumentParser:
    """Build the developer-only CLI; all paths are explicit."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    command = commands.add_parser("compare", help="Validate retained runs/campaigns and write a comparison")
    command.add_argument("--baseline", required=True, type=Path)
    command.add_argument("--candidate", required=True, type=Path)
    command.add_argument("--output", required=True, type=Path)
    command.add_argument("--overwrite", action="store_true", help="Atomically replace an existing report")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Return 0 written report, 1 failed gate, or 2 usage/schema/identity/file error."""
    args = build_parser().parse_args(argv)
    try:
        report = compare(args.baseline, args.candidate, output=args.output, overwrite=args.overwrite)
    except BenchmarkInputError as exc:
        print(f"native_delivery_benchmark: {exc}", file=sys.stderr)
        return 2
    except (OSError, ValueError):
        print("native_delivery_benchmark: file_or_json_error; check paths and retained inputs", file=sys.stderr)
        return 2
    print(f"{args.output} {report['status']}")
    if report["status"] != "PASS":
        print("native_delivery_benchmark: inspect report status and limitations", file=sys.stderr)
    return 1 if report["status"] == "FAIL" else 0


if __name__ == "__main__":
    raise SystemExit(main())
