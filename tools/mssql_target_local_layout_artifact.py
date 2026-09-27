"""Produce the exact-commit MSSQL target-local layout capability receipt."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dpone.runtime.sinks.mssql_native_target_local_layout import TARGET_LOCAL_LAYOUT_MATRIX_V1


def write_layout_artifact(*, source_commit: str, output: Path) -> None:
    """Write one deterministic, content-addressed layout receipt."""

    payload = TARGET_LOCAL_LAYOUT_MATRIX_V1.artifact(source_commit)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    write_layout_artifact(source_commit=args.source_commit, output=args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
