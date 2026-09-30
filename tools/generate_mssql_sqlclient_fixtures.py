#!/usr/bin/env python3
"""Write or verify the closed synthetic SqlClient fixture descriptors."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from tests.integration.mssql.mssql_sqlclient_live_cases import sqlclient_live_cases

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_ROOT = ROOT / "tests" / "fixtures" / "mssql-sqlclient-v1"


def canonical_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()


def expected_files() -> dict[Path, bytes]:
    return {
        FIXTURE_ROOT / f"{case.fixture_id}.json": canonical_bytes(case.descriptor()) for case in sqlclient_live_cases()
    }


def write() -> None:
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)
    for path, content in expected_files().items():
        path.write_bytes(content)


def check() -> None:
    expected = expected_files()
    actual = set(FIXTURE_ROOT.glob("*.json")) if FIXTURE_ROOT.exists() else set()
    if actual != set(expected) or any(path.read_bytes() != content for path, content in expected.items()):
        raise SystemExit("mssql_sqlclient.fixture_descriptors_stale")


def main() -> int:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--write", action="store_true")
    mode.add_argument("--check", action="store_true")
    arguments = parser.parse_args()
    write() if arguments.write else check()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
