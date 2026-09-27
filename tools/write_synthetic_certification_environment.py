"""Produce a host-free environment receipt for synthetic live certification."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

_SHA256 = re.compile(r"sha256:[0-9a-f]{64}\Z")
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._ +()-]{0,79}\Z")
_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_PLATFORM = re.compile(r"linux-(?:amd64|arm64)\Z")


def write_environment_receipt(
    *,
    output: Path,
    source_commit: str,
    platform: str,
    python_version: str,
    pyodbc_version: str,
    odbc_driver: str,
    mssql_version: str,
    clickhouse_version: str,
    runner_image: str,
    mssql_image: str,
    clickhouse_image: str,
) -> None:
    """Write only allowlisted versions and content-addressed image identities."""

    versions = (python_version, pyodbc_version, odbc_driver, mssql_version, clickhouse_version)
    images = (runner_image, mssql_image, clickhouse_image)
    if _COMMIT.fullmatch(source_commit) is None:
        raise ValueError("certification_environment.source_commit")
    if _PLATFORM.fullmatch(platform) is None or any(_VERSION.fullmatch(value) is None for value in versions):
        raise ValueError("certification_environment.version")
    if any(_SHA256.fullmatch(value) is None for value in images):
        raise ValueError("certification_environment.image")
    payload = {
        "schema_version": 1,
        "kind": "dpone.synthetic-certification-environment",
        "source_commit": source_commit,
        "runtime": {
            "platform": platform,
            "python_version": python_version,
            "pyodbc_version": pyodbc_version,
            "odbc_driver": odbc_driver,
            "image_sha256": runner_image,
        },
        "services": {
            "mssql": {"version": mssql_version, "image_sha256": mssql_image},
            "clickhouse": {"version": clickhouse_version, "image_sha256": clickhouse_image},
        },
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--source-commit", required=True)
    parser.add_argument("--platform", required=True)
    parser.add_argument("--python-version", required=True)
    parser.add_argument("--pyodbc-version", required=True)
    parser.add_argument("--odbc-driver", required=True)
    parser.add_argument("--mssql-version", required=True)
    parser.add_argument("--clickhouse-version", required=True)
    parser.add_argument("--runner-image", required=True)
    parser.add_argument("--mssql-image", required=True)
    parser.add_argument("--clickhouse-image", required=True)
    args = parser.parse_args(argv)
    write_environment_receipt(**vars(args))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
