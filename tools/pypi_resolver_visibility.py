#!/usr/bin/env python3
"""Wait until the standard pip resolver can see an exact dpone release."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

VERSION_PATTERN = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
MAX_ATTEMPTS = 60
MAX_INTERVAL_SECONDS = 300
MAX_TOTAL_TIMEOUT_SECONDS = 3600

CommandRunner = Callable[[Sequence[str], float], subprocess.CompletedProcess[str]]
Sleeper = Callable[[float], None]
Clock = Callable[[], float]


def _run_command(command: Sequence[str], timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else ""
        stderr = exc.stderr if isinstance(exc.stderr, str) else ""
        return subprocess.CompletedProcess(
            list(command), 124, stdout=stdout, stderr=stderr or "resolver attempt timed out"
        )


def resolver_command(version: str) -> tuple[str, ...]:
    """Return a probe for the public pip/Simple package-resolution surface."""

    if not VERSION_PATTERN.fullmatch(version):
        raise ValueError("version must use stable X.Y.Z syntax")
    return (
        sys.executable,
        "-m",
        "pip",
        "install",
        "--dry-run",
        "--ignore-installed",
        "--no-cache-dir",
        "--index-url",
        "https://pypi.org/simple",
        "--retries",
        "2",
        "--timeout",
        "60",
        f"dpone[full,accel]=={version}",
    )


def wait_for_resolver(
    version: str,
    *,
    attempts: int,
    interval_seconds: int,
    attempt_timeout_seconds: int = 120,
    total_timeout_seconds: int = 1800,
    runner: CommandRunner = _run_command,
    sleeper: Sleeper = time.sleep,
    clock: Clock = time.monotonic,
) -> dict[str, Any]:
    """Poll boundedly and return a stable PASS/FAIL resolver receipt."""

    if not 1 <= attempts <= MAX_ATTEMPTS:
        raise ValueError(f"attempts must be between 1 and {MAX_ATTEMPTS}")
    if not 0 <= interval_seconds <= MAX_INTERVAL_SECONDS:
        raise ValueError(f"interval_seconds must be between 0 and {MAX_INTERVAL_SECONDS}")
    if attempt_timeout_seconds < 1:
        raise ValueError("attempt_timeout_seconds must be positive")
    if not 1 <= total_timeout_seconds <= MAX_TOTAL_TIMEOUT_SECONDS:
        raise ValueError(f"total_timeout_seconds must be between 1 and {MAX_TOTAL_TIMEOUT_SECONDS}")
    command = resolver_command(version)
    last_output = ""
    started_at = clock()
    deadline = started_at + total_timeout_seconds
    for attempt in range(1, attempts + 1):
        remaining = deadline - clock()
        if remaining <= 0:
            break
        result = runner(command, min(float(attempt_timeout_seconds), remaining))
        last_output = (result.stdout + "\n" + result.stderr).strip()[-4000:]
        if result.returncode == 0:
            return {
                "schema": "dpone.pypi-resolver-visibility.v1",
                "status": "PASS",
                "version": version,
                "package_spec": f"dpone[full,accel]=={version}",
                "attempt": attempt,
                "max_attempts": attempts,
                "index_url": "https://pypi.org/simple",
            }
        if attempt < attempts:
            print(last_output, file=sys.stderr)
            remaining = deadline - clock()
            if remaining <= 0:
                break
            sleeper(min(float(interval_seconds), remaining))
    return {
        "schema": "dpone.pypi-resolver-visibility.v1",
        "status": "FAIL",
        "version": version,
        "package_spec": f"dpone[full,accel]=={version}",
        "attempt": attempts,
        "max_attempts": attempts,
        "total_timeout_seconds": total_timeout_seconds,
        "index_url": "https://pypi.org/simple",
        "last_output": last_output,
    }


def _write_receipt(path: Path, receipt: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--interval-seconds", type=int, default=30)
    parser.add_argument("--attempt-timeout-seconds", type=int, default=120)
    parser.add_argument("--total-timeout-seconds", type=int, default=1800)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        receipt = wait_for_resolver(
            args.version,
            attempts=args.attempts,
            interval_seconds=args.interval_seconds,
            attempt_timeout_seconds=args.attempt_timeout_seconds,
            total_timeout_seconds=args.total_timeout_seconds,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    _write_receipt(args.output, receipt)
    print(json.dumps(receipt, sort_keys=True))
    return 0 if receipt["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
