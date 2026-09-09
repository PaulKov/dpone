#!/usr/bin/env python3
"""Run synthetic native BCP corner cases in owned, disposable Docker services."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(args: list[str], *, env=None, timeout=120) -> subprocess.CompletedProcess:
    return subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout)


def require(args: list[str], *, env=None) -> str:
    result = command(args, env=env)
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or result.stdout.strip())
    return result.stdout.strip()


def source_digest() -> str:
    """Bind test/source/dependency content, including uncommitted files, without paths outside the repo."""
    names = require(
        [
            "git",
            "ls-files",
            "-co",
            "--exclude-standard",
            "src",
            "packages",
            "tests",
            "tools",
            "pyproject.toml",
            "uv.lock",
        ]
    )
    digest = hashlib.sha256()
    for name in sorted(set(names.splitlines())):
        path = ROOT / name
        if path.is_file():
            digest.update(name.encode() + b"\x00" + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def run(args) -> int:
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    token = secrets.token_hex(6)
    network = "dpone-bcp-corners-" + token
    sql, ch = network + "-sql", network + "-ch"
    docker = args.docker
    created = []
    receipt = {
        "schema_version": "dpone.local-native-corners.v1",
        "status": "FAIL",
        "git_head": require(["git", "rev-parse", "HEAD"]),
        "source_sha256": source_digest(),
    }
    started = time.monotonic()
    try:
        require([docker, "network", "create", "--internal", network])
        created.append(network)
        secret_env = os.environ | {"MSSQL_SA_PASSWORD": "Dp1!" + secrets.token_urlsafe(24)}
        created.append(sql)
        require(
            [
                docker,
                "run",
                "-d",
                "--platform",
                "linux/amd64",
                "--name",
                sql,
                "--network",
                network,
                "--memory",
                "4g",
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                "MSSQL_PID=Developer",
                "-e",
                "MSSQL_SA_PASSWORD",
                args.sql_image,
            ],
            env=secret_env,
        )
        del secret_env
        created.append(ch)
        require([docker, "run", "-d", "--name", ch, "--network", network, "--memory", "2g", args.ch_image])
        probe = [
            docker,
            "exec",
            sql,
            "bash",
            "-c",
            '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W -Q "SET NOCOUNT ON; SELECT CAST(SERVERPROPERTY(\'ProductVersion\') AS varchar(128))"',
        ]
        deadline = time.monotonic() + 180
        while True:
            result = command(probe, timeout=20)
            if result.returncode == 0:
                receipt["sql_server_version"] = result.stdout.strip()
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Local SQL Server did not become ready")
            time.sleep(2)
        receipt["bcp_version"] = require([docker, "exec", sql, "/opt/mssql-tools18/bin/bcp", "-v"])
        receipt["clickhouse_version"] = require(
            [docker, "exec", ch, "clickhouse-client", "--query", "SELECT version()"]
        )
        receipt["images"] = {}
        for kind, name in (("sql", sql), ("clickhouse", ch)):
            image_id = require([docker, "inspect", "--format", "{{.Image}}", name])
            metadata = json.loads(require([docker, "image", "inspect", image_id]))[0]
            receipt["images"][kind] = {key: metadata.get(key) for key in ("Id", "RepoDigests", "Architecture", "Os")}
        env = os.environ | {
            "DPONE_IT_DOCKER": docker,
            "DPONE_IT_NATIVE_DOCKER_SQL": sql,
            "DPONE_IT_NATIVE_DOCKER_CH": ch,
        }
        env.pop("PYTEST_ADDOPTS", None)
        pytest = command(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests/integration/mssql/test_native_docker_corners_live.py",
                "-q",
                "-o",
                "addopts=",
                "--tb=short",
            ],
            env=env,
            timeout=1800,
        )
        log = (pytest.stdout + pytest.stderr).replace(str(ROOT), "<workspace>")
        log = re.sub(r"/private/var/folders/[^\s'\"]+", "<temporary-path>", log)
        (output / "pytest.log").write_text(log)
        receipt["pytest_exit_code"] = pytest.returncode
        receipt["source_unchanged"] = receipt["source_sha256"] == source_digest()
        receipt["pytest_summary"] = log.strip().splitlines()[-1] if log.strip() else "missing"
        outcomes = ("passed", "skipped", "deselected", "xfailed", "xpassed", "failed", "error", "errors")
        for outcome in outcomes:
            count = re.search(rf"\b(\d+) {outcome}\b", receipt["pytest_summary"])
            receipt[outcome] = int(count[1]) if count else 0
        if (
            pytest.returncode == 0
            and receipt["passed"] > 0
            and not any(receipt[outcome] for outcome in outcomes[1:])
            and receipt["source_unchanged"]
        ):
            receipt["status"] = "PASS"
    except Exception as error:
        receipt["error"] = str(error).replace(str(ROOT), "<workspace>")
    finally:
        failures = []
        for name in reversed(created[1:]):
            if command([docker, "rm", "-f", name]).returncode:
                failures.append("container_cleanup_failed")
        if created and command([docker, "network", "rm", network]).returncode:
            failures.append("network_cleanup_failed")
        receipt["cleanup"] = "FAIL" if failures else "PASS"
        if failures:
            receipt["status"] = "FAIL"
        receipt["duration_seconds"] = round(time.monotonic() - started, 2)
        (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({key: receipt.get(key) for key in ("status", "pytest_summary", "cleanup", "duration_seconds")}))
    return 0 if receipt["status"] == "PASS" else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--sql-image", default="mcr.microsoft.com/mssql/server:2022-latest")
    parser.add_argument("--ch-image", default="clickhouse/clickhouse-server:24.8")
    parser.add_argument("--output", required=True, help="New directory for sanitized logs and version-bound receipt")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
