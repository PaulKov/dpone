#!/usr/bin/env python3
"""Run bounded SQL Server component tests on disposable Linux x86-64 only.

Prerequisites: clean checkout, Docker Linux/amd64 daemon, Microsoft ODBC Driver
18, pyodbc, pytest and this checkout's dpone dependencies. Invoke with
``uv run python tools/composition_mssql_synthetic.py --output-dir PATH`` where
PATH is a new directory outside the checkout. No existing service is accepted.
Only JUnit and a sanitized component summary are retained; no driver errors,
commands, DSNs, container environment, credentials or broad logs are published.
This is control-ledger SQL evidence, never downstream/route certification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import secrets
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from contextlib import closing
from dataclasses import asdict
from importlib.metadata import version
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SQL_IMAGE = (
    "mcr.microsoft.com/mssql/server:2022-latest@sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89"
)
TEST_FILE = "tests/integration/composition/test_composition_mssql_store_live.py"
TEST_CLASS = "tests.integration.composition.test_composition_mssql_store_live"
EXPECTED_TESTS = (
    "test_external_ddl_and_mixed_engine_lifecycle",
    "test_conflict_and_epoch_ceiling_leave_no_partial_mutation",
    "test_concurrent_same_domain_prepare_has_one_winner",
    "test_lost_commit_ack_uses_independent_sql_readback",
    "test_foreign_control_identity_and_corrupt_request_fail_closed",
    "test_unresolved_attempt_blocks_retirement[RUNNING]",
    "test_unresolved_attempt_blocks_retirement[COMMIT_UNKNOWN]",
)


class RunFailure(RuntimeError):
    """A fixed, secret-free failure reason suitable for public evidence."""


def command(args, *, env=None, timeout=120):
    """Capture child output in memory only; callers never publish diagnostics."""
    return subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True, timeout=timeout, check=False)


def checked(args, *, env=None, timeout=120):
    try:
        result = command(args, env=env, timeout=timeout)
        if result.returncode:
            raise RunFailure("command_failed")
        return result.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        raise RunFailure("command_unavailable_or_timeout") from None


def source_identity():
    """Require exact committed, clean source instead of blessing a dirty HEAD."""
    sha = checked(["git", "rev-parse", "HEAD"])
    if re.fullmatch(r"[0-9a-f]{40}", sha) is None:
        raise RunFailure("source_identity_invalid")
    clean = not checked(["git", "status", "--porcelain", "--untracked-files=all"])
    return sha, clean


def validate_results(path):
    """Reuse existing JUnit readers, then bind exact cases and observed counts."""
    from tools.ci.assert_junit_executed import evaluate_junit, junit_cases, summarize_junit

    try:
        ok, _ = evaluate_junit(path, min_passed=len(EXPECTED_TESTS), max_skipped=0)
        totals, cases = summarize_junit(path), junit_cases(path)
        expected = {f"{TEST_CLASS}::{name}" for name in EXPECTED_TESTS}
        if (
            not ok
            or len(cases) != len(expected)
            or totals.tests != len(cases)
            or {case.node_id for case in cases} != expected
            or any(case.status != "passed" for case in cases)
        ):
            raise RunFailure("junit_incomplete_or_not_green")
        return {
            "totals": asdict(totals),
            "cases": [asdict(case) for case in cases],
            "junit_sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
        }
    except (OSError, ValueError, ET.ParseError):
        raise RunFailure("junit_invalid") from None


def provision_database(env):
    """Bound readiness; create only a new database on the owned loopback port."""
    import pyodbc
    from tests.integration.composition.mssql_store_live_support import ODBC_DRIVER, OwnedDatabase

    if ODBC_DRIVER not in pyodbc.drivers():
        raise RunFailure("odbc18_required")
    database = OwnedDatabase.from_environment(env)
    deadline = time.monotonic() + 180
    while True:
        try:
            connection = database.connect(master=True)
            break
        except RuntimeError:
            if time.monotonic() >= deadline:
                raise RunFailure("sql_readiness_timeout") from None
            time.sleep(2)
    try:
        with closing(connection), closing(connection.cursor()) as cursor:
            server_version = str(
                cursor.execute("SELECT CAST(SERVERPROPERTY('ProductVersion') AS varchar(128))").fetchone()[0]
            )
            driver_version = str(connection.getinfo(pyodbc.SQL_DRIVER_VER))
            if any(re.fullmatch(r"[0-9.]{1,128}", value) is None for value in (server_version, driver_version)):
                raise RunFailure("version_observation_invalid")
            cursor.execute(f"CREATE DATABASE [{database.database}]")
            while cursor.nextset():
                pass
            return {
                "sql_server_version": server_version,
                "odbc_driver_version": driver_version,
                "pyodbc_version": version("pyodbc"),
            }
    except RunFailure:
        raise
    except Exception:
        raise RunFailure("database_provision_failed") from None


def execute_component(output, env):
    """One bounded child pytest process, with sanitized JUnit failure reports."""
    child_env = env | {
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPATH": os.pathsep.join((str(ROOT / "src"), str(ROOT))),
    }
    for name in ("PYTEST_ADDOPTS", "PYTEST_PLUGINS"):
        child_env.pop(name, None)
    result = command(
        [
            sys.executable,
            "-m",
            "pytest",
            TEST_FILE,
            "-q",
            "-o",
            "addopts=",
            "-p",
            "no:cacheprovider",
            "-p",
            "tests.integration.composition.mssql_store_live_support",
            "--tb=no",
            "--show-capture=no",
            "-o",
            "junit_logging=no",
            "-o",
            "junit_log_passing_tests=false",
            "--junitxml",
            str(output / "junit.xml"),
        ],
        env=child_env,
        timeout=600,
    )
    if result.returncode:
        raise RunFailure("component_pytest_failed")
    return validate_results(output / "junit.xml")


def run(output):
    """Own the full lifecycle; cleanup failure overrides any otherwise green run."""
    output = output.resolve()
    if output == ROOT or ROOT in output.parents:
        print('{"status":"FAIL","reason":"output_must_be_outside_checkout"}')
        return 1
    try:
        output.mkdir(parents=True, exist_ok=False)
    except OSError:
        print('{"status":"FAIL","reason":"new_output_directory_required"}')
        return 1
    report = {
        "schema_version": "dpone.composition-mssql-component.v1",
        "status": "FAIL",
        "component_scope": {
            "sql_server": "real_dbapi_control_ledger",
            "clickhouse": "synthetic_enrollment_metadata_only",
            "route_certification": "UNVERIFIED",
            "native_v2_and_logon_guarantees": "UNVERIFIED",
            "worker_execution": "UNVERIFIED",
            "terminal_proof_producers": "UNVERIFIED",
        },
        "image_reference": SQL_IMAGE,
        "host_system": platform.system(),
        "host_machine": platform.machine(),
        "python_version": platform.python_version(),
        "pytest_version": version("pytest"),
        "expected_test_count": len(EXPECTED_TESTS),
        "cleanup": "N/A",
        "junit_path": "junit.xml",
    }
    started, owned_name = time.monotonic(), None
    try:
        report["stage"] = "host_admission"
        if platform.system() != "Linux" or platform.machine() not in {"x86_64", "AMD64"}:
            report["status"] = "UNVERIFIED"
            raise RunFailure("linux_x86_64_required")
        report["stage"] = "source_admission"
        sha, clean = source_identity()
        report.update(source_commit=sha, source_clean_before=clean)
        if not clean:
            raise RunFailure("source_not_clean")
        report["stage"] = "docker_admission"
        if checked(["docker", "version", "--format", "{{.Server.Os}}/{{.Server.Arch}}"]) != "linux/amd64":
            raise RunFailure("docker_linux_amd64_required")
        docker_version = checked(["docker", "version", "--format", "{{.Server.Version}}"])
        if re.fullmatch(r"[A-Za-z0-9.+_-]{1,80}", docker_version) is None:
            raise RunFailure("docker_version_invalid")
        report["docker_server_version"] = docker_version
        token = secrets.token_hex(12)
        env = os.environ | {
            "MSSQL_SA_PASSWORD": "Dp1!" + secrets.token_urlsafe(32),
            "DPONE_RUN_COMPOSITION_MSSQL_LIVE": "1",
            "DPONE_COMPOSITION_RUN_TOKEN": token,
            "DPONE_COMPOSITION_SQL_DATABASE": "dpone_composition_" + token,
        }
        owned_name = "dpone-composition-" + token
        report["owned_container_name"] = owned_name
        report["stage"] = "container_start"
        checked(
            [
                "docker",
                "run",
                "-d",
                "--platform",
                "linux/amd64",
                "--name",
                owned_name,
                "--memory",
                "4g",
                "-p",
                "127.0.0.1::1433",
                "-e",
                "ACCEPT_EULA=Y",
                "-e",
                "MSSQL_PID=Developer",
                "-e",
                "MSSQL_SA_PASSWORD",
                SQL_IMAGE,
            ],
            env=env,
            timeout=300,
        )
        report["stage"] = "container_identity"
        port = checked(["docker", "port", owned_name, "1433/tcp"])
        match = re.fullmatch(r"127\.0\.0\.1:([0-9]{1,5})", port)
        if match is None or not 1024 <= int(match[1]) <= 65535:
            raise RunFailure("loopback_port_required")
        env["DPONE_COMPOSITION_SQL_PORT"] = match[1]
        image_id = checked(["docker", "inspect", "--format", "{{.Image}}", owned_name])
        if re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None:
            raise RunFailure("image_identity_invalid")
        if checked(["docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", image_id]) != "linux/amd64":
            raise RunFailure("image_platform_invalid")
        report["actual_image_id"] = image_id
        report["stage"] = "database_provision"
        report.update(provision_database(env))
        report["stage"] = "component_tests"
        report.update(execute_component(output, env))
        report["stage"] = "source_readback"
        after_sha, after_clean = source_identity()
        report.update(source_commit_after=after_sha, source_clean_after=after_clean)
        if (sha, True) != (after_sha, after_clean):
            raise RunFailure("source_changed_during_execution")
        report["status"] = "PASS"
        report["stage"] = "complete"
    except RunFailure as error:
        report["reason"] = str(error)
    except BaseException:
        report["reason"] = "component_runner_failed"
    finally:
        if owned_name:
            try:
                checked(["docker", "rm", "-f", owned_name], timeout=60)
                report["cleanup"] = "PASS"
            except BaseException:
                report["cleanup"] = report["status"] = "FAIL"
                report["reason"] = "owned_container_cleanup_failed"
        report["duration_seconds"] = round(time.monotonic() - started, 2)
        (output / "summary.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({name: report[name] for name in ("status", "cleanup", "component_scope")}))
    return 0 if report["status"] == "PASS" else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    return run(parser.parse_args(argv).output_dir)


if __name__ == "__main__":
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "src"))
    raise SystemExit(main())
