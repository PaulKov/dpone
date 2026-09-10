"""Source attribution and non-mutating timing probes for the owned Docker fixture."""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
from pathlib import Path
from threading import Event
from time import perf_counter_ns
from typing import Any

import psycopg

from dpone.runtime.connectors.postgres import PostgresConnector

DOCKER = "/Applications/Docker.app/Contents/Resources/bin/docker"
CONTAINER = "dpone-pg-preservation-ec30"
ROOT = Path(__file__).resolve().parents[2]


def source_identity() -> dict[str, str]:
    """Hash tracked execution inputs, independently of documentation-only commits."""
    paths = ("src", "tests", "packages", "pyproject.toml", "uv.lock")
    subprocess.run(["git", "diff", "--exit-code", "HEAD", "--", *paths], cwd=ROOT, check=True, capture_output=True)
    tracked = subprocess.check_output(["git", "ls-files", "-z", "--", *paths], cwd=ROOT).decode().split("\0")
    digest = hashlib.sha256()
    for name in sorted(filter(None, tracked)):
        digest.update(name.encode() + b"\0")
        digest.update((ROOT / name).read_bytes())
    return {
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
        "execution_inputs_sha256": digest.hexdigest(),
    }


def docker_fixture() -> tuple[dict[str, Any], dict[str, Any]]:
    """Read credentials in memory; return only explicit non-secret provenance."""
    info = json.loads(subprocess.check_output([DOCKER, "inspect", CONTAINER], text=True))[0]
    assert info["State"]["Running"], "The approved fixture must already be running"
    assert info["Config"]["Labels"].get("dpone.campaign") == "postgres-strategy-preservation"
    settings = dict(item.split("=", 1) for item in info["Config"]["Env"])
    connection = {
        "host": "127.0.0.1",
        "port": info["NetworkSettings"]["Ports"]["5432/tcp"][0]["HostPort"],
        "database": settings["POSTGRES_DB"],
        "user": settings["POSTGRES_USER"],
        "password": settings["POSTGRES_PASSWORD"],
    }
    resources = json.loads(
        subprocess.check_output(
            [
                DOCKER,
                "info",
                "--format",
                '{"cpus":{{.NCPU}},"memory_bytes":{{.MemTotal}},"architecture":"{{.Architecture}}"}',
            ],
            text=True,
        )
    )
    identity = {
        "container_id": info["Id"],
        "image_id": info["Image"],
        "image_ref": info["Config"]["Image"],
        "container_memory_limit": info["HostConfig"]["Memory"],
        "container_nano_cpus": info["HostConfig"]["NanoCpus"],
        "docker_resources": resources,
        "python": platform.python_version(),
        "psycopg": psycopg.__version__,
    }
    return connection, identity


def observer_connection(settings: dict[str, Any]) -> psycopg.Connection:
    values = dict(settings)
    values["dbname"] = values.pop("database")
    return psycopg.connect(**values, autocommit=True, application_name="dpone-partition-benchmark-observer")


class TimedPostgresConnector(PostgresConnector):
    """Run unchanged connector SQL while recording wall-clock operation timings."""

    def __init__(self, settings: dict[str, Any], observer: psycopg.Connection, reader_pid: int):
        super().__init__(**settings, application_name="dpone-partition-benchmark")
        self.observer = observer
        self.reader_pid = reader_pid
        self.lock_acquired = Event()
        self.lock_acquired_ns: int | None = None
        self.commit_ack_ns: int | None = None
        self.old_child_count_sql = ""
        self.operations: list[dict[str, Any]] = []
        self.reader_blocked_by_sink = False
        self.observer_probe_ns = 0

    @staticmethod
    def statement(query: Any) -> str:
        return query.as_string() if hasattr(query, "as_string") else str(query)

    def execute_query(self, query: Any, params: Any = None) -> int:
        text = self.statement(query).strip()
        start = perf_counter_ns()
        result = super().execute_query(query, params)
        end = perf_counter_ns()
        self.operations.append({"sql": text, "elapsed_ns": end - start, "rowcount": result})
        if text.startswith("LOCK TABLE"):
            self.lock_acquired_ns = end
            self.lock_acquired.set()
        return result

    def get_records(self, query: Any, params: Any = None, as_dict: bool = False) -> list[Any]:
        text = self.statement(query).strip()
        start = perf_counter_ns()
        result = super().get_records(query, params, as_dict)
        elapsed = perf_counter_ns() - start
        if text.startswith("SELECT COUNT(*)"):
            self.operations.append(
                {
                    "sql": text,
                    "elapsed_ns": elapsed,
                    "old_child_count": text == self.old_child_count_sql,
                    "count": result[0][0],
                }
            )
        return result

    def commit_transaction(self) -> None:
        start = perf_counter_ns()
        self.reader_blocked_by_sink = self.observer.execute(
            "SELECT %s = ANY(pg_blocking_pids(%s))", (self.connection.info.backend_pid, self.reader_pid)
        ).fetchone()[0]
        self.observer_probe_ns = perf_counter_ns() - start
        super().commit_transaction()
        self.commit_ack_ns = perf_counter_ns()


def timed_reader(connection: psycopg.Connection, lock: Event, schema: str, result: dict[str, Any]) -> None:
    """Attempt a real SELECT after the sink has acquired its parent-table lock."""
    if not lock.wait(300):
        result["error_type"] = "LockEventTimeout"
        return
    result["started_ns"] = perf_counter_ns()
    try:
        result["observed_rows"] = connection.execute(f"SELECT COUNT(*) FROM {schema}.target").fetchone()[0]
    except BaseException as error:
        result["error_type"] = type(error).__name__
    finally:
        result["finished_ns"] = perf_counter_ns()
