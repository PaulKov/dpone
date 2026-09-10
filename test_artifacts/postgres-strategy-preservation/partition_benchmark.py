"""Measure native partition refresh and exact-count lock cost on synthetic data.

Uses only the explicitly owned PostgreSQL Docker container. No runtime behavior
is patched, and measured timings are not an assertion of a production SLA.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import uuid
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from threading import Thread
from time import perf_counter_ns
from typing import Any

from partition_benchmark_support import (
    TimedPostgresConnector,
    docker_fixture,
    observer_connection,
    source_identity,
    timed_reader,
)

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.etl.result_metrics import populate_success_result
from dpone.runtime.internal_query_artifact import InternalQueryArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.postgres import PostgresSink

HERE = Path(__file__).resolve().parent
SCHEMA = [("id", "bigint"), ("name", "text"), ("event_day", "date")]


def prepare(observer: Any, schema: str, rows: int) -> dict[str, Any]:
    observer.execute(f"CREATE SCHEMA {schema}")
    observer.execute(
        f"CREATE TABLE {schema}.target(id bigint,name text NOT NULL CHECK(length(name)=128),event_day date,"
        "PRIMARY KEY(event_day,id)) PARTITION BY LIST(event_day)"
    )
    observer.execute(f"CREATE TABLE {schema}.current_rows PARTITION OF {schema}.target FOR VALUES IN ('2026-01-01')")
    observer.execute(f"CREATE TABLE {schema}.untouched PARTITION OF {schema}.target FOR VALUES IN ('2026-02-01')")
    observer.execute(
        f"INSERT INTO {schema}.target SELECT id,repeat(md5(id::text||':old'),4),DATE '2026-01-01' "
        "FROM generate_series(1,%s) AS id",
        (rows,),
    )
    observer.execute(
        f"INSERT INTO {schema}.target SELECT id,repeat(md5(id::text||':untouched'),4),DATE '2026-02-01' "
        "FROM generate_series(1,1000) AS id"
    )
    observer.execute(
        f"CREATE TABLE {schema}.source AS SELECT id::bigint,repeat(md5(id::text||':new'),4) AS name,"
        "DATE '2026-01-01' AS event_day FROM generate_series(1,%s) AS id",
        (rows,),
    )
    observer.execute(f"ANALYZE {schema}.target")
    observer.execute(f"ANALYZE {schema}.source")
    oid, size, heap_size = observer.execute(
        "SELECT %s::regclass::oid,pg_total_relation_size(%s::regclass),pg_relation_size(%s::regclass)",
        (f"{schema}.target", f"{schema}.current_rows", f"{schema}.current_rows"),
    ).fetchone()
    return {"parent_oid": oid, "partition_total_bytes": size, "partition_heap_bytes": heap_size}


def verify_rows(observer: Any, schema: str, rows: int, parent_oid: int) -> dict[str, Any]:
    actual = observer.execute(
        f"SELECT event_day::text,count(*),min(id),max(id),sum(id),"
        "bool_and(name=repeat(md5(id::text||CASE WHEN event_day=DATE '2026-01-01' THEN ':new' ELSE ':untouched' END),4)) "
        f"FROM {schema}.target GROUP BY event_day ORDER BY event_day"
    ).fetchall()
    expected = [
        ("2026-01-01", rows, 1, rows, rows * (rows + 1) // 2, True),
        ("2026-02-01", 1000, 1, 1000, 500500, True),
    ]
    assert actual == expected, "Committed rows/payloads must exactly match the synthetic input and untouched child"
    assert observer.execute("SELECT %s::regclass::oid", (f"{schema}.target",)).fetchone()[0] == parent_oid
    assert (
        observer.execute(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname LIKE 'stg_%%'",
            (schema,),
        ).fetchone()[0]
        == 0
    )
    return {"groups": actual, "parent_oid_preserved": True, "no_staging_leaks": True}


def measure(settings: dict[str, Any], observer: Any, schema: str, rows: int, attempt: int, parent_oid: int) -> dict:
    reader = observer_connection(settings)
    reader.execute("SET statement_timeout='300s'")
    connector = TimedPostgresConnector(settings, observer, reader.info.backend_pid)
    connector.connection.execute("SET statement_timeout='300s'")
    connector.connection.execute("SET lock_timeout='30s'")
    old_child = observer.execute(
        f"SELECT tableoid::regclass::text FROM {schema}.target WHERE event_day=DATE '2026-01-01' LIMIT 1"
    ).fetchone()[0]
    connector.old_child_count_sql = f"SELECT COUNT(*) FROM {old_child}"
    reader_result: dict[str, Any] = {}
    worker = Thread(target=timed_reader, args=(reader, connector.lock_acquired, schema, reader_result), daemon=True)
    config = LoadConfig(
        source_conn_id="synthetic-source",
        target_conn_id="synthetic-target",
        source_schema=schema,
        source_table="source",
        target_schema=schema,
        target_table="target",
        staging_schema=schema,
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        log_sample_rows=0,
        partition={"column": "event_day", "values_from_staging": True, "native_mode": "required"},
        options={"technical_columns": "forbidden"},
    )
    payload = LoadPayload(InternalQueryArtifact(f"SELECT id,name,event_day FROM {schema}.source"), SCHEMA)
    worker.start()
    try:
        start = perf_counter_ns()
        result = PostgresSink(connector, None).load(config, payload)
        elapsed = perf_counter_ns() - start
        worker.join(30)
        assert not worker.is_alive() and "error_type" not in reader_result
        assert reader_result["observed_rows"] == rows + 1000
        assert connector.reader_blocked_by_sink, "Must observe the competing reader waiting on the sink lock"
        public: dict[str, Any] = {}
        populate_success_result(public, result, validation_info=None, reconciliation_metrics=None)
        assert public["loaded_rows"] == public["replaced_rows"] == public["staging_rows"] == rows
        assert public["hard_deleted_rows"] == rows and public["final_rows"] == rows + 1000
        counts = [op for op in connector.operations if op.get("old_child_count")]
        assert len(counts) == 1 and counts[0]["count"] == rows
        assert connector.commit_ack_ns and connector.lock_acquired_ns
        lock_ns = connector.commit_ack_ns - connector.lock_acquired_ns
        count_ns = counts[0]["elapsed_ns"]
        return {
            "status": "PASS",
            "rows": rows,
            "attempt": attempt,
            "elapsed_seconds": elapsed / 1e9,
            "rows_per_second": rows / (elapsed / 1e9),
            "lock_seconds": lock_ns / 1e9,
            "old_count_seconds": count_ns / 1e9,
            "old_count_share_of_lock": count_ns / lock_ns,
            "observer_probe_seconds": connector.observer_probe_ns / 1e9,
            "reader_elapsed_seconds": (reader_result["finished_ns"] - reader_result["started_ns"]) / 1e9,
            "reader_blocked_by_sink": True,
            "reader": reader_result,
            "load_result": asdict(result),
            "public_result": public,
            "verification": verify_rows(observer, schema, rows, parent_oid),
            "operations": connector.operations,
        }
    finally:
        connector.close()
        connector.lock_acquired.set()
        worker.join(30)
        reader.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, nargs="+", default=[100_000, 1_000_000, 5_000_000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert all(0 < rows <= 10_000_000 for rows in args.rows) and 1 <= args.repeats <= 5
    args.output.mkdir(parents=True, exist_ok=False)
    identity = source_identity()
    producers = {
        name: hashlib.sha256((HERE / name).read_bytes()).hexdigest()
        for name in (Path(__file__).name, "partition_benchmark_support.py")
    }
    settings, environment = docker_fixture()
    report: dict[str, Any] = {
        "status": "RUNNING",
        "started_at": datetime.now(UTC).isoformat(),
        "source": identity,
        "producer_sha256": producers,
        "environment": environment,
        "cases": [],
        "method": "Server-generated synthetic 128-byte payload, PK/CHECK, native required, one replaced child plus 1000 untouched rows; first change then same-input replays; no cache flushing; verification excluded from load timings.",
        "limits": "Local Docker measurements only; no production SLA or cold-cache claim; 3-point summaries are not p95 estimates. Lock timing includes a measured observer probe before commit.",
    }
    output = args.output / "report.json"
    with observer_connection(settings) as observer:
        report["postgres_settings"] = dict(
            observer.execute(
                "SELECT name,setting FROM pg_settings WHERE name IN "
                "('server_version','shared_buffers','work_mem','maintenance_work_mem','max_parallel_workers_per_gather','fsync','synchronous_commit','full_page_writes')"
            ).fetchall()
        )
        try:
            for rows in args.rows:
                schema = f"bench_pg_{uuid.uuid4().hex[:16]}"
                print(f"Preparing {rows:,} rows in owned schema {schema}", flush=True)
                try:
                    physical = prepare(observer, schema, rows)
                    for attempt in range(1, args.repeats + 1):
                        case = {
                            **measure(settings, observer, schema, rows, attempt, physical["parent_oid"]),
                            **physical,
                            "schema": schema,
                        }
                        report["cases"].append(case)
                        output.write_text(json.dumps(report, indent=2, default=str) + "\n")
                        print(
                            f"PASS rows={rows} attempt={attempt} load={case['elapsed_seconds']:.3f}s lock={case['lock_seconds']:.3f}s count={case['old_count_seconds']:.3f}s",
                            flush=True,
                        )
                finally:
                    observer.execute(f"DROP TABLE IF EXISTS {schema}.target")
                    observer.execute(f"DROP TABLE IF EXISTS {schema}.source")
                    observer.execute(f"DROP SCHEMA IF EXISTS {schema}")
            assert source_identity() == identity, "Execution inputs changed during the benchmark"
            assert all(
                hashlib.sha256((HERE / name).read_bytes()).hexdigest() == digest for name, digest in producers.items()
            )
            report["status"] = "PASS"
            report["summary"] = [
                {
                    "rows": rows,
                    **{
                        metric: {"min": min(values), "median": statistics.median(values), "max": max(values)}
                        for metric in ("elapsed_seconds", "lock_seconds", "old_count_seconds", "rows_per_second")
                        if (values := [case[metric] for case in report["cases"] if case["rows"] == rows])
                    },
                }
                for rows in args.rows
            ]
        except BaseException as error:
            report.update(status="FAIL", error_type=type(error).__name__)
            raise
        finally:
            report["finished_at"] = datetime.now(UTC).isoformat()
            output.write_text(json.dumps(report, indent=2, default=str) + "\n")


if __name__ == "__main__":
    main()
