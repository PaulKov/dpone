#!/usr/bin/env python
"""Run a repeatable MSSQL/Postgres/ClickHouse benchmark matrix.

The suite wraps ``tools/mssql_stress.py`` and writes one JSON result per
scenario plus a machine-readable summary. It is intended for long-running
x86_64 runners where SQL Server, PostgreSQL, ClickHouse, ODBC Driver 18, and
``bcp`` are installed natively.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dpone.strategy_intelligence.benchmark_certification import (
    NativeBenchmarkCertificationPolicy,
    NativeBenchmarkCertificationService,
)
from dpone.strategy_intelligence.benchmark_matrix import (
    BenchmarkScenario,
    build_benchmark_scenarios,
    parse_csv_ints,
)


@dataclass(frozen=True)
class ScenarioResult:
    name: str
    rows: int
    partitions: int
    export_workers: int
    load_workers: int
    returncode: int
    seconds: float
    output_file: str
    certification: dict[str, Any] | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dpone MSSQL benchmark/SLO suite")
    parser.add_argument("--rows", default="1000000,15000000", help="Comma-separated row counts")
    parser.add_argument("--partitions", default="1", help="Comma-separated partition counts (safe default: 1)")
    parser.add_argument("--workers", type=int, default=1, help="Legacy default for export/load workers")
    parser.add_argument("--export-workers", default=None, help="Comma-separated source export worker counts")
    parser.add_argument("--load-workers", default=None, help="Comma-separated target load worker counts")
    parser.add_argument("--batch-size", type=int, default=250000)
    parser.add_argument("--bcp-path", default="bcp")
    parser.add_argument("--optimizer-profile", choices=["high_throughput_safe"], default=None)
    parser.add_argument("--output-dir", default="/tmp/dpone-benchmarks")
    parser.add_argument("--slo-pg-mssql-rps", type=float, default=None)
    parser.add_argument("--slo-mssql-clickhouse-rps", type=float, default=None)
    parser.add_argument("--clickhouse-bulk-mode", default="auto", choices=["auto", "python", "client", "http"])
    parser.add_argument("--clickhouse-client-command", default=None)
    parser.add_argument("--clickhouse-client-host", default=None)
    parser.add_argument("--clickhouse-client-port", type=int, default=None)
    parser.add_argument("--clickhouse-http-host", default=None)
    parser.add_argument("--clickhouse-http-port", type=int, default=None)
    parser.add_argument(
        "--markdown-output",
        default=None,
        help="Optional Markdown evidence file. Defaults to <output-dir>/summary.md when omitted.",
    )
    parser.add_argument("--continue-on-fail", action="store_true")
    parser.add_argument("--stress-script", default=str(Path(__file__).with_name("mssql_stress.py")))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results: list[ScenarioResult] = []
    failed = False
    for scenario in iter_scenarios(args, output_dir):
        started = _perf_counter()
        completed = subprocess.run(build_command(args, scenario), check=False)
        seconds = _perf_counter() - started
        result = ScenarioResult(
            name=scenario.name,
            rows=scenario.rows,
            partitions=scenario.partitions,
            export_workers=scenario.export_workers,
            load_workers=scenario.load_workers,
            returncode=completed.returncode,
            seconds=round(seconds, 3),
            output_file=scenario.output_file,
            certification=certify_scenario_output(args, scenario) if Path(scenario.output_file).exists() else None,
        )
        results.append(result)
        if completed.returncode != 0:
            failed = True
            if not args.continue_on_fail:
                break

    summary: dict[str, Any] = {
        "failed": failed,
        "certification_passed": all(
            bool(result.certification and result.certification.get("passed")) for result in results
        )
        if results
        else False,
        "results": [asdict(result) for result in results],
    }
    summary_file = output_dir / "summary.json"
    summary_file.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    markdown_file = Path(args.markdown_output) if args.markdown_output else output_dir / "summary.md"
    markdown_file.parent.mkdir(parents=True, exist_ok=True)
    markdown_file.write_text(render_markdown_summary(summary), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 1 if failed else 0


def iter_scenarios(args: argparse.Namespace, output_dir: Path) -> list[BenchmarkScenario]:
    return build_benchmark_scenarios(
        rows=parse_csv_ints(args.rows),
        partitions=parse_csv_ints(args.partitions),
        export_workers=parse_csv_ints(args.export_workers, default=args.workers),
        load_workers=parse_csv_ints(args.load_workers, default=args.workers),
        output_dir=output_dir,
    )


def build_command(args: argparse.Namespace, scenario: BenchmarkScenario) -> list[str]:
    command = [
        sys.executable,
        args.stress_script,
        "--rows",
        str(scenario.rows),
        "--batch-size",
        str(args.batch_size),
        "--bcp-path",
        args.bcp_path,
        "--clickhouse-bulk-mode",
        args.clickhouse_bulk_mode,
        "--json-output",
        scenario.output_file,
    ]
    if args.optimizer_profile:
        command += ["--optimizer-profile", args.optimizer_profile]
    if args.clickhouse_client_command:
        command += ["--clickhouse-client-command", args.clickhouse_client_command]
    if args.clickhouse_client_host:
        command += ["--clickhouse-client-host", args.clickhouse_client_host]
    if args.clickhouse_client_port:
        command += ["--clickhouse-client-port", str(args.clickhouse_client_port)]
    if args.clickhouse_http_host:
        command += ["--clickhouse-http-host", args.clickhouse_http_host]
    if args.clickhouse_http_port:
        command += ["--clickhouse-http-port", str(args.clickhouse_http_port)]
    if scenario.partitions > 1:
        command += [
            "--partition-column",
            "id",
            "--lower-bound",
            "1",
            "--upper-bound",
            str(scenario.rows),
            "--num-partitions",
            str(scenario.partitions),
            "--export-workers",
            str(scenario.export_workers),
            "--load-workers",
            str(scenario.load_workers),
        ]
    if args.slo_pg_mssql_rps is not None:
        command += ["--slo-pg-mssql-rps", str(args.slo_pg_mssql_rps)]
    if args.slo_mssql_clickhouse_rps is not None:
        command += ["--slo-mssql-clickhouse-rps", str(args.slo_mssql_clickhouse_rps)]
    return command


def certify_scenario_output(args: argparse.Namespace, scenario: BenchmarkScenario) -> dict[str, Any]:
    artifact = json.loads(Path(scenario.output_file).read_text(encoding="utf-8"))
    result = NativeBenchmarkCertificationService().certify(
        artifact,
        NativeBenchmarkCertificationPolicy(
            min_rows=scenario.rows,
            min_postgres_to_mssql_rows_per_second=args.slo_pg_mssql_rps,
            min_mssql_to_clickhouse_rows_per_second=args.slo_mssql_clickhouse_rps,
        ),
    )
    return result.to_dict()


def render_markdown_summary(summary: dict[str, Any], *, generated_at: str | None = None) -> str:
    """Render human-readable benchmark certification evidence."""

    generated = generated_at or datetime.now(UTC).isoformat()
    lines = [
        "# Native transfer benchmark certification",
        "",
        f"- generated_at: `{generated}`",
        f"- failed: `{bool(summary.get('failed'))}`",
        f"- certification_passed: `{bool(summary.get('certification_passed'))}`",
        "",
        "| Scenario | Rows | Partitions | Export workers | Load workers | Run | Certification | PG -> MSSQL rps | MSSQL -> CH rps | Bottleneck |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | --- |",
    ]
    for item in summary.get("results", []):
        certification = item.get("certification") or {}
        rows_per_second = certification.get("rows_per_second") or {}
        lines.append(
            "| "
            f"`{item.get('name')}` | "
            f"{_fmt_int(item.get('rows'))} | "
            f"{_fmt_int(item.get('partitions'))} | "
            f"{_fmt_int(item.get('export_workers'))} | "
            f"{_fmt_int(item.get('load_workers'))} | "
            f"{_status_from_returncode(item.get('returncode'))} | "
            f"{_status_bool(certification.get('passed'))} | "
            f"{_fmt_float(rows_per_second.get('postgres_to_mssql'))} | "
            f"{_fmt_float(rows_per_second.get('mssql_to_clickhouse'))} | "
            f"`{certification.get('bottleneck_phase') or 'n/a'}` |"
        )
    lines.extend(
        [
            "",
            "## How to interpret",
            "",
            "- `PG -> MSSQL rps` is the certified native PostgreSQL COPY -> SQL Server bcp path.",
            "- `MSSQL -> CH rps` is included when the suite also exercises the downstream ClickHouse leg.",
            "- A scenario is release-ready only when both `Run` and `Certification` are `passed`.",
            "- Use bottleneck phase diagnostics to decide whether to tune source export, target load/finalizer, or reconciliation.",
            "",
        ]
    )
    return "\n".join(lines)


def _perf_counter() -> float:
    import time

    return time.perf_counter()


def _status_from_returncode(value: object) -> str:
    return "passed" if int(value or 0) == 0 else "failed"


def _status_bool(value: object) -> str:
    return "passed" if bool(value) else "failed"


def _fmt_int(value: object) -> str:
    return f"{int(value or 0):,}"


def _fmt_float(value: object) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):,.2f}"


if __name__ == "__main__":
    raise SystemExit(main())
