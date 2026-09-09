"""Plan GitHub Actions matrix legs for route-live-wide-certification.

Emits docker/bq matrix JSON for ``fromJson`` strategy inputs. BigQuery legs are
never planned for ``schedule`` events.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass

DOCKER_SOURCES = ("postgres", "mysql", "mssql")
DOCKER_SINKS = ("postgres", "mssql", "clickhouse", "kafka")


@dataclass(frozen=True, slots=True)
class RouteLiveWidePlan:
    docker_include: list[dict[str, str]]
    bq_include: list[dict[str, str]]

    @property
    def run_docker(self) -> bool:
        return bool(self.docker_include)

    @property
    def run_bq(self) -> bool:
        return bool(self.bq_include)

    def docker_matrix(self) -> dict[str, list[dict[str, str]]]:
        return {"include": list(self.docker_include)}

    def bq_matrix(self) -> dict[str, list[dict[str, str]]]:
        return {"include": list(self.bq_include)}


def plan_route_live_wide_matrix(
    *,
    event_name: str,
    source: str = "all",
    sink: str = "all",
    include_bigquery: bool = False,
) -> RouteLiveWidePlan:
    """Expand intentional CI legs. Raises ValueError for invalid dispatch combos."""
    source = (source or "all").strip().lower()
    sink = (sink or "all").strip().lower()
    if event_name == "schedule":
        return RouteLiveWidePlan(
            docker_include=[{"source": src, "sink": snk} for src in DOCKER_SOURCES for snk in DOCKER_SINKS],
            bq_include=[],
        )

    if event_name not in {"workflow_dispatch", "workflow_call"}:
        raise ValueError(f"unsupported event_name: {event_name!r}")

    if sink == "bigquery" and not include_bigquery:
        raise ValueError("sink=bigquery requires include_bigquery=true")

    sources = list(DOCKER_SOURCES) if source == "all" else [source]
    if any(item not in DOCKER_SOURCES for item in sources):
        raise ValueError(f"unsupported source filter: {source!r}")

    docker_include: list[dict[str, str]] = []
    if sink == "bigquery":
        docker_sinks: list[str] = []
    elif sink == "all":
        docker_sinks = list(DOCKER_SINKS)
    else:
        if sink not in DOCKER_SINKS:
            raise ValueError(f"unsupported sink filter: {sink!r}")
        docker_sinks = [sink]

    for src in sources:
        for snk in docker_sinks:
            docker_include.append({"source": src, "sink": snk})

    bq_include: list[dict[str, str]] = []
    if include_bigquery and sink in {"all", "bigquery"}:
        for src in sources:
            bq_include.append({"source": src})

    return RouteLiveWidePlan(docker_include=docker_include, bq_include=bq_include)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-name", required=True)
    parser.add_argument("--source", default="all")
    parser.add_argument("--sink", default="all")
    parser.add_argument("--include-bigquery", action="store_true")
    parser.add_argument(
        "--github-output",
        action="store_true",
        help="Write docker_matrix/bq_matrix/run_* lines for GITHUB_OUTPUT",
    )
    args = parser.parse_args(argv)

    try:
        plan = plan_route_live_wide_matrix(
            event_name=args.event_name,
            source=args.source,
            sink=args.sink,
            include_bigquery=args.include_bigquery,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = {
        "docker_matrix": plan.docker_matrix(),
        "bq_matrix": plan.bq_matrix(),
        "run_docker": plan.run_docker,
        "run_bq": plan.run_bq,
    }
    if args.github_output:
        docker_matrix = json.dumps(plan.docker_matrix(), separators=(",", ":"))
        bq_matrix = json.dumps(plan.bq_matrix(), separators=(",", ":"))
        # Delimiter form is required so JSON quotes survive GITHUB_OUTPUT parsing.
        sys.stdout.write(
            "\n".join(
                [
                    "docker_matrix<<EOF",
                    docker_matrix,
                    "EOF",
                    "bq_matrix<<EOF",
                    bq_matrix,
                    "EOF",
                    f"run_docker={'true' if plan.run_docker else 'false'}",
                    f"run_bq={'true' if plan.run_bq else 'false'}",
                    "",
                ]
            )
        )
        return 0

    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
