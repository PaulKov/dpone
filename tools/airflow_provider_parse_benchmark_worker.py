#!/usr/bin/env python3
"""Sterile process-cold worker for the installed-provider parse benchmark."""

from __future__ import annotations

import json
import resource
import sys
import time
from collections.abc import Mapping
from importlib import import_module
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import Any

_PROCESS_COLD_STARTED = time.perf_counter()


def inspect_parse_topology(
    namespace: Mapping[str, Any],
    contract: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Require exact DAG, spec, task, edge, and digest authority."""

    from tools.airflow_provider_parse_benchmark_support import BenchmarkIntegrityError, topology_summary

    expected = _contract_records(contract)
    if tuple(sorted(str(key) for key in namespace)) != tuple(item["dag_id"] for item in expected):
        raise BenchmarkIntegrityError("provider parse did not materialize the exact DAG namespace")
    observed: list[dict[str, Any]] = []
    for item in expected:
        dag_id = item["dag_id"]
        dag = namespace[dag_id]
        tasks = list(getattr(dag, "tasks", ()) or ())
        task_ids = tuple(sorted(str(getattr(task, "task_id", "")) for task in tasks))
        fingerprint = getattr(dag, "_dpone_spec_fingerprint", None)
        edges = _observed_edges(tasks, frozenset(task_ids))
        checks = (
            (bool(tasks), "materialized zero runtime tasks"),
            (len(task_ids) == len(set(task_ids)) and task_ids == tuple(item["task_ids"]), "exact task IDs differ"),
            (fingerprint == item["spec_fingerprint"], "exact spec fingerprints differ"),
            (edges == tuple(tuple(edge) for edge in item["edges"]), "exact edges differ"),
        )
        for passed, message in checks:
            if not passed:
                raise BenchmarkIntegrityError(f"DAG {dag_id!r} {message} from the fixture")
        observed.append(
            {
                "dag_id": dag_id,
                "spec_fingerprint": fingerprint,
                "task_ids": list(task_ids),
                "edges": [list(edge) for edge in edges],
            }
        )
    summary, expected_summary = topology_summary(observed), topology_summary(expected)
    if summary != expected_summary:
        raise BenchmarkIntegrityError("provider parse topology digest differs from the fixture")
    return {
        "status": "PASS",
        "passed": True,
        **summary,
        "workload_ids_match": True,
        "topology_fingerprint": summary["topology_sha256"],
        "expected_topology_fingerprint": expected_summary["topology_sha256"],
    }


def load_expected_topology(index_path: Path) -> dict[str, dict[str, Any]]:
    """Load exact DAG/spec/task/edge authority from checksum-bound bytes."""

    from tools.airflow_provider_parse_benchmark_fixture import _json_object, _verified_artifact_bytes
    from tools.airflow_provider_parse_benchmark_support import BenchmarkIntegrityError, expected_layout

    artifacts = _json_object(index_path.read_bytes()).get("dag_specs")
    if not isinstance(artifacts, list):
        raise BenchmarkIntegrityError("benchmark index has no DAG topology authority")
    topology: dict[str, dict[str, Any]] = {}
    for descriptor in artifacts:
        spec = _json_object(_verified_artifact_bytes(descriptor, index_path.parents[3]))
        dag_id, record = _topology_record(spec)
        if dag_id in topology:
            raise BenchmarkIntegrityError("benchmark DAG topology authority is malformed")
        topology[dag_id] = record
    if tuple(sorted(topology)) != tuple(expected_layout()):
        raise BenchmarkIntegrityError("benchmark DAG topology authority has extras or omissions")
    return topology


def _topology_record(spec: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    from tools.airflow_provider_parse_benchmark_support import RUNTIME_SUFFIX, BenchmarkIntegrityError

    dag_id, nodes, edges = spec.get("dag_id"), spec.get("nodes"), spec.get("edges")
    fingerprint = spec.get("spec_fingerprint")
    if not isinstance(dag_id, str) or not isinstance(nodes, list) or not isinstance(edges, list):
        raise BenchmarkIntegrityError("benchmark DAG topology authority is malformed")
    if not isinstance(fingerprint, str):
        raise BenchmarkIntegrityError("benchmark DAG topology authority is malformed")
    tasks = {
        str(node.get("node_id")): f"{node.get('workload_id')}{RUNTIME_SUFFIX}"
        for node in nodes
        if isinstance(node, Mapping)
        and isinstance(node.get("node_id"), str)
        and isinstance(node.get("workload_id"), str)
    }
    if len(tasks) != len(nodes):
        raise BenchmarkIntegrityError("benchmark DAG topology nodes are malformed")
    declared: list[list[str]] = []
    for edge in edges:
        upstream = tasks.get(str(edge.get("upstream"))) if isinstance(edge, Mapping) else None
        downstream = tasks.get(str(edge.get("downstream"))) if isinstance(edge, Mapping) else None
        if upstream is None or downstream is None:
            raise BenchmarkIntegrityError("benchmark DAG topology edge is outside the DAG")
        declared.append([upstream, downstream])
    return dag_id, {"spec_fingerprint": fingerprint, "task_ids": sorted(tasks.values()), "edges": sorted(declared)}


def _contract_records(
    contract: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    records = [
        {
            "dag_id": dag_id,
            "spec_fingerprint": item.get("spec_fingerprint"),
            "task_ids": sorted(str(value) for value in item.get("task_ids", ())),
            "edges": sorted(
                [str(edge[0]), str(edge[1])]
                for edge in item.get("edges", ())
                if isinstance(edge, (list, tuple)) and len(edge) == 2
            ),
        }
        for dag_id, item in contract.items()
    ]
    return sorted(records, key=lambda item: item["dag_id"])


def _observed_edges(
    tasks: list[Any],
    task_ids: frozenset[str],
) -> tuple[tuple[str, str], ...]:
    from tools.airflow_provider_parse_benchmark_support import BenchmarkIntegrityError

    edges: set[tuple[str, str]] = set()
    for task in tasks:
        task_id = str(task.task_id)
        edges.update((task_id, str(value)) for value in getattr(task, "downstream_task_ids", ()))
        edges.update((str(value), task_id) for value in getattr(task, "upstream_task_ids", ()))
    if any(source not in task_ids or target not in task_ids for source, target in edges):
        raise BenchmarkIntegrityError("DAG exact edges reference a task outside the fixture")
    return tuple(sorted(edges))


def run_worker(request: Mapping[str, Any]) -> dict[str, Any]:
    """Execute one first parse and optional same-process replay."""

    repo_root = Path(__file__).resolve().parents[1]
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())
    imports_at_clock = _imports_at_process_clock()

    from tools.airflow_provider_parse_benchmark_evidence import failure_diagnostic
    from tools.airflow_provider_parse_benchmark_fixture import verify_index_digest
    from tools.airflow_provider_parse_benchmark_support import DEPLOYMENT_ID, RELEASE_ID
    from tools.airflow_provider_parse_benchmark_tripwire import ParseSideEffectAttempt, ParseSideEffectTripwire

    timing_names = "process_cold_seconds cold_loader_seconds import_plus_first_load_seconds warm_loader_seconds".split()
    timings: dict[str, float | None] = dict.fromkeys(timing_names)
    parses: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, str]] = []
    loader_module: str | None = None
    rss_baseline: int | None = None
    tripwire = ParseSideEffectTripwire()
    index_path = Path(str(request.get("index_path") or ""))
    expected_digest = str(request.get("expected_index_sha256") or "")
    topology = request.get("expected_topology")
    measure_cold = request.get("measure_cold") is True
    measure_warm = request.get("measure_warm") is True
    try:
        if not index_path.is_absolute() or not isinstance(topology, Mapping):
            raise ValueError("sterile worker request is incomplete")
        with tripwire:
            rss_baseline = _rss_bytes()
            tripwire.arm_installed_environment()
            verify_index_digest(index_path, expected_digest)
            loader = getattr(
                import_module("airflow.providers.dpone"),
                "load_dpone_dags",
            )
            loader_module = getattr(loader, "__module__", None)

            def load_once() -> dict[str, Any]:
                return _load_once(
                    loader,
                    index_path,
                    topology,
                    expected_digest,
                    release_id=RELEASE_ID,
                    deployment_id=DEPLOYMENT_ID,
                )

            load_started = time.perf_counter()
            first = load_once()
            load_finished = time.perf_counter()
            parses["first"] = first
            if measure_cold:
                timings["cold_loader_seconds"] = load_finished - load_started
                elapsed = load_finished - _PROCESS_COLD_STARTED
                timings["process_cold_seconds"] = elapsed
                timings["import_plus_first_load_seconds"] = elapsed
            if measure_warm:
                verify_index_digest(index_path, expected_digest)
                warm_started = time.perf_counter()
                parses["warm"] = load_once()
                timings["warm_loader_seconds"] = time.perf_counter() - warm_started
            tripwire.record_forbidden_imports(set(imports_at_clock), set(sys.modules))
    except Exception as exc:  # noqa: BLE001 - failure is bounded evidence.
        tripwire.record_forbidden_imports(set(imports_at_clock), set(sys.modules))
        code = (
            "DPONE_AIRFLOW_PARSE_SIDE_EFFECT"
            if isinstance(exc, ParseSideEffectAttempt)
            else "DPONE_AIRFLOW_PARSE_BENCHMARK_FAILED"
        )
        failures.append(failure_diagnostic(exc, stage="provider_parse", code=code))
        if measure_cold:
            elapsed = time.perf_counter() - _PROCESS_COLD_STARTED
            timings["process_cold_seconds"] = elapsed
            timings["import_plus_first_load_seconds"] = elapsed

    side_effects = tripwire.report()
    complete = "first" in parses and (not measure_warm or "warm" in parses)
    passed = not failures and complete and side_effects["passed"] is True
    rss_peak = _rss_bytes()
    return {
        "status": "PASS" if passed else "FAIL",
        **{key: round(value, 9) if value is not None else None for key, value in timings.items()},
        "measurement": {
            "clock_started_before_forbidden_imports": not imports_at_clock,
            "pre_clock_forbidden_imports": imports_at_clock,
        },
        "rss": {
            "baseline_bytes": rss_baseline,
            "peak_bytes": rss_peak,
            "additional_bytes": max(0, rss_peak - rss_baseline) if rss_baseline is not None else None,
        },
        "side_effects": side_effects,
        "parses": parses,
        "loader_module": loader_module,
        "import_origins": _import_origins(),
        "termination": _completed_termination(),
        "failures": failures,
    }


def _load_once(
    loader: Any,
    index_path: Path,
    topology: Mapping[str, Any],
    expected_digest: str,
    *,
    release_id: str,
    deployment_id: str,
) -> dict[str, Any]:
    from tools.airflow_provider_parse_benchmark_fixture import verify_index_digest
    from tools.airflow_provider_parse_benchmark_support import BenchmarkIntegrityError

    namespace: dict[str, Any] = {}
    report = loader(namespace, index_path=index_path)
    expected_dags = tuple(sorted(str(value) for value in topology))
    if (
        tuple(report.loaded) != expected_dags
        or report.errors
        or report.skipped
        or report.fatal
        or report.release_id != release_id
        or report.deployment_id != deployment_id
    ):
        raise BenchmarkIntegrityError("provider loader did not load the complete pinned benchmark fixture")
    parsed = inspect_parse_topology(namespace, topology)
    parsed["index_sha256"] = verify_index_digest(index_path, expected_digest)
    return parsed


def _imports_at_process_clock() -> list[str]:
    forbidden_roots = (
        "airflow",
        "dpone_airflow_pack",
        "kubernetes",
        "tools.airflow_provider_parse_benchmark_fixture",
    )
    return sorted(
        name
        for name in sys.modules
        if name in forbidden_roots or name.startswith(tuple(f"{root}." for root in forbidden_roots))
    )


def _import_origins() -> dict[str, dict[str, Any]]:
    expected = {
        "airflow.providers.cncf.kubernetes": "apache-airflow-providers-cncf-kubernetes",
        "airflow.providers.dpone": "apache-airflow-providers-dpone",
        "dpone_airflow_pack": "dpone-airflow-pack",
    }
    return {
        module_name: _import_origin(module_name, distribution_name)
        for module_name, distribution_name in expected.items()
    }


def _import_origin(module_name: str, distribution_name: str) -> dict[str, Any]:
    try:
        module = import_module(module_name)
        package = distribution(distribution_name)
        origin = Path(str(module.__file__)).resolve(strict=True)
        distribution_root = Path(package.locate_file("")).resolve(strict=True)
        relative = origin.relative_to(distribution_root)
        installed_files = {Path(package.locate_file(item)).resolve(strict=False) for item in (package.files or ())}
        within_environment = origin in installed_files and origin.is_relative_to(Path(sys.prefix).resolve(strict=True))
        return {
            "distribution": distribution_name,
            "version": package.version,
            "relative_path": relative.as_posix(),
            "within_environment": within_environment,
        }
    except (AttributeError, ImportError, OSError, PackageNotFoundError, ValueError):
        return {
            "distribution": distribution_name,
            "version": None,
            "relative_path": None,
            "within_environment": False,
        }


def _rss_bytes() -> int:
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(value if sys.platform == "darwin" else value * 1024)


def _completed_termination() -> dict[str, Any]:
    return {"status": "completed", "terminate_sent": False, "kill_sent": False}


def _read_request(path: Path, maximum: int) -> dict[str, Any]:
    raw = path.read_bytes()
    if not raw or len(raw) > maximum:
        raise ValueError("sterile worker request exceeds its size boundary")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("sterile worker request must be an object")
    return payload


def _write_result(path: Path, payload: Mapping[str, Any], maximum: int) -> None:
    from tools.airflow_provider_parse_benchmark_evidence import redact_evidence

    public = redact_evidence(payload)
    raw = json.dumps(public, allow_nan=False, separators=(",", ":"), sort_keys=True).encode()
    if len(raw) > maximum:
        raise ValueError("sterile worker result exceeds its size boundary")
    path.write_bytes(raw)


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if len(arguments) != 2:
        return 2
    repo_root = Path(__file__).resolve().parents[1]
    if repo_root.as_posix() not in sys.path:
        sys.path.insert(0, repo_root.as_posix())
    from tools.airflow_provider_parse_benchmark_support import MAX_WORKER_REQUEST_BYTES, MAX_WORKER_RESULT_BYTES

    output_path = Path(arguments[1])
    try:
        request = _read_request(Path(arguments[0]), MAX_WORKER_REQUEST_BYTES)
        result = run_worker(request)
        _write_result(output_path, result, MAX_WORKER_RESULT_BYTES)
    except Exception as exc:  # noqa: BLE001 - never serialize the raw request.
        from tools.airflow_provider_parse_benchmark_evidence import failure_diagnostic

        failure = {
            "status": "FAIL",
            "process_cold_seconds": round(time.perf_counter() - _PROCESS_COLD_STARTED, 9),
            "measurement": {
                "clock_started_before_forbidden_imports": not _imports_at_process_clock(),
                "pre_clock_forbidden_imports": _imports_at_process_clock(),
            },
            "termination": _completed_termination(),
            "failures": [
                failure_diagnostic(
                    exc,
                    stage="sterile_worker",
                    code="DPONE_AIRFLOW_PARSE_WORKER_FAILED",
                )
            ],
        }
        _write_result(output_path, failure, MAX_WORKER_RESULT_BYTES)
        return 1
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
