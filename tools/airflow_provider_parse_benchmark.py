#!/usr/bin/env python3
"""Benchmark installed Airflow provider parsing with fail-closed evidence."""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import replace
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

if not __package__:  # Direct ``python tools/...py`` execution.
    sys.path.insert(0, Path(__file__).resolve().parent.parent.as_posix())

# The CLI intentionally re-exports its split support surface for compatibility.
# ruff: noqa: E402, F401
# isort: off
from tools import airflow_provider_parse_benchmark_evidence as _evidence_support  # noqa: E402
from tools.airflow_provider_parse_benchmark_evidence import evaluate_fixture_integrity  # noqa: E402
from tools.airflow_provider_parse_benchmark_fixture import build_fixture  # noqa: E402
from tools.airflow_provider_parse_benchmark_fixture import inspect_fixture_index  # noqa: E402
from tools.airflow_provider_parse_benchmark_fixture import load_expected_topology, runtime_artifact_delivery  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import DAG_COUNT, DEFAULT_SAMPLES, DEPLOYMENT_ID  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import MAX_SAMPLES, MAX_WORKER_REQUEST_BYTES  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import MAX_WORKER_RESULT_BYTES, MAX_WORKER_TIMEOUT_SECONDS  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import RELEASE_ID, RUNTIME_SUFFIX, WORKLOAD_COUNT  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import WORKLOADS_PER_DAG, BenchmarkConfig, BenchmarkContext  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import BenchmarkIntegrityError, evaluate_duration_budget  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import canonical_json_sha256, evaluate_rss_budget  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import forbidden_parse_imports, nearest_rank_percentile  # noqa: E402
from tools.airflow_provider_parse_benchmark_support import resolved_temp_root  # noqa: E402
from tools.airflow_provider_parse_benchmark_tripwire import ParseSideEffectAttempt, ParseSideEffectTripwire  # noqa: E402
from tools.airflow_provider_parse_benchmark_worker import inspect_parse_topology  # noqa: E402
# isort: on

DEFAULT_OUTPUT = Path("test_artifacts/airflow-self-service-global-review-v0731/airflow-provider-parse-benchmark.json")
_WORKER_TERMINATION_GRACE_SECONDS = 0.5


def run_benchmark(
    config: BenchmarkConfig,
    context: BenchmarkContext,
) -> dict[str, Any]:
    """Run fresh-process first loads and same-process warm replays."""

    _validate_config(config)
    runner = _runner_metadata()
    matrix = _evidence_support.matrix_evidence(context, runner)
    if matrix["status"] != "PASS" or matrix["passed"] is not True:
        raise RuntimeError("installed Airflow/Python does not match the supplied matrix")
    with tempfile.TemporaryDirectory(prefix="dpone-airflow-parse-benchmark-") as directory:
        index_path, fixture = _fixture(resolved_temp_root(directory))
        if not evaluate_fixture_integrity(fixture)["passed"]:
            return _evidence_support.build_evidence(config, context, fixture, (), runner)
        expected_topology = load_expected_topology(index_path)
        workers: list[dict[str, Any]] = []
        pair_count = max(config.cold_samples, config.warm_samples)
        for index in range(pair_count):
            worker = _run_worker_process(
                {
                    "index_path": index_path.resolve(strict=True).as_posix(),
                    "expected_index_sha256": fixture["index_sha256"],
                    "expected_topology": expected_topology,
                    "measure_cold": index < config.cold_samples,
                    "measure_warm": index < config.warm_samples,
                },
                working_root=index_path.parent,
                timeout_seconds=config.worker_timeout_seconds,
            )
            workers.append(worker)
            if worker["status"] != "PASS":
                break
        return _evidence_support.build_evidence(config, context, fixture, workers, runner)


def _run_worker_process(
    request: Mapping[str, Any],
    *,
    working_root: Path,
    timeout_seconds: float,
) -> dict[str, Any]:
    """Run one isolated worker with bounded input, output, and termination."""

    working_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".dpone-parse-worker-",
        dir=working_root,
    ) as directory:
        exchange = Path(directory)
        request_path = exchange / "request.json"
        result_path = exchange / "result.json"
        raw_request = json.dumps(
            request,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(raw_request) > MAX_WORKER_REQUEST_BYTES:
            return _worker_process_failure(
                "DPONE_AIRFLOW_PARSE_WORKER_REQUEST_LIMIT",
                "sterile worker request exceeds its size boundary",
            )
        request_path.write_bytes(raw_request)
        try:
            process = subprocess.Popen(
                _worker_command(request_path, result_path),
                cwd=working_root,
                env=_sterile_worker_environment(working_root),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=os.name == "posix",
            )
        except OSError:
            return _worker_process_failure(
                "DPONE_AIRFLOW_PARSE_WORKER_START_FAILED",
                "sterile worker could not be started",
            )
        try:
            process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            terminate_sent, kill_sent = _terminate_worker(process)
            return _worker_process_failure(
                "DPONE_AIRFLOW_PARSE_WORKER_TIMEOUT",
                "sterile worker exceeded its wall-clock timeout",
                termination={
                    "status": "terminated",
                    "terminate_sent": terminate_sent,
                    "kill_sent": kill_sent,
                },
            )
        try:
            raw_result = result_path.read_bytes()
            if not raw_result or len(raw_result) > MAX_WORKER_RESULT_BYTES:
                raise ValueError
            result = json.loads(raw_result)
            if not isinstance(result, dict):
                raise ValueError
        except (OSError, ValueError, json.JSONDecodeError):
            return _worker_process_failure(
                "DPONE_AIRFLOW_PARSE_WORKER_RESULT_INVALID",
                "sterile worker did not emit one bounded result",
            )
        result["termination"] = {
            "status": "completed",
            "terminate_sent": False,
            "kill_sent": False,
        }
        return result


def _worker_command(request_path: Path, result_path: Path) -> tuple[str, ...]:
    worker = Path(__file__).with_name("airflow_provider_parse_benchmark_worker.py")
    return (
        sys.executable,
        "-I",
        worker.resolve(strict=True).as_posix(),
        request_path.as_posix(),
        result_path.as_posix(),
    )


def _sterile_worker_environment(working_root: Path) -> dict[str, str]:
    """Return a credential-free, user-site-free worker environment."""

    environment = {
        "HOME": working_root.as_posix(),
        "PYTHONHASHSEED": "0",
        "PYTHONNOUSERSITE": "1",
        "TMPDIR": working_root.as_posix(),
    }
    for name in ("LANG", "LC_ALL", "PATH", "SYSTEMROOT", "TZ"):
        value = os.environ.get(name)
        if value:
            environment[name] = value
    return environment


def _terminate_worker(process: subprocess.Popen[bytes]) -> tuple[bool, bool]:
    terminate_sent = kill_sent = False
    try:
        if os.name == "posix":
            os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        terminate_sent = True
        process.wait(timeout=_WORKER_TERMINATION_GRACE_SECONDS)
    except (OSError, subprocess.TimeoutExpired):
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            kill_sent = True
            process.wait(timeout=_WORKER_TERMINATION_GRACE_SECONDS)
        except (OSError, subprocess.TimeoutExpired):
            pass
    return terminate_sent, kill_sent


def _worker_process_failure(
    code: str,
    message: str,
    *,
    termination: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    stopped = termination or {
        "status": "not_started",
        "terminate_sent": False,
        "kill_sent": False,
    }
    return {
        "status": "FAIL",
        "process_cold_seconds": None,
        "measurement": {"clock_started_before_forbidden_imports": False, "pre_clock_forbidden_imports": []},
        "parses": {},
        "termination": dict(stopped),
        "failures": [{"code": code, "message": message, "stage": "sterile_worker", "type": "WorkerProcessError"}],
    }


_fixture = build_fixture
_runtime_artifact_delivery = runtime_artifact_delivery


def _worker_error(exc: Exception, index_path: Path) -> dict[str, str]:
    del index_path  # Physical paths are handled by the shared public redactor.
    code = (
        "DPONE_AIRFLOW_PARSE_SIDE_EFFECT"
        if isinstance(exc, ParseSideEffectAttempt)
        else "DPONE_AIRFLOW_PARSE_BENCHMARK_FAILED"
    )
    return _evidence_support.failure_diagnostic(exc, stage="provider_parse", code=code)


def _runner_metadata() -> dict[str, Any]:
    names = (
        "apache-airflow",
        "apache-airflow-providers-cncf-kubernetes",
        "apache-airflow-providers-dpone",
        "dpone-airflow-pack",
        "dpone",
    )
    installed = {name: _package_version(name) for name in names}
    return {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor() or None,
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "cpu_count": os.cpu_count(),
        "memory_total_bytes": _memory_total_bytes(),
        "package_versions": installed,
        "installed_distributions": installed,
    }


def _package_version(name: str) -> str | None:
    try:
        return version(name)
    except PackageNotFoundError:
        return None


def _memory_total_bytes() -> int | None:
    try:
        page_size = int(os.sysconf("SC_PAGE_SIZE"))
        page_count = int(os.sysconf("SC_PHYS_PAGES"))
    except (AttributeError, OSError, TypeError, ValueError):
        return None
    total = page_size * page_count
    return total if total > 0 else None


def matrix_evidence(context: BenchmarkContext) -> dict[str, Any]:
    return _evidence_support.matrix_evidence(context, _runner_metadata())


def _evidence(
    config: BenchmarkConfig,
    context: BenchmarkContext,
    fixture: Mapping[str, Any],
    workers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return _evidence_support.build_evidence(
        config,
        context,
        fixture,
        workers,
        _runner_metadata(),
    )


def _failure_evidence(
    config: BenchmarkConfig,
    context: BenchmarkContext,
    exc: Exception,
) -> dict[str, Any]:
    return _evidence_support.failure_evidence(
        config,
        context,
        exc,
        _runner_metadata(),
    )


def _validate_config(config: BenchmarkConfig) -> None:
    for name, value in (
        ("cold_samples", config.cold_samples),
        ("warm_samples", config.warm_samples),
    ):
        if not 1 <= value <= MAX_SAMPLES:
            raise ValueError(f"{name} must be between 1 and {MAX_SAMPLES}")
    if (
        isinstance(config.worker_timeout_seconds, bool)
        or not 0 < config.worker_timeout_seconds <= MAX_WORKER_TIMEOUT_SECONDS
    ):
        raise ValueError(
            f"worker_timeout_seconds must be greater than zero and no more than {MAX_WORKER_TIMEOUT_SECONDS}"
        )


def _source_commit(explicit: str | None) -> str:
    if explicit:
        return explicit
    for name in ("DPONE_BENCHMARK_SOURCE_COMMIT", "GITHUB_SHA"):
        if os.environ.get(name):
            return os.environ[name]
    try:
        return subprocess.run(
            ("git", "rev-parse", "HEAD"),
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cold-samples", type=int, default=DEFAULT_SAMPLES)
    parser.add_argument("--warm-samples", type=int, default=DEFAULT_SAMPLES)
    parser.add_argument("--worker-timeout-seconds", type=float, default=BenchmarkConfig().worker_timeout_seconds)
    parser.add_argument("--source-commit")
    matrix_arguments = (
        (
            ("--airflow-version", "--matrix-airflow-version", "--expected-airflow-version"),
            "AIRFLOW_VERSION",
            "AIRFLOW_VERSION",
        ),
        (
            ("--python-version", "--matrix-python-version", "--expected-python-version"),
            "PYTHON_VERSION",
            "PYTHON_VERSION",
        ),
        (("--support", "--matrix-support"), "SUPPORT", "AIRFLOW_SUPPORT"),
        (("--constraints", "--constraints-ref"), "CONSTRAINTS", "AIRFLOW_CONSTRAINTS"),
        (("--constraints-sha256",), "CONSTRAINTS_SHA256", None),
        (("--cncf-provider-version",), "CNCF_PROVIDER_VERSION", None),
        (("--candidate-manifest-sha256",), "CANDIDATE_MANIFEST_SHA256", None),
    )
    for flags, suffix, legacy in matrix_arguments:
        fallback = os.environ.get(legacy) if legacy else None
        parser.add_argument(*flags, default=os.environ.get(f"DPONE_BENCHMARK_{suffix}") or fallback)
    parser.add_argument(
        "--candidate-manifest",
        type=Path,
        default=(
            Path(os.environ["DPONE_BENCHMARK_CANDIDATE_MANIFEST"])
            if os.environ.get("DPONE_BENCHMARK_CANDIDATE_MANIFEST")
            else None
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = BenchmarkConfig(
        args.cold_samples,
        args.warm_samples,
        args.worker_timeout_seconds,
    )
    context = BenchmarkContext(
        source_commit=_source_commit(args.source_commit),
        airflow_version=args.airflow_version,
        python_version=args.python_version,
        support=args.support,
        constraints=args.constraints,
        cncf_provider_version=args.cncf_provider_version,
        constraints_sha256=args.constraints_sha256,
        candidate_manifest=None,
        candidate_manifest_sha256=args.candidate_manifest_sha256,
    )
    try:
        if args.candidate_manifest is not None:
            manifest = _load_candidate_manifest(args.candidate_manifest)
            manifest_sha256 = args.candidate_manifest_sha256 or canonical_json_sha256(manifest)
            context = replace(
                context,
                candidate_manifest=manifest,
                candidate_manifest_sha256=manifest_sha256,
            )
        payload = run_benchmark(config, context)
    except Exception as exc:  # noqa: BLE001 - top-level evidence is required.
        payload = _failure_evidence(config, context, exc)
    try:
        persisted = _evidence_support.write_evidence(args.output, payload)
    except Exception:  # noqa: BLE001 - invalid evidence must become valid FAIL.
        schema_error = ValueError("provider parse benchmark evidence failed schema validation")
        payload = _failure_evidence(config, context, schema_error)
        payload["failures"] = [
            _evidence_support.failure_diagnostic(
                schema_error,
                stage="evidence_validation",
                code="DPONE_AIRFLOW_PARSE_EVIDENCE_SCHEMA_INVALID",
            )
        ]
        persisted = _evidence_support.write_evidence(args.output, payload)
    print(args.output.as_posix())
    return 0 if persisted.get("status") == "PASS" and persisted.get("passed") is True else 1


def _load_candidate_manifest(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_WORKER_REQUEST_BYTES:
        raise ValueError("candidate manifest exceeds its size boundary")
    payload = json.loads(raw)
    if not isinstance(payload, dict):
        raise ValueError("candidate manifest must be a JSON object")
    return payload


if __name__ == "__main__":
    raise SystemExit(main())
