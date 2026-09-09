"""Fail-closed aggregation and persistence for provider parse evidence."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping, Sequence
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import Any

from tools import airflow_provider_parse_benchmark_support as _support
from tools import airflow_provider_parse_benchmark_tripwire as _tripwire

try:
    from dpone.security_redaction import redact_public_text, redact_public_value
except ModuleNotFoundError as exc:
    if exc.name != "dpone":
        raise
    redactor_path = Path(__file__).resolve().parents[1] / "src/dpone/security_redaction.py"
    redactor_spec = spec_from_file_location("_dpone_benchmark_security_redaction", redactor_path)
    if redactor_spec is None or redactor_spec.loader is None:
        raise RuntimeError("shared public redactor is unavailable") from exc
    redactor_module = module_from_spec(redactor_spec)
    redactor_spec.loader.exec_module(redactor_module)
    redact_public_text = redactor_module.redact_public_text
    redact_public_value = redactor_module.redact_public_value

_SCHEMA_PATH = Path(__file__).resolve().parents[1] / "docs/schemas/gitops/airflow-provider-parse-benchmark.schema.json"
_TIMING_MODELS = {
    "process_cold": ("process_cold_seconds", "sterile subprocess startup through first complete provider load"),
    "cold_loader": ("cold_loader_seconds", "first provider loader call in a fresh worker after provider import"),
    "import_plus_first_load": (
        "import_plus_first_load_seconds",
        "provider import plus first loader call in a fresh worker",
    ),
    "warm_loader": ("warm_loader_seconds", "second provider loader call in the same worker"),
}


def matrix_evidence(
    context: _support.BenchmarkContext,
    runner: Mapping[str, Any],
) -> dict[str, Any]:
    """Compare supplied matrix versions with the installed runtime."""

    installed = _support._installed_distributions(runner)
    installed_airflow = installed.get("apache-airflow")
    installed_python = runner.get("python")
    identity = {
        "airflow_version": context.airflow_version,
        "python_version": context.python_version,
        "support": context.support,
        "constraints": context.constraints,
        "cncf_provider_version": context.cncf_provider_version,
    }
    missing_identity = [name for name, value in identity.items() if not isinstance(value, str) or not value.strip()]
    airflow_match = isinstance(context.airflow_version, str) and installed_airflow == context.airflow_version
    python_match = isinstance(context.python_version, str) and _python_matches(
        installed_python,
        context.python_version,
    )
    installed_cncf = installed.get("apache-airflow-providers-cncf-kubernetes")
    cncf_match = isinstance(context.cncf_provider_version, str) and installed_cncf == context.cncf_provider_version
    candidate_available = isinstance(context.candidate_manifest, Mapping) and _support.is_canonical_sha256(
        context.candidate_manifest_sha256
    )
    passed = not missing_identity and candidate_available and airflow_match and python_match and cncf_match
    if missing_identity:
        status = "N/A"
    elif not candidate_available:
        status = "UNVERIFIED"
        missing_identity = ["candidate_manifest"]
    else:
        status = "PASS" if passed else "FAIL"
    return {
        "status": status,
        "passed": passed,
        "missing_identity": missing_identity,
        "airflow": {
            "expected": context.airflow_version,
            "installed": installed_airflow,
            "match": airflow_match,
        },
        "python": {
            "expected": context.python_version,
            "installed": installed_python,
            "match": python_match,
        },
        "support": context.support,
        "constraints": context.constraints,
        "constraints_sha256": context.constraints_sha256,
        "cncf_provider": {
            "expected": context.cncf_provider_version,
            "installed": installed_cncf,
            "match": cncf_match,
        },
        "candidate_manifest_sha256": context.candidate_manifest_sha256,
    }


def evaluate_fixture_integrity(fixture: Mapping[str, Any]) -> dict[str, Any]:
    """Evaluate strict-v2 fixture metadata without trusting producer status."""

    connection = fixture.get("runtime_connection_artifacts")
    topology = fixture.get("expected_topology")
    observed, expected = fixture.get("index_sha256"), fixture.get("expected_index_sha256")
    dimensions = (fixture.get("dags"), fixture.get("workloads"), fixture.get("workloads_per_dag"))
    files_valid = (
        isinstance(fixture.get("index_bytes"), int)
        and fixture.get("index_bytes", 0) > 0
        and isinstance(fixture.get("total_bytes"), int)
        and fixture.get("total_bytes", 0) > 0
        and fixture.get("file_count") == _support.EXPECTED_FIXTURE_FILE_COUNT
        and _support.is_canonical_sha256(fixture.get("sha256"))
    )
    checks = {
        "fixture_status": fixture.get("status") == "READY",
        "cache_boundary": fixture.get("cache_only") is True,
        "topology_dimensions": dimensions == (_support.DAG_COUNT, _support.WORKLOAD_COUNT, _support.WORKLOADS_PER_DAG),
        "fixture_identity": (fixture.get("release_id"), fixture.get("deployment_id"))
        == (_support.RELEASE_ID, _support.DEPLOYMENT_ID),
        "schema": fixture.get("index_schema") == _support.STRICT_INDEX_SCHEMA,
        "delivery_mode": fixture.get("delivery_mode") == "init_fetch",
        "trust_tier": fixture.get("trust_tier") in {"production", "non_production"}
        and fixture.get("delivery_trust_tier") == fixture.get("trust_tier"),
        "strict_v2_validation": fixture.get("strict_v2_validated") is True,
        "runtime_connection_artifacts": isinstance(connection, Mapping)
        and connection.get("passed") is True
        and connection.get("verified") == 3,
        "topology_authority": isinstance(topology, Mapping)
        and topology.get("dag_count") == _support.DAG_COUNT
        and topology.get("runtime_task_count") == _support.WORKLOAD_COUNT
        and _support.is_canonical_sha256(topology.get("topology_sha256")),
        "index_digest": _support.is_canonical_sha256(observed)
        and observed == expected
        and fixture.get("index_digest_match") is True,
        "fixture_files": files_valid,
    }
    failures = [name for name, passed in checks.items() if not passed]
    projection_fields = (
        "index_schema index_sha256 expected_index_sha256 delivery_mode trust_tier "
        "delivery_trust_tier validation_error_code"
    )
    projection = {name: fixture.get(name) for name in projection_fields.split()}
    return {
        "status": "PASS" if not failures else "FAIL",
        "passed": not failures,
        "failure_reasons": failures,
        **projection,
        "index_digest_match": fixture.get("index_digest_match") is True,
        "strict_v2_validated": fixture.get("strict_v2_validated") is True,
        "runtime_connection_artifacts": connection,
        "expected_topology": topology,
    }


def build_evidence(
    config: _support.BenchmarkConfig,
    context: _support.BenchmarkContext,
    fixture: Mapping[str, Any],
    workers: Sequence[Mapping[str, Any]],
    runner: Mapping[str, Any],
) -> dict[str, Any]:
    """Aggregate workers without allowing incomplete evidence to pass."""

    expected_workers = max(config.cold_samples, config.warm_samples)
    requested = dict.fromkeys(_TIMING_MODELS, config.cold_samples)
    requested["warm_loader"] = config.warm_samples
    limits = dict.fromkeys(_TIMING_MODELS, _support.COLD_LIMIT_SECONDS)
    limits["warm_loader"] = _support.WARM_LIMIT_SECONDS
    durations = {name: _support.timings(workers, model[0]) for name, model in _TIMING_MODELS.items()}
    duration_budgets = {
        name: _support.evaluate_duration_budget(
            durations[name], expected_samples=requested[name], limit_seconds=limits[name]
        )
        for name in _TIMING_MODELS
    }
    budgets = {f"{name}_p95_seconds": budget for name, budget in duration_budgets.items()}
    budgets["additional_rss_bytes"] = _support.evaluate_rss_budget(
        _support.rss_additional_samples(workers),
        limit_bytes=_support.RSS_LIMIT_BYTES,
        expected_workers=expected_workers,
    )
    for alias, target in (("cold", "cold_loader"), ("warm", "warm_loader")):
        budgets[f"{alias}_p95_seconds"] = {
            **duration_budgets[target],
            "compatibility_alias_of": f"{target}_p95_seconds",
        }
    sampling = {
        name: _support.sample(model, requested[name], durations[name]) for name, (_, model) in _TIMING_MODELS.items()
    }
    for alias, target in (("cold", "cold_loader"), ("warm", "warm_loader")):
        sampling[alias] = {
            **sampling[target],
            "compatibility_alias_of": target,
        }
    measurement = _support.measurement_integrity(
        config,
        process_cold=durations["process_cold"],
        cold_loader=durations["cold_loader"],
        warm_loader=durations["warm_loader"],
    )
    expected_topology = fixture.get("expected_topology")
    fixture_integrity = evaluate_fixture_integrity(fixture)
    load = _tripwire.load_integrity(
        config,
        workers,
        expected_workers,
        expected_index_sha256=fixture.get("index_sha256"),
        expected_topology=expected_topology if isinstance(expected_topology, Mapping) else {},
    )
    process = _tripwire.process_integrity(workers, expected_workers)
    side_effects = _tripwire.aggregate_side_effect_evidence(workers, expected_workers)
    matrix = matrix_evidence(context, runner)
    candidate = _support.candidate_identity(context, runner, workers)
    gates = (
        matrix,
        fixture_integrity,
        candidate,
        measurement,
        process,
        load,
        side_effects,
        *duration_budgets.values(),
        budgets["additional_rss_bytes"],
    )
    passed = all(gate.get("status") == "PASS" and gate.get("passed") is True for gate in gates)
    return {
        "schema": _support.SCHEMA,
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "source_commit": context.source_commit,
        "matrix": matrix,
        "candidate_identity": candidate,
        "fixture": dict(fixture),
        "fixture_integrity": fixture_integrity,
        "sampling": sampling,
        "measurement_integrity": measurement,
        "process_integrity": process,
        "budgets": budgets,
        "load_integrity": load,
        "side_effects": side_effects,
        "loader": {
            "import_path": "airflow.providers.dpone:load_dpone_dags",
            "resolved_modules": sorted(
                {str(worker["loader_module"]) for worker in workers if worker.get("loader_module")}
            ),
        },
        "runner": dict(runner),
        "failures": _aggregate_failures(workers),
        "live_certification": {
            "status": "N/A",
            "reason": "credential-free synthetic local-cache parse benchmark",
        },
    }


def failure_evidence(
    config: _support.BenchmarkConfig,
    context: _support.BenchmarkContext,
    exc: Exception,
    runner: Mapping[str, Any],
) -> dict[str, Any]:
    """Return the completed evidence shape with a setup failure attached."""

    unavailable_fields = (
        "index_bytes total_bytes file_count sha256 index_schema index_sha256 "
        "expected_index_sha256 delivery_mode trust_tier delivery_trust_tier validation_error_code"
    )
    unavailable = dict.fromkeys(unavailable_fields.split())
    fixture = {
        "status": "UNAVAILABLE",
        "cache_only": None,
        "dags": _support.DAG_COUNT,
        "workloads": _support.WORKLOAD_COUNT,
        "workloads_per_dag": _support.WORKLOADS_PER_DAG,
        "release_id": _support.RELEASE_ID,
        "deployment_id": _support.DEPLOYMENT_ID,
        **unavailable,
        "index_digest_match": False,
        "strict_v2_validated": False,
        "runtime_connection_artifacts": {
            "status": "FAIL",
            "passed": False,
            "expected": 3,
            "verified": 0,
            "descriptors_sha256": _support.canonical_json_sha256([]),
        },
        "expected_topology": {
            "dag_count": 0,
            "runtime_task_count": 0,
            "topology_sha256": _support.canonical_json_sha256([]),
        },
    }
    payload = build_evidence(config, context, fixture, (), runner)
    payload["failures"] = [failure_diagnostic(exc, stage="benchmark_setup")]
    payload["status"] = "FAIL"
    payload["passed"] = False
    return payload


def redact_evidence(payload: object) -> Any:
    """Recursively apply the shared public redaction policy."""

    return redact_public_value(payload)


def validate_evidence(payload: Mapping[str, Any]) -> None:
    """Require the exact owned JSON Schema before persistence or success."""

    from jsonschema import Draft202012Validator

    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    if next(iter(validator.iter_errors(payload)), None) is not None:
        raise ValueError("provider parse benchmark evidence schema validation failed")


def write_evidence(path: Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Atomically replace canonical JSON evidence in the destination directory."""

    public = redact_evidence(payload)
    if not isinstance(public, dict):
        raise ValueError("provider parse benchmark evidence must be an object")
    validate_evidence(public)
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (
        json.dumps(
            public,
            allow_nan=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temporary = Path(handle.name)
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return public


def _aggregate_failures(
    workers: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    for index, worker in enumerate(workers):
        values = worker.get("failures")
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            values = ()
        for value in values:
            if isinstance(value, Mapping):
                failures.append({"worker": index, **dict(value)})
        if worker.get("status") != "PASS" and not any(failure.get("worker") == index for failure in failures):
            failures.append(
                {
                    "worker": index,
                    "code": "DPONE_AIRFLOW_PARSE_WORKER_FAILED",
                    "message": "sterile worker failed without a diagnostic",
                    "stage": "sterile_worker",
                    "type": "WorkerProcessError",
                }
            )
    return failures


def _python_matches(installed: object, expected: str) -> bool:
    actual = str(installed or "")
    return actual == expected or actual.startswith(f"{expected}.")


def failure_diagnostic(
    exc: Exception,
    *,
    stage: str,
    code: str = "DPONE_AIRFLOW_PARSE_BENCHMARK_FAILED",
) -> dict[str, str]:
    """Build one bounded, single-line diagnostic through the public redactor."""

    fallback = exc.__class__.__name__
    redacted = redact_public_text(
        exc,
        fallback=fallback,
        strip_traceback=True,
    )
    message = " ".join(redacted.splitlines()).strip() or fallback
    return {
        "code": code,
        "message": message[:500],
        "stage": stage,
        "type": exc.__class__.__name__,
    }
