"""Shared models, budgets, and exact topology checks for the parse benchmark."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SCHEMA = "dpone.airflow-provider-parse-benchmark.v1"
CANDIDATE_MANIFEST_SCHEMA = "dpone.airflow-provider-candidate-manifest.v1"
STRICT_INDEX_SCHEMA = "dpone.airflow-deployment-index.v2"
DAG_COUNT, WORKLOADS_PER_DAG = 100, 5
WORKLOAD_COUNT = DAG_COUNT * WORKLOADS_PER_DAG
FIXTURE_CONTROL_FILE_COUNT = 7
EXPECTED_FIXTURE_FILE_COUNT = DAG_COUNT + WORKLOAD_COUNT + FIXTURE_CONTROL_FILE_COUNT
DEFAULT_SAMPLES, MAX_SAMPLES = 30, 100
# Cold process/import+first-load budget keeps headroom for GitHub-hosted
# Airflow 3.2 / py3.12 runners (observed process-cold p95 ~5.02s). Warm budget
# keeps headroom for the same fixture class (observed warm loader p95 ~2.1-2.8s)
# while remaining well below cold.
COLD_LIMIT_SECONDS, WARM_LIMIT_SECONDS = 5.5, 3.0
RSS_LIMIT_BYTES = 250 * 1024 * 1024
DEFAULT_WORKER_TIMEOUT_SECONDS, MAX_WORKER_TIMEOUT_SECONDS = 30.0, 120.0
MAX_WORKER_RESULT_BYTES = 256 * 1024
MAX_WORKER_REQUEST_BYTES = 512 * 1024
RELEASE_ID = "sha256:" + "a" * 64
DEPLOYMENT_ID = "sha256:" + "b" * 64
RUNTIME_SUFFIX = "__dpone_runtime"
REQUIRED_CANDIDATE_DISTRIBUTIONS = frozenset({"apache-airflow-providers-dpone", "dpone-airflow-pack"})
REQUIRED_INSTALLED_DISTRIBUTIONS = (
    "apache-airflow",
    "apache-airflow-providers-cncf-kubernetes",
    "apache-airflow-providers-dpone",
    "dpone-airflow-pack",
)
REQUIRED_IMPORT_ORIGINS = {
    "airflow.providers.cncf.kubernetes": "apache-airflow-providers-cncf-kubernetes",
    "airflow.providers.dpone": "apache-airflow-providers-dpone",
    "dpone_airflow_pack": "dpone-airflow-pack",
}


@dataclass(frozen=True)
class BenchmarkConfig:
    """Bounded sample counts and per-worker wall-clock timeout."""

    cold_samples: int = DEFAULT_SAMPLES
    warm_samples: int = DEFAULT_SAMPLES
    worker_timeout_seconds: float = DEFAULT_WORKER_TIMEOUT_SECONDS


@dataclass(frozen=True)
class BenchmarkContext:
    """Exact source, matrix, constraints, and candidate identity."""

    source_commit: str
    airflow_version: str | None = None
    python_version: str | None = None
    support: str | None = None
    constraints: str | None = None
    cncf_provider_version: str | None = None
    constraints_sha256: str | None = None
    candidate_manifest: Mapping[str, Any] | None = None
    candidate_manifest_sha256: str | None = None


class BenchmarkIntegrityError(RuntimeError):
    """The provider parse did not reproduce the complete benchmark contract."""


def nearest_rank_percentile(values: Sequence[float], percentile: float) -> float:
    """Return the nearest-rank percentile using the ceiling rank."""

    if not values or not 0 < percentile <= 1:
        raise ValueError("percentile requires samples and a fraction in (0, 1]")
    ordered = sorted(float(value) for value in values)
    return ordered[math.ceil(len(ordered) * percentile) - 1]


def evaluate_duration_budget(
    samples: Sequence[float],
    *,
    expected_samples: int,
    limit_seconds: float,
) -> dict[str, Any]:
    """Evaluate an inclusive nearest-rank p95 budget and retain max diagnostics."""

    observed = nearest_rank_percentile(samples, 0.95) if samples else None
    passed = len(samples) == expected_samples and observed is not None and observed <= limit_seconds
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "limit_seconds": limit_seconds,
        "expected_samples": expected_samples,
        "observed_samples": len(samples),
        "observed_p95_seconds": round(observed, 6) if observed is not None else None,
        "observed_max_seconds": round(max(samples), 6) if samples else None,
    }


def evaluate_rss_budget(
    samples: Sequence[int],
    *,
    limit_bytes: int,
    expected_workers: int | None = None,
) -> dict[str, Any]:
    """Evaluate the maximum measured per-worker RSS increase."""

    observed = max(samples) if samples else None
    complete = expected_workers is None or len(samples) == expected_workers
    passed = observed is not None and observed <= limit_bytes and complete
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "limit_bytes": limit_bytes,
        "expected_workers": expected_workers,
        "observed_workers": len(samples),
        "observed_max_additional_rss_bytes": observed,
    }


def expected_layout() -> dict[str, tuple[str, ...]]:
    """Return the exact DAG-to-workload identity contract for the fixture."""

    return {
        f"dpone_parse_benchmark_{dag_index:03d}": tuple(
            f"parse_benchmark_{dag_index:03d}_{workload_index:02d}" for workload_index in range(WORKLOADS_PER_DAG)
        )
        for dag_index in range(DAG_COUNT)
    }


def forbidden_parse_imports(before: set[str], after: set[str]) -> list[str]:
    """Return newly imported Vault or legacy cache-mutation modules."""

    forbidden_segments = {"vault_kv_client", "cache_sync", "cache_refresh"}
    return sorted(
        name for name in after - before if forbidden_segments.intersection(name.split(".")) or ".cache.refresh" in name
    )


def resolved_temp_root(path: str | Path) -> Path:
    """Resolve macOS ``/var`` and ``/private/var`` aliases before confinement."""

    return Path(path).resolve(strict=True)


def topology_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize canonical topology records without persisting the full namespace."""

    normalized = _normalized_records(records)
    dag_ids = [item["dag_id"] for item in normalized]
    digest_fields = {
        "spec_fingerprints_sha256": "spec_fingerprint",
        "task_ids_sha256": "task_ids",
        "edges_sha256": "edges",
    }
    return {
        "dag_count": len(normalized),
        "runtime_task_count": sum(len(item["task_ids"]) for item in normalized),
        "dag_ids_sha256": canonical_json_sha256(dag_ids),
        **{
            digest: canonical_json_sha256([[item["dag_id"], item[field]] for item in normalized])
            for digest, field in digest_fields.items()
        },
        "topology_sha256": canonical_json_sha256(normalized),
    }


def build_candidate_manifest(
    *,
    source_commit: str,
    wheels_dir: Path,
    constraints_ref: str,
    constraints_sha256: str,
    cncf_provider_version: str,
) -> dict[str, Any]:
    """Build the closed candidate-wheel identity used by parse-benchmark evidence."""

    checksums = _wheel_checksums(wheels_dir)
    wheels: list[dict[str, str]] = []
    for distribution in sorted(REQUIRED_CANDIDATE_DISTRIBUTIONS):
        wheel = _wheel_entry(distribution, checksums)
        wheels.append(wheel)
    constraints_name = Path(constraints_ref.rstrip("/")).name or "constraints.txt"
    return {
        "schema": CANDIDATE_MANIFEST_SCHEMA,
        "source_commit": source_commit,
        "wheels": wheels,
        "constraints": {
            "ref": constraints_ref,
            "filename": constraints_name,
            "sha256": constraints_sha256,
        },
        "cncf_provider": {
            "distribution": "apache-airflow-providers-cncf-kubernetes",
            "version": cncf_provider_version,
        },
    }


def _wheel_checksums(wheels_dir: Path) -> dict[str, str]:
    sums_path = wheels_dir / "SHA256SUMS"
    if not sums_path.is_file():
        raise ValueError(f"missing wheel checksum ledger: {sums_path}")
    checksums: dict[str, str] = {}
    for line in sums_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        digest, separator, raw_filename = text.partition(" ")
        filename = Path(raw_filename.lstrip(" *")).name
        if (
            not separator
            or len(digest) != 64
            or any(character not in "0123456789abcdefABCDEF" for character in digest)
            or not filename
        ):
            raise ValueError(f"invalid SHA256SUMS line: {line!r}")
        if not filename.endswith(".whl"):
            continue
        checksums[filename] = "sha256:" + digest.lower()
    return checksums


def _wheel_entry(distribution: str, checksums: Mapping[str, str]) -> dict[str, str]:
    prefix = distribution.replace("-", "_") + "-"
    matches = sorted(name for name in checksums if name.startswith(prefix) and name.endswith(".whl"))
    if len(matches) != 1:
        raise ValueError(f"expected exactly one wheel for {distribution}, found {matches!r}")
    filename = matches[0]
    remainder = filename[len(prefix) :]
    version = remainder.split("-", 1)[0]
    if not version:
        raise ValueError(f"unable to derive version from wheel filename: {filename}")
    return {
        "distribution": distribution,
        "filename": filename,
        "version": version,
        "sha256": checksums[filename],
    }


def canonical_json_sha256(value: object) -> str:
    """Hash canonical compact JSON with the repository SHA-256 prefix."""

    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def is_canonical_sha256(value: object) -> bool:
    """Return whether a value is one canonical lowercase SHA-256 identity."""

    text = value if isinstance(value, str) else ""
    return (
        text.startswith("sha256:")
        and len(text) == 71
        and all(character in "0123456789abcdef" for character in text[7:])
    )


def candidate_identity(
    context: BenchmarkContext,
    runner: Mapping[str, Any],
    workers: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Bind PASS to one manifest, installed distribution set, and import origin."""

    manifest = context.candidate_manifest
    installed = _installed_distributions(runner)
    if not isinstance(manifest, Mapping):
        return _candidate_result(
            False,
            ["candidate_manifest"],
            installed=installed,
        )
    reasons: list[str] = []
    manifest_sha256 = canonical_json_sha256(manifest)
    if (
        manifest.get("schema") != CANDIDATE_MANIFEST_SCHEMA
        or context.candidate_manifest_sha256 != manifest_sha256
        or not is_canonical_sha256(context.candidate_manifest_sha256)
    ):
        reasons.append("candidate_manifest")
    source_commit = manifest.get("source_commit")
    if source_commit != context.source_commit or not _is_git_commit(source_commit):
        reasons.append("source_commit")
    wheels, wheel_reasons = _candidate_wheels(manifest.get("wheels"))
    reasons.extend(wheel_reasons)
    versions = {item["distribution"]: item["version"] for item in wheels}
    expected_installed: dict[str, object] = {
        "apache-airflow": context.airflow_version,
        "apache-airflow-providers-cncf-kubernetes": context.cncf_provider_version,
        **versions,
    }
    if any(not isinstance(value, str) or installed.get(name) != value for name, value in expected_installed.items()):
        reasons.append("installed_distributions")
    constraints = manifest.get("constraints")
    constraints_valid = _valid_constraints(constraints, context)
    if not constraints_valid:
        reasons.append("constraints")
    cncf = manifest.get("cncf_provider")
    cncf_valid = (
        isinstance(cncf, Mapping)
        and set(cncf) == {"distribution", "version"}
        and cncf.get("distribution") == "apache-airflow-providers-cncf-kubernetes"
        and cncf.get("version") == context.cncf_provider_version
        and installed.get("apache-airflow-providers-cncf-kubernetes") == context.cncf_provider_version
    )
    if not cncf_valid:
        reasons.append("cncf_provider")
    verified_origins = sum(_worker_origins_match(worker, expected_installed) for worker in workers)
    if not workers or verified_origins != len(workers):
        reasons.append("import_origins")
    reasons = list(dict.fromkeys(reasons))
    resolved_constraints = dict(constraints) if constraints_valid and isinstance(constraints, Mapping) else None
    resolved_cncf = dict(cncf) if cncf_valid and isinstance(cncf, Mapping) else None
    return _candidate_result(
        not reasons,
        reasons,
        installed=installed,
        manifest_sha256=manifest_sha256,
        source_commit=source_commit if _is_git_commit(source_commit) else None,
        wheels=wheels,
        constraints=resolved_constraints,
        cncf_provider=resolved_cncf,
        verified_origins=verified_origins,
    )


def _candidate_result(
    passed: bool,
    reasons: list[str],
    *,
    installed: Mapping[str, Any],
    manifest_sha256: str | None = None,
    source_commit: str | None = None,
    wheels: list[dict[str, str]] | None = None,
    constraints: dict[str, Any] | None = None,
    cncf_provider: dict[str, Any] | None = None,
    verified_origins: int = 0,
) -> dict[str, Any]:
    status = "PASS" if passed else ("UNVERIFIED" if manifest_sha256 is None else "FAIL")
    return {
        "status": status,
        "passed": passed,
        "failure_reasons": reasons,
        "manifest_sha256": manifest_sha256,
        "source_commit": source_commit,
        "wheels": wheels or [],
        "constraints": constraints,
        "cncf_provider": cncf_provider,
        "installed_distributions": {name: installed.get(name) for name in REQUIRED_INSTALLED_DISTRIBUTIONS},
        "verified_import_origin_workers": verified_origins,
    }


def _candidate_wheels(value: object) -> tuple[list[dict[str, str]], list[str]]:
    if not isinstance(value, list):
        return [], ["wheel_manifest", "wheel_set"]
    wheels: list[dict[str, str]] = []
    malformed = False
    expected_fields = {"distribution", "filename", "version", "sha256"}
    for item in value:
        if not isinstance(item, Mapping) or set(item) != expected_fields:
            malformed = True
            continue
        name, filename, version = item.get("distribution"), item.get("filename"), item.get("version")
        valid = (
            isinstance(name, str)
            and isinstance(filename, str)
            and Path(filename).name == filename
            and isinstance(version, str)
            and 0 < len(version) <= 64
            and filename.startswith(name.replace("-", "_") + f"-{version}-")
            and filename.endswith(".whl")
            and is_canonical_sha256(item.get("sha256"))
        )
        if not valid:
            malformed = True
            continue
        wheels.append({key: str(item[key]) for key in expected_fields})
    reasons = ["wheel_manifest"] if malformed else []
    distributions = {item["distribution"] for item in wheels}
    if distributions != REQUIRED_CANDIDATE_DISTRIBUTIONS or len(wheels) != len(REQUIRED_CANDIDATE_DISTRIBUTIONS):
        reasons.append("wheel_set")
    return sorted(wheels, key=lambda item: item["distribution"]), reasons


def _valid_constraints(value: object, context: BenchmarkContext) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"ref", "filename", "sha256"}
        and value.get("ref") == context.constraints
        and isinstance(value.get("filename"), str)
        and Path(str(value["filename"])).name == value["filename"]
        and value.get("sha256") == context.constraints_sha256
        and is_canonical_sha256(context.constraints_sha256)
    )


def _worker_origins_match(
    worker: Mapping[str, Any],
    expected_versions: Mapping[str, object],
) -> bool:
    origins = worker.get("import_origins")
    if not isinstance(origins, Mapping):
        return False
    for module_name, distribution_name in REQUIRED_IMPORT_ORIGINS.items():
        origin = origins.get(module_name)
        relative = origin.get("relative_path") if isinstance(origin, Mapping) else None
        if (
            not isinstance(origin, Mapping)
            or origin.get("distribution") != distribution_name
            or origin.get("version") != expected_versions.get(distribution_name)
            or origin.get("within_environment") is not True
            or not isinstance(relative, str)
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
        ):
            return False
    return True


def _installed_distributions(runner: Mapping[str, Any]) -> dict[str, Any]:
    value = runner.get("installed_distributions")
    if not isinstance(value, Mapping):
        value = runner.get("package_versions")
    return dict(value) if isinstance(value, Mapping) else {}


def _is_git_commit(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) in {40, 64}
        and all(character in "0123456789abcdef" for character in value)
    )


def measurement_integrity(
    config: BenchmarkConfig,
    *,
    process_cold: Sequence[float],
    cold_loader: Sequence[float],
    warm_loader: Sequence[float],
) -> dict[str, Any]:
    """Require every requested timing and a bounded sterile-worker clock."""

    observed = {
        "process_cold": len(process_cold),
        "cold_loader": len(cold_loader),
        "import_plus_first_load": len(process_cold),
        "warm_loader": len(warm_loader),
    }
    expected = dict.fromkeys(("process_cold", "cold_loader", "import_plus_first_load"), config.cold_samples)
    expected["warm_loader"] = config.warm_samples
    passed = observed == expected
    return {
        "status": "PASS" if passed else "FAIL",
        "passed": passed,
        "expected_samples": expected,
        "observed_samples": observed,
        "worker_timeout_seconds": config.worker_timeout_seconds,
    }


def timings(workers: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    """Collect finite, non-negative optional durations."""

    values: list[float] = []
    for worker in workers:
        value = worker.get(key)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            number = float(value)
            if math.isfinite(number) and number >= 0:
                values.append(number)
    return values


def rss_additional_samples(workers: Sequence[Mapping[str, Any]]) -> list[int]:
    """Collect valid non-negative per-worker RSS deltas."""

    values = [worker.get("rss", {}).get("additional_bytes") for worker in workers]
    return [value for value in values if isinstance(value, int) and not isinstance(value, bool) and value >= 0]


def sample(model: str, requested: int, durations: Sequence[float]) -> dict[str, Any]:
    """Render one timing sample series."""

    return {
        "model": model,
        "requested": requested,
        "observed": len(durations),
        "durations_seconds": list(durations),
    }


def inspect_parse_topology(
    namespace: Mapping[str, Any],
    contract: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    """Preserve the support-module import while delegating to the sterile worker."""

    from tools.airflow_provider_parse_benchmark_worker import inspect_parse_topology as inspect

    return inspect(namespace, contract)


def _normalized_records(
    records: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    return sorted(
        (
            {
                "dag_id": str(item.get("dag_id", "")),
                "spec_fingerprint": item.get("spec_fingerprint"),
                "task_ids": sorted(str(value) for value in item.get("task_ids", ())),
                "edges": sorted(
                    [str(edge[0]), str(edge[1])]
                    for edge in item.get("edges", ())
                    if isinstance(edge, Sequence) and len(edge) == 2
                ),
            }
            for item in records
        ),
        key=lambda item: item["dag_id"],
    )
