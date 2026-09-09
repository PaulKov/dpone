from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
import subprocess
import sys
import time
from dataclasses import dataclass, field, replace
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator
from tools import airflow_provider_parse_benchmark as benchmark
from tools.airflow_provider_parse_benchmark_support import (
    COLD_LIMIT_SECONDS,
    EXPECTED_FIXTURE_FILE_COUNT,
    build_candidate_manifest,
)

SOURCE_COMMIT = "a" * 40
CANDIDATE_VERSION = "0.7.31"
CONSTRAINTS_REF = "constraints-3.3.0/constraints-3.12.txt"
CONSTRAINTS_SHA256 = "sha256:" + "8" * 64
CNCF_PROVIDER_VERSION = "10.20.0"


@dataclass
class FakeTask:
    task_id: str
    upstream_task_ids: set[str] = field(default_factory=set)
    downstream_task_ids: set[str] = field(default_factory=set)


@dataclass
class FakeDag:
    tasks: list[FakeTask]
    _dpone_spec_fingerprint: str = "sha256:" + "c" * 64


def _topology_contract(
    *,
    edges: tuple[tuple[str, str], ...] = (),
) -> dict[str, object]:
    return {
        "dpone_parse_benchmark_000": {
            "spec_fingerprint": "sha256:" + "c" * 64,
            "task_ids": (
                "parse_benchmark_000_00__dpone_runtime",
                "parse_benchmark_000_01__dpone_runtime",
            ),
            "edges": edges,
        }
    }


def test_nearest_rank_p95_uses_ceiling_rank() -> None:
    samples = [float(value) for value in range(30, 0, -1)]

    assert benchmark.nearest_rank_percentile(samples, 0.95) == 29.0


def test_budget_evaluation_uses_inclusive_p95_not_max() -> None:
    cold_at_limit = benchmark.evaluate_duration_budget(
        [5.0],
        expected_samples=1,
        limit_seconds=5.0,
    )
    cold_over_limit = benchmark.evaluate_duration_budget(
        [5.000001],
        expected_samples=1,
        limit_seconds=5.0,
    )
    missing_warm_sample = benchmark.evaluate_duration_budget(
        [],
        expected_samples=1,
        limit_seconds=2.0,
    )
    warm_with_one_outlier = benchmark.evaluate_duration_budget(
        [1.0] * 19 + [9.0],
        expected_samples=20,
        limit_seconds=2.0,
    )
    rss_over_limit = benchmark.evaluate_rss_budget(
        [262_144_001],
        limit_bytes=262_144_000,
    )

    assert cold_at_limit["status"] == "PASS"
    assert cold_over_limit["status"] == "FAIL"
    assert missing_warm_sample["status"] == "FAIL"
    assert missing_warm_sample["observed_p95_seconds"] is None
    assert warm_with_one_outlier["status"] == "PASS"
    assert warm_with_one_outlier["observed_p95_seconds"] == 1.0
    assert warm_with_one_outlier["observed_max_seconds"] == 9.0
    assert rss_over_limit["status"] == "FAIL"


def test_candidate_manifest_accepts_checksum_bound_non_wheel_artifacts(tmp_path: Path) -> None:
    (tmp_path / "SHA256SUMS").write_text(
        "\n".join(
            (
                f"{'1' * 64}  apache_airflow_providers_dpone-1.2.3-py3-none-any.whl",
                f"{'2' * 64}  dpone_airflow_pack-1.2.3-py3-none-any.whl",
                f"{'3' * 64}  airflow-cache-values-3.2.yaml",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = build_candidate_manifest(
        source_commit=SOURCE_COMMIT,
        wheels_dir=tmp_path,
        constraints_ref=CONSTRAINTS_REF,
        constraints_sha256=CONSTRAINTS_SHA256,
        cncf_provider_version=CNCF_PROVIDER_VERSION,
    )

    assert [item["distribution"] for item in manifest["wheels"]] == [
        "apache-airflow-providers-dpone",
        "dpone-airflow-pack",
    ]


def test_candidate_manifest_rejects_malformed_non_wheel_checksum(tmp_path: Path) -> None:
    (tmp_path / "SHA256SUMS").write_text(
        f"{'not-hex'.ljust(64, 'x')}  airflow-cache-values-3.2.yaml\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="invalid SHA256SUMS line"):
        build_candidate_manifest(
            source_commit=SOURCE_COMMIT,
            wheels_dir=tmp_path,
            constraints_ref=CONSTRAINTS_REF,
            constraints_sha256=CONSTRAINTS_SHA256,
            cncf_provider_version=CNCF_PROVIDER_VERSION,
        )


def test_rss_budget_evaluates_additional_memory_not_absolute_peak() -> None:
    under_limit = benchmark.evaluate_rss_budget(
        [200_000_000],
        limit_bytes=262_144_000,
    )
    over_limit = benchmark.evaluate_rss_budget(
        [262_144_001],
        limit_bytes=262_144_000,
    )

    assert under_limit["status"] == "PASS"
    assert under_limit["observed_max_additional_rss_bytes"] == 200_000_000
    assert over_limit["status"] == "FAIL"


def test_fixture_is_deterministic_cache_only_and_release_sized(tmp_path: Path) -> None:
    first_index, first = benchmark._fixture(tmp_path / "first")
    _, second = benchmark._fixture(tmp_path / "second")

    index = json.loads(first_index.read_text(encoding="utf-8"))
    assert first == second
    assert first["dags"] == 100
    assert first["workloads"] == 500
    # The writer-owned promotion lock is part of every managed cache root.
    assert first["file_count"] == EXPECTED_FIXTURE_FILE_COUNT
    assert first["release_id"] == benchmark.RELEASE_ID
    assert first["deployment_id"] == benchmark.DEPLOYMENT_ID
    assert len(index["dag_specs"]) == 100
    assert len(index["workload_packs"]) == 500
    assert index["schema"] == "dpone.airflow-deployment-index.v2"
    assert first["index_schema"] == index["schema"]
    assert first["index_sha256"] == _sha256(first_index.read_bytes())
    assert first["expected_index_sha256"] == first["index_sha256"]
    assert first["index_digest_match"] is True
    assert first["delivery_mode"] == "init_fetch"
    assert first["trust_tier"] == "non_production"
    assert first["delivery_trust_tier"] == first["trust_tier"]
    assert first["strict_v2_validated"] is True
    assert first["runtime_connection_artifacts"]["status"] == "PASS"
    assert first["runtime_connection_artifacts"]["verified"] == 3
    assert first["expected_topology"]["dag_count"] == 100
    assert first["expected_topology"]["runtime_task_count"] == 500
    assert all("pack_fingerprint" in artifact for artifact in index["workload_packs"])
    cache_root = first_index.parents[3]
    for artifact_field, filename in (
        ("binding_set", "binding-set.json"),
        ("connection_registry", "connection-registry.json"),
        ("credential_runtime", "credential-runtime.json"),
    ):
        descriptor = index[artifact_field]
        artifact_path = cache_root / descriptor["artifact_ref"].removeprefix("cache://")
        assert artifact_path.name == filename
        assert descriptor["bytes"] == artifact_path.stat().st_size
        assert descriptor["sha256"] == _sha256(artifact_path.read_bytes())
    assert index["runtime_artifact_delivery"] == benchmark._runtime_artifact_delivery()
    assert index["runtime_artifact_delivery"]["mode"] == "init_fetch"
    assert all(
        node["pack_ref"].startswith(f"cached://deployments/{benchmark.DEPLOYMENT_ID}/")
        for artifact in index["dag_specs"]
        for node in json.loads(
            (first_index.parents[3] / artifact["artifact_ref"].removeprefix("cache://")).read_text(encoding="utf-8")
        )["nodes"]
    )


def test_parse_topology_rejects_zero_task_dag() -> None:
    contract = {
        "dpone_parse_benchmark_000": {
            "spec_fingerprint": "sha256:" + "c" * 64,
            "task_ids": ("parse_benchmark_000_00__dpone_runtime",),
            "edges": (),
        }
    }
    namespace = {"dpone_parse_benchmark_000": FakeDag(tasks=[])}

    with pytest.raises(benchmark.BenchmarkIntegrityError, match="zero runtime tasks"):
        benchmark.inspect_parse_topology(namespace, contract)


def test_parse_topology_rejects_unexpected_non_runtime_task() -> None:
    contract = {
        "dpone_parse_benchmark_000": {
            "spec_fingerprint": "sha256:" + "c" * 64,
            "task_ids": ("parse_benchmark_000_00__dpone_runtime",),
            "edges": (),
        }
    }
    namespace = {
        "dpone_parse_benchmark_000": FakeDag(
            tasks=[
                FakeTask("parse_benchmark_000_00__dpone_runtime"),
                FakeTask("unexpected_task"),
            ]
        )
    }

    with pytest.raises(benchmark.BenchmarkIntegrityError, match="exact task IDs"):
        benchmark.inspect_parse_topology(namespace, contract)


def test_parse_topology_is_deterministic_and_requires_exact_edges() -> None:
    contract = _topology_contract(
        edges=(
            (
                "parse_benchmark_000_00__dpone_runtime",
                "parse_benchmark_000_01__dpone_runtime",
            ),
        )
    )
    first_task = FakeTask(
        "parse_benchmark_000_00__dpone_runtime",
        downstream_task_ids={"parse_benchmark_000_01__dpone_runtime"},
    )
    second_task = FakeTask(
        "parse_benchmark_000_01__dpone_runtime",
        upstream_task_ids={"parse_benchmark_000_00__dpone_runtime"},
    )
    replay = benchmark.inspect_parse_topology(
        {"dpone_parse_benchmark_000": FakeDag(tasks=[second_task, first_task])},
        contract,
    )
    original = benchmark.inspect_parse_topology(
        {"dpone_parse_benchmark_000": FakeDag(tasks=[first_task, second_task])},
        contract,
    )

    assert original["dag_count"] == 1
    assert original["runtime_task_count"] == 2
    assert original["workload_ids_match"] is True
    assert replay["topology_fingerprint"] == original["topology_fingerprint"]
    with pytest.raises(benchmark.BenchmarkIntegrityError, match="exact edges"):
        benchmark.inspect_parse_topology(
            {
                "dpone_parse_benchmark_000": FakeDag(
                    tasks=[
                        FakeTask("parse_benchmark_000_00__dpone_runtime"),
                        FakeTask("parse_benchmark_000_01__dpone_runtime"),
                    ]
                )
            },
            contract,
        )


def test_parse_topology_rejects_extra_dag_and_wrong_spec_fingerprint() -> None:
    contract = _topology_contract()
    tasks = [
        FakeTask("parse_benchmark_000_00__dpone_runtime"),
        FakeTask("parse_benchmark_000_01__dpone_runtime"),
    ]

    with pytest.raises(benchmark.BenchmarkIntegrityError, match="exact DAG namespace"):
        benchmark.inspect_parse_topology(
            {
                "dpone_parse_benchmark_000": FakeDag(tasks=tasks),
                "unexpected": FakeDag(tasks=[]),
            },
            contract,
        )
    with pytest.raises(benchmark.BenchmarkIntegrityError, match="spec fingerprints"):
        benchmark.inspect_parse_topology(
            {
                "dpone_parse_benchmark_000": FakeDag(
                    tasks=tasks,
                    _dpone_spec_fingerprint="sha256:" + "d" * 64,
                )
            },
            contract,
        )


def test_side_effect_tripwire_fails_when_required_named_groups_are_missing() -> None:
    tripwire = benchmark.ParseSideEffectTripwire()

    with tripwire:
        report = tripwire.report()

    assert report["status"] == "FAIL"
    assert report["coverage_complete"] is False
    assert "airflow_metadata" in report["missing_required_groups"]
    assert "kubernetes_api" in report["missing_required_groups"]
    assert report["required_groups"]["network"]["missing_probes"] == []


def test_forbidden_parse_imports_cover_vault_and_legacy_cache_modules() -> None:
    imported = benchmark.forbidden_parse_imports(
        {"already.loaded"},
        {
            "already.loaded",
            "dpone.adapters.vault_kv_client",
            "dpone_airflow_pack.cache_sync",
            "legacy.cache.refresh",
        },
    )

    assert imported == [
        "dpone.adapters.vault_kv_client",
        "dpone_airflow_pack.cache_sync",
        "legacy.cache.refresh",
    ]


def _expected_topology_summary() -> dict[str, object]:
    return {
        "dag_count": 100,
        "runtime_task_count": 500,
        "dag_ids_sha256": "sha256:" + "1" * 64,
        "spec_fingerprints_sha256": "sha256:" + "2" * 64,
        "task_ids_sha256": "sha256:" + "3" * 64,
        "edges_sha256": "sha256:" + "4" * 64,
        "topology_sha256": "sha256:" + "5" * 64,
    }


def _import_origins() -> dict[str, dict[str, object]]:
    return {
        "airflow.providers.dpone": {
            "distribution": "apache-airflow-providers-dpone",
            "version": CANDIDATE_VERSION,
            "relative_path": "airflow/providers/dpone/__init__.py",
            "within_environment": True,
        },
        "dpone_airflow_pack": {
            "distribution": "dpone-airflow-pack",
            "version": CANDIDATE_VERSION,
            "relative_path": "dpone_airflow_pack/__init__.py",
            "within_environment": True,
        },
        "airflow.providers.cncf.kubernetes": {
            "distribution": "apache-airflow-providers-cncf-kubernetes",
            "version": CNCF_PROVIDER_VERSION,
            "relative_path": "airflow/providers/cncf/kubernetes/__init__.py",
            "within_environment": True,
        },
    }


def _passing_worker(
    *,
    fingerprint: str | None = None,
    status: str = "PASS",
) -> dict[str, object]:
    topology = _expected_topology_summary()
    topology_fingerprint = fingerprint or str(topology["topology_sha256"])
    integrity = {
        "status": "PASS",
        "passed": True,
        "dag_count": 100,
        "runtime_task_count": 500,
        "workload_ids_match": True,
        "dag_ids_sha256": topology["dag_ids_sha256"],
        "spec_fingerprints_sha256": topology["spec_fingerprints_sha256"],
        "task_ids_sha256": topology["task_ids_sha256"],
        "edges_sha256": topology["edges_sha256"],
        "topology_fingerprint": topology_fingerprint,
        "expected_topology_fingerprint": topology["topology_sha256"],
        "index_sha256": "sha256:" + "f" * 64,
    }
    return {
        "status": status,
        "cold_loader_seconds": 1.0,
        "import_plus_first_load_seconds": 1.25,
        "process_cold_seconds": 1.25,
        "warm_loader_seconds": 0.5,
        "measurement": {
            "clock_started_before_forbidden_imports": True,
            "pre_clock_forbidden_imports": [],
        },
        "rss": {"baseline_bytes": 1_000, "peak_bytes": 1_100, "additional_bytes": 100},
        "side_effects": {
            "status": "PASS",
            "passed": True,
            "coverage_complete": True,
            "counters": {
                "network_calls": 0,
                "database_calls": 0,
                "secret_calls": 0,
                "kubernetes_calls": 0,
            },
            "required_groups": {},
            "forbidden_imports": [],
        },
        "parses": {"first": integrity, "warm": integrity},
        "loader_module": "airflow.providers.dpone",
        "import_origins": _import_origins(),
        "termination": {
            "status": "completed",
            "terminate_sent": False,
            "kill_sent": False,
        },
        "failures": [] if status == "PASS" else [{"code": "FAILED"}],
    }


def _fixture_metadata() -> dict[str, object]:
    return {
        "status": "READY",
        "cache_only": True,
        "dags": 100,
        "workloads": 500,
        "workloads_per_dag": 5,
        "release_id": benchmark.RELEASE_ID,
        "deployment_id": benchmark.DEPLOYMENT_ID,
        "index_bytes": 1,
        "total_bytes": 1,
        "file_count": EXPECTED_FIXTURE_FILE_COUNT,
        "sha256": "sha256:" + "d" * 64,
        "index_schema": "dpone.airflow-deployment-index.v2",
        "index_sha256": "sha256:" + "f" * 64,
        "expected_index_sha256": "sha256:" + "f" * 64,
        "index_digest_match": True,
        "delivery_mode": "init_fetch",
        "trust_tier": "non_production",
        "delivery_trust_tier": "non_production",
        "strict_v2_validated": True,
        "runtime_connection_artifacts": {
            "status": "PASS",
            "passed": True,
            "expected": 3,
            "verified": 3,
            "descriptors_sha256": "sha256:" + "6" * 64,
        },
        "expected_topology": _expected_topology_summary(),
    }


def _candidate_manifest() -> dict[str, object]:
    return {
        "schema": "dpone.airflow-provider-candidate-manifest.v1",
        "source_commit": SOURCE_COMMIT,
        "wheels": [
            {
                "distribution": "apache-airflow-providers-dpone",
                "filename": (f"apache_airflow_providers_dpone-{CANDIDATE_VERSION}-py3-none-any.whl"),
                "version": CANDIDATE_VERSION,
                "sha256": "sha256:" + "6" * 64,
            },
            {
                "distribution": "dpone-airflow-pack",
                "filename": f"dpone_airflow_pack-{CANDIDATE_VERSION}-py3-none-any.whl",
                "version": CANDIDATE_VERSION,
                "sha256": "sha256:" + "7" * 64,
            },
        ],
        "constraints": {
            "ref": CONSTRAINTS_REF,
            "filename": "constraints-3.3.0-3.12.txt",
            "sha256": CONSTRAINTS_SHA256,
        },
        "cncf_provider": {
            "distribution": "apache-airflow-providers-cncf-kubernetes",
            "version": CNCF_PROVIDER_VERSION,
        },
    }


def _candidate_manifest_sha256() -> str:
    return _manifest_sha256(_candidate_manifest())


def _manifest_sha256(payload: dict[str, object]) -> str:
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return _sha256(raw)


def _certifying_context() -> benchmark.BenchmarkContext:
    return benchmark.BenchmarkContext(
        source_commit=SOURCE_COMMIT,
        airflow_version="3.3.0",
        python_version="3.12",
        support="latest",
        constraints=CONSTRAINTS_REF,
        cncf_provider_version=CNCF_PROVIDER_VERSION,
        constraints_sha256=CONSTRAINTS_SHA256,
        candidate_manifest=_candidate_manifest(),
        candidate_manifest_sha256=_candidate_manifest_sha256(),
    )


def _use_certifying_runner(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        benchmark,
        "_runner_metadata",
        lambda: {
            "python": "3.12.4",
            "python_implementation": "CPython",
            "package_versions": {
                "apache-airflow": "3.3.0",
                "apache-airflow-providers-cncf-kubernetes": CNCF_PROVIDER_VERSION,
                "apache-airflow-providers-dpone": CANDIDATE_VERSION,
                "dpone-airflow-pack": CANDIDATE_VERSION,
            },
            "installed_distributions": {
                "apache-airflow": "3.3.0",
                "apache-airflow-providers-cncf-kubernetes": CNCF_PROVIDER_VERSION,
                "apache-airflow-providers-dpone": CANDIDATE_VERSION,
                "dpone-airflow-pack": CANDIDATE_VERSION,
            },
        },
    )


def test_evidence_requires_strict_fixture_integrity_for_top_level_pass(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [_passing_worker()],
    )

    assert payload["status"] == "PASS"
    assert payload["fixture_integrity"]["status"] == "PASS"
    assert payload["fixture_integrity"]["passed"] is True


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"index_schema": "dpone.airflow-deployment-index.v1"}, "schema"),
        ({"delivery_mode": "local_preview"}, "delivery_mode"),
        ({"strict_v2_validated": False}, "strict_v2_validation"),
        (
            {
                "index_sha256": "sha256:" + "e" * 64,
                "index_digest_match": False,
            },
            "index_digest",
        ),
    ],
)
def test_evidence_fails_closed_for_non_strict_fixture(
    override: dict[str, object],
    reason: str,
) -> None:
    fixture = {**_fixture_metadata(), **override}

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        benchmark.BenchmarkContext(source_commit="abc123"),
        fixture,
        [_passing_worker()],
    )

    assert payload["status"] == "FAIL"
    assert payload["fixture_integrity"]["status"] == "FAIL"
    assert reason in payload["fixture_integrity"]["failure_reasons"]


@pytest.mark.parametrize("case", ["v1", "local_preview", "malformed_strict_v2"])
def test_written_index_inspection_fails_closed_for_non_strict_fixture(
    tmp_path: Path,
    case: str,
) -> None:
    index_path, _ = benchmark._fixture(tmp_path / case)
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    if case == "v1":
        payload["schema"] = "dpone.airflow-deployment-index.v1"
    elif case == "local_preview":
        payload["runtime_artifact_delivery"] = {"mode": "local_preview"}
    else:
        payload.pop("runtime_image_ref")
    raw = (json.dumps(payload, separators=(",", ":"), sort_keys=True) + "\n").encode()
    index_path.write_bytes(raw)
    observed_digest = _sha256(raw)

    metadata = benchmark.inspect_fixture_index(
        index_path,
        expected_index_sha256=observed_digest,
    )
    integrity = benchmark.evaluate_fixture_integrity({**_fixture_metadata(), **metadata})

    assert metadata["index_schema"] == payload["schema"]
    assert metadata["index_sha256"] == observed_digest
    assert metadata["strict_v2_validated"] is False
    assert integrity["status"] == "FAIL"


def test_written_index_inspection_detects_digest_mismatch(tmp_path: Path) -> None:
    index_path, fixture = benchmark._fixture(tmp_path)

    metadata = benchmark.inspect_fixture_index(
        index_path,
        expected_index_sha256="sha256:" + "0" * 64,
    )
    integrity = benchmark.evaluate_fixture_integrity({**fixture, **metadata})

    assert metadata["index_sha256"] == fixture["index_sha256"]
    assert metadata["index_digest_match"] is False
    assert integrity["status"] == "FAIL"
    assert "index_digest" in integrity["failure_reasons"]


def test_evidence_names_loader_timings_and_records_import_observation() -> None:
    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        benchmark.BenchmarkContext(source_commit="abc123"),
        _fixture_metadata(),
        [_passing_worker()],
    )

    assert payload["budgets"]["cold_loader_p95_seconds"]["status"] == "PASS"
    assert payload["budgets"]["import_plus_first_load_p95_seconds"]["status"] == "PASS"
    assert payload["budgets"]["warm_loader_p95_seconds"]["status"] == "PASS"
    assert payload["sampling"]["cold_loader"] == {
        "model": "first provider loader call in a fresh worker after provider import",
        "requested": 1,
        "observed": 1,
        "durations_seconds": [1.0],
    }
    assert payload["sampling"]["import_plus_first_load"] == {
        "model": "provider import plus first loader call in a fresh worker",
        "requested": 1,
        "observed": 1,
        "durations_seconds": [1.25],
    }
    assert payload["sampling"]["warm_loader"]["durations_seconds"] == [0.5]
    assert payload["measurement_integrity"]["status"] == "PASS"


def test_import_plus_first_load_over_cold_budget_is_non_certifying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    worker = _passing_worker()
    worker["import_plus_first_load_seconds"] = 99.0

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [worker],
    )

    assert payload["status"] == "FAIL"
    assert payload["passed"] is False
    assert payload["budgets"]["import_plus_first_load_p95_seconds"] == {
        "status": "FAIL",
        "passed": False,
        "limit_seconds": COLD_LIMIT_SECONDS,
        "expected_samples": 1,
        "observed_samples": 1,
        "observed_p95_seconds": 99.0,
        "observed_max_seconds": 99.0,
    }


def test_evidence_fails_for_partial_and_failed_workers() -> None:
    config = benchmark.BenchmarkConfig(cold_samples=2, warm_samples=2)
    context = benchmark.BenchmarkContext(source_commit="abc123")

    partial = benchmark._evidence(config, context, _fixture_metadata(), [_passing_worker()])
    failed = benchmark._evidence(
        config,
        context,
        _fixture_metadata(),
        [_passing_worker(), _passing_worker(status="FAIL")],
    )

    assert partial["status"] == "FAIL"
    assert partial["load_integrity"]["observed_workers"] == 1
    assert failed["status"] == "FAIL"
    assert failed["load_integrity"]["failed_workers"] == [1]


def test_evidence_fails_when_topology_differs_between_workers() -> None:
    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=2, warm_samples=2),
        benchmark.BenchmarkContext(source_commit="abc123"),
        _fixture_metadata(),
        [_passing_worker(), _passing_worker(fingerprint="sha256:" + "9" * 64)],
    )

    assert payload["status"] == "FAIL"
    assert payload["load_integrity"]["topology_replay_match"] is False


def test_evidence_fails_when_stable_topology_does_not_match_fixture_authority(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    workers = [
        _passing_worker(fingerprint="sha256:" + "9" * 64),
        _passing_worker(fingerprint="sha256:" + "9" * 64),
    ]

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=2, warm_samples=2),
        _certifying_context(),
        _fixture_metadata(),
        workers,
    )

    assert payload["status"] == "FAIL"
    assert payload["load_integrity"]["topology_replay_match"] is True
    assert payload["load_integrity"]["expected_topology_match"] is False


def test_resolved_temp_root_uses_physical_path(tmp_path: Path) -> None:
    physical = tmp_path / "physical"
    physical.mkdir()
    lexical = tmp_path / "lexical"
    try:
        lexical.symlink_to(physical, target_is_directory=True)
    except OSError:
        pytest.skip("directory symlinks are unavailable")

    assert benchmark.resolved_temp_root(lexical) == physical.resolve()


def _invalid_worker_request(tmp_path: Path) -> dict[str, object]:
    return {
        "index_path": (tmp_path / "missing-airflow-index.json").as_posix(),
        "expected_index_sha256": "sha256:" + "f" * 64,
        "expected_topology": _topology_contract(),
        "measure_cold": True,
        "measure_warm": False,
    }


def test_sterile_worker_starts_clock_before_benchmark_and_provider_imports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:must-not-leak@example.invalid/db")
    environment = benchmark._sterile_worker_environment(tmp_path)

    result = benchmark._run_worker_process(
        _invalid_worker_request(tmp_path),
        working_root=tmp_path,
        timeout_seconds=benchmark.BenchmarkConfig().worker_timeout_seconds,
    )

    assert "DATABASE_URL" not in environment
    assert "PYTHONPATH" not in environment
    assert environment["PYTHONNOUSERSITE"] == "1"
    assert result["measurement"]["clock_started_before_forbidden_imports"] is True
    assert result["measurement"]["pre_clock_forbidden_imports"] == []
    assert result["process_cold_seconds"] > 0


def test_worker_timeout_terminates_process_and_emits_bounded_redacted_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "worker-timeout-secret-must-not-leak"
    monkeypatch.setattr(
        benchmark,
        "_worker_command",
        lambda *_args: (
            sys.executable,
            "-I",
            "-c",
            "import time; time.sleep(30)",
        ),
    )
    started = time.monotonic()

    result = benchmark._run_worker_process(
        {"caller": {"token": secret}},
        working_root=tmp_path,
        timeout_seconds=0.05,
    )

    assert time.monotonic() - started < 2.0
    assert result["status"] == "FAIL"
    assert result["failures"][0]["code"] == "DPONE_AIRFLOW_PARSE_WORKER_TIMEOUT"
    assert result["termination"]["status"] == "terminated"
    assert result["termination"]["terminate_sent"] is True
    assert secret not in json.dumps(result, sort_keys=True)


def test_main_writes_atomic_shape_stable_json_when_benchmark_setup_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "benchmark.json"
    replacements: list[tuple[Path, Path]] = []
    real_replace = os.replace

    def fail_benchmark(
        config: benchmark.BenchmarkConfig,
        context: benchmark.BenchmarkContext,
    ) -> dict[str, object]:
        del config, context
        raise RuntimeError("forced benchmark setup failure")

    def record_replace(source: str | Path, destination: str | Path) -> None:
        replacements.append((Path(source), Path(destination)))
        real_replace(source, destination)

    monkeypatch.setattr(benchmark, "run_benchmark", fail_benchmark)
    monkeypatch.setattr(benchmark.os, "replace", record_replace)

    exit_code = benchmark.main(
        [
            "--output",
            output.as_posix(),
            "--cold-samples",
            "1",
            "--warm-samples",
            "1",
            "--source-commit",
            "abc123",
        ]
    )

    payload = json.loads(output.read_text(encoding="utf-8"))
    assert exit_code == 1
    assert payload["schema"] == "dpone.airflow-provider-parse-benchmark.v1"
    assert payload["status"] == "FAIL"
    assert payload["source_commit"] == "abc123"
    assert payload["failures"] == [
        {
            "code": "DPONE_AIRFLOW_PARSE_BENCHMARK_FAILED",
            "message": "forced benchmark setup failure",
            "stage": "benchmark_setup",
            "type": "RuntimeError",
        }
    ]
    assert {budget["status"] for budget in payload["budgets"].values()} == {"FAIL"}
    assert output.read_text(encoding="utf-8") == json.dumps(payload, indent=2, sort_keys=True) + "\n"
    assert len(replacements) == 1
    temporary, destination = replacements[0]
    assert temporary.parent == output.parent
    assert destination == output
    assert not temporary.exists()

    success = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        benchmark.BenchmarkContext(source_commit="abc123"),
        _fixture_metadata(),
        [_passing_worker()],
    )
    assert success.keys() == payload.keys()


def test_evidence_schema_accepts_exact_pass_and_rejects_incoherent_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    schema_path = Path(__file__).parents[1] / "docs/schemas/gitops/airflow-provider-parse-benchmark.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [_passing_worker()],
    )

    validator.validate(payload)
    incoherent = {**payload, "passed": False}
    assert list(validator.iter_errors(incoherent))


@pytest.mark.parametrize("invalid_case", ["passed_false", "caller_metadata"])
def test_cli_never_succeeds_or_persists_unredacted_schema_invalid_evidence(
    invalid_case: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    secret = "schema-caller-secret-must-not-leak"
    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [_passing_worker()],
    )
    if invalid_case == "passed_false":
        payload["passed"] = False
    else:
        payload["caller_metadata"] = {"nested": {"token": secret}}
    monkeypatch.setattr(benchmark, "run_benchmark", lambda *_args: payload)
    output = tmp_path / f"{invalid_case}.json"

    exit_code = benchmark.main(
        [
            "--output",
            output.as_posix(),
            "--cold-samples",
            "1",
            "--warm-samples",
            "1",
            "--source-commit",
            SOURCE_COMMIT,
        ]
    )

    persisted = json.loads(output.read_text(encoding="utf-8"))
    schema = json.loads(
        (Path(__file__).parents[1] / "docs/schemas/gitops/airflow-provider-parse-benchmark.schema.json").read_text(
            encoding="utf-8"
        )
    )
    Draft202012Validator(schema).validate(persisted)
    assert exit_code == 1
    assert persisted["status"] == "FAIL"
    assert persisted["passed"] is False
    assert persisted["failures"][0]["code"] == "DPONE_AIRFLOW_PARSE_EVIDENCE_SCHEMA_INVALID"
    assert secret not in output.read_text(encoding="utf-8")


def test_worker_and_setup_failure_evidence_redacts_bounded_single_line_text(
    tmp_path: Path,
) -> None:
    secret = "must-not-leak"
    workstation_path = "/Users/alice/private/runtime.py"
    raw = (
        "Traceback (most recent call last):\n"
        f'password="{secret}" token={secret} '
        f"https://alice:{secret}@db.example.test/dwh failed at {workstation_path}"
    )
    error = RuntimeError(raw)
    worker = benchmark._worker_error(
        error,
        tmp_path / "cache" / "releases" / "release" / "deployment-index.json",
    )
    setup = benchmark._failure_evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        benchmark.BenchmarkContext(source_commit="abc123"),
        error,
    )["failures"][0]

    for failure in (worker, setup):
        message = failure["message"]
        assert secret not in message
        assert workstation_path not in message
        assert "alice:" not in message
        assert "Traceback (most recent call last)" not in message
        assert "\n" not in message
        assert len(message) <= 500
        assert "[REDACTED]" in message
        assert "$ABSOLUTE_PATH" in message


def test_failure_redactor_loads_without_installing_dpone() -> None:
    environment = dict(os.environ)
    environment.pop("PYTHONPATH", None)
    result = subprocess.run(
        (
            sys.executable,
            "-S",
            "-c",
            "from tools.airflow_provider_parse_benchmark_evidence "
            "import failure_diagnostic; "
            "print(failure_diagnostic("
            "RuntimeError('password=must-not-leak at /Users/alice/private.py'), "
            "stage='test')['message'])",
        ),
        cwd=Path(__file__).parents[1],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )

    assert "must-not-leak" not in result.stdout
    assert result.stdout.strip() == "password=[REDACTED] at $ABSOLUTE_PATH"


def test_side_effect_tripwires_block_and_count_without_leaking_arguments() -> None:
    network = benchmark.ParseSideEffectTripwire()
    with network:
        with pytest.raises(benchmark.ParseSideEffectAttempt):
            socket.create_connection(("token-do-not-leak.invalid", 443))

    database = benchmark.ParseSideEffectTripwire()
    with database:
        with pytest.raises(benchmark.ParseSideEffectAttempt):
            sqlite3.connect("password-do-not-leak.sqlite")

    class FakeSecrets:
        @staticmethod
        def get(secret_name: str) -> str:
            return secret_name

    secrets = benchmark.ParseSideEffectTripwire()
    with secrets:
        secrets.arm(
            FakeSecrets,
            "get",
            category="secrets",
            probe="test.FakeSecrets.get",
        )
        with pytest.raises(benchmark.ParseSideEffectAttempt) as error:
            FakeSecrets.get("api-key-do-not-leak")

    assert network.report()["counters"]["network_calls"] == 1
    assert database.report()["counters"]["database_calls"] == 1
    assert secrets.report()["counters"]["secret_calls"] == 1
    assert "api-key-do-not-leak" not in str(error.value)


def test_side_effect_tripwire_arms_every_binding_for_one_logical_probe() -> None:
    class LegacyVariable:
        @staticmethod
        def get(name: str) -> str:
            return name

    class PublicVariable:
        @staticmethod
        def get(name: str) -> str:
            return name

    tripwire = benchmark.ParseSideEffectTripwire()
    with tripwire:
        assert tripwire.arm(
            LegacyVariable,
            "get",
            category="secret",
            group="airflow_metadata",
            probe="airflow.Variable.get",
        )
        assert tripwire.arm(
            PublicVariable,
            "get",
            category="secret",
            group="airflow_metadata",
            probe="airflow.Variable.get",
        )
        with pytest.raises(benchmark.ParseSideEffectAttempt):
            LegacyVariable.get("legacy-secret")
        with pytest.raises(benchmark.ParseSideEffectAttempt):
            PublicVariable.get("public-secret")

    report = tripwire.report()
    assert report["probes"]["airflow.Variable.get"]["armed_bindings"] == 2
    assert report["probes"]["airflow.Variable.get"]["calls"] == 2
    assert report["counters"]["secret_calls"] == 2


def test_matrix_metadata_checks_supplied_installed_versions(monkeypatch: pytest.MonkeyPatch) -> None:
    _use_certifying_runner(monkeypatch)

    matched = benchmark.matrix_evidence(_certifying_context())
    mismatched = benchmark.matrix_evidence(
        replace(
            _certifying_context(),
            airflow_version="2.10.5",
            python_version="3.11",
            support="compatibility",
        )
    )

    assert matched["status"] == "PASS"
    assert matched["support"] == "latest"
    assert mismatched["status"] == "FAIL"


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda manifest: manifest.update(source_commit="b" * 40), "source_commit"),
        (
            lambda manifest: manifest["wheels"][0].update(sha256="not-a-digest"),
            "wheel_manifest",
        ),
        (
            lambda manifest: manifest["wheels"].append(
                {
                    "distribution": "unexpected",
                    "filename": "unexpected-1.0-py3-none-any.whl",
                    "version": "1.0",
                    "sha256": "sha256:" + "9" * 64,
                }
            ),
            "wheel_set",
        ),
        (
            lambda manifest: manifest["constraints"].update(sha256="sha256:" + "9" * 64),
            "constraints",
        ),
        (
            lambda manifest: manifest["cncf_provider"].update(version="0.0.1"),
            "cncf_provider",
        ),
    ],
)
def test_exact_candidate_manifest_mismatch_is_non_certifying(
    mutate: object,
    reason: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    manifest = json.loads(json.dumps(_candidate_manifest()))
    mutate(manifest)
    context = replace(
        _certifying_context(),
        candidate_manifest=manifest,
        candidate_manifest_sha256=_manifest_sha256(manifest),
    )

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        context,
        _fixture_metadata(),
        [_passing_worker()],
    )

    assert payload["status"] == "FAIL"
    assert payload["candidate_identity"]["status"] == "FAIL"
    assert reason in payload["candidate_identity"]["failure_reasons"]


def test_import_origin_outside_installed_environment_is_non_certifying(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    worker = _passing_worker()
    worker["import_origins"]["dpone_airflow_pack"]["within_environment"] = False

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [worker],
    )

    assert payload["status"] == "FAIL"
    assert payload["candidate_identity"]["status"] == "FAIL"
    assert "import_origins" in payload["candidate_identity"]["failure_reasons"]


@pytest.mark.parametrize(
    "missing",
    [
        "airflow_version",
        "python_version",
        "support",
        "constraints",
        "cncf_provider_version",
    ],
)
def test_each_missing_matrix_identity_is_na_and_never_passed(
    missing: str,
) -> None:
    matrix = benchmark.matrix_evidence(replace(_certifying_context(), **{missing: None}))

    assert matrix["status"] == "N/A"
    assert matrix["passed"] is False
    assert matrix["missing_identity"] == [missing]


@pytest.mark.parametrize(
    ("airflow_version", "python_version", "cncf_provider_version", "support"),
    [
        ("2.10.5", "3.11", "10.1.0", "compatibility"),
        ("2.10.5", "3.12", "10.1.0", "compatibility"),
        ("2.11.0", "3.11", "10.5.0", "compatibility"),
        ("2.11.0", "3.12", "10.5.0", "compatibility"),
        ("3.2.0", "3.11", "10.14.0", "primary"),
        ("3.2.0", "3.12", "10.14.0", "primary"),
        ("3.3.0", "3.11", "10.19.0", "compatibility"),
        ("3.3.0", "3.11", "10.20.0", "latest"),
        ("3.3.0", "3.12", "10.20.0", "latest"),
    ],
)
def test_installed_wheel_matrix_cell_without_exact_candidate_is_unverified(
    airflow_version: str,
    python_version: str,
    cncf_provider_version: str,
    support: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        benchmark,
        "_runner_metadata",
        lambda: {
            "python": f"{python_version}.9",
            "package_versions": {
                "apache-airflow": airflow_version,
                "apache-airflow-providers-cncf-kubernetes": cncf_provider_version,
            },
        },
    )
    context = benchmark.BenchmarkContext(
        source_commit=SOURCE_COMMIT,
        airflow_version=airflow_version,
        python_version=python_version,
        support=support,
        constraints=f"constraints-{airflow_version}/constraints-{python_version}.txt",
        cncf_provider_version=cncf_provider_version,
    )

    matrix = benchmark.matrix_evidence(context)

    assert matrix["status"] == "UNVERIFIED"
    assert matrix["passed"] is False
    assert matrix["missing_identity"] == ["candidate_manifest"]


@pytest.mark.parametrize("status", ["N/A", "UNVERIFIED"])
def test_non_pass_matrix_status_cannot_certify_even_with_passed_true(
    status: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    monkeypatch.setattr(
        benchmark._evidence_support,
        "matrix_evidence",
        lambda context, runner: {"status": status, "passed": True},
    )

    payload = benchmark._evidence(
        benchmark.BenchmarkConfig(cold_samples=1, warm_samples=1),
        _certifying_context(),
        _fixture_metadata(),
        [_passing_worker()],
    )

    assert payload["status"] == "FAIL"
    assert payload["passed"] is False


def test_missing_matrix_identity_is_na_and_exits_nonzero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_certifying_runner(monkeypatch)
    fixture_called = False

    def unexpected_fixture(root: Path) -> tuple[Path, dict[str, object]]:
        nonlocal fixture_called
        del root
        fixture_called = True
        raise AssertionError("fixture must not be built without matrix identity")

    monkeypatch.setattr(benchmark, "_fixture", unexpected_fixture)
    matrix = benchmark.matrix_evidence(benchmark.BenchmarkContext(source_commit="abc123"))
    output = tmp_path / "missing-matrix.json"
    exit_code = benchmark.main(
        [
            "--output",
            output.as_posix(),
            "--cold-samples",
            "1",
            "--warm-samples",
            "1",
            "--source-commit",
            "abc123",
        ]
    )
    payload = json.loads(output.read_text(encoding="utf-8"))

    assert matrix["status"] == "N/A"
    assert matrix["passed"] is False
    assert matrix["missing_identity"] == [
        "airflow_version",
        "python_version",
        "support",
        "constraints",
        "cncf_provider_version",
    ]
    assert exit_code == 1
    assert fixture_called is False
    assert payload["status"] == "FAIL"
    assert payload["matrix"]["status"] == "N/A"
    assert payload["matrix"]["passed"] is False


def _sha256(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()
