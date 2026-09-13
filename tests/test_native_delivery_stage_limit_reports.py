"""Versioned limits bind diagnostics without relaxing comparison authority."""

import json

import pytest
from tools.native_delivery_live_support.artifacts import ArtifactStore, digest
from tools.native_delivery_live_support.execution import ExecutionAdapter
from tools.native_delivery_live_support.hermetic import LIMITS, HermeticRouteFactory
from tools.native_delivery_live_support.profiles import Dataset
from tools.native_delivery_live_support.runner import configuration, route_record, run_benchmark
from tools.native_delivery_live_support.validation import validate_run

from dpone.runtime.native_delivery_benchmark import BenchmarkInputError, compare
from tests.test_native_delivery_baseline_compatibility import baseline_python as _baseline_python


def produce(root, limits):
    root.mkdir(exist_ok=True)
    return run_benchmark(
        adapter=ExecutionAdapter(HermeticRouteFactory(), "candidate"),
        dataset=Dataset("unicode", 16),
        config=configuration(limits),
        route=route_record("partition_replace", "bounded_native"),
        store=ArtifactStore(root / "run.json"),
    )


def test_extended_producer_and_both_consumers_keep_receipt_hashes(tmp_path):
    limits = {**LIMITS, "encoding_parallelism": 2, "import_parallelism": 1}
    root = tmp_path / "extended"
    run = produce(root, limits)
    assert run["schema_version"] == 2
    assert run["configuration"] == {"limits": limits, "sha256": digest(limits)}
    assert validate_run(run, root) == []  # Hermetic cannot certify live behavior.
    report = compare(root / "run.json", root / "run.json")
    assert report["status"] == "UNVERIFIED" and report["workloads"][0]["ratio"] is None
    for name in ("fidelity_receipt", "recovery_receipt"):
        receipt = json.loads((root / run[name]["path"]).read_text())
        assert receipt["schema_version"] == 1
        assert receipt["configuration_sha256"] == digest(limits)


def test_v1_and_v2_policy_changes_are_not_comparable(tmp_path):
    produce(tmp_path / "old", LIMITS)
    produce(tmp_path / "new", {**LIMITS, "encoding_parallelism": 2, "import_parallelism": 1})
    with pytest.raises(BenchmarkInputError, match="workload_or_configuration_drift"):
        compare(tmp_path / "old/run.json", tmp_path / "new/run.json")


@pytest.mark.parametrize("mutation", ["wrong_version", "missing", "extra", "null", "equal", "stale_digest"])
def test_extended_report_corruption_fails_both_readers(tmp_path, mutation):
    run = produce(tmp_path, {**LIMITS, "encoding_parallelism": 2, "import_parallelism": 1})
    limits = run["configuration"]["limits"]
    if mutation == "wrong_version":
        run["schema_version"] = 1
    elif mutation == "missing":
        limits.pop("import_parallelism")
    elif mutation == "extra":
        limits["unexpected"] = 1
    elif mutation == "null":
        limits["import_parallelism"] = None
    elif mutation == "equal":
        limits["encoding_parallelism"] = 1
    else:
        limits["encoding_parallelism"] = 3
    (tmp_path / "run.json").write_text(json.dumps(run))
    with pytest.raises(ValueError):
        validate_run(run, tmp_path)
    with pytest.raises(BenchmarkInputError):
        compare(tmp_path / "run.json", tmp_path / "run.json")


def test_current_harness_rejects_extended_policy_on_frozen_subject_before_factory(baseline_python, tmp_path):
    from tests.test_native_delivery_baseline_compatibility import invoke_baseline

    result, output, identity = invoke_baseline(
        baseline_python, tmp_path, {**LIMITS, "encoding_parallelism": 2, "import_parallelism": 1}, approved=True
    )
    assert result.returncode != 0
    assert not output.exists() and not identity.exists()


@pytest.fixture(scope="module")
def baseline_python(tmp_path_factory):
    return _baseline_python.__wrapped__(tmp_path_factory)
