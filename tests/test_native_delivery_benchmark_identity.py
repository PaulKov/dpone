"""Producer-to-consumer identity regressions; synthetic fixtures certify no live route."""

from __future__ import annotations

import json
import subprocess
import tomllib
from copy import deepcopy
from pathlib import Path

import pytest
from tools.native_delivery_live_support import execution, runner
from tools.native_delivery_live_support.artifacts import ArtifactStore, digest
from tools.native_delivery_live_support.execution import BASELINE_COMMIT, DeliveryClock, ExecutionAdapter, unavailable
from tools.native_delivery_live_support.hermetic import LIMITS, HermeticRouteFactory
from tools.native_delivery_live_support.profiles import Dataset
from tools.native_delivery_live_support.validation import require_comparable, validate_run

from dpone.runtime.native_delivery_benchmark import BenchmarkInputError, compare

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def produce(tmp_path, monkeypatch):
    """Run the actual producer with row/session doubles and deterministic clocks."""

    class NoRss:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def metric(self):
            return unavailable("bytes", "synthetic_identity_fixture")

    monkeypatch.setattr(runner, "ProcessTreeRss", NoRss)
    monkeypatch.setattr(runner, "git_identity", lambda _: {"commit": "c" * 40, "dirty": False})

    def create(name, *, version="0.77.1", seconds=10, change=None):
        directory = tmp_path / name
        directory.mkdir()
        factory = HermeticRouteFactory()
        # Only exercise consumer eligibility branches; this factory has no services.
        factory.execution = "live"
        environment = factory.describe()
        environment["versions"]["dpone"] = version
        if change is not None:
            change(environment)
        factory.describe = lambda: deepcopy(environment)
        subject = "a" * 40 if name == "baseline" else "b" * 40
        monkeypatch.setattr(execution, "git_identity", lambda _: {"commit": subject, "dirty": False})

        def clock():
            ticks = iter((0, seconds * 10**9, (seconds + 1) * 10**9))
            return DeliveryClock(now=lambda: next(ticks))

        monkeypatch.setattr(runner, "DeliveryClock", clock)
        report = runner.run_benchmark(
            adapter=ExecutionAdapter(factory, "candidate"),
            dataset=Dataset("narrow", 8),
            config=runner.configuration(LIMITS),
            route=runner.route_record("partition_replace", "bounded_native"),
            store=ArtifactStore(directory / "run.json"),
        )
        assert report["status"] == "PASS"
        assert len(validate_run(report, directory)) == 3
        return directory / "run.json", report

    return create


def save(path, report):
    path.write_text(json.dumps(report), encoding="utf-8")


@pytest.mark.parametrize("field", ["configuration", "environment"])
@pytest.mark.parametrize("both", [False, True], ids=["one-subject", "both-subjects"])
def test_offline_comparison_rejects_stale_bodies_before_overwriting_output(produce, tmp_path, field, both):
    baseline, before = produce("baseline")
    candidate, after = produce("candidate", seconds=8)
    assert compare(baseline, candidate)["status"] == "PASS"
    for path, report in [(candidate, after), *([(baseline, before)] if both else [])]:
        if field == "configuration":
            report[field]["limits"]["parallelism"] = 2
        else:
            report[field]["versions"]["mssql"] = "changed"
        save(path, report)
        with pytest.raises(ValueError, match="invalid_run_contract"):
            validate_run(report, path.parent)
    output = tmp_path / "comparison.json"
    output.write_text("existing report")
    with pytest.raises(BenchmarkInputError, match=f"{field}_digest_mismatch"):
        compare(baseline, candidate, output=output, overwrite=True)
    assert output.read_text() == "existing report"


@pytest.mark.parametrize("field", ["configuration", "environment"])
def test_rehashed_body_cannot_rebind_existing_receipts(produce, field):
    baseline, _ = produce("baseline")
    candidate, report = produce("candidate", seconds=8)
    if field == "configuration":
        report[field]["limits"]["parallelism"] = 2
        body = report[field]["limits"]
    else:
        report[field]["versions"]["mssql"] = "changed"
        body = {k: v for k, v in report[field].items() if k != "sha256"}
    report[field]["sha256"] = digest(body)
    save(candidate, report)
    with pytest.raises(BenchmarkInputError, match="receipt_identity_mismatch"):
        compare(baseline, candidate)
    with pytest.raises(ValueError, match="invalid_run_contract"):
        validate_run(report, candidate.parent)


def test_documented_baseline_to_current_subject_version_pair(produce):
    baseline_version = tomllib.loads(
        subprocess.check_output(["git", "show", f"{BASELINE_COMMIT}:pyproject.toml"], cwd=ROOT, text=True)
    )["project"]["version"]
    candidate_version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    assert baseline_version == "0.76.0" and baseline_version != candidate_version
    baseline, before = produce("baseline", version=baseline_version)
    candidate, after = produce("candidate", version=candidate_version, seconds=8)
    original = deepcopy((before, after))
    assert before["environment"]["sha256"] != after["environment"]["sha256"]
    require_comparable(before, after)
    result = compare(baseline, candidate)
    assert result["status"] == "PASS" and result["workloads"][0]["ratio"] == 0.8
    assert (before, after) == original
    assert json.loads(baseline.read_text()) == before and json.loads(candidate.read_text()) == after


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(lambda e: e["versions"].update(python="different"), id="python"),
        pytest.param(lambda e: e["versions"].update(mssql="different"), id="mssql"),
        pytest.param(lambda e: e["versions"].update(clickhouse="different"), id="clickhouse"),
        pytest.param(lambda e: e["versions"].update(bcp="different"), id="bcp"),
        pytest.param(lambda e: e["versions"].update(pyodbc="different"), id="additional-dependency"),
        pytest.param(lambda e: e["resource_profile"].update(cpu_count=2), id="resource-value"),
        pytest.param(lambda e: e["resource_profile"].update(cpu_count=1.0), id="resource-type"),
        pytest.param(lambda e: e.update(target_layout_sha256="d" * 64), id="layout"),
    ],
)
def test_other_environment_drift_still_rejects_valid_producer_evidence(produce, change):
    baseline, before = produce("baseline", version="0.76.0")
    candidate, after = produce("candidate", seconds=8, change=change)
    with pytest.raises(BenchmarkInputError, match="environment_drift"):
        compare(baseline, candidate)
    with pytest.raises(ValueError, match="invalid_run_contract"):
        require_comparable(before, after)


def test_direct_comparison_rejects_stale_environment_digest(produce):
    _, before = produce("baseline")
    _, after = produce("candidate")
    for report in (before, after):
        report["environment"]["versions"]["dpone"] = "changed"
    with pytest.raises(ValueError, match="invalid_run_contract"):
        require_comparable(before, after)


def test_comparison_preserves_dpone_version_presence_and_canonical_order(produce):
    _, before = produce("baseline")
    _, after = produce("candidate")
    environment = after["environment"]
    environment["versions"] = dict(reversed(list(environment["versions"].items())))
    require_comparable(before, after)
    del environment["versions"]["dpone"]
    environment["sha256"] = digest({k: v for k, v in environment.items() if k != "sha256"})
    with pytest.raises(ValueError, match="invalid_run_contract"):
        require_comparable(before, after)


def test_comparison_preserves_two_absent_subject_versions(produce):
    _, before = produce("baseline")
    _, after = produce("candidate")
    for report in (before, after):
        environment = report["environment"]
        del environment["versions"]["dpone"]
        environment["sha256"] = digest({k: v for k, v in environment.items() if k != "sha256"})
    require_comparable(before, after)
