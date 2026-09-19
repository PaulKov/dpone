"""Immutable evidence contract for external-publication performance observations."""

from __future__ import annotations

import hashlib
import json
import os
import platform
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import yaml

from dpone.runtime.sinks.clickhouse_external_replication_canonical import canonical_rows_digest
from tests.integration.clickhouse_cluster.evidence_identity import fixture_digest, source_binding

EXTERNAL_PERFORMANCE_RECEIPT = Path("test_artifacts/clickhouse-external-publication/benchmark-receipt.json")
EXTERNAL_PERFORMANCE_BUDGET = Path("tests/integration/clickhouse_cluster/external_replication_performance_budget.json")
CLICKHOUSE_CLUSTER_COMPOSE = Path("tests/integration/clickhouse_cluster/docker-compose.yml")
EXTERNAL_PERFORMANCE_TEST_NODEID = (
    "tests/integration/clickhouse_cluster/"
    "test_clickhouse_external_replication_performance_live.py::"
    "test_external_replication_performance_budget"
)
EXTERNAL_PERFORMANCE_COMMAND = (
    f"DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run --extra clickhouse pytest {EXTERNAL_PERFORMANCE_TEST_NODEID} -q"
)


def write_external_performance_receipt(
    *,
    canonical_digest: dict[str, Any],
    member_fanout: dict[str, Any],
    observation: dict[str, str],
    server_version: str,
) -> dict[str, Any]:
    """Write one immutable exact-commit observation; never replace prior evidence."""

    source_commit, source_tree = source_binding()
    benchmark_config = EXTERNAL_PERFORMANCE_BUDGET.read_bytes()
    budget = json.loads(benchmark_config)
    bound_fixture_digest = fixture_digest(source_commit)
    benchmark_config_sha256 = hashlib.sha256(benchmark_config).hexdigest()
    passed = canonical_digest.get("status") == "PASS" and member_fanout.get("status") == "PASS"
    evidence = {
        "schema_version": "dpone.clickhouse.external-publication-benchmark.v1",
        "status": "PASS" if passed else "FAIL",
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": source_commit,
        "source_tree": source_tree,
        "tracked_tree_status": "clean",
        "fixture_digest": bound_fixture_digest,
        "benchmark_config_sha256": benchmark_config_sha256,
        "environment": {
            "clickhouse_version": server_version,
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count() or 0,
        },
        "observation": observation,
        "canonical_digest": canonical_digest,
        "member_fanout": member_fanout,
    }
    validate_external_performance_receipt(
        evidence,
        source_commit=source_commit,
        source_tree=source_tree,
        fixture_digest=bound_fixture_digest,
        benchmark_config_sha256=benchmark_config_sha256,
        budget=budget,
    )
    _write_json_exclusive(EXTERNAL_PERFORMANCE_RECEIPT, evidence)
    return evidence


def verify_external_performance_receipt() -> dict[str, Any]:
    """Re-read and validate the persisted receipt against the current exact commit."""

    source_commit, source_tree = source_binding()
    evidence = json.loads(EXTERNAL_PERFORMANCE_RECEIPT.read_text(encoding="utf-8"))
    validate_external_performance_receipt(
        evidence,
        source_commit=source_commit,
        source_tree=source_tree,
        fixture_digest=fixture_digest(source_commit),
        benchmark_config_sha256=hashlib.sha256(EXTERNAL_PERFORMANCE_BUDGET.read_bytes()).hexdigest(),
        budget=json.loads(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8")),
    )
    return evidence


def validate_external_performance_receipt(
    evidence: Mapping[str, Any],
    *,
    source_commit: str,
    source_tree: str,
    fixture_digest: str,
    benchmark_config_sha256: str,
    budget: Mapping[str, Any],
) -> None:
    """Fail closed when identity, budget, correctness, or verdict evidence drifts."""

    errors = _identity_errors(
        evidence,
        source_commit=source_commit,
        source_tree=source_tree,
        fixture_digest=fixture_digest,
        benchmark_config_sha256=benchmark_config_sha256,
    )
    canonical = evidence.get("canonical_digest")
    fanout = evidence.get("member_fanout")
    if not isinstance(canonical, Mapping) or not isinstance(fanout, Mapping):
        errors.append("sections")
    else:
        _validate_sections(evidence, canonical, fanout, budget, errors)
    observation = evidence.get("observation")
    if not isinstance(observation, Mapping) or not _valid_observation(observation):
        errors.append("observation")
    environment = evidence.get("environment")
    if not isinstance(environment, Mapping) or not _valid_environment(environment):
        errors.append("environment")
    if errors:
        raise ValueError("external performance receipt failed validation: " + ",".join(sorted(set(errors))))


def _identity_errors(
    evidence: Mapping[str, Any],
    *,
    source_commit: str,
    source_tree: str,
    fixture_digest: str,
    benchmark_config_sha256: str,
) -> list[str]:
    expected = {
        "schema_version": "dpone.clickhouse.external-publication-benchmark.v1",
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": source_commit,
        "source_tree": source_tree,
        "tracked_tree_status": "clean",
        "fixture_digest": fixture_digest,
        "benchmark_config_sha256": benchmark_config_sha256,
    }
    errors = [field for field, value in expected.items() if evidence.get(field) != value]
    if set(evidence) != _TOP_LEVEL_FIELDS:
        errors.append("top_level_fields")
    return errors


def _validate_sections(
    evidence: Mapping[str, Any],
    canonical: Mapping[str, Any],
    fanout: Mapping[str, Any],
    budget: Mapping[str, Any],
    errors: list[str],
) -> None:
    if not set(canonical).issubset(_CANONICAL_FIELDS) or not set(fanout).issubset(_FANOUT_FIELDS):
        errors.append("section_fields")
    canonical_passed = (
        canonical.get("stable_across_trials") is True
        and _within_budget(canonical)
        and _valid_measurement(canonical)
        and canonical.get("digest") == _expected_canonical_digest(budget)
        and canonical.get("status") == "PASS"
    )
    counts = fanout.get("per_member_row_counts")
    members = fanout.get("members")
    logical_rows = fanout.get("logical_rows")
    fanout_passed = (
        isinstance(counts, list)
        and isinstance(members, int)
        and isinstance(logical_rows, int)
        and len(counts) == members
        and all(isinstance(count, int) for count in counts)
        and counts == [logical_rows] * members
        and sum(counts) == fanout.get("physical_rows")
        and fanout.get("content_digests_equal") is True
        and fanout.get("publication_phase") == "COMMITTED"
        and fanout.get("cleanup_proven") is True
        and _within_budget(fanout)
        and _valid_measurement(fanout)
        and fanout.get("status") == "PASS"
    )
    expected_status = "PASS" if canonical_passed and fanout_passed else "FAIL"
    if evidence.get("status") != expected_status:
        errors.append("aggregate_status")
    _validate_measured_budget(canonical, fanout, budget, errors)


def _within_budget(section: Mapping[str, Any]) -> bool:
    maximum = section.get("max_seconds")
    budget = section.get("budget_max_seconds")
    return isinstance(maximum, int | float) and isinstance(budget, int | float) and maximum <= budget


def _valid_measurement(section: Mapping[str, Any]) -> bool:
    trials = section.get("trial_seconds")
    trial_count = section.get("trials")
    maximum = section.get("max_seconds")
    if not isinstance(trials, list) or not isinstance(trial_count, int) or len(trials) != trial_count:
        return False
    if not trials or not all(isinstance(value, int | float) and value >= 0 for value in trials):
        return False
    return maximum == max(trials)


def _validate_measured_budget(
    canonical: Mapping[str, Any],
    fanout: Mapping[str, Any],
    budget: Mapping[str, Any],
    errors: list[str],
) -> None:
    canonical_budget = budget.get("canonical_digest", {})
    fanout_budget = budget.get("member_fanout", {})
    canonical_expected = {
        "rows": canonical_budget.get("rows"),
        "columns": canonical_budget.get("columns"),
        "warmups": canonical_budget.get("warmups"),
        "trials": canonical_budget.get("trials"),
        "budget_max_seconds": canonical_budget.get("max_trial_seconds"),
    }
    fanout_expected = {
        "members": fanout_budget.get("members"),
        "logical_rows": fanout_budget.get("logical_rows"),
        "physical_rows": fanout_budget.get("physical_rows"),
        "columns": fanout_budget.get("columns"),
        "warmups": fanout_budget.get("warmups"),
        "trials": fanout_budget.get("trials"),
        "budget_max_seconds": fanout_budget.get("max_trial_seconds"),
        "max_source_bytes": fanout_budget.get("max_source_bytes"),
    }
    if any(canonical.get(field) != value for field, value in canonical_expected.items()):
        errors.append("canonical_budget")
    if any(fanout.get(field) != value for field, value in fanout_expected.items()):
        errors.append("fanout_budget")


def _valid_observation(observation: Mapping[str, Any]) -> bool:
    if set(observation) != {"command", "finished_at", "observation_id", "started_at", "test_nodeid"}:
        return False
    if observation.get("command") != EXTERNAL_PERFORMANCE_COMMAND:
        return False
    if observation.get("test_nodeid") != EXTERNAL_PERFORMANCE_TEST_NODEID:
        return False
    observation_id = observation.get("observation_id")
    if not isinstance(observation_id, str) or len(observation_id) != 32:
        return False
    try:
        int(observation_id, 16)
        started = datetime.fromisoformat(str(observation["started_at"]))
        finished = datetime.fromisoformat(str(observation["finished_at"]))
    except (KeyError, TypeError, ValueError):
        return False
    return started.tzinfo is not None and finished.tzinfo is not None and started <= finished


def _valid_environment(environment: Mapping[str, Any]) -> bool:
    if set(environment) != _ENVIRONMENT_FIELDS:
        return False
    return (
        environment.get("clickhouse_version") == _pinned_clickhouse_version()
        and isinstance(environment.get("python_version"), str)
        and bool(environment["python_version"])
        and isinstance(environment.get("platform"), str)
        and bool(environment["platform"])
        and isinstance(environment.get("cpu_count"), int)
        and environment["cpu_count"] > 0
    )


def _pinned_clickhouse_version() -> str:
    payload = yaml.safe_load(CLICKHOUSE_CLUSTER_COMPOSE.read_text(encoding="utf-8"))
    services = payload.get("services") if isinstance(payload, Mapping) else None
    if not isinstance(services, Mapping):
        raise ValueError("ClickHouse fixture does not declare services")
    versions = {
        str(service["image"]).rsplit(":", 1)[1]
        for service in services.values()
        if isinstance(service, Mapping)
        and isinstance(service.get("image"), str)
        and str(service["image"]).startswith("clickhouse/clickhouse-server:")
    }
    if len(versions) != 1:
        raise ValueError("ClickHouse fixture must pin one exact server version")
    return versions.pop()


def _expected_canonical_digest(budget: Mapping[str, Any]) -> str:
    canonical_budget = budget.get("canonical_digest")
    if not isinstance(canonical_budget, Mapping):
        return ""
    rows = canonical_budget.get("rows")
    columns = canonical_budget.get("columns")
    if not isinstance(rows, int) or rows <= 0 or columns != 3:
        return ""
    return canonical_rows_digest(
        (index, f"value-{index % 1000:04d}", None if index % 7 == 0 else index % 97) for index in range(rows)
    )


def _write_json_exclusive(target: Path, evidence: Mapping[str, Any]) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(evidence, stream, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target)
        directory = os.open(target.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


_CANONICAL_FIELDS = {
    "budget_max_seconds",
    "columns",
    "digest",
    "error_type",
    "max_seconds",
    "median_seconds",
    "rows",
    "stable_across_trials",
    "status",
    "trial_seconds",
    "trials",
    "warmups",
}
_FANOUT_FIELDS = {
    "budget_max_seconds",
    "cleanup_proven",
    "columns",
    "content_digests_equal",
    "error_type",
    "logical_rows",
    "max_seconds",
    "max_source_bytes",
    "median_seconds",
    "members",
    "per_member_row_counts",
    "physical_rows",
    "publication_phase",
    "status",
    "trial_seconds",
    "trials",
    "warmups",
}
_TOP_LEVEL_FIELDS = {
    "benchmark_config_sha256",
    "canonical_digest",
    "environment",
    "evidence_scope",
    "fixture_digest",
    "member_fanout",
    "observation",
    "production_certification",
    "schema_version",
    "source_commit",
    "source_tree",
    "status",
    "tracked_tree_status",
}
_ENVIRONMENT_FIELDS = {"clickhouse_version", "cpu_count", "platform", "python_version"}


__all__ = [
    "EXTERNAL_PERFORMANCE_BUDGET",
    "EXTERNAL_PERFORMANCE_COMMAND",
    "EXTERNAL_PERFORMANCE_RECEIPT",
    "EXTERNAL_PERFORMANCE_TEST_NODEID",
    "validate_external_performance_receipt",
    "verify_external_performance_receipt",
    "write_external_performance_receipt",
]
