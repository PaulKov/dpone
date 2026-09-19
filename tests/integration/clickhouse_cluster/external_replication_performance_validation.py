"""Fail-closed validation for versioned external-publication benchmark evidence."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from dpone.runtime.sinks.clickhouse_external_replication_canonical import canonical_rows_digest

SCHEMA_V1 = "dpone.clickhouse.external-publication-benchmark.v1"
SCHEMA_V2 = "dpone.clickhouse.external-publication-benchmark.v2"
CLICKHOUSE_CLUSTER_COMPOSE = Path("tests/integration/clickhouse_cluster/docker-compose.yml")
EXTERNAL_PERFORMANCE_TEST_NODEID = (
    "tests/integration/clickhouse_cluster/"
    "test_clickhouse_external_replication_performance_live.py::"
    "test_external_replication_performance_budget"
)
EXTERNAL_PERFORMANCE_COMMAND = (
    f"DPONE_RUN_CLICKHOUSE_CLUSTER_PUBLICATION=1 uv run --extra clickhouse pytest {EXTERNAL_PERFORMANCE_TEST_NODEID} -q"
)


def validate_external_performance_receipt(
    evidence: Mapping[str, Any],
    *,
    source_commit: str,
    source_tree: str,
    fixture_digest: str,
    benchmark_config_sha256: str,
    budget: Mapping[str, Any],
) -> None:
    """Validate v2 strictly while retaining a bounded reader for historical v1."""

    schema = evidence.get("schema_version")
    errors = _identity_errors(
        evidence,
        schema=schema,
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
        _validate_sections(evidence, canonical, fanout, budget, schema=schema, errors=errors)
    if schema == SCHEMA_V2:
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
    schema: Any,
    source_commit: str,
    source_tree: str,
    fixture_digest: str,
    benchmark_config_sha256: str,
) -> list[str]:
    expected = {
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": source_commit,
        "source_tree": source_tree,
        "tracked_tree_status": "clean",
        "fixture_digest": fixture_digest,
        "benchmark_config_sha256": benchmark_config_sha256,
    }
    errors = [field for field, value in expected.items() if evidence.get(field) != value]
    expected_fields = _TOP_LEVEL_FIELDS.get(schema)
    if expected_fields is None:
        errors.append("schema_version")
    elif set(evidence) != expected_fields:
        errors.append("top_level_fields")
    if evidence.get("status") not in {"PASS", "FAIL"}:
        errors.append("status")
    return errors


def _validate_sections(
    evidence: Mapping[str, Any],
    canonical: Mapping[str, Any],
    fanout: Mapping[str, Any],
    budget: Mapping[str, Any],
    *,
    schema: Any,
    errors: list[str],
) -> None:
    if not _valid_section_fields(canonical, _CANONICAL_REQUIRED, _CANONICAL_FIELDS):
        errors.append("canonical_fields")
    if not _valid_section_fields(fanout, _FANOUT_REQUIRED, _FANOUT_FIELDS):
        errors.append("fanout_fields")
    canonical_measurement = _valid_measurement(canonical)
    fanout_measurement = _valid_measurement(fanout)
    if canonical.get("status") == "PASS" and not canonical_measurement:
        errors.append("canonical_measurement")
    if fanout.get("status") == "PASS" and not fanout_measurement:
        errors.append("fanout_measurement")
    _validate_canonical_digest(canonical, budget, errors)
    canonical_passed = (
        canonical.get("stable_across_trials") is True
        and canonical_measurement
        and _within_budget(canonical)
        and canonical.get("digest") == _expected_canonical_digest(budget)
        and canonical.get("status") == "PASS"
    )
    counts = fanout.get("per_member_row_counts")
    members = fanout.get("members")
    logical_rows = fanout.get("logical_rows")
    fanout_passed = (
        isinstance(counts, list)
        and _positive_int(members)
        and _positive_int(logical_rows)
        and len(counts) == members
        and all(_non_negative_int(count) for count in counts)
        and counts == [logical_rows] * members
        and sum(counts) == fanout.get("physical_rows")
        and fanout.get("content_digests_equal") is True
        and fanout.get("publication_phase") == "COMMITTED"
        and fanout.get("cleanup_proven") is True
        and fanout_measurement
        and _within_budget(fanout)
        and fanout.get("status") == "PASS"
    )
    expected_status = "PASS" if canonical_passed and fanout_passed else "FAIL"
    if evidence.get("status") != expected_status:
        errors.append("aggregate_status")
    _validate_measured_budget(canonical, fanout, budget, schema=schema, errors=errors)


def _valid_section_fields(
    section: Mapping[str, Any],
    required: frozenset[str],
    allowed: frozenset[str],
) -> bool:
    fields = set(section)
    if not required.issubset(fields) or not fields.issubset(allowed):
        return False
    status = section.get("status")
    if status not in {"PASS", "FAIL"}:
        return False
    if status == "PASS" and "median_seconds" not in fields:
        return False
    error_type = section.get("error_type")
    return error_type is None or (status == "FAIL" and isinstance(error_type, str) and bool(error_type))


def _valid_measurement(section: Mapping[str, Any]) -> bool:
    trials = section.get("trial_seconds")
    trial_count = section.get("trials")
    maximum = section.get("max_seconds")
    median = section.get("median_seconds")
    if not isinstance(trials, list) or not _positive_int(trial_count) or len(trials) != trial_count:
        return False
    if not trials or not all(_finite_non_negative(value) for value in trials):
        return False
    if not _finite_non_negative(maximum) or not _finite_non_negative(median):
        return False
    return math.isclose(maximum, max(trials), abs_tol=1e-6) and math.isclose(
        median,
        statistics.median(trials),
        abs_tol=1e-6,
    )


def _within_budget(section: Mapping[str, Any]) -> bool:
    maximum = section.get("max_seconds")
    limit = section.get("budget_max_seconds")
    return _finite_non_negative(maximum) and _finite_positive(limit) and maximum <= limit


def _validate_canonical_digest(
    canonical: Mapping[str, Any],
    budget: Mapping[str, Any],
    errors: list[str],
) -> None:
    digest = canonical.get("digest")
    if digest is None and canonical.get("status") == "FAIL":
        return
    expected = _expected_canonical_digest(budget)
    if not isinstance(digest, str) or len(digest) != 64 or digest != expected:
        errors.append("canonical_digest")


def _validate_measured_budget(
    canonical: Mapping[str, Any],
    fanout: Mapping[str, Any],
    budget: Mapping[str, Any],
    *,
    schema: Any,
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
    }
    if schema == SCHEMA_V2:
        fanout_expected["max_source_bytes"] = fanout_budget.get("max_source_bytes")
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
        and _positive_int(environment.get("cpu_count"))
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
    if not _positive_int(rows) or columns != 3:
        return ""
    return canonical_rows_digest(
        (index, f"value-{index % 1000:04d}", None if index % 7 == 0 else index % 97) for index in range(rows)
    )


def _finite_non_negative(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def _finite_positive(value: Any) -> bool:
    return _finite_non_negative(value) and value > 0


def _positive_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def _non_negative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


_CANONICAL_REQUIRED = frozenset(
    {"budget_max_seconds", "columns", "max_seconds", "rows", "status", "trial_seconds", "trials", "warmups"}
)
_CANONICAL_FIELDS = _CANONICAL_REQUIRED | {"digest", "error_type", "median_seconds", "stable_across_trials"}
_FANOUT_REQUIRED = frozenset(
    {
        "budget_max_seconds",
        "columns",
        "logical_rows",
        "max_seconds",
        "members",
        "per_member_row_counts",
        "physical_rows",
        "status",
        "trial_seconds",
        "trials",
        "warmups",
    }
)
_FANOUT_FIELDS = _FANOUT_REQUIRED | {
    "cleanup_proven",
    "content_digests_equal",
    "error_type",
    "max_source_bytes",
    "median_seconds",
    "publication_phase",
}
_BASE_TOP_LEVEL_FIELDS = frozenset(
    {
        "benchmark_config_sha256",
        "canonical_digest",
        "environment",
        "evidence_scope",
        "fixture_digest",
        "member_fanout",
        "production_certification",
        "schema_version",
        "source_commit",
        "source_tree",
        "status",
        "tracked_tree_status",
    }
)
_TOP_LEVEL_FIELDS = {SCHEMA_V1: _BASE_TOP_LEVEL_FIELDS, SCHEMA_V2: _BASE_TOP_LEVEL_FIELDS | {"observation"}}
_ENVIRONMENT_FIELDS = {"clickhouse_version", "cpu_count", "platform", "python_version"}


__all__ = [
    "EXTERNAL_PERFORMANCE_COMMAND",
    "EXTERNAL_PERFORMANCE_TEST_NODEID",
    "SCHEMA_V1",
    "SCHEMA_V2",
    "validate_external_performance_receipt",
]
