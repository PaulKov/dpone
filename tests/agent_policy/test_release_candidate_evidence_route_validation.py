from __future__ import annotations

from typing import Any

import pytest
from tests.agent_policy._release_candidate_evidence_helpers import (
    canonical,
    load_module,
    policy,
    valid_route_execution,
    valid_route_verification,
    valid_stress_benchmark,
)

validation = load_module(
    "dpone_release_candidate_evidence_route_validation_test",
    "tools/agent_policy/release_candidate_evidence_route_validation.py",
)

ROUTE = "mssql_clickhouse"
TRANSFER = "mssql_to_clickhouse_full_refresh"


def _replace(payload: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    current: Any = payload
    for element in path[:-1]:
        current = current[element]
    current[path[-1]] = value


def test_accepts_fully_reconciled_route_and_stress_evidence() -> None:
    execution = valid_route_execution(ROUTE)
    execution_raw = canonical(execution)
    verification = valid_route_verification(ROUTE, execution_raw=execution_raw)

    assert validation.validate_route_execution(execution, route=ROUTE)["status"] == "PASS"
    assert (
        validation.validate_route_verification(
            verification,
            route=ROUTE,
            execution_raw=execution_raw,
        )["status"]
        == "PASS"
    )
    assert validation.validate_stress(valid_stress_benchmark())["status"] == "PASS"


def test_stress_policy_freezes_meaningful_performance_floor() -> None:
    assert policy.MINIMUM_ROWS_PER_SECOND == 500.0
    assert policy.POLICY_PROJECTION["minimum_rows_per_second"] == policy.MINIMUM_ROWS_PER_SECOND


@pytest.mark.parametrize(
    ("rows_per_second", "accepted"),
    [(499.999, False), (500.0, True)],
)
def test_stress_performance_floor_is_inclusive_and_fail_closed(
    rows_per_second: float,
    accepted: bool,
) -> None:
    payload = valid_stress_benchmark()
    metric = payload["metrics"][0]
    metric["seconds"] = policy.ROW_COUNT / rows_per_second
    metric["rows_per_second"] = rows_per_second

    if accepted:
        assert validation.validate_stress(payload)["status"] == "PASS"
    else:
        with pytest.raises(ValueError, match="below.*performance floor"):
            validation.validate_stress(payload)


@pytest.mark.parametrize(
    ("seconds", "rows_per_second", "accepted"),
    [
        (10.0, 2499.90, True),
        (10.0, 2499.80, False),
        (50.0, 500.0, True),
    ],
)
def test_stress_rate_and_duration_rounding_intervals_must_intersect(
    seconds: float,
    rows_per_second: float,
    accepted: bool,
) -> None:
    payload = valid_stress_benchmark()
    metric = payload["metrics"][0]
    metric["seconds"] = seconds
    metric["rows_per_second"] = rows_per_second

    if accepted:
        assert validation.validate_stress(payload)["status"] == "PASS"
    else:
        with pytest.raises(ValueError, match="rows_per_second.*inconsistent"):
            validation.validate_stress(payload)


@pytest.mark.parametrize(
    ("route", "source", "sink"),
    [
        ("mssql_clickhouse", "mssql", "clickhouse"),
        ("postgres_mssql", "postgres", "mssql"),
    ],
)
def test_frozen_route_profile_matches_actual_model_serialization(
    route: str,
    source: str,
    sink: str,
) -> None:
    from dpone.ops.routes import RouteKey, RouteProfileCatalog

    actual = RouteProfileCatalog.default().get(RouteKey.of(source, sink, "incremental_merge"))

    assert actual is not None
    assert actual.to_dict() == policy.ROUTE_PROFILES[route]


@pytest.mark.parametrize(
    ("kind", "runner_id"),
    [
        ("execution", "refresh-live-certification"),
        ("verification", "refresh-live-certification-replay"),
    ],
)
def test_route_evidence_rejects_non_frozen_producer_runner(kind: str, runner_id: str) -> None:
    execution = valid_route_execution(ROUTE)
    execution_raw = canonical(execution)
    if kind == "execution":
        execution["runner_id"] = runner_id
        with pytest.raises(ValueError, match="runner identity"):
            validation.validate_route_execution(execution, route=ROUTE)
        return

    verification = valid_route_verification(ROUTE, execution_raw=execution_raw)
    verification["runner_id"] = runner_id
    with pytest.raises(ValueError, match="runner identity"):
        validation.validate_route_verification(
            verification,
            route=ROUTE,
            execution_raw=execution_raw,
        )


def test_route_evidence_rejects_one_field_profile_mutation() -> None:
    execution = valid_route_execution(ROUTE)
    execution["profile"]["native_fast_path"] = "untrusted-adapter"

    with pytest.raises(ValueError, match="profile"):
        validation.validate_route_execution(execution, route=ROUTE)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("summary", "chunks_total"), 2),
        (("summary", "chunks_succeeded"), 0),
        (("summary", "chunks_failed"), 1),
        (("summary", "chunks_skipped"), 1),
        (("chunks", 0, "passed"), False),
        (("chunks", 0, "blockers"), ["injected"]),
        (("chunks", 0, "rows_read"), policy.ROW_COUNT - 1),
        (("chunks", 0, "rows_written"), policy.ROW_COUNT - 1),
    ],
)
def test_execution_rejects_each_single_field_false_green(
    path: tuple[str | int, ...],
    value: Any,
) -> None:
    payload = valid_route_execution(ROUTE)
    _replace(payload, path, value)

    with pytest.raises(ValueError):
        validation.validate_route_execution(payload, route=ROUTE)


def test_execution_summary_has_closed_counter_schema() -> None:
    payload = valid_route_execution(ROUTE)
    payload["summary"]["untrusted_successes"] = 1

    with pytest.raises(ValueError, match="field set is invalid"):
        validation.validate_route_execution(payload, route=ROUTE)


def test_execution_chunk_has_closed_native_producer_schema() -> None:
    payload = valid_route_execution(ROUTE)
    payload["chunks"][0]["untrusted_success"] = True

    with pytest.raises(ValueError, match="field set is invalid"):
        validation.validate_route_execution(payload, route=ROUTE)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("summary", "chunks_total"), 2),
        (("summary", "chunks_verified"), 0),
        (("summary", "chunks_failed"), 1),
        (("summary", "source_rows"), policy.ROW_COUNT - 1),
        (("summary", "sink_rows"), policy.ROW_COUNT - 1),
        (("chunks", 0, "source", "row_count"), policy.ROW_COUNT - 1),
        (("chunks", 0, "sink", "row_count"), policy.ROW_COUNT - 1),
        (("chunks", 0, "source", "duplicate_keys"), 1),
        (("chunks", 0, "sink", "null_keys"), 1),
    ],
)
def test_verification_rejects_each_single_field_counter_mismatch(
    path: tuple[str | int, ...],
    value: Any,
) -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    _replace(payload, path, value)

    with pytest.raises(ValueError):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


def test_verification_summary_has_closed_counter_schema() -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    payload["summary"]["untrusted_verified"] = 1

    with pytest.raises(ValueError, match="field set is invalid"):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


@pytest.mark.parametrize("surface", ["chunk", "source", "sink"])
def test_verification_nested_objects_have_closed_native_producer_schema(surface: str) -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    target = payload["chunks"][0] if surface == "chunk" else payload["chunks"][0][surface]
    target["untrusted_success"] = True

    with pytest.raises(ValueError, match="field set is invalid"):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


@pytest.mark.parametrize("field", ["min_boundary", "max_boundary", "typed_hash"])
def test_verification_rejects_same_missing_comparison_field_on_both_sides(field: str) -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    del payload["chunks"][0]["source"][field]
    del payload["chunks"][0]["sink"][field]

    with pytest.raises(ValueError, match="field set is invalid"):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


@pytest.mark.parametrize("typed_hash", ["", "not-a-digest", "g" * 64, "c" * 63])
def test_verification_typed_hash_must_be_nonempty_sha256_hex(typed_hash: str) -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    payload["chunks"][0]["source"]["typed_hash"] = typed_hash
    payload["chunks"][0]["sink"]["typed_hash"] = typed_hash

    with pytest.raises(ValueError, match="typed_hash"):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [("ordinal", 2), ("idempotency_key", "attacker:replayed-chunk")],
)
def test_verification_chunk_identity_must_bind_execution_chunk(
    field: str,
    value: object,
) -> None:
    execution_raw = canonical(valid_route_execution(ROUTE))
    payload = valid_route_verification(ROUTE, execution_raw=execution_raw)
    payload["chunks"][0][field] = value

    with pytest.raises(ValueError, match="chunk identit|ordinal|idempotency"):
        validation.validate_route_verification(
            payload,
            route=ROUTE,
            execution_raw=execution_raw,
        )


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("transfers", TRANSFER, "artifact", "partitions", 0, "index"), 1),
        (("transfers", TRANSFER, "artifact", "partitions", 0, "exists"), False),
        (("transfers", TRANSFER, "artifact", "partitions", 0, "bytes"), 1023),
        (("transfers", TRANSFER, "artifact", "total_bytes"), 4097),
    ],
)
def test_stress_rejects_each_single_field_partition_mismatch(
    path: tuple[str | int, ...],
    value: Any,
) -> None:
    payload = valid_stress_benchmark()
    _replace(payload, path, value)

    with pytest.raises(ValueError):
        validation.validate_stress(payload)


def test_stress_rejects_observed_zero_partition_physical_chunk_shape() -> None:
    payload = valid_stress_benchmark()
    artifact = payload["transfers"][TRANSFER]["artifact"]
    artifact.update(
        {
            "artifact_type": "PhysicalChunkedFileExportArtifact",
            "estimated_rows": None,
            "mb_per_second": 0.0,
            "partition_count": 0,
            "partition_row_skew": 0,
            "partitions": [],
            "slowest_partition": None,
            "total_bytes": 0,
            "total_mb": 0.0,
        }
    )

    with pytest.raises(ValueError, match="artifact type is invalid"):
        validation.validate_stress(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [("seconds", 2.0), ("rows_per_second", 12_500.0)],
)
def test_stress_transfer_metric_must_exactly_match_validated_top_level_metric(
    field: str,
    value: float,
) -> None:
    payload = valid_stress_benchmark()
    payload["transfers"][TRANSFER]["metric"][field] = value

    with pytest.raises(ValueError, match="transfer.*metric.*top-level"):
        validation.validate_stress(payload)


def test_stress_rejects_non_mapping_partition_as_validation_error() -> None:
    payload = valid_stress_benchmark()
    partitions = payload["transfers"][TRANSFER]["artifact"]["partitions"]
    partitions[0] = ["not", "a", "mapping"]

    with pytest.raises(ValueError, match="must be a mapping"):
        validation.validate_stress(payload)


def test_stress_rejects_stale_slowest_partition_projection() -> None:
    payload = valid_stress_benchmark()
    payload["transfers"][TRANSFER]["artifact"]["slowest_partition"]["index"] = 1

    with pytest.raises(ValueError, match="partition diagnostics are inconsistent"):
        validation.validate_stress(payload)
