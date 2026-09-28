from __future__ import annotations

import threading
import time
from dataclasses import replace

import pytest

from dpone.contracts.columnar_range_parallelism import (
    ColumnarRangeExecutionEvidence,
    RangeParallelismPolicy,
)
from dpone.runtime.columnar_range_parallelism import (
    AggregateRangeBudget,
    BoundedRangeExecutor,
    RangeExecutionResult,
    RangeParallelismPreflight,
    build_columnar_range_plan,
)
from dpone.runtime.partitioning import RangePartitioner


def _options(**parallelism: object) -> dict[str, object]:
    return {
        "partitioning": {
            "column": "record_id",
            "num_partitions": 3,
            "export_workers": 2,
            "load_workers": 2,
            "bounds": {"lower": 0, "upper": 30},
            "range_parallelism": {
                "mode": "required",
                "upload_workers": 2,
                "max_inflight_ranges": 2,
                "max_inflight_rows": 100,
                "max_inflight_bytes": 4096,
                "consistency": "immutable",
                **parallelism,
            },
        }
    }


def test_plan_identity_includes_normalized_policy_and_ranges() -> None:
    partitioner = RangePartitioner.from_options(_options())

    first = build_columnar_range_plan(partitioner, query_identity="sha256:query")
    second = build_columnar_range_plan(partitioner, query_identity="sha256:query")
    changed = build_columnar_range_plan(
        RangePartitioner.from_options(_options(staging_topology="per_partition")),
        query_identity="sha256:query",
    )

    assert first == second
    assert first.plan_fingerprint.startswith("sha256:")
    assert first.plan_fingerprint != changed.plan_fingerprint
    assert first.policy.reader_workers == 2
    assert len(first.ranges) == 3


@pytest.mark.parametrize("topology", ["shared_per_run", "per_partition"])
def test_preflight_accepts_supported_topology_and_declared_group_key(topology: str) -> None:
    policy = RangeParallelismPolicy.from_mapping(
        {
            "mode": "required",
            "consistency": "database_snapshot",
            "consistency_authority": {"database_snapshot": "synthetic_snapshot"},
            "staging_topology": topology,
            "group_key": ["account_id"],
        },
        reader_workers=2,
        load_workers=1,
    )

    RangeParallelismPreflight.validate(
        policy,
        partition_column="account_id",
        query_has_window_functions=True,
        window_partition_key=("account_id",),
        window_policy_verified=True,
        supported_topologies={"shared_per_run", "per_partition"},
    )


def test_preflight_rejects_window_split_and_unsafe_consistency() -> None:
    policy = RangeParallelismPolicy.from_mapping(
        {"mode": "required", "consistency": "immutable", "group_key": ["tenant_id"]},
        reader_workers=2,
        load_workers=1,
    )

    with pytest.raises(ValueError, match="part of the declared group key"):
        RangeParallelismPreflight.validate(
            policy,
            partition_column="record_id",
            query_has_window_functions=True,
            window_partition_key=("tenant_id", "month"),
            supported_topologies={"shared_per_run"},
        )

    with pytest.raises(ValueError, match="consistency"):
        RangeParallelismPolicy.from_mapping(
            {"mode": "required", "consistency": "snapshot"},
            reader_workers=2,
            load_workers=1,
        )


def test_window_group_key_is_not_accepted_without_machine_checkable_policy() -> None:
    policy = RangeParallelismPolicy.from_mapping(
        {"mode": "required", "consistency": "immutable", "group_key": ["account_id"]},
        reader_workers=2,
        load_workers=1,
    )

    with pytest.raises(ValueError, match="machine-checkable"):
        RangeParallelismPreflight.validate(
            policy,
            partition_column="account_id",
            query_has_window_functions=True,
            window_partition_key=("account_id",),
            supported_topologies={"shared_per_run"},
        )


@pytest.mark.parametrize(
    ("consistency", "authority_key"),
    [
        ("database_snapshot", "database_snapshot"),
        ("temporal_as_of", "as_of"),
        ("write_exclusion", "write_exclusion_ref"),
    ],
)
def test_mutable_source_consistency_requires_structured_authority(consistency: str, authority_key: str) -> None:
    with pytest.raises(ValueError, match="consistency_authority"):
        RangeParallelismPolicy.from_mapping(
            {"mode": "required", "consistency": consistency}, reader_workers=2, load_workers=1
        )

    policy = RangeParallelismPolicy.from_mapping(
        {
            "mode": "required",
            "consistency": consistency,
            "consistency_authority": {authority_key: "synthetic_authority"},
        },
        reader_workers=2,
        load_workers=1,
    )
    assert dict(policy.consistency_authority)[authority_key] == "synthetic_authority"


def test_aggregate_budget_rejects_oversized_items_and_tracks_high_water() -> None:
    budget = AggregateRangeBudget(max_ranges=2, max_rows=10, max_bytes=100)
    lease = budget.acquire(rows=6, retained_bytes=70)

    assert budget.snapshot().rows_high_water == 6
    assert budget.snapshot().bytes_high_water == 70
    with pytest.raises(ValueError, match="row budget"):
        budget.acquire(rows=11, retained_bytes=1)
    with pytest.raises(ValueError, match="byte budget"):
        budget.acquire(rows=1, retained_bytes=101)

    lease.release()
    assert budget.snapshot().current_rows == 0


def test_executor_uses_independent_sessions_observes_bound_and_closes() -> None:
    partitioner = RangePartitioner.from_options(_options())
    plan = build_columnar_range_plan(partitioner, query_identity="sha256:query")
    sessions: list[_Session] = []
    lock = threading.Lock()
    active = 0
    observed = 0

    def session_factory(_range):
        session = _Session()
        sessions.append(session)
        return session

    def worker(session, item, cancelled, budget):
        nonlocal active, observed
        assert not cancelled.is_set()
        with budget.acquire(rows=1, retained_bytes=8):
            with lock:
                active += 1
                observed = max(observed, active)
            time.sleep(0.01)
            with lock:
                active -= 1
        return RangeExecutionResult(item.range_id, rows=1, retained_bytes=8, eof_confirmed=True)

    result = BoundedRangeExecutor(session_factory=session_factory, worker=worker).execute(plan)

    assert [item.range_id for item in result.ranges] == [item.range_id for item in plan.ranges]
    assert result.observed_reader_concurrency == observed == 2
    assert len({id(session) for session in sessions}) == len(plan.ranges)
    assert all(session.closed for session in sessions)


def test_executor_cancels_other_sessions_and_never_returns_partial_success() -> None:
    partitioner = RangePartitioner.from_options(_options())
    plan = build_columnar_range_plan(partitioner, query_identity="sha256:query")
    sessions: list[_Session] = []

    def session_factory(_range):
        session = _Session()
        sessions.append(session)
        return session

    def worker(session, item, cancelled, budget):
        del session, budget
        if item.ordinal == 0:
            raise RuntimeError("synthetic reader failure")
        cancelled.wait(0.2)
        return RangeExecutionResult(item.range_id, rows=0, retained_bytes=0, eof_confirmed=True)

    with pytest.raises(RuntimeError, match="synthetic reader failure"):
        BoundedRangeExecutor(session_factory=session_factory, worker=worker).execute(plan)

    assert sessions
    assert all(session.closed for session in sessions)
    assert any(session.cancelled for session in sessions)


def test_evidence_rejects_missing_range_or_unconfirmed_eof() -> None:
    partitioner = RangePartitioner.from_options(_options())
    plan = build_columnar_range_plan(partitioner, query_identity="sha256:query")
    complete = tuple(
        RangeExecutionResult(item.range_id, rows=1, retained_bytes=8, eof_confirmed=True) for item in plan.ranges
    )

    evidence = ColumnarRangeExecutionEvidence.from_results(
        plan=plan,
        results=complete,
        observed_reader_concurrency=2,
        rows_high_water=2,
        bytes_high_water=16,
    )
    assert evidence.all_ranges_confirmed

    with pytest.raises(ValueError, match="cover every planned range"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=complete[:-1],
            observed_reader_concurrency=1,
            rows_high_water=1,
            bytes_high_water=8,
        )
    with pytest.raises(ValueError, match="EOF"):
        ColumnarRangeExecutionEvidence.from_results(
            plan=plan,
            results=(replace(complete[0], eof_confirmed=False), *complete[1:]),
            observed_reader_concurrency=2,
            rows_high_water=2,
            bytes_high_water=16,
        )


class _Session:
    def __init__(self) -> None:
        self.closed = False
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True

    def close(self) -> None:
        self.closed = True
