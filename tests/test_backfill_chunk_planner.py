from __future__ import annotations

import pytest

from dpone.backfill.models import (
    chunk_spec_from_options,
    inner_mode_from_options,
    parallel_workers_from_options,
    parse_step,
)
from dpone.backfill.planner import backfill_run_key, plan_chunks
from dpone.contracts.portable_relation_scope import portable_scope_contract, portable_scope_sha256


def _spec(**chunk):
    payload = {"column": "business_date", "from": "2025-01-01", "to": "2025-01-10", "step": "3d"}
    payload.update(chunk)
    return chunk_spec_from_options({"chunk": payload})


def test_chunk_spec_is_optional_without_chunk_block() -> None:
    assert chunk_spec_from_options({}) is None
    assert chunk_spec_from_options({"inner_mode": "replace"}) is None


def test_chunk_spec_requires_all_window_keys() -> None:
    with pytest.raises(ValueError, match="backfill.chunk requires keys: to, step"):
        chunk_spec_from_options({"chunk": {"column": "d", "from": "2025-01-01"}})


def test_chunk_spec_rejects_inverted_boundaries() -> None:
    with pytest.raises(ValueError, match="from must be <= "):
        _spec(**{"from": "2025-02-01", "to": "2025-01-01"})


def test_step_parsing_contract() -> None:
    assert parse_step("6h", kind="timestamp").unit == "h"
    assert parse_step("1mo", kind="date").unit == "mo"
    assert parse_step("4", kind="integer").count == 4
    with pytest.raises(ValueError, match="requires kind: timestamp"):
        parse_step("6h", kind="date")
    with pytest.raises(ValueError, match="is temporal but chunk kind is integer"):
        parse_step("1d", kind="integer")
    with pytest.raises(ValueError, match="not supported"):
        parse_step("fortnight", kind="date")


def test_inner_mode_and_parallel_workers_validation() -> None:
    assert inner_mode_from_options(None) == "partition_replace"
    assert inner_mode_from_options({"inner_mode": "full_refresh"}) == "full_refresh"
    with pytest.raises(ValueError, match="backfill.inner_mode"):
        inner_mode_from_options({"inner_mode": "snapshot_diff"})
    assert parallel_workers_from_options({"parallel_workers": 4}) == 4
    with pytest.raises(ValueError, match="parallel_workers"):
        parallel_workers_from_options({"parallel_workers": 0})


def test_date_chunks_are_half_open_and_cover_inclusive_to() -> None:
    chunks = plan_chunks(_spec(), run_key="k")

    assert [chunk.start for chunk in chunks] == ["2025-01-01", "2025-01-04", "2025-01-07", "2025-01-10"]
    assert chunks[-1].end == "2025-01-11"
    assert portable_scope_contract(chunks[0].portable_scope) == {
        "column": "business_date",
        "kind": "range",
        "lower": {"inclusive": True, "value": {"type": "date", "value": "2025-01-01"}},
        "upper": {"inclusive": False, "value": {"type": "date", "value": "2025-01-04"}},
        "version": 1,
    }
    assert all(chunks[i].end == chunks[i + 1].start for i in range(len(chunks) - 1))


def test_integer_chunks_are_inclusive() -> None:
    spec = chunk_spec_from_options({"chunk": {"column": "id", "from": "1", "to": "10", "step": "4"}})

    chunks = plan_chunks(spec, run_key="k")

    assert [(chunk.start, chunk.end) for chunk in chunks] == [("1", "4"), ("5", "8"), ("9", "10")]
    assert chunks[0].portable_scope.lower is not None and chunks[0].portable_scope.lower.inclusive is True
    assert chunks[0].portable_scope.upper is not None and chunks[0].portable_scope.upper.inclusive is True
    assert chunks[0].portable_scope.upper.value.value == 4


def test_uuid_chunks_cover_complete_keyspace_with_indexable_adjacent_ranges() -> None:
    spec = chunk_spec_from_options(
        {
            "max_chunks": 64,
            "chunk": {"column": "id", "kind": "uuid", "buckets": 64},
        }
    )

    chunks = plan_chunks(spec, run_key="uuid-seed")

    assert len(chunks) == 64
    assert chunks[0].start == "00000000-0000-0000-0000-000000000000"
    assert chunks[-1].end == "ffffffff-ffff-ffff-ffff-ffffffffffff"
    assert chunks[-1].portable_scope.upper is not None
    assert chunks[-1].portable_scope.upper.inclusive is True
    assert all(
        chunk.portable_scope.upper is not None
        and chunk.portable_scope.upper.inclusive is False
        and chunk.end == chunks[index + 1].start
        for index, chunk in enumerate(chunks[:-1])
    )
    assert len({chunk.idempotency_key for chunk in chunks}) == 64


@pytest.mark.parametrize(
    "chunk",
    (
        {"column": "id", "kind": "uuid"},
        {"column": "id", "kind": "uuid", "buckets": 0},
        {"column": "id", "kind": "uuid", "buckets": 4, "from": 0},
    ),
)
def test_uuid_chunk_authoring_is_closed_and_fail_fast(chunk: dict[str, object]) -> None:
    with pytest.raises(ValueError, match=r"backfill\.chunk\.(?:buckets|kind=uuid)"):
        chunk_spec_from_options({"chunk": chunk})


def test_timestamp_chunks_use_exclusive_end() -> None:
    spec = chunk_spec_from_options(
        {"chunk": {"column": "ts", "from": "2025-01-01T00:00:00", "to": "2025-01-01T12:00:00", "step": "6h"}}
    )

    chunks = plan_chunks(spec, run_key="k")

    assert len(chunks) == 2
    assert chunks[-1].portable_scope.lower is not None
    assert chunks[-1].portable_scope.lower.value.to_contract() == {
        "type": "timestamp",
        "value": "2025-01-01T06:00:00",
    }
    assert chunks[-1].portable_scope.upper is not None and chunks[-1].portable_scope.upper.inclusive is False


def test_month_step_clamps_to_month_end() -> None:
    spec = chunk_spec_from_options({"chunk": {"column": "d", "from": "2025-01-31", "to": "2025-03-31", "step": "1mo"}})

    chunks = plan_chunks(spec, run_key="k")

    assert chunks[0].end == "2025-02-28"


def test_plan_is_deterministic_and_keys_are_stable() -> None:
    spec = _spec()
    key = backfill_run_key(dataset="dwh.orders", spec=spec, inner_mode="partition_replace")

    first = plan_chunks(spec, run_key=key)
    second = plan_chunks(spec, run_key=key)

    assert first == second
    assert first[0].idempotency_key.startswith(f"{key}:1:")
    assert first[0].portable_scope_sha256 == portable_scope_sha256(first[0].portable_scope).hex()
    assert first[0].portable_scope_sha256 in first[0].idempotency_key
    assert key != backfill_run_key(dataset="dwh.orders", spec=spec, inner_mode="replace")


def test_max_chunks_guard_blocks_chunk_explosions() -> None:
    spec = chunk_spec_from_options(
        {"max_chunks": 3, "chunk": {"column": "d", "from": "2025-01-01", "to": "2025-12-31", "step": "1d"}}
    )

    with pytest.raises(ValueError, match="above max_chunks=3"):
        plan_chunks(spec, run_key="k")
