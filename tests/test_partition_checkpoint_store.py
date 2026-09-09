from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_checkpoint_store import (
    JsonlPartitionCheckpointStore,
    PartitionCheckpointDecodeError,
)


def _checkpoint(
    status: PartitionCheckpointStatus, partition: int, *, query_hash: str = "query-a"
) -> PartitionCheckpoint:
    return PartitionCheckpoint(
        transfer_partition_id=f"{partition:064x}",
        status=status,
        query_hash=query_hash,
        schema_hash="schema-a",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"lower": partition * 100, "upper": (partition + 1) * 100},
        started_at=datetime(2026, 6, 9, tzinfo=UTC),
        completed_at=datetime(2026, 6, 9, 0, 1, tzinfo=UTC),
        rows_exported=100,
        bytes_exported=4096,
        diagnostics={"artifact_checksum": "sha256:abc"},
    )


def test_jsonl_checkpoint_store_persists_latest_checkpoint_state(tmp_path) -> None:
    store_path = tmp_path / "checkpoints.jsonl"
    store = JsonlPartitionCheckpointStore(store_path)

    store.upsert(_checkpoint(PartitionCheckpointStatus.EXPORTED, 0))
    store.upsert(_checkpoint(PartitionCheckpointStatus.COMMITTED, 0))
    store.upsert(_checkpoint(PartitionCheckpointStatus.LOADED, 1))

    reloaded = JsonlPartitionCheckpointStore(store_path)
    checkpoints = reloaded.list_latest()

    assert [checkpoint.status for checkpoint in checkpoints] == [
        PartitionCheckpointStatus.COMMITTED,
        PartitionCheckpointStatus.LOADED,
    ]
    assert reloaded.summary() == {
        "planned": 0,
        "exported": 0,
        "loaded": 1,
        "finalized": 0,
        "committed": 1,
        "failed": 0,
    }


def test_jsonl_checkpoint_store_returns_only_safe_skip_candidates(tmp_path) -> None:
    store = JsonlPartitionCheckpointStore(tmp_path / "checkpoints.jsonl")
    store.upsert(_checkpoint(PartitionCheckpointStatus.COMMITTED, 0))
    store.upsert(_checkpoint(PartitionCheckpointStatus.COMMITTED, 1, query_hash="query-old"))
    store.upsert(_checkpoint(PartitionCheckpointStatus.EXPORTED, 2))

    safe = store.safe_to_skip(query_hash="query-a", schema_hash="schema-a")

    assert [checkpoint.transfer_partition_id for checkpoint in safe] == [f"{0:064x}"]


def test_jsonl_checkpoint_store_is_append_only_audit_log(tmp_path) -> None:
    store_path = tmp_path / "checkpoints.jsonl"
    store = JsonlPartitionCheckpointStore(store_path)

    store.upsert(_checkpoint(PartitionCheckpointStatus.EXPORTED, 0))
    store.upsert(_checkpoint(PartitionCheckpointStatus.COMMITTED, 0))

    lines = store_path.read_text(encoding="utf-8").strip().splitlines()

    assert len(lines) == 2
    assert '"status":"exported"' in lines[0]
    assert '"status":"committed"' in lines[1]


@pytest.mark.parametrize(
    ("rows_exported", "field_present"),
    [
        pytest.param(True, True, id="boolean"),
        pytest.param("5", True, id="string"),
        pytest.param(5.5, True, id="float"),
        pytest.param(-1, True, id="negative"),
        pytest.param(None, False, id="missing"),
    ],
)
def test_jsonl_checkpoint_store_rejects_invalid_rows_exported(
    tmp_path,
    rows_exported: object,
    field_present: bool,
) -> None:
    store_path = tmp_path / "checkpoints.jsonl"
    store = JsonlPartitionCheckpointStore(store_path)
    store.upsert(_checkpoint(PartitionCheckpointStatus.COMMITTED, 0))
    payload = json.loads(store_path.read_text(encoding="utf-8"))
    if field_present:
        payload["rows_exported"] = rows_exported
    else:
        payload.pop("rows_exported")
    store_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(PartitionCheckpointDecodeError) as raised:
        store.list_latest()

    assert raised.value.code == "partition_checkpoint_counter_invalid"
    assert raised.value.field == "rows_exported"
    assert str(raised.value) == "partition_checkpoint_counter_invalid: rows_exported"
