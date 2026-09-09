"""Bounded MSSQL chunk-journal verification after an unknown commit."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Sequence
from itertools import islice
from typing import Any

from dpone.backfill.state import BackfillChunkRecord

# SQL Server accepts at most 2,100 parameters per statement.  Keeping each
# point read at 1,000 chunk indexes plus the run key leaves ample headroom for
# future predicates without coupling recovery to the backend ceiling.
MSSQL_CHUNK_RECOVERY_BATCH_SIZE = 1_000


def iter_mssql_parameter_batches(
    indexes: Iterable[int],
    *,
    batch_size: int = MSSQL_CHUNK_RECOVERY_BATCH_SIZE,
) -> Iterator[tuple[int, ...]]:
    """Yield one-pass, bounded index batches suitable for MSSQL ``IN`` reads."""

    if batch_size < 1 or batch_size >= 2_100:
        raise ValueError("mssql_backfill_state.recovery_batch_size_invalid")
    iterator = iter(indexes)
    while batch := tuple(islice(iterator, batch_size)):
        yield batch


def committed_chunk_revisions_match(
    *,
    load_chunks: Callable[[str, Any, tuple[int, ...]], tuple[BackfillChunkRecord, ...]],
    run_key: str,
    connector: Any,
    expected: Sequence[BackfillChunkRecord],
) -> bool:
    """Compare a possibly committed revision in bounded, constant-memory reads."""

    for offset in range(0, len(expected), MSSQL_CHUNK_RECOVERY_BATCH_SIZE):
        expected_batch = expected[offset : offset + MSSQL_CHUNK_RECOVERY_BATCH_SIZE]
        expected_by_index = {record.index: record for record in expected_batch}
        if len(expected_by_index) != len(expected_batch):
            return False
        indexes = tuple(record.index for record in expected_batch)
        recovered = load_chunks(run_key, connector, indexes)
        recovered_by_index = {record.index: record for record in recovered}
        if set(recovered_by_index) != set(indexes):
            return False
        if any(recovered_by_index[index].to_jsonable() != expected_by_index[index].to_jsonable() for index in indexes):
            return False
    return True


__all__ = [
    "MSSQL_CHUNK_RECOVERY_BATCH_SIZE",
    "committed_chunk_revisions_match",
    "iter_mssql_parameter_batches",
]
