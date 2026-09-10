# Reusing bounded native frame sizes

This guide is for maintainers integrating the bounded native MSSQL scheduler.
It explains how to reuse a frame's native byte reservation while preserving the
existing source, worker and recovery contracts. Operators can use the limit and
failure tables to interpret failures without treating a size as data evidence.

DDA-03 supplies the frame component and a local scheduler handoff fixture.
The [DDA-06 integration overview](https://github.com/PaulKov/dpone/blob/fefeab978749f930bc53143f7ccb25a45874973a/docs/delivery-acceleration/index.md)
describes the integrated scheduler, which now reuses the producer's reservation
without a second scheduler sizing pass. The standalone DDA-03 component commit
keeps scheduler wiring outside its scope. The integration recipe below records
the implemented wiring for review and backports.

Live route correctness and throughput remain **UNVERIFIED** until measured in an
explicitly approved disposable environment. See the
[delivery acceleration plan](../data-delivery-acceleration-tasks.md) and
[approved design](../feature-design-data-delivery-acceleration-v1.md).

## Try the component locally

The example needs the repository's Python environment and no database. Run it
with `uv run python` from the repository root:

```python
from dpone.contracts.mssql_native_chunks import NativeChunkLimits
from dpone.runtime.mssql_native_sized_frames import sized_native_frames
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

contract = build_mssql_bcp_native_contract(
    schema=[("value", "int")], query="SELECT synthetic"
)
limits = NativeChunkLimits(
    max_total_encoded_bytes=10000,
    stage_allocated_bytes_stop_threshold=10000,
    max_rows=2,
    max_bytes=4096,
    max_row_bytes=1024,
)
frames = list(sized_native_frames(iter([(1,), (2,), (3,)]), contract, limits))
print([(frame.rows, frame.encoded_bytes) for frame in frames])
```

Expected output:

```text
[(((1,), (2,)), 8), (((3,),), 4)]
```

The example creates no files, receipts, cache or target objects. A source
iterator with a `close()` method still belongs to its caller.

## Frame and limit contracts

`dpone.runtime.mssql_native_sized_frames` defines the internal frozen dataclass
`SizedNativeFrame(rows, encoded_bytes)` and generator
`sized_native_frames(rows, contract, limits, check=None, ipc_overhead=0)`.
`rows` is a tuple; `encoded_bytes` is the sum already calculated while finding
that frame's boundary. The existing `mssql_native_chunks_files.native_frames`
signature still yields row tuples, and its `NativeRow` import remains available.

The producer copies sequence rows into tuples and Mapping rows into plain
dictionaries, converting `bytearray` and `memoryview` field values to `bytes`
before sizing or requesting another source row. This detaches reused driver
containers and buffers. The envelope is frozen, but its dictionary snapshots are
not deeply immutable: consumers must not mutate them. Plain dictionaries retain
the existing pickle representation and IPC boundaries. Arbitrary nested mutable
values do not gain new support. The public source Mapping/transformation
boundary and supported scalar types are unchanged.

The stream is consumed once, with the existing one-row lookahead at a frame
boundary. It yields one empty, zero-byte frame for empty input and no trailing
empty frame for nonempty input. Cancellation callbacks run after pulling indices
0, 1024, and each following multiple of 1024. Scheduler admission and settlement
checks still run independently. Closing the tuple adapter closes its sized
iterator; neither closes the caller's source.

| Limit or measurement | Meaning and owner |
| --- | --- |
| `max_row_bytes` | Native bytes for one row, including field prefixes; sizing and worker encoding enforce it. |
| `max_rows` | Maximum rows per frame; the producer splits before appending the next row. |
| Native `max_bytes` | Maximum native payload per frame; the accumulated size is reused as the scheduler reservation. |
| Conservative producer IPC bound | Starts at `64 + ipc_overhead`, adds `len(pickle.dumps(row, protocol=5)) + 16` per detached row, and splits before exceeding `max_bytes`. One row plus overhead must fit independently. |
| Exact frame IPC bound | The scheduler checks `len(pickle.dumps(frame.rows, protocol=5)) <= max_bytes`. |
| Exact submitted-task IPC bound | The scheduler checks `len(pickle.dumps(args, protocol=5)) + 128 <= max_bytes`; `args` contains the complete worker positional arguments, unchanged by optional observations. |
| `max_total_encoded_bytes` | The scheduler bounds cumulative reservations across all frames. |
| Spool payload bound | Existing pending/concurrency limits and `spool_payload_bound` bound retained native payload; format/receipt files are separately accounted. |
| `stage_allocated_bytes_stop_threshold` | Observed SQL allocation stop threshold, checked through the importer. |
| Python heap / RSS | Measured separately; native and pickle byte limits are not a heap or RSS ceiling. |

Neither sizing nor pickle serialization validates every scalar value. Workers
continue to validate, encode, hash, fsync and seal exclusive files. Their actual
encoded size must match the reservation before acceptance. Typed digest versions,
file bytes, ordinal assignment, durable receipts and retained-file retries stay
unchanged. The size is transient memory metadata, never retained evidence.

## DDA-06 scheduler integration recipe

This records the wiring already implemented by DDA-06 in
`src/dpone/runtime/mssql_native_chunks.py`; it is a review checklist, not pending
work for users of that integration. The immutable
[integration test source](https://github.com/PaulKov/dpone/blob/fefeab978749f930bc53143f7ccb25a45874973a/tests/test_mssql_native_delivery_integration.py)
binds the structural expectations. Shared scheduler changes remain DDA-06-owned.

1. The scheduler imports `sized_native_frames` from
   `dpone.runtime.mssql_native_sized_frames` and consumes it with the existing
   source, contract, limits, cancellation callback and `ipc_overhead` arguments.
2. The worst-case worker envelope remains independently checked:
   `ipc_overhead = len(pickle.dumps(envelope, protocol=5)) + 128`, followed by the
   `ipc_overhead + 64 > limits.max_bytes` metadata rejection.
3. `sized_frame = next(frames, None)` uses `is None` for EOF. Empty rows are still
   required authority. The scheduler takes `frame = sized_frame.rows` and
   `size = sized_frame.encoded_bytes`.
4. The scheduler's sizing-only `MssqlNativeEncoder` instance and repeated
   `sum(encoder.encoded_row_size(row) for row in frame)` are removed. Staging-object,
   exact frame pickle, cumulative byte and importer capacity checks retain their
   existing order before `total += size`. Independent source adaptation and
   worker value validation remain in place.
5. Worker positional arguments remain
   `(contract, frame, directory / f"{ordinal}.native", ordinal,
   limits.max_row_bytes, size)`. The exact task check serializes `args` with
   protocol 5 and adds 128 bytes. Optional observations select `_encode_observed`
   instead of `_encode`; both receive the same positional arguments. The worker
   receives the same row tuple, not a `SizedNativeFrame`; its last positional
   argument remains the reservation.
6. `_Work(ordinal, size)` retains the reservation. Before `journal.attempt` or
   import, `file.encoded_bytes == work.encoded_bytes` is required; a mismatch raises
   `mssql_native.encoder_size_authority_changed`. Worker validation, file
   verification, capacity, fencing, retries and EOF/receipt completion remain.
7. `_stage` closes `frames` during cleanup; `stage` closes its source and preserves
   primary errors. The frame helper does not add a second source close.

The executable handoff double is `_submission` in
`tests/test_mssql_native_sized_frames.py`. Its call-counter test compares the old
scheduler pattern (two size calls per row) with the cached-size consumer (one per
row), preserves identical frame groups, and runs the unchanged file encoder.
This is deterministic structural proof, not a latency or throughput benchmark.
The component double and integrated scheduler retain the same positional task
envelope; optional observation dispatch adds no serialized keyword arguments.

DDA-06 has exercised real spawned workers with local target doubles for cached
reservation reuse, Mapping/tuple inputs, mutable buffers and poisoned reservations
that must fail before acceptance. Its preliminary
[82-case frame log](https://github.com/PaulKov/dpone/blob/fefeab978749f930bc53143f7ccb25a45874973a/test_artifacts/delivery-acceleration/dda-06/green-frames.log)
and [57-case structural/observation log](https://github.com/PaulKov/dpone/blob/fefeab978749f930bc53143f7ccb25a45874973a/test_artifacts/delivery-acceleration/dda-06/observation-wiring-reviewed.log)
record focused hermetic results. These are distinct from DDA-03's local handoff
fixture and do not establish a complete final integration gate or live timings.
The subsequent [417-case focused receipt](https://github.com/PaulKov/dpone/blob/7b3b14559f4820394501dfa19dc628cd49430b03/test_artifacts/delivery-acceleration/dda-06/native-focused.json)
records PASS on the pinned `fefeab9` integration source, including the unchanged
positional task envelope described above.
DDA-06 must retain and rerun the full frame/task/cumulative-limit, empty-authority,
cancellation/closure, retry and recovery coverage on its final frozen integration
commit. DDA-05/DDA-06 own approved live route evidence.

## Diagnose and verify

| Diagnostic | Next action |
| --- | --- |
| `mssql_native_row_bytes_exceeded` / `mssql_native.row_exceeds_frame_limit` | Inspect the declared row/native and IPC limits and schema width. A single row must fit; reducing rows per frame cannot repair an oversized row. |
| `mssql_native.IPC_metadata_limit_exceeded` / `IPC_frame_limit_exceeded` / `IPC_task_limit_exceeded` | Inspect the full serialized envelope and configured limit. Cached native size cannot replace IPC admission. |
| `mssql_native.total_encoded_bytes_exceeded` | Reassess the full workload and approved spool budget; do not bypass cumulative admission. |
| `mssql_native_invalid_value` and other scalar errors | Correct the source/schema incompatibility; size success alone never certified the values. |
| `mssql_native.encoder_size_authority_changed` | Stop acceptance and inspect producer/worker parity and snapshot mutation; do not replace the reservation with the observed size. |

Recovery continues through the existing
[bounded native transport lifecycle](../mssql-native-transport.md). Incomplete
staging requires settlement and complete-query re-extraction, not a mutable row
offset. Complete stages use existing verified receipts and retained-file rules.

Run the credential-free component checks:

```bash
uv run pytest tests/test_mssql_native_chunks_files.py tests/test_mssql_native_sized_frames.py tests/test_mssql_native_encoder.py tests/test_mssql_native_staged_values.py -q
```

These cover literal golden bytes and hashes, duplicate multiplicity, ordinals,
Mapping/sequence parity, mutable buffers, boundary errors, cancellation,
iterator ownership and the deterministic sizing counter. No configuration,
manifest, CLI, source transformation or migration change is required. The public
native partition SWITCH prohibition remains in force. Use the
[task plan](../data-delivery-acceleration-tasks.md) for integration and
certification ownership; a component PASS is not release readiness.
