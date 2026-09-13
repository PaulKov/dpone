# Tune native encoding and import concurrency

Data engineers can give native encoding and BCP import different worker limits
while retaining one acquired ClickHouse stream. This how-to covers the approved
[stage 2 contract](../feature-specs/dda-independent-stage-limits.md), targeting a
separate minor release (initial candidate **0.80.0**, release pending). Stage 2
live correctness and performance remain **UNVERIFIED** until evidence for the
exact source and environment is retained. A larger limit is no speed guarantee.

## Prepare and inspect a configuration

Complete the [native transport prerequisites](../mssql-native-transport.md#prepare-and-configure),
including owned state, spool capacity, ClickHouse/BCP/MSSQL dependencies and the
application's runtime composition. Offline planning needs no service credentials.
The original [native example](../../examples/native/clickhouse-to-mssql-native.yaml)
keeps its settings. Inspect the new
[concurrency example](../../examples/native/clickhouse-to-mssql-native-concurrency.yaml):

```bash
uv run dpone plan examples/native/clickhouse-to-mssql-native-concurrency.yaml --format md
```

The example sets `chunking.parallelism: 2`, `native_chunks.encoding_parallelism: 2`
and `native_chunks.import_parallelism: 1`, with `max_pending: 2`. These keys live
under `defaults.source.options.native_transfer.execution` in this batch manifest.
The plan's `mssql_native.stage_concurrency` object contains exactly:

```json
{
  "encoding_parallelism": 2,
  "import_parallelism": 1,
  "retained_work_capacity": 4
}
```

Planning still reports `composition_required`, `live_preflight: not_run` and
`certification_status: unverified`; it does not execute a load. The
`stage_concurrency` object appears if and only if canonical limits are extended.
Omitted overrides, or overrides both effectively equal to `parallelism`, retain
the previous planner shape. There are no new CLI flags, exit codes or output
locations. Execute through the existing
[Python composition](../mssql-native-transport.md#compose-the-runtime).

## Choose explicit limits

| Setting | Default and admission | Meaning |
| --- | --- | --- |
| `execution.chunking.parallelism` | 1; strict integer 1–64 | Fallback for each unspecified stage |
| `execution.native_chunks.encoding_parallelism` | Omitted; strict integer 1–64 if authored | Maximum encoding process workers |
| `execution.native_chunks.import_parallelism` | Omitted; strict integer 1–64 if authored | Maximum concurrent file import/verification tasks |
| `execution.native_chunks.max_pending` | 2; strict integer 1–64 | Additional slots in the shared retained-work capacity |

Manifest `null`, booleans, strings, floats and out-of-range values are invalid
for either override. Omit a key to use the fallback. Validation occurs before
connector row I/O. Import parallelism does not cap all target connections:
parent-side admission and retry settlement can open other contexts.

The Python API accepts `None` as fallback. Both new arguments are keyword-only;
all eight historical positional arguments and their defaults retain their order:
`max_total_encoded_bytes`, `stage_allocated_bytes_stop_threshold`, `max_rows`,
`max_bytes`, `max_row_bytes`, `max_pending`, `max_staging_tables`, `parallelism`.
The two cumulative/allocation limits remain required. Other defaults and bounds
are in the [resource reference](../mssql-native-transport.md#resource-limits-and-observations).
Non-`None` Python overrides require exact `int` values from 1 through 64; `True`
and `False` are rejected.

```python
from dpone.contracts.mssql_native_chunks import NativeChunkLimits

limits = NativeChunkLimits(
    max_total_encoded_bytes=1073741824,
    stage_allocated_bytes_stop_threshold=10737418240,
    parallelism=2,
    encoding_parallelism=None,
    import_parallelism=1,
)
assert limits.effective_encoding_parallelism == 2
assert limits.effective_import_parallelism == 1
assert limits.retained_work_capacity == 4
assert limits.spool_payload_bound == 83886080  # 80 MiB of native payload
assert len(limits.to_dict()) == 10
```

## Understand the shared capacity

Let `E` and `I` be the resolved encoding and import counts. The scheduler uses
one retained-work map across both stages:

```text
C = max(E, I) + max_pending
payload reservation = (C + 1) * max_bytes
```

Encoding completion becomes an import task in the same slot. A transient retry
settles the previous writer and reuses the identical sealed file, ordinal and
slot. Receipt acceptance and successful file removal release a slot. The parent
loop does not refill slots during these completion transitions. There are no
parallel source queries or independent per-stage queues.

For the example, `C = max(2, 1) + 2 = 4`; five 16 MiB frames reserve 80 MiB of
native payload, including the existing extra-frame allowance. Format/receipt
files and the free-space reserve are additional. This is neither a Python RSS
bound nor an exclusive disk reservation or SQL allocation cap. Using `E + I`
would misstate the policy. The max-based bound also does not promise that both
worker pools stay saturated.

## Observe, recover and upgrade

Use [phase observations](observations.md) to inspect actual encode, BCP and
verification spans. Configured limits do not prove observed overlap. For tuning,
retain separate narrow and wide/Unicode experiments for `E=2/I=1` and `E=1/I=2`
with each policy, configuration hash, environment, timings and correctness
receipts. The [local Docker procedure](local-docker.md) requires an approved
environment, one warmup and three measured trials per configuration. Comparisons
require identical full configurations: a symmetric baseline and asymmetric
candidate cannot produce a certified cross-policy speedup.

Durable limits are an exact recovery binding. `to_dict()` emits the historical
eight fields when both effective counts equal `parallelism`; otherwise it adds
both resolved counts for ten fields. Even when both overrides are supplied,
`parallelism` remains part of the policy. Changing any canonical field causes the
existing resource-limit mismatch failure on recovery. Restore the original
settings and follow the [operations runbook](operations.md); never rewrite a
journal to make recovery accept a new policy.

Upgrade diagnostic readers before sharing extended v2 run reports; see the
[eight-field/v1 and ten-field/v2 migration](observations.md#shared-limit-validation).
Finish or safely settle an extended-policy invocation on its supporting version
before downgrading. Remove overrides only for future invocations. Empty input,
NULLs, duplicates, typed verification, EOF authority and evidence-before-checkpoint
ordering retain their existing contracts.

Return to the [delivery overview](index.md) or inspect
[frame sizing and byte admission](frames.md).
