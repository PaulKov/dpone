# Atomic rolling windows

This guide is for developers composing a bounded PostgreSQL-to-ClickHouse refresh
through dpone's Python runtime. It replaces one half-open UTC interval, preserves
rows outside it, and preserves duplicate multiplicity.

This is an opt-in composition API. A rolling manifest alone does not supply the
source snapshot, target writer authority, evidence writer, or state writer. The
ordinary process runner refuses execution without these capabilities.

## Prerequisites

- PostgreSQL ordinary source table with a `timestamptz` window column. Worker
  connections must be independent connections to the same source.
- ClickHouse local `Atomic` database and plain `MergeTree` target. Its window
  column must be UTC `DateTime64` with precision at most six.
- No target TTL, projections, dependent views, computed/default columns, active
  mutations, or row policies. Replicated and Distributed engines are unsupported.
- A real `ExclusiveWindowWriterGuard` covering every target and staging writer,
  including direct SQL and other processes. It must retain exclusion through
  server completion and fence/join old requests before replacement. No production
  guard implementation is bundled. A YAML boolean or SQLite lease does not
  establish this authority.
- Durable access-controlled storage under `runtime.storage.work_dir`.
  `SQLiteWindowStore` supports a durable local filesystem, not network storage.
- Durable idempotent evidence and state callbacks. Each must fence its actual
  mutation and validate its expected prior state. State advancement must be
  monotonic or compare-and-swap protected.

Keep credentials in injected connection factories. Use stable sanitized route and
target identifiers; bind the route identifier to immutable source query
configuration. A runtime instance belongs to one route and schema; its factory
must reject compiled configurations for a different route.

Legacy configurable quality/acceptance policies, hooks, CDC, backfill,
reconciliation, schema evolution, deduplication, and additional predicates are
currently rejected before factories run. The window path supplies its own typed
staging reconciliation; it never silently skips an authored legacy check.

## Configure the window

Add these fields to an otherwise valid source/sink manifest:

```yaml
source:
  options:
    native_transfer:
      execution:
        chunking:
          mode: bounded_window
          parallelism: 4
          checkpointing: resumable
sink:
  strategy:
    mode: replace
    atomicity: target_atomic
    window:
      column: observed_at
      anchor: data_interval_end
      lookback: P7D
      chunk_interval: P1D
      timezone: UTC
```

Do not combine the window with `custom_predicate` or `portable_scope`. Supply the
aware interval end through the existing `load_config.options.interval.interval_end`
context. No wall-clock fallback is used. At `2026-09-09T00:00:00Z`, this selects
`[2026-09-02T00:00:00Z, 2026-09-09T00:00:00Z)`.

Parallelism is 1–64, default 1. Chunks partition the interval without overlaps or
gaps. Fixed durations support days, hours, minutes, and seconds to microsecond
precision; days are 24 hours in UTC. Calendar months and years are unsupported.
NULL source window values are outside the interval; existing NULL-window target
rows are retained. An empty source window removes existing target rows only
inside that interval after verification.

## Compose execution

The following factory assembles the runtime from supplied capabilities. Connection
factories and durable callbacks are application-owned; no placeholder authority is
provided. The executor journal factory and wrapper invocation registry must share the same
durable store and key contract, as in this example. Construct this once per
compiled route and pass it through the existing
`DefaultProcessRunner(window_runtime_factory=...)` injection point.

```python
from pathlib import Path
from time import sleep, time

from dpone.adapters.bounded_window_journal import WindowJournal
from dpone.adapters.window_metadata_files import FileWindowMetadataStore
from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.runtime.bounded_window_execution import BoundedWindowExecutor
from dpone.runtime.rolling_window_runtime import RollingWindowRuntime
from dpone.runtime.sinks.clickhouse_window_target import (
    ClickHouseWindowTarget,
    window_schema_fingerprint,
)
from dpone.runtime.sources.postgres_window_source import PostgresWindowSource


def build_window_runtime(
    *, pg_connect, ch_connect, http_runner_factory, writer_guard,
    write_evidence, advance_state, work_dir: Path,
):
    schema = (
        ("id", "Int64"),
        ("observed_at", "Nullable(DateTime64(6, 'UTC'))"),
        ("amount", "Nullable(Decimal(18,2))"),
    )
    target_id = "synthetic-clickhouse/analytics/events"
    fingerprint = window_schema_fingerprint(schema)
    store = SQLiteWindowStore(work_dir / "window-journal.sqlite", clock=time)
    target = ClickHouseWindowTarget(
        schema=schema, database="analytics", table="events",
        window_column="observed_at", target_id=target_id,
        connector_factory=ch_connect, http_runner_factory=http_runner_factory,
        work_dir=work_dir,
        metadata_store=FileWindowMetadataStore(), max_encoded_bytes=4 * 1024 * 1024,
        writer_guard=writer_guard,
    )

    def source_factory(window):
        return PostgresWindowSource(
            connection_factory=pg_connect, schema_name="public", table_name="events",
            columns=tuple(name for name, _ in schema), window_column=window.column,
            schema_fingerprint=fingerprint, batch_rows=8192,
        )

    def record_evidence(plan, generation, receipts, lease):
        write_evidence(
            plan, generation, receipts, lease,
            metrics=target.generation_evidence(plan, generation),
        )

    def executor_factory(source):
        return BoundedWindowExecutor(
            source=source, target=target, store=store, evidence=record_evidence,
            journal_factory=lambda lease, run_id: WindowJournal(store, lease, run_id),
            advance_state=advance_state, sleeper=sleep,
        )

    return RollingWindowRuntime(
        source_factory=source_factory, executor_factory=executor_factory,
        route_id="postgres-public-events-to-clickhouse-v1", target_id=target_id,
        schema_fingerprint=fingerprint, store=store,
        generation_total=lambda plan, result: target.generation_total(plan, result.generation),
    )
```

The ClickHouse connector factory returns the existing connector interface with
`execute_query`, and `http_runner_factory()` returns the existing HTTP
bulk runner. See the synthetic route test for concrete local-service wiring.
In this example, the injected durable writer accepts
`write_evidence(plan, generation, receipts, lease, *, metrics)`;
`advance_state(plan, lease)` keeps its ordinary signature. The local
`record_evidence` adapter binds the target and satisfies the executor's
four-argument evidence callback. Evidence must be durable before the writer
returns. Its `metrics` argument supplies generated window/chunk metrics, UTC-day
counts, bounds, NULL counts, source/staging/target counts, encoded bytes, and
observed phase durations.
`stage_seconds` measures overlapped source/HTTP/verification work; it does not
invent independent extract/load times. A publication duration lost with the
process is explicitly unavailable. Source metric parity inferred from a matching
typed digest remains probabilistic.

Callback retries are possible; enforce idempotency and fencing at their storage
boundary, including after a lost acknowledgement.

`run(load_config, owner=invocation_id)` returns `ProcessResult`. Reuse the same
invocation ID for recovery. `DefaultProcessRunner()` without a window factory
raises `rolling_window_capability_required`. A chunking declaration without a
rolling window also fails rather than silently selecting legacy execution.

## Observe completion and recovery

A successful result includes window row counts, verified full-generation
`final_rows`, and `details["rolling_window"]` with the fixed boundaries,
fingerprint, run identifier, generation, and chunk count. The evidence callback
owns durable serialization under the configured work directory or an explicitly
injected durable store. No new standalone CLI JSON/file-output contract is added.

Publication precedes evidence; source state advances only after evidence succeeds.
An evidence or state failure can occur after data has become visible.

| Failure | Recovery |
|---|---|
| Transient chunk error | At most two retries with backoff; fence and discard unverified attempts before reloading. |
| Schema, quality, identity, or lease failure | Correct the cause; these errors are not classified for automatic retry. |
| PostgreSQL snapshot expired before publication | Use a new invocation and fully re-extract; receipts from a different snapshot cannot be reused. |
| Publication reply lost | Inspect persisted generation UUIDs; never blindly repeat EXCHANGE. |
| Publication outcome unknown | Stop and reconcile physical target and generation identities; retain journals and tables. |
| Evidence/state failed after publication | Retry the original invocation; restore its persisted plan without opening a source snapshot. |
| Target data or schema changed between preparation and publication | Refuse publication and preserve the changed target; do not publish the stale generation. |

A fresh invocation cannot bypass unresolved publication from an older run. Keep
its invocation registry, chunk journal, and publication metadata together.

## Capacity and verification

Publication builds a complete replacement generation: retained outside-window
rows plus verified replacement chunks. Its cost and temporary storage depend on
the full target, not only the lookback interval. EXCHANGE retains the old
generation as a backup. Automatic backup cleanup is not provided; retain owned
tables until publication and recovery obligations are settled.

Typed multiset digests preserve multiplicity and supplement counts. Digest
equality is probabilistic; synthetic integration tests also compare actual
multisets. Immediately before publication, the target additionally compares exact
outside-window multiplicities against the prepared generation under exclusion.
Physical schema and topology are revalidated under the same all-writer guard
before preparing a generation and before publication. The guard must also cover
DDL writers. Recovery of an already published UUID pair remains valid after a
later schema change and performs no new exchange.

Encoded byte limits bound a row and emitted payload chunks, not driver buffers,
source objects, retained consumer chunks, or process RSS. Independent worker
count and source fetch cardinality are bounded separately. No throughput
improvement is claimed before reproducible measurement.

## Synthetic validation

Use the repository's local Docker integration prerequisites, then run:

```bash
uv run pytest tests/test_bounded_window_execution.py tests/test_bounded_window_store.py tests/test_rolling_window_runtime.py tests/test_postgres_window_source.py tests/test_bounded_typed_stream.py -q
uv run pytest tests/integration/postgres/test_postgres_window_source_integration.py tests/integration/clickhouse/test_clickhouse_window_target_integration.py tests/integration/postgres/test_bounded_window_route_integration.py -q
```

Integration results require local services and configured fixtures. A skipped test
is not certification. [Impact-aware acceptance](impact-aware-acceptance.md)
explains selection and JUnit receipts.

Return to [Load strategies](load-strategies.md) or
[Adaptive native transfer](native-transfer-industrial-runtime.md).
