# Quality validation after a committed replay

This guide is for Python integrators who need to retry a committed ClickHouse
full refresh while keeping quality validation enabled. The opt-in durable store
retains the original validated observations and binds them to the exact managed
target generation. A fresh execution checks them without reading the source or
redispatching `EXCHANGE`.

This is a bounded, synthetically tested capability. Live certification is
**UNVERIFIED**. Start with the synthetic tutorial below; platform preparation is
required before using a database. See the [reference](committed-replay-quality-reference.md)
for supported configurations and the [recovery runbook](committed-replay-quality-runbook.md)
for blocked retries.

## First success without a database

From a development checkout with the locked development dependencies installed:

```bash
uv run pytest tests/test_quality_replay_runtime.py::test_lost_read_ack_fresh_session_reconciles_and_completes_without_redispatch -q
uv run pytest tests/test_quality_replay_runtime.py::test_processor_replay_uses_real_authority_with_source_and_mutation_traps -q
```

Both tests must pass. They use public synthetic fixtures, the real publication
and governance services, and an in-memory authority. The first loses an
acknowledgement after publication, recreates the quality session/store, and
finishes governance. The second retries a successful publication twice through
`ETLProcessor`; source extraction, source-state reads, and target-load calls are
traps. Exactly one publication dispatch remains recorded. These tests demonstrate
the algorithm, not a ClickHouse deployment or Keeper durability certification.

## Prepare and configure the Python integration

1. Select the existing bounded, one-shard replicated ClickHouse cluster
   [full-refresh route](clickhouse.md#bounded-cluster-full-refresh-publication).
   Its normal inventory, engine, source-byte bound and staging requirements apply.
2. Have the platform owner prepare the authority described in the
   [reference](committed-replay-quality-reference.md#authority-preconditions).
   Admission performs read-only verification; it never creates or replaces it.
3. Compose the sink explicitly at the application's dependency-injection boundary:

   ```python
   from dpone.runtime.sinks.clickhouse_sink import ClickHouseSink

   def build_quality_replay_sink(connector):
       return ClickHouseSink(connector, durable_quality_replay=True)
   ```

   `connector` is the application's already configured ClickHouse connector.
   Keep credentials in the existing credential provider. Pass the returned sink
   to the application's ordinary `ETLProcessor` composition.
4. Retain the existing quality policy, for example this `LoadConfig.options`
   fragment:

   ```python
   quality_options = {
       "quality": {
           "gates": [{"id": "rows", "type": "row_count_reconciliation"}]
       }
   }
   ```

   Merge that fragment into the route's existing options; it is not a complete
   route configuration. To add supported acceptance capture:

   ```python
   quality_options["quality"]["acceptance"] = {
       "enabled": True,
       "mode": "required",
       "capture": {"source": True, "staged": True, "target": False},
   }
   ```

   Target capture defaults to enabled when acceptance is selected, so explicitly
   setting it to `False` is required for this version, including `warn_only`.
5. Keep the same scheduler invocation, process identity and semantic
   configuration on retry. Supply a stable `RunContext.run_id` and `dag_id`
   to `ETLProcessor.run`; setting `options["run_id"]` alone is insufficient.
   A different invocation is a successor operation, not a recovery attempt.

The constructor defaults to `False`. There is currently no manifest key or CLI
switch for selecting this store. CLI/Airflow factory composition and external
replication are not enabled by the Python constructor example.

## Observe the result

A successful replay includes `reconciliation_metrics.quality_replay` in the
processor result. Its `kind` is `dpone.quality.replay.result.v1`; `core_digest`
identifies the immutable original evidence, `replayed_from` records the original
run/load IDs, and `quality_gates` and `acceptance` expose validated projections.
These result fields are diagnostics; copying them does not authorize replay.

A target commit and quality success are separate outcomes. A blocked replay
preserves the committed generation but raises a blocking typed error. Do not
interpret a committed target, a row count, or a report file as a successful
quality result. Continue with the [runbook](committed-replay-quality-runbook.md).

## Continue the journey

Use the [reference](committed-replay-quality-reference.md) to check policy and
identity limits before rollout. Use [ADR 0073](adr/0073-durable-committed-replay-quality.md)
for the producer, receipt, and concurrency model. The
[approved design](feature-design-committed-replay-quality-evidence.md) records the
larger protocol; its target-capture and additional-backend work remains deferred.
