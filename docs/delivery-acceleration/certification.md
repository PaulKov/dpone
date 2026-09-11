# Certify a native delivery experiment

Use this guide to prepare reproducible synthetic ClickHouse-to-MSSQL experiments
and inspect their correctness evidence. It is for platform engineers with an
explicitly approved disposable environment. The harness is implementation
infrastructure: **live fidelity, recovery and performance remain UNVERIFIED until
the real route factory and environment are supplied and exercised**.

The [approved design](../feature-design-data-delivery-acceleration-v1.md) defines
the report envelope. Read the [native transport prerequisites](../mssql-native-transport.md)
before enabling services. DDA-06 owns shared runtime integration, shared fixtures,
navigation and activation decisions. An approved application/environment supplies
the real route factory and authoritative visibility probe. No production command, manifest, wire,
journal, checkpoint or receipt format changes here. Native SWITCH remains
publicly rejected; its fixtures exercise an isolated component only.

## Start without services

From the repository, these commands require no credentials or vendor services:

```bash
uv run python tools/native_delivery_live_benchmark.py --help
uv run pytest tests/test_native_delivery_live_benchmark.py -q
uv run pytest --collect-only -q \
  tests/integration/mssql/test_clickhouse_mssql_bounded_native_delivery_integration.py \
  tests/integration/mssql/test_clickhouse_mssql_partition_switch_integration.py
mkdir -p /tmp/dpone-dda5
cat > /tmp/dpone-dda5/limits.json <<'JSON'
{
  "max_total_encoded_bytes": 104857600,
  "stage_allocated_bytes_stop_threshold": 104857600,
  "max_rows": 65536,
  "max_bytes": 16777216,
  "max_row_bytes": 1048576,
  "max_pending": 2,
  "max_staging_tables": 1024,
  "parallelism": 1
}
JSON
uv run python tools/native_delivery_live_benchmark.py run \
  --profile unicode --rows 10000 --seed 7 --trials 3 \
  --limits /tmp/dpone-dda5/limits.json \
  --output /tmp/dpone-dda5/absence.json
uv run python tools/native_delivery_live_benchmark.py inspect /tmp/dpone-dda5/absence.json
```

Without all three approval flags below, the run writes `SKIP` with reason
`disposable_environment_not_approved`, unavailable timings and unverified
receipts. An approved run with no `--factory` writes `SKIP` with reason
`real_route_factory_unavailable`. Neither path imports the factory. `inspect`
validates retained hashes and identities and reports `eligible_trials=0`.
Import, help and test collection create no service clients or artifacts.

## Prepare an approved experiment

Obtain explicit approval for disposable ClickHouse and SQL Server databases,
native BCP tools, filesystem capacity and SQL allocation limits. Do not start
containers, reuse a shared database, infer credentials, change recovery models
or remove target indexes to run this guide. Supply credentials privately through
the reviewed factory's environment integration; never put them in arguments,
limits JSON, reports or version strings.

The harness requires these exact opt-in flags. Setting them records operator
intent; it does not replace the maintainer's environment approval.

```bash
export DPONE_RUN_INTEGRATION=1
export DPONE_RUN_INTEGRATION_LIVE=1
export DPONE_DDA_DISPOSABLE_APPROVED=1
```

Set `DPONE_DDA_ROUTE_FACTORY` to the reviewed application/environment `module:callable` available
in the selected interpreter, and `DPONE_DDA_LIMITS_FILE` to the absolute limits
file. Neither this harness nor the shared integration bundles a default live
factory or invents visibility-probe authority. The application/environment must
implement the protocol below using its approved services. A manifest's
plan alone cannot compose the route. The factory must use actual
`NativeMssqlRuntime`, native source, bounded importer and BCP. Isolated SWITCH
uses its own catalog admission and caller-owned transaction authority.

Use the same workload, limits, server versions, resource profile and physical
target layout for both subjects. Each run freezes these descriptions and checks
for drift around every trial. Run outputs outside both checkouts so report files
do not change Git dirty state. Review exact commits and environments separately;
a factory label cannot prove which code ran.

The version of dpone under test may differ between baseline and candidate. Both
reports retain that version in their complete environment checksums and receipt
bindings. Cross-subject comparison excludes only its value; Python, dependency,
server and BCP versions, version-key presence, layout and resource values/types
must still match. Run comparison with the candidate installation; the baseline
interpreter remains supported for the current harness's `run`, `inspect` and help.

The `baseline` adapter requires the actual imported dpone checkout to be
`d5ad9aaecc900c24df421b160ed36b4cfc726e45`. The `candidate` adapter records the
actual imported checkout. The producer records its own checkout independently.
Both tracked and untracked changes affect the dirty flag. Dirty or hermetic
execution cannot establish performance eligibility. Baseline and candidate use
the same protocol; no optimization-branch import is needed.

## Run and observe

Run the following in the candidate checkout after preparing the approved factory:

```bash
uv run python tools/native_delivery_live_benchmark.py run \
  --adapter candidate --factory "$DPONE_DDA_ROUTE_FACTORY" \
  --profile unicode --rows 10000 --seed 7 --trials 3 \
  --strategy partition_replace --mode bounded_native \
  --limits "$DPONE_DDA_LIMITS_FILE" \
  --output /tmp/dpone-dda5/candidate-unicode.json
uv run python tools/native_delivery_live_benchmark.py inspect /tmp/dpone-dda5/candidate-unicode.json
```

For the baseline, run its environment's Python from the baseline checkout and
pass the absolute path to this harness script, with `--adapter baseline` and a
different output. The factory must be available in that interpreter and must
identify the same checkout as the imported dpone. Selecting `baseline` in the
candidate environment fails instead of relabeling current code.

The baseline interpreter must resolve dpone only from the pinned baseline source.
The current harness uses that source's `NativeChunkLimits` model; it requires no
candidate-only contract helper. Keep the factory module and generated outputs
outside both checkouts, and do not add candidate `src` to the baseline import
path. A hermetic factory can verify this launch path, but its report remains
UNVERIFIED and supplies no live certification.

Set `DPONE_DDA_BASELINE_CHECKOUT` to the existing audited baseline checkout and
`DPONE_DDA_HARNESS_PATH` to the absolute path of this reviewed harness script.
After preparing that checkout's locked dependencies, run:

```bash
(
  cd "$DPONE_DDA_BASELINE_CHECKOUT" || exit 2
  uv run --locked --no-sync python "$DPONE_DDA_HARNESS_PATH" run \
    --adapter baseline --factory "$DPONE_DDA_ROUTE_FACTORY" \
    --profile unicode --rows 10000 --seed 7 --trials 3 \
    --strategy partition_replace --mode bounded_native \
    --limits "$DPONE_DDA_LIMITS_FILE" \
    --output /tmp/dpone-dda5/baseline-unicode.json
)
```

Inspect the final report status as well as its component receipts. The opt-in
bounded-delivery and isolated-SWITCH tests reject FAIL, SKIP, missing or unknown
status even when every trial passed: the final identity check can still fail
after cleanup. Both tests require live execution, successful fidelity/recovery
receipts and all four successful samples (one warmup and three trials).
For development, UNVERIFIED is accepted only when the subject or producer has
an explicit boolean `dirty: true` and all the same proofs pass. This allowance
does not authorize performance certification; clean UNVERIFIED and hermetic
execution cannot pass the live-test assertion.

Profiles are `narrow`, `wide` (200 columns total), `unicode`, `decimal`, `null`,
`binary` and `skewed`. All include an integer and UTC temporal column, deterministic
seeds and intentional adjacent duplicate rows. Unicode includes supplementary
characters and distinct normalization forms; Decimal retains precision/scale;
NULL, empty text and empty binary remain distinct. The description hash binds
the generator version, ordered schema, seed, row count and authored UTC window.
Rows stream from the generator; exact target readback uses memory proportional
to unique rows and occurs outside delivery timing. The CLI caps a workload at
one million rows. No source values enter reports.

The producer runs a 32-row exact fidelity fixture, recovery scenarios, one warmup
and at least three declared trials. Failed fidelity/recovery prevents larger
timed experiments. Every trial is retained; failed, skipped, unverified or
pre-warmup samples cannot establish a median. Three trials support no p95 claim.
The separate DDA-01 comparison tool determines acceptance across comparable runs;
this producer supplies no speed multiplier or campaign-wide claim.

The output directory contains the envelope and a unique `delivery-…` directory
with immutable correctness, observation and owner-inventory artifacts. Keep them
together. The envelope is published last with atomic replacement; use
`--overwrite` explicitly to replace an existing output. Previous run objects
remain intact. `inspect` verifies this producer's deterministic workload,
configuration and environment digests, then rejects changed bytes, mismatched receipt identities,
path traversal and symlinks. Hashes protect retained bytes, not the trustworthiness
of a self-authored claim.

Offline comparison also recomputes the normalized configuration and complete
environment checksums before inspecting their proofs. A digest mismatch is an
input error, even when both subjects contain the same stale description. Restore
the original evidence or regenerate the affected run through its producer;
editing an envelope or its checksum does not update the retained proof bindings.

| Observation | Meaning and availability |
|---|---|
| `visibility_seconds` | Source acquisition through confirmed commit and successful independent target visibility probe |
| `pipeline_seconds` | Same start through runtime evidence/checkpoint completion; excludes post-run correctness and cleanup |
| `rows`, `encoded_bytes` | Target exact-multiset count and actual retained native-file byte observations |
| `process_set_rss` | Sampled simultaneous RSS of coordinator and descendants, with root/PIDs/interval/sample count; excludes external servers and can miss short-lived processes |
| SQL allocation/log/waits | Numeric observations only when supplied; unavailable values are null with a reason |
| Phase sidecar | Null until an optional observer sidecar is supplied through integration; no phase claims |

The process sampler uses numeric `ps` output at 50 ms intervals. A failed or empty
sampling sequence reports unavailable rather than parent-only RSS or zero.
Configured limits never substitute for observations. Dependency, Python, server,
BCP and layout identities come from the selected reviewed factory.

Stdout contains the report path and status. Diagnostics use stable codes on
stderr without connector exception text. Exit 0 means a valid report was written
(including `SKIP`/`UNVERIFIED`), 1 means a recorded failure, and 2 means invalid
input, factory preparation or file-output failure. A zero exit is not live PASS.

## Recover and clean up

Every opened fixture records its UUID in a `*-owner.json` artifact. That UUID
locates the factory's durable ownership inventory; the diagnostic report itself
cannot authorize cleanup. The factory owns its synthetic source, target, raw,
prepared and switch-out objects and spool files. It must reject caller-owned
objects, a mismatching owner generation and unsupported layouts. It must never
drop a database or alter shared tables.

Known outcomes receive owned cleanup and connection closure automatically.
Unknown outcomes retain resources and block replay. Use the recorded UUID as
`DPONE_DDA_INVOCATION_ID`, with the original reviewed factory/configuration:

```bash
uv run python tools/native_delivery_live_benchmark.py recover \
  --invocation-id "$DPONE_DDA_INVOCATION_ID" \
  --factory "$DPONE_DDA_ROUTE_FACTORY" --limits "$DPONE_DDA_LIMITS_FILE" \
  --strategy partition_replace --mode bounded_native \
  --output /tmp/dpone-dda5/recovery.json
uv run python tools/native_delivery_live_benchmark.py cleanup \
  --invocation-id "$DPONE_DDA_INVOCATION_ID" \
  --factory "$DPONE_DDA_ROUTE_FACTORY" --limits "$DPONE_DDA_LIMITS_FILE" \
  --strategy partition_replace --mode bounded_native \
  --output /tmp/dpone-dda5/cleanup.json
```

These commands attach an existing inventory and never create a replacement
fixture. Recovery injects a poison source opener, checks source-query count and
requires a known commit result. Cleanup refuses an unknown outcome; the factory
must recheck durable ownership and outcome at deletion time. An unresolved
operation writes `UNVERIFIED`; retain resources and investigate the authoritative
journal/target receipt before trying again. Do not fabricate a receipt, manually
erase stages or re-extract after complete EOF. Incomplete extraction still follows
the existing [transport recovery contract](../mssql-native-transport.md).

## Factory integration reference

`tools/native_delivery_live_support/execution.py` defines the feature-local
`RouteFactory`, `RouteSession`, `Snapshot` and `DeliveryClock` protocols.
`load_factory` calls the factory with keyword arguments `configuration` and
`route`. The factory declares `execution` (`live` or `hermetic`) and
`subject_checkout`, provides sanitized `describe()`, provisions with
`open(dataset, case=…, clock=…)`, and reopens durable ownership with `attach(uuid)`.

`describe()` returns exactly `versions`, `target_layout_sha256` and
`resource_profile`. Versions require Python, dpone, ClickHouse, MSSQL and BCP and
should include every installed dependency under its distribution name. Report
concise versions, never raw server banners. Resource keys are `cpu_count`,
`memory_bytes`, `sql_memory_bytes`, `sql_log_bytes` and `disk_bytes`; omit unknown
resources. The layout hash must describe physical index/partition/storage shape
independently of randomly generated object names.

Provision outside-window sentinels on both sides of the half-open
`[2026-01-01T00:00:00Z, 2026-01-02T00:00:00Z)` window, including the exact upper
boundary, and old in-window rows before each fixture. Read `Dataset.schema()`
for source/target types; ClickHouse binary String values must reach VARBINARY,
text must reach NVARCHAR. No key deduplication is permitted.

The session must call `source_acquired()` immediately before acquiring the single
real source query and `committed_visible()` only after known commit plus an
independent visibility probe, before evidence/checkpoint completion. Calls share
the harness process's monotonic clock domain. `snapshot()` independently reads
business/outside rows, canonical metadata hashes and exact operation-receipt
hashes. Missing metadata/receipt authority produces UNVERIFIED, not PASS.

Source-query/publication counters are scoped to the invocation, start at zero,
and exclude fixture preparation and target-observation queries. Each successful
fresh delivery requires exactly one source query and one atomic publication.
Correct final rows cannot compensate for a repeated extraction or publication.

Fault names are `before_commit`, `after_eof`, `lost_ack` and `unknown_commit`.
Snapshots record actual `fault_events`, the count of exact `receipt_probes`, and
`pipeline_complete` after evidence/checkpoint success. Every injected fault must
be newly observed; lost ACK requires a receipt probe even when runtime recovery
returns normally. Known rollback alone cannot establish recovery success.
Rollback and the post-EOF boundary must preserve business rows, outside-window
rows and metadata before recovery, with no publication or completed pipeline.
For an unknown commit, the initial target must be exactly its prior state or the
complete replacement, consistent with zero or one publication. Recovery must
not observe a newly appeared operation receipt while the target remains in its
prior state: target mutation and receipt publication are atomic. It must
leave that state and the incomplete pipeline unchanged while outcome authority
remains unavailable. An unavailable receipt is allowed only in this negative
replay-blocking fixture; metadata still needs independent authority. For known
commit recovery, the initial target and receipt must already be correct and
remain unchanged; completing pending evidence/checkpoint work is allowed.
Recovery evidence retains structured state and binding checks inside the existing
`expected`/`observed` fields, including mismatching metadata/receipt hashes and
both known/unknown branches. A FAIL therefore preserves the observed mismatch
instead of reducing every condition to a single boolean.
`recover(source_allowed=False)` must install a source opener that raises if used,
not merely accept the flag. Recovery fixtures assert no duplicate publication;
receipt-first recovery asserts no extra stage reads after known commit. For
isolated SWITCH, the `before_commit` fault occurs between SWITCH operations;
additional live tests exercise `nonempty_switch_out`, `layout_drift`, `owner_drift`
and `between_switches`. All decisions still belong to existing runtime and
transaction authorities. `close()` only closes connections; `cleanup()` validates
durable ownership and known outcome before deleting owned resources.

To exercise the live modules after approval:

```bash
uv run pytest -q \
  tests/integration/mssql/test_clickhouse_mssql_bounded_native_delivery_integration.py \
  tests/integration/mssql/test_clickhouse_mssql_partition_switch_integration.py
```

Hermetic fixtures validate this seam and producer/parser contracts. They do not
prove actual BCP fidelity, SQL rollback, server performance or public SWITCH
admission. DDA-06 must connect the reviewed producer and DDA-01 consumer, run
fresh integration validation, and preserve this distinction in any release claim.
