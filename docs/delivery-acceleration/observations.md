# Native delivery observations and comparisons

This guide helps platform engineers and maintainers explain bounded native
ClickHouse-to-MSSQL work and compare retained benchmark runs. Start with the
[native transport guide](../mssql-native-transport.md) and the
[approved design](../feature-design-data-delivery-acceleration-v1.md). The integrated
journey is documented in the [delivery acceleration overview](index.md).

The DDA-01 component adds optional diagnostics and an offline developer tool.
DDA-06 owns runtime composition, navigation and the operations journey; DDA-05
owns real-row benchmark production. In the integrated runtime,
`NativeDeliverySession` connects bounded phase observations. Diagnostic delivery and
pipeline totals intentionally remain unavailable because the runtime has no
independent target visibility probe; the DDA-05 harness owns those measured
boundaries. The standalone DDA-01 component does not wire runtime call sites or
establish measured acceleration. Existing manifests,
CLI defaults, tuple frames, journals, receipts and recovery remain compatible.
No migration is required. Public native partition SWITCH remains rejected.

## Collect a bounded diagnostic sidecar

A Python application supplies sanitized identifiers and a clock domain describing
an actual shared monotonic clock. The following executable example uses an
injected deterministic clock. Its PASS means diagnostic collection completed;
it is a hermetic example, with no database or performance certification.

```python
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver

collector = BoundedNativeDeliveryObserver(max_observations=64)
ticks = iter([0, 10, 20, 30, 50, 80])
recorder = collector.recorder(
    clock=lambda: next(ticks),
    clock_domain="example-host-boot",
    process_id=1,
    worker_id="coordinator",
)
with recorder.duration("pipeline"):
    with recorder.duration("delivery"):
        with recorder.phase("prepare_insert", reason="preparation", rows=3):
            pass
sidecar = collector.snapshot()
assert sidecar["status"] == "PASS"
assert sidecar["recorders"][0]["durations"]["delivery"]["value"] == 4e-8
```

Use `NativeDeliveryRecorder(observer=None, *, clock_domain, process_id, worker_id)`
when diagnostics are omitted. Its phase/duration scopes perform no clock reads.
The frozen port is `NativeDeliveryObserver.record(observation) -> None`. Recorders
catch ordinary clock/observer errors, preserve the original business exception,
and expose stable failure codes; connector exception messages are never copied.
Python cancellation (`CancelledError` or `KeyboardInterrupt`) records a separate
cancelled attempt. Cooperative native cancellation and lease loss retain
`WindowContractError` and therefore record failed when raised inside an observed
phase; instrumentation does
not translate business exceptions. Retry identifiers do not replace attempts.

`collector.recorder(...)` connects the recorder's diagnostic callback explicitly.
`collector.snapshot()` includes all retained recorder failure channels. When
constructing a recorder directly, provide
`diagnostics=collector.record_diagnostics`, or merge its final snapshot through
`collector.snapshot(recorder_reports=[recorder.snapshot()])`. A snapshot of an
observer alone cannot reveal failures that happened before `record()` succeeded.
Always retain this channel, including failed runs.

For spawned workers, return `observation.to_dict()` and `recorder.snapshot()` as
bounded serializable data. The coordinator reconstructs observations using
`NativeDeliveryObservation.from_dict(payload)`, calls `collector.record(...)`,
and calls `collector.record_diagnostics(worker_snapshot)`. A collector, client,
clock or observer is not a process-transfer payload. Each recorder belongs to
one worker; the collector serializes concurrent updates with a lock.

The default capacity is 4096 observations and 4096 worker reports, configurable
from 1 through 65536. Every observation has at most 32 metrics; identifiers and
metric provenance use bounded tokens. Raw retention, aggregate cardinalities and
worker reports are bounded. Excess observations/reports make the sidecar
UNVERIFIED with `capacity_exceeded`; work and overlap aggregates then become
unavailable, never a sampled claim of complete work. Retained raw attempts are
still available for diagnosis.

## Measurement reference

The immutable `NativeDeliveryObservation` has the exact v1 fields from the
[diagnostic schema](../feature-design-data-delivery-acceleration-v1.md#frozen-diagnostic-schema):
`schema_version`, `phase`, nullable `reason`, `clock_domain`, `process_id`,
`worker_id`, nullable `ordinal`/`attempt_id`, `start_monotonic_ns`,
`end_monotonic_ns`, `outcome`, nullable `rows`/`encoded_bytes`, and `metrics`.
Counters are nonnegative integers; outcomes are completed, failed or cancelled.
Metrics are defensively copied and immutable.

Phases are source_read, source_adapt, frame_build, ipc_submit, encode, bcp,
raw_verify, prepare_insert, metadata_project, prepared_verify, quality, publish,
evidence and checkpoint. Use reason codes to distinguish raw verification during
import, immediate inspection, preparation and prepublication. Observations are
separate from the legacy journal's existing observation dictionaries.

| Value | Unit and provenance | Interpretation |
| --- | --- | --- |
| Start/end | Integer monotonic nanoseconds in the named domain | Half-open interval; never subtract timestamps from different domains |
| `work_seconds` | Seconds, `sum_of_spans`, per phase/domain | Includes failed/cancelled attempts; nested spans may overlap |
| `observed_worker_overlap` | Workers, `distinct_active_workers`, per phase/domain | Maximum distinct `(process_id, worker_id)` identities simultaneously active; adjacent spans do not overlap |
| Delivery duration | Seconds, `monotonic_span` | Caller starts at source acquisition and ends after confirmed commit and a successful independent target visibility probe |
| Pipeline duration | Seconds, `monotonic_span` | Independent span through evidence/checkpoint completion |
| Rows/encoded bytes | Rows/bytes for that span | Optional existing counters; never execute extra count queries for diagnostics |
| Resource metric | Provider-specific units and explicit provenance | A process-only RSS measurement is not total pipeline RSS; limits are configuration |

Never sum phase work into delivery or pipeline duration, combine clock domains
into an overlap claim, or substitute configured `parallelism` for observed
workers. Zero-length intervals contribute zero overlap. Repeated/failed total
spans are unavailable. The caller supplies the visibility boundary; the recorder
cannot infer database visibility from successful Python scope completion.

A metric has `value`, `unit`, `availability`, nullable `reason` and `provenance`.
Values must be finite numbers. Unavailable means **null plus a reason**, for
example `ObservationMetric(None, "bytes", "unavailable", "provider_absent",
"parent_process_rss")`. It does not mean zero. An empty metrics mapping makes no
resource claim. Supply explicit unavailable entries for requested providers that
are absent. SQL work, RSS and timings must identify their actual measurement
method; no connector clients or providers are constructed by the collector.

For source iteration, aggregate actual `next()` work into a fixed set of counters
per bounded frame. Attach metrics such as `source_read_work_seconds` to a
`frame_build` observation with `reason="inclusive_frame_build"`. These measured
counters use `unit="seconds"` and `provenance="sum_next_monotonic_spans"`; they
do not create a source phase interval or establish source-worker overlap. Keep
inclusive adaptation unavailable when an exclusive boundary cannot be measured.
Do not log a record per row or label the lifetime of a source context as read time.

No row values, SQL text, connection URLs, credentials or exception text belong in
observations. Supply synthetic identifiers and stable reason codes. The sidecar
cannot authorize admission, successful data delivery, state advancement,
publication, rollback, recovery, or cleanup.

## Compare retained benchmark runs

Prerequisites: a repository checkout with its locked Python dependencies, two
retained v1 run envelopes (or two v1 campaign files), their referenced evidence,
and an existing writable output directory. Input paths in these examples are
caller-provided artifact locations; they are not distributed benchmark results.
DDA-05's producer and DDA-06's composed runtime supply real inputs. Live execution
requires a separately approved disposable environment; the compare command itself
performs no SQL, network calls or service startup.

```bash
uv run python tools/native_delivery_benchmark.py compare --help
uv run python tools/native_delivery_benchmark.py compare --baseline baseline/run.json --candidate candidate/run.json --output comparison.json
uv run python tools/native_delivery_benchmark.py compare --baseline baseline/run.json --candidate candidate/run.json --output comparison.json --overwrite
```

Stdout contains the report path and status, for example
`comparison.json UNVERIFIED`. Diagnostics go to stderr. The output is UTF-8 JSON
written through a flushed/fsynced temporary file in the destination directory.
Without `--overwrite`, publication uses an atomic no-clobber operation, including
when another writer creates the output concurrently. With overwrite it atomically
replaces the old report. Failed writes remove temporary files and preserve the
previous report. The tool never creates an output directory implicitly and never
replaces input envelopes or referenced artifacts, even with overwrite.

| Exit | Meaning | Next action |
| --- | --- | --- |
| 0 | A valid report was written, possibly UNVERIFIED | Inspect report status and limitations before drawing a conclusion |
| 1 | A correctness or measured acceptance gate failed; report retained | Inspect failed receipts/cases and rerun the responsible producer after correction |
| 2 | Usage, schema, identity, retained bytes, or output failure | Correct the input/path problem; do not hand-edit evidence to manufacture PASS |

An existing output produces `output_exists_use_overwrite`. Wrong versions or
missing required identity produce `invalid_schema`; escaping paths and tampered
bytes produce `artifact_path_escape` and `artifact_hash_mismatch`. Missing timing,
dirty code, absent live proof, missing declared cases and too few eligible samples
produce an UNVERIFIED report. Unknown schemas are rejected; no coercion or legacy
format migration is attempted. Retain the complete input directories when moving
reports to another machine.

## Evidence and acceptance

The producer identity describes the harness; the subject identity is the exact
full Git SHA and dirty flag of the dpone checkout executed. Every receipt binds
subject, workload, configuration, environment, route, scope and sample. The
consumer validates the frozen schema, exact NativeChunkLimits fields, identical
workload/configuration/environment descriptions and physical layout across sides,
and retained SHA-256 bytes. Configuration/environment/workload hashes are opaque
producer identities: comparison checks their bindings and descriptive equality;
it does not invent missing content preimages or claim to recompute dataset bytes.

Artifact paths are relative to their envelope directory and must remain beneath
it. Absolute references, traversal and symlink escape are rejected. Evidence
references inside receipts also resolve relative to the run envelope. The
comparison keeps envelope references relative to its own directory and original
relative sample references; resolve sample references against their envelope
directory. Accepted envelope symlinks retain that logical directory on export;
hashes still bind the resolved file bytes. If inputs are outside the output directory, the producer retains an
immutable content-addressed evidence bundle beside the report. Existing matching
bundles are verified and reused; conflicts fail closed. For `compare()` without
an output path, envelope references are relative to the common input ancestor.
Its `sha256` hashes canonical sorted compact UTF-8 JSON **excluding that sha256
field**. Hashes detect accidental changes; self-authored JSON is not an independent
proof that a database ran.

When a sample supplies an observations sidecar, comparison reconstructs bounded
observations, recorder diagnostics and aggregates through the collector contracts.
Unknown nested versions, malformed records or inconsistent derived fields are
input errors (exit 2). A valid sidecar with observer failures or capacity overflow
keeps the comparison UNVERIFIED and prevents an eligible performance ratio.

Sample correctness requires typed content, duplicate multiplicity, metadata
parity and commit-receipt binding. Partition replacement also requires unchanged
outside-window data; full refresh records that check N/A with a reason. Profile
fidelity requires exact typed multisets on its own small fixture. Recovery covers
empty input, rollback, receipt-first recovery and source-free resume. The raw
`native-delivery-live-observation` evidence binds the same execution identity and
retains the matching assertion before a correctness receipt references it.
Required failed/missing checks cannot authorize a successful comparison.

A live eligible sample has a PASS receipt with retained live-observation evidence,
finite positive visibility/pipeline timings, and pipeline completion at or after
visibility. Both profile receipts must pass. Hermetic execution proves consumer
contracts only and produces UNVERIFIED, with no performance ratio. All attempts
remain referenced, including failures. `successful_samples` counts declared PASS
timed non-warmup samples; `eligible_samples` additionally requires verified live
correctness and a preceding successful warmup. At least three eligible trials
are required. Their median is reported; three trials do not establish p95.

Campaigns predeclare workload IDs and retained run references. Both sides must
have the same declared IDs and workload/configuration cases. An omitted case
remains UNVERIFIED. Acceptance targets require at least one median ratio
(candidate / baseline) at most 0.85, and every ratio at most 1.05. They are targets,
not claimed measured improvements. A single run comparison explicitly limits its
result to one workload. `structural_checks` remains UNVERIFIED because v1 has no
input representation authorizing structural counters; DDA-06 supplies separate
structural test evidence. Optional missing resources limit only their own claims.

## Shared limit validation

`normalize_delivery_limits` in `dpone.contracts.native_delivery_observations`
owns exact field-set validation beside the v1 run schema. Both the configuration
producer and offline consumer call it. It uses the existing `NativeChunkLimits`
validation and returns detached canonical values; missing fields are not filled
with defaults. Producer field-set errors remain `exact_limits_required`, value
errors retain their canonical codes, and the consumer maps invalid limits to
`BenchmarkInputError("invalid_limits")`. Normalized configuration bytes and
digests are unchanged.

The recorder's observer parameter retains its postponed annotation. Developer
tools using `typing.get_type_hints` supply the canonical observer type namespace
under [ADR 0058](../adr/0058-verified-release-composition.md); runtime dataclass
field reflection and supported exports remain unchanged.

## Validate and hand off

Run the local focused contract suite without services:

```bash
uv run pytest tests/test_mssql_native_delivery_observations.py tests/test_mssql_native_delivery_benchmark.py -q
```

The suite exercises the Python example and parses all compare examples, tests
clock/retry/error isolation and actual filesystem failure modes, and uses local
producer doubles to test receipt validation. Synthetic fixtures bearing a `live`
field test a branch of the consumer; they are not live measurements.

DDA-06 must connect optional keyword-only observer arguments at composition
seams, forward bounded worker payloads, retain recorder failure channels, and
place the two duration boundaries at actual visibility and pipeline completion.
The integrated runtime must leave total durations unavailable until an
independent visibility probe exists; it must not infer visibility from a finalizer
return. DDA-05's harness owns the current measured total boundaries. Preserve the
legacy journal dictionaries and add the English navigation,
overview/runbook and changelog entries in its owned paths. Validate the independent
DDA-05 producer and this consumer together before merging integration. Review the
[task plan](../data-delivery-acceleration-tasks.md) for ownership and the
[testing guide](../testing/index.md) for broader gates. Live performance remains
UNVERIFIED until approved real-row evidence exists.
