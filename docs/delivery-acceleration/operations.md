# Operate bounded native delivery

Audience: data engineers diagnosing delivery time and operators recovering an
interrupted native load. This runbook assumes the existing
[native runtime composition](../mssql-native-transport.md#compose-the-runtime)
and its durable invocation, target receipt and fenced journal authorities.

## Diagnose preparation work

Start with the exact invocation, subject commit, workload and configured limits.
Compare equivalent runs only: dataset identity, target layout, third-party
environment and resource limits must match. The tested dpone package version may
differ across subjects; every run still retains and verifies its full environment
checksum, including that version. A harness's producer commit identifies the harness;
the subject commit identifies the dpone implementation actually executed.

`configuration_digest_mismatch` or `environment_digest_mismatch` means an envelope
description disagrees with its checksum. Restore retained original bytes or rerun
the producer. Updating only a checksum cannot repair existing receipt bindings.

| Observation | Interpretation | Next action |
|---|---|---|
| Raw verification work | Independent inspection across import, preparation and publication boundaries | Retain all four checks; inspect SQL/client costs in an approved measurement run |
| Prepared verification work | Initial business/full integrity plus independent prepublication integrity | Verify actual iterator counts alongside timings |
| Metadata projection | Canonical metadata shares the preparation INSERT | Separate it from the mandatory finalizer target-clock UPDATE |
| Repeated BCP attempts | Each retry has its own attempt identity | Investigate the original error and retained-byte receipt; do not combine attempts into one success interval |
| Configured parallelism | A resource limit | Inspect observed worker intervals before claiming overlap |
| Missing SQL/resource metric | Unavailable observation with an explicit reason | Limit the claim; never substitute zero |

Worker clocks in different domains cannot be combined into elapsed time or
overlap. Phase totals may overlap; they are not end-to-end delivery duration. Live
delivery time ends only after confirmed commit and a successful target visibility
probe. Evidence/checkpoint completion has a separate pipeline duration.

Performance reports are diagnostic sidecars. They contain identities, counters
and sanitized measurement metadata, never source values, SQL text, credentials
or connection URLs. A PASS for an offline schema/producer test is not live
performance certification. Report hashes detect retained-artifact changes; they
do not grant deployment authority.

## Recover by the durable boundary

Use the same invocation identity and the established runtime recovery entry
point. Let the current fenced owner reconcile target state before making a new
source request.

| Last authoritative state | Required recovery behavior | Success evidence |
|---|---|---|
| Partial extraction, no complete EOF | Settle owned writers, then start a new full source query under the existing re-extraction contract | New complete source lifecycle and contiguous verified receipts |
| Completed staging or prepared table | Reverify retained ownership, identity and content without reopening ClickHouse | Unchanged receipt coverage, typed digests and prepared object identity |
| Publication intent | Resolve the exact target receipt before inspecting or replaying mutation | Confirmed matching target commit receipt |
| Unknown commit outcome | Retain recoverable resources and stop replay until the target outcome is authoritative | Exact receipt or existing authoritative recovery result |
| Published, evidence incomplete | Complete idempotent durable evidence | Journal reaches evidence-complete |
| Evidence complete, checkpoint incomplete | Advance the fenced idempotent checkpoint | Journal reaches succeeded |

Never drop a table to bypass an ownership or digest failure. Do not fabricate EOF,
change a saved digest, remove publication intent or create a new generation to
work around a lost acknowledgement. Cleanup belongs to the fixture/runtime owner
after a known transaction outcome.

## Investigate failures

For a missing/changed raw table or prepared content mismatch, retain the journal
and receipt identifiers. Determine which verified boundary detected the change.
The same owner, schema, object, count, key, content and fencing checks continue to
apply after the optimizations.

For capacity or cancellation failures, verify that source closure and worker
settlement completed. Encoded-byte bounds do not promise an RSS bound, and an
observed SQL allocation threshold is not an exclusive reservation. Restore
capacity before retrying through the existing recovery policy.

For a native SWITCH rejection, keep the documented full-refresh or predicate
partition-replacement publication path. The isolated SWITCH component is not a
production mode. A future activation review must supply exact SQL Server catalog,
aligned-stage ownership, retention and live transaction/recovery proof.

## Verify and upgrade

Run the [first-success checks](index.md#first-success-without-database-access) in a
development checkout. An approved disposable environment is additionally required
for real SQL/BCP and performance checks. If none is available, record live work as
**SKIP** and live interoperability/performance as **UNVERIFIED**. Structural
counters alone cannot fill that gap.

Existing manifests, Mapping/tuple consumers, wire files and recovery journals
require no migration. Preserve the same physical target, invocation and source
lifecycle identities when resuming an interrupted invocation after upgrade.

## Connect optional observations

Use one collector for the application-owned runtime and stage composition within
one delivery invocation. These factories accept the same required capabilities listed in the
[native runtime guide](../mssql-native-transport.md#compose-the-runtime):

```python
from dpone.runtime.mssql_native_runtime import NativeMssqlRuntime
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context

observer = BoundedNativeDeliveryObserver(max_observations=4096)


def make_stage_context(**stage_capabilities):
    return compose_native_stage_context(observer=observer, **stage_capabilities)


def make_runtime(**runtime_capabilities):
    return NativeMssqlRuntime(observer=observer, **runtime_capabilities)
```

The application's existing `bindings` callback uses `make_stage_context` with its
fresh lease. After `runtime.run(...)`, including on failure, read
`observer.snapshot()` and persist it separately from the recovery journal.
Snapshots accumulate accepted spans and do not reset the collector. Create a
fresh collector for each independent invocation so its report does not include
spans or capacity usage from a previous run.

For a custom observer, explicitly share one session across both factories:

```python
from dpone.runtime.mssql_native_chunks_observations import NativeDeliverySession


def make_shared_session(custom_observer):
    return NativeDeliverySession(custom_observer)
```

Create that session once per invocation, pass it as `observer=session` to both
runtime and stage composition, and read `session.snapshot()` even if the custom
observer throws.
Separately constructed sessions keep separate diagnostic histories;
`runtime.observations.snapshot()` alone does not include another session's staging
failures. The bounded collector example above already shares its failure channel.
Neither a failed observation nor collector overflow changes business admission,
verification or error propagation. Overflow makes aggregate claims unavailable.
Omitting the observer preserves the original source adapter without extra
per-row clocks; it creates no diagnostic file automatically.

`frame_build` includes source reads, adaptation, sizing and frame assembly. Its
`source_read_work_seconds` sums actual raw iterator `next()` spans; it excludes
suspension while encoding/import runs. A frame builder may read ahead one row, so
this is work performed during that call, not a latency attribution to exactly the
rows it emits. Exclusive adaptation is unavailable with reason
`inclusive_source_boundary`. No observation is emitted for each source row.

`metadata_project` with reason `insert_projection_build` measures SQL construction;
`prepare_insert` measures execution of that statement, including its canonical
metadata expressions. `prepared_verify` includes the digest comparison. Runtime
`publish` with reason `service_finalize` includes prepublication checks and
transaction handling. Evidence/checkpoint spans include their journal markers.
The source context still remains open through publication. Cooperative native
cancellation or lease loss preserves its existing `WindowContractError` and is
recorded as failed work; it is not relabeled as explicit user cancellation.

Each process has its own declared clock domain. Child encode records cross IPC
as bounded serialized values; no observer or client crosses that boundary. Runtime
composition has no independent visibility probe, so its observation sidecar
leaves delivery/pipeline duration unavailable. The experiment harness measures
those boundaries through its explicit source/visibility/checkpoint protocol.

## Create and compare retained reports

First follow [Start without services](certification.md#start-without-services) to
create a limits file. The following complete offline continuation produces two
honest absence reports. Both output directories must already exist, and every
output path must be new:

```bash
mkdir -p /tmp/dpone-dda5/baseline /tmp/dpone-dda5/candidate
for subject in baseline candidate; do
  env -u DPONE_RUN_INTEGRATION -u DPONE_RUN_INTEGRATION_LIVE \
    -u DPONE_DDA_DISPOSABLE_APPROVED \
    uv run python tools/native_delivery_live_benchmark.py run \
    --profile unicode --rows 10000 --seed 7 --trials 3 \
    --limits /tmp/dpone-dda5/limits.json \
    --output "/tmp/dpone-dda5/$subject/run.json"
done
uv run python tools/native_delivery_benchmark.py compare \
  --baseline /tmp/dpone-dda5/baseline/run.json \
  --candidate /tmp/dpone-dda5/candidate/run.json \
  --output /tmp/dpone-dda5/comparison.json
```

These directory names label comparison sides; they do not claim execution of the
historical baseline. The producers retain actual subject/producer identities,
profile/sample receipts and hashes. Keep each report together with its referenced
artifacts. Without approved execution the comparator reports `UNVERIFIED`, ratio
`null`, and the missing-evidence reason. No source service or BCP runs here.

The comparator writes UTF-8 JSON atomically. It refuses existing output unless
`--overwrite` is explicitly supplied, and never overwrites retained inputs. Its
stdout contains the output path and status; stderr contains actionable sanitized
diagnostics. Exit **0** means a report was written, including UNVERIFIED; **1**
means a failed comparison gate; **2** means invalid arguments, identity, schema,
retained bytes or file access. Inspect the JSON status before making any claim.
For a real comparison, use the approved baseline/candidate experiment procedure
in the [certification guide](certification.md), retaining all attempted trials.
