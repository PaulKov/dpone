# Operate bounded native delivery

Audience: data engineers diagnosing delivery time and operators recovering an
interrupted native load. This runbook assumes the existing
[native runtime composition](../mssql-native-transport.md#compose-the-runtime)
and its durable invocation, target receipt and fenced journal authorities.

## Diagnose preparation work

Start with the exact invocation, subject commit, workload and configured limits.
Compare equivalent runs only: dataset identity, target layout, environment and
resource limits must match. A harness's producer commit identifies the harness;
the subject commit identifies the dpone implementation actually executed.

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
