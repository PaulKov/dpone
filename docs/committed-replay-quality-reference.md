# Committed replay quality reference

This reference is for platform engineers and connector authors composing the
opt-in capability described in the [guide](committed-replay-quality.md).

## Supported scope

| Surface | Behavior |
|---|---|
| Python selector | `ClickHouseSink(connector, durable_quality_replay=True)`; default `False` |
| Publication route | Existing bounded internal replicated cluster `full_refresh` only |
| Gates | `row_count_reconciliation`, `min_rows`, `typed_hash_reconciliation` |
| Acceptance | Original source/staged observations and bounded post-commit target observations; required remains blocking |
| Target acceptance | Row, null and distinct counts on the exact managed generation; shared 60-second observation deadline, at most 256 selected columns |
| Other routes | No durable store shipped for local, external-replication, MSSQL, or other sinks |
| CLI/manifest selector | `sink.options.durable_quality_replay`: strict boolean, default false; batch, flow and fragments; same runtime selection for Airflow |
| Historical evidence | No backfill from reports, target counts, or serialized process-local receipts |
| Envelope bounds | Canonical finite JSON, at most 256 KiB, immutable core and at most two completion transitions |

Selecting the new sink option on an unsupported publication route fails closed.
Without that option, existing behavior and the legacy non-inert replay guard
remain. Empty policies retain their existing inert behavior. The option does not
change quality thresholds or authorize a fallback to a different publication mode.

## Declarative and Python selection

See the [declarative tutorial](declarative-replay-quality.md) for complete manifests.
Nulls, strings, numbers, source placement, unknown selector spellings and conflicting
normalized copies are rejected before credential resolution. Batch defaults merge
into each process; an explicit process value wins. Unsupported true selections
fail before source access, DDL or authority mutation. False preserves legacy behavior.

Trusted Python callers may pass a keyword-only `target_acceptance_reader` to
`ClickHouseSink` with `durable_quality_replay=True`. Source/staged-only callers
require no subprocess capability. Declarative factories construct the reader from
the admitted sink connection. Worker requests contain typed plans, never arbitrary
SQL; credentials travel in private pipes, never argv, files or public reports.

## Authority preconditions

The platform must externally provision the exact
`__dpone_cluster_publication_authority` table in the target database, on every
expected replica, with this ordered schema and no column defaults:

| Column | Type |
|---|---|
| `target_key` | `String` |
| `operation_id` | `String` |
| `fence_token` | `String` |
| `phase` | `String` |
| `dispatch_epoch` | `UInt64` |
| `payload` | `String` |
| `payload_sha256` | `FixedString(64)` |

Its primary key must be `target_key`. All facades must report the same
`KeeperMap` engine expression with a literal absolute Keeper path, optionally a
numeric capacity. Macros and inconsistent engines/columns are rejected.
The platform must separately attest that replicas reach the same Keeper service
and use the same configured Keeper path prefix, and restrict writer access to
trusted runtime/platform principals. SQL metadata cannot authenticate those facts.
Keep the table and Keeper data throughout the promised replay window.

The runtime uses strict KeeperMap mutations and zero insertion retries. A mutation
must be acknowledged and read back with the exact payload, caller-specific write
identity and expected next version. Unknown acknowledgement grants no dispatch
permit. Preflight is read-only: absent, legacy or unverified storage fails with
`UNSUPPORTED`, without table replacement or migration.

## Identity and evidence

The producer records normalized policy identity, admission configuration digest,
effective plan and ordered source/payload/schema digests, original run/load IDs,
validated row/hash probes, quality report and requested acceptance observations.
Before publication it binds that immutable core to the target key, operation,
fence, inventory, plan, staged rows, and exact desired generation. The authority
record uses `dpone.clickhouse.cluster-full-refresh.v2`; legacy v1 records retain
their original encoding when the new fields are absent.

Configuration identity is exhaustive and source-free. Full refresh must retain
unchanged effective semantics. Unknown options, non-JSON values, query files or
templates, enabled nested normalization, reconciliation and extraction-time
semantic overrides are unsupported. Inline queries and ordered schema observations
are hashed. Attempt IDs, injected runtime clients/results, explicitly registered
credentials and diagnostic sample sizes are excluded individually; logical
connection references and semantic options remain bound. Root path options such
as `manifest_dir` and `repo_root` are identity-bearing.

`dpone.runtime.governance.quality_replay_identity` owns the versioned registries.
Adding a `LoadConfig` field or semantic option requires an explicit classification
and identity tests. There is no recursive exclusion of arbitrary nested fields.

On every replay, the store checks current authority, completion-version continuity,
exact live inventory and target generation/schema/health, even for a completed
capsule. A durable reader fence prevents managed successor operations during
validation. UUID equality is not a content audit: unmanaged writes, data-changing
TTL/merges and privileged authority recreation are outside the supported managed
generation boundary.

## Target completion and historical compatibility

Target obligations use `dpone.quality.replay.v2`; source/staged-only evidence retains
its v1 encoding. The state path is PREPARED → TARGET_PENDING → COMPLETE or FAILED.
The original core is immutable. A target observation is added only under an exact
generation guard, persisted with acknowledged CAS/readback, then consumed through
a fresh local receipt before verified guard release. COMPLETE retries validate
current authority and the stored target observation without rescanning.

| Existing evidence | Recovery behavior |
|---|---|
| Inert historical policy | Existing inert behavior remains |
| Valid source/staged-only v1 capsule | Read under unchanged semantics; no conversion |
| Target v2 TARGET_PENDING | Same invocation may attempt bounded observation after safe guard recovery |
| Target v2 COMPLETE | Validate original proof and current generation; no target rescan |
| FAILED, missing original core or corrupt capsule | Block; no reconstruction from reports or current counts |

The worker shares one monotonic 60-second budget across metadata and aggregate
reads. Counts must be nonnegative integers, never booleans; an empty target must
return a real zero aggregate. Missing aliases, missing rows, partial results,
identity drift and late frames cannot become success. One admitted replica is
scanned; counts are never summed across replicas. All-replica generation/schema/health
checks surround the scan. Timeout and cancellation are not warn-only outcomes.

## Failure reference

All selected quality failures block committed-success fallback. The public prefix
is `DPONE_REPLAY_QUALITY_EVIDENCE_`:

| Suffix | Meaning | Next action |
|---|---|---|
| `REQUIRED` | Exact durable evidence is absent | Preserve state; verify original producer/store selection |
| `MISMATCH` | Identity, generation, report or authority-version continuity differs | Compare original invocation/configuration and platform observations |
| `INVALID` | Malformed, oversized, noncanonical or impossible evidence | Preserve the original bytes; investigate producer/storage integrity |
| `FAILED` | Original quality result or durable terminal state failed | Resolve the quality failure; do not relabel the capsule |
| `INCOMPLETE` | Governance or a read guard remains unresolved, or store access failed | Follow the retained-state recovery procedure |
| `UNSUPPORTED` | Store, policy, configuration or bounded observation capability unavailable | Use only an admitted integration; plan explicit capability work |

Store preflight and existing publication admission can also return their existing
publication errors. All remain failures. See the [runbook](committed-replay-quality-runbook.md)
for diagnosis and recovery, and [ADR 0073](adr/0073-durable-committed-replay-quality.md)
for trust and concurrency limits.

### Sealed target plan

The v2 core includes an immutable `target_plan`: ordered physical columns and types,
dataset, requested metrics, acceptance mode, selection/schema digests, and the sorted
admitted replica list. It supplies the bounded worker request on recovery and is
covered by the core digest and pre-dispatch completion-size reservation. It cannot
be replaced using current manifest values or reconstructed from historical reports.
