# Feature design: independent native stage limits

- Status: APPROVED
- Owner: DDA integrator
- Issue: staged delivery acceleration, stage 2
- Target release: next available minor (initial candidate 0.80.0)
- Last verified: 2026-09-13

Approval: the maintainer approved continuing the announced independent encoding
and import parallelism stage on 2026-09-13. This specification makes that approved
scope concrete; it does not authorize dbt or composition changes. The base is
`46830976b214262c7772800523e832a5a6f6d78f` (0.79.2).

## Executive summary

Encoding is CPU work and BCP import is network/database work. One shared worker
count prevents a data engineer from tuning either stage independently. Introduce
two explicit optional limits while retaining the single acquired source iterator,
bounded retained work and all existing validation/publication authority. This is
an additive minor feature, not automatic tuning or a promised throughput increase.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data engineer | Give encoding more CPU without overloading SQL | One count controls both pools | Planner shows resolved limits; correct typed data arrives |
| Operator | Bound spool use and recover safely | CPU and SQL capacity differ | Capacity estimate and unchanged-policy recovery are deterministic |

Discover the native transport guide and complete its ClickHouse/BCP/MSSQL and
owned-state prerequisites. Start from the existing manifest. Leave overrides
absent for the previous behavior, then set one or both under `native_chunks`.
Inspect the offline plan before running. Inspect phase observations and retained
correctness receipts after running. An invalid override fails before connector
row I/O; a changed durable policy fails recovery with the existing resource-limit
error. Settle/recover old invocations with their original settings before tuning
new invocations. Upgrade readers before sharing extended run reports.

## Scope

In scope: independent worker counts, one shared retained-work capacity, canonical
journal/readiness limits, v2 diagnostic run admission/production, tests and docs.
Non-goals: source parallel queries, automatic tuning, Arrow, SWITCH activation,
SQL layout/recovery-model changes, removed digest passes, dbt/composition, and
cross-configuration benchmark certification.

## Public contract

### Python and manifest

`NativeChunkLimits` retains all eight existing positional fields and defaults.
New keyword-only `encoding_parallelism: int | None = None` and
`import_parallelism: int | None = None` independently fall back to `parallelism`.
Non-None values must have exact type int and range 1..64. Booleans are invalid.
Properties expose `effective_encoding_parallelism`, `effective_import_parallelism`
and `retained_work_capacity`. `to_dict()` returns the canonical durable policy.

Manifest path: `native_transfer.execution.native_chunks`. Both optional fields
are strict integers 1..64. Explicit null, strings, floats, booleans and out-of-range
values fail; omission means fallback. Existing `chunking.parallelism` stays in
place and retains its default. Both shipped manifest schemas admit the additions.
No new CLI flags, exit codes or output destinations. Existing planner output
keeps its shape for legacy-effective settings; extended policies add a bounded
`stage_concurrency` explanation with both effective counts and retained capacity.
`import_parallelism` bounds concurrent file import/verify tasks, not all target
connections: parent-side admission and retry settlement may use other contexts.

### Durable representation and evidence

Freeze the legacy field names explicitly: max_total_encoded_bytes,
stage_allocated_bytes_stop_threshold, max_rows, max_bytes, max_row_bytes,
max_pending, max_staging_tables, parallelism. If both effective counts equal
parallelism, serialization returns exactly these eight fields. Otherwise it adds
both resolved worker counts (ten fields total). The authored parallelism fallback
remains part of policy even when both overrides are supplied. Recovery requires
exact equality of this canonical record and never rewrites old journal bytes.

`normalize_delivery_limits` retains its exact v1 eight-field behavior by default.
An explicit v2 normalization path requires ten fields, concrete strict counts,
and a non-legacy-effective policy; it rejects noncanonical extended records.
The producer chooses run schema v1 for eight fields and v2 for ten. `RUN_SCHEMA`
remains v1; a separate v2 schema changes only the version and limit admission.
Correctness receipts, observations, maintenance envelopes, campaign and comparison
schemas stay v1 because their fields/authority are unchanged. Consumers dispatch
on exact integer run version 1 or 2, never bool/string/unknown versions.
Configuration hashes are over the exact canonical record. Retained historical
bytes and hashes are never filled, normalized on disk, or silently upgraded.

Comparisons retain exact configuration equality for all supported run versions.
A symmetric baseline and asymmetric candidate are different experiments; they
must not obtain a benchmark comparison PASS by relaxing equality. Tuning tables
retain each configuration and evidence separately and make no certified speedup
claim. Old consumers reject v2; old v1 producers/consumers retain their behavior.

## Detailed algorithm

1. Validate manifest and construct limits before source/target row I/O.
2. Resolve E and I independently. Set C = max(E,I) + max_pending.
3. Use canonical limits for target journal binding and offline capacity planning.
4. Acquire the existing lease and single source iterator. Preserve source schema,
   row-size, frame, task IPC and cumulative native-byte admission.
5. Create spawn process pool with E encoders and thread pool with I importers.
6. Retain one Future-to-Work map across both stages. Admit another frame only
   while map size is below C. Existing one-row lookahead remains bounded.
7. An encode completion is replaced by an import future in the same slot, with
   attempt intent durable before mutation. A transient retry settles its previous
   attempt and replaces the same slot using the identical sealed file/ordinal.
8. Release a slot only after receipt acceptance and successful file removal.
   The sequential parent loop does not refill during completion transitions.
9. Complete only after source EOF and all contiguous independently verified
   receipts. Retain fencing, renewal, target allocation checks and publication,
   evidence/checkpoint ordering. Recovery never rereads a partial query offset.
10. On cancellation, lease loss, submit/pool/worker/settlement/CAS/unlink errors,
    preserve existing failure classification, settle active writers before return,
    preserve primary errors and prohibit false stage-complete authority.

```text
E = encoding_override if supplied else parallelism
I = import_override if supplied else parallelism
C = max(E, I) + max_pending
while not EOF or retained:
    while not EOF and len(retained) < C: validate_and_submit_next_frame(E)
    for completed in wait(retained):
        encode -> durable_attempt -> import(I)    # same slot
        transient_import -> settle -> retry(I)   # same sealed bytes/slot
        verified_import -> receipt -> unlink -> release_slot
complete_only_after_EOF_and_all_verified_receipts()
```

Payload reservation is `(C + 1) * max_bytes`, including the existing extra frame
allowance; format/receipt overhead and free-space reserve remain separate. This
is not a Python RSS bound, exclusive disk reservation, SQL allocation cap or
promise that both pools are simultaneously saturated. Do not replace max by sum.

Empty input still completes via verified EOF; duplicates and NULL values retain
typed semantics. Schema drift and unsupported capabilities still fail existing
admission. Partial writes/timeouts retain receipt-first reconciliation and unknown
outcomes. Nested normalization is unchanged. Optional overrides never change SQL.

## Architecture and alternatives

| Component | Responsibility |
|---|---|
| contracts.mssql_native_chunks | Strict limits, effective policy and finite serializer |
| manifest.mssql_native_policy and JSON schemas | Strict authored admission |
| runtime.mssql_native_chunks | Existing bounded scheduler, journal binding and pools |
| readiness.mssql_native_planning | Canonical limits and understandable resolved plan |
| contracts.native_delivery_observations | Version-specific finite diagnostic admission |
| runtime benchmark and tools live producer | Preserve hashes, dispatch run versions |

Reuse injected importer factories, leases/stores and composition roots. Introduce
no scheduler port, backend registry or vendor dependency. The same policy handles
CPU-heavy wide Unicode rows and SQL-bound narrow rows. Reject independent queues
(sum-bound retention increase), automatic tuning (unproven control/recovery policy)
and silently extended v1 records (breaks retained evidence). ADR 0063 records the
bounded concurrency and compatibility decisions. Existing 400-SLOC and graph
budgets apply; do not increase existing debt or change thresholds.

## Market comparison

Official documentation checked 2026-09-13. Facts below describe documented
mechanisms; adoption decisions are dpone design choices, not comparative results.

| System/version | Observation and strength | Relevant limitation; adopt/reject | Source |
|---|---|---|---|
| dlt 1.30.0 | Separate normalization process workers and load thread workers; file rotation enables concurrency | Adopt separate CPU/I/O controls; reject copying defaults or parallel source queries | [Performance](https://dlthub.com/docs/reference/performance) |
| Informatica 10.5.9 Data Integration | Reader, transformation and writer pipeline stages use separate threads | Adopt stage distinction; pipeline partitioning is broader than one acquired stream | [Pipeline threads](https://docs.informatica.com/data-engineering/common-content-for-data-engineering/10-5-9/application-service-guide/data-integration-service-management/maximize-parallelism-for-mappings-and-profiles/one-thread-for-each-pipeline-stage.html) |
| Airbyte current hosted docs | N/A for this bounded BCP worker-control contract; connector replication is the product scope | No inferred equivalence or unsupported worker setting adopted | [Documentation](https://docs.airbyte.com/) |
| Fivetran HVR 6 | Separate integrate jobs can serve different table groups | Adopt awareness of target concurrency; reject table/channel repartitioning for this single-stream stage | [Integrate jobs](https://beta.fivetran.com/docs/hvr6/faq/how-to/multiple-integrate-jobs-running-simultaneously) |
| Pentaho current Pipeline Designer | Step copies and row-distribution choices expose parallel execution | Adopt explicit counts; reject copying every row to every worker | [Step copies](https://docs.pentaho.com/pdia-data-integration/pipeline-designer/edit-a-transformation-or-job) |
| SSIS SQL Server 17 docs | Buffer and data-flow task/thread tuning are separate; EngineThreads is a suggestion | Adopt stage/buffer diagnostics; dpone capacity is its own tested accounting rule | [Performance](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/data-flow-performance-features?view=sql-server-ver17) |
| gusty | N/A: DAG authoring, not native ingestion worker scheduling | No scheduling semantics copied | N/A |
| Astronomer Cosmos | N/A: dbt orchestration, outside this task | No dbt changes | N/A |
| Apache Beam current execution model | Runner chooses pipeline execution and dependent parallelism | Adopt explicit statement of concurrency limits; reject a distributed-runner abstraction | [Execution model](https://beam.apache.org/documentation/runtime/model/) |

## Measurable outcome

Axis: independently controllable stages with bounded retained payload.
Baseline: 0.79.2 shared count. Scenarios: E=2/I=1 and E=1/I=2, narrow and wide
Unicode workloads. Targets: deterministic observed concurrency never exceeds E/I;
retained items never exceed C; typed content/duplicates/metadata/receipts unchanged;
legacy serialization and old recovery identical. Procedure: event-controlled unit
scheduler tests, spawned-worker integration and approved local Docker runs with
one warmup and three measured trials per diagnostic configuration. Retain timing,
config hashes, source/producer commit, environment limits and correctness receipts
under the stage evidence directory. No percentage improvement is a release claim.

## Security, tests and operations

Credentials stay in memory in the existing approved Docker helper. Unique owned
resources only; no pruning or shared target cleanup. Tests cover strict positive/
negative admission, positional compatibility, omitted/one-sided/equal overrides,
old journals and v1 hashes, noncanonical v2 refusal, same-policy report consumption,
capacity/spool arithmetic, blocked encoders/importers, retry saturation,
out-of-order completions, lease loss, cancellation, closure and failed futures.
Run focused then broad repository gates, architecture/docs/schema checks, real-row
full-refresh/window tests, empty/recovery cases and narrow/wide measurements.
A fresh-context reviewer assesses final source and evidence before integration.

Documentation includes native transport reference and example, phase/frame guide,
observations v1/v2 migration, local Docker/certification procedure, architecture,
ADR index, changelog and generated schema/reference output. CLI help remains
compatible, but exact-limit input help explains eight versus ten fields.

## Rollout and rollback

Upgrade consumers first. Use old settings initially, tune new invocations only.
Do not downgrade while an extended-policy invocation is unfinished: complete or
safely settle it on this version before removing overrides. Revert configuration
to legacy defaults for future runs if resource or throughput behavior worsens.
Do not rewrite journals or disable verification to obtain recovery.

## Agent execution plan and definition of done

The parent is sole integrator and shared semantic owner. Read-only explorer,
architect, test/certification and docs/UX agents map the scope first. Any delegated
writer gets a separate worktree and validated path contract; no shared-file
ownership. DoD: approved algorithm implemented, compatibility tests PASS, supported
Docker route/recovery PASS, measurements truthfully retained, docs PASS, independent
review resolves blockers, normal PR checks/merge receipt and separate minor release.
No production readiness claim extends beyond the exact tested environment.
