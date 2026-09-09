# Feature design: bounded streaming windows and impact-aware acceptance

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Target release: unassigned; no publication requested
- Last verified: 2026-09-09

This design is for framework maintainers and connector authors. It separates
two independently reviewable changes: runtime transport and CI acceptance
selection. The maintainer approved implementation on 2026-09-09. Each implementation phase
freezes its exact schemas and task contracts under the
[feature design standard](feature-design-standard.md); this records implementation
detail within the approved scope, not a new approval gate.

## Executive summary

Extend the existing typed streaming route with bounded resource use, recoverable
staging, and capability-checked atomic window publication. Independently extend
acceptance selection with conservative semantic impact classification.

All examples, fixtures, performance workloads, and acceptance procedures use
synthetic data and reproducible open-source framework tooling. Changes are limited
to dpone and its maintained integrations. No deployment, runtime pin changes, or
release publication form part of this proposal.

## Personas and customer journey

| Persona | Goal | Success signal |
|---|---|---|
| Data engineer | Refresh a bounded interval without hand-written SQL | Plan explains the fixed interval, selected transport, and publication scope |
| Connector author | Supply typed rows and target capabilities | Shared contracts pass without connector-specific runtime policy |
| Operator | Recover safely after interruption | Status distinguishes staged, published, and unknown outcomes |
| Maintainer | Select useful acceptance checks | Plan explains each selected check and retains mandatory gates |

The journey is: discover supported capabilities; check prerequisites; configure
the existing manifest surface; inspect the plan; run a small synthetic example;
inspect counts, bounds, and publication status; diagnose a failure; reconcile or
resume using durable identity; upgrade with old manifests retaining their behavior.

## Current implementation and actual gaps

Existing components to extend:

- `dpone.runtime.bulk_wire.BulkWirePlanner`: typed-binary route selection.
- `dpone.runtime.clickhouse_rowbinary.ClickHouseRowBinaryEncoder`: RowBinary encoding.
- `dpone.runtime.sources.strategies.mssql.mssql_row_stream_artifacts`: typed source stream.
- `dpone.runtime.byte_stream_artifacts.ByteStreamArtifact`: stream artifact contract.
- `dpone.runtime.native_transfer_transport.TransferTransportResolver`: stream eligibility.
- `dpone.runtime.native_transfer_checkpoint_lifecycle` and
  `dpone.runtime.lineage.partition_resume`: current checkpoint/resume behavior.
- `dpone.runtime.physical_chunk_policy`: physical resource policy.
- `dpone.runtime.sinks.clickhouse_staged_load`: staging and replacement orchestration.
- `tools/agent_policy/select_checks.py`: existing path-based validation selection.

`tests/test_native_typed_binary_wire.py` already exercises RowBinary planning,
NULL handling, row streaming, and HTTP ingestion. These tests establish existing
coverage, not throughput or whole-window recovery certification.

The high-level native-transfer capability planner and actual artifact path need
reconciliation: the former can reject streaming where the latter supports a typed
stream. Row-count batching alone does not bound encoded bytes. Existing logical
partition resume must not silently become physical-fragment resume. The current
ClickHouse swap helper uses multi-table RENAME, so its existence does not establish
atomic publication. Replacing partitions in a loop is not whole-window atomicity.

## Scope and non-goals

Workstream A covers existing transport selection, byte limits, typed fidelity,
logical bounded chunks, durable staging recovery, and rolling-window publication.
Workstream B covers acceptance planning and its integration into existing CI.
Neither depends on the other being merged first.

No second transport framework, generic plugin registry, new scheduler, automatic
switch to a different route, or removal of legacy wire formats is proposed.
RowBinary remains a ClickHouse adapter capability. Existing routes remain usable.
No cross-cluster atomicity or general exactly-once claim is made.

## Public contract and compatibility

Preserve the existing configuration locations:

| Concern | Existing configuration path |
|---|---|
| Typed wire selection | `source.options.native_transfer.wire.mode` |
| RowBinary format | `source.options.native_transfer.wire.binary_format` |
| ODBC row stream | `source.options.native_transfer.wire.source_native_format` |
| Physical streaming | `source.options.native_transfer.execution.transport.mode` |
| HTTP ingestion | `sink.options.clickhouse_bulk.mode` |
| Typed staging | `sink.options.clickhouse_bulk.ingest_contract` |

Do not introduce a competing top-level `transport` block. The rolling fields `window`, `anchor`, and `lookback` are additive manifest
syntax compiled into the opt-in Python composition described in
[Atomic rolling windows](rolling-window.md). The default runner refuses execution
until a verified writer authority and snapshot runtime are injected. `state.atomicity` must retain
its existing meaning; it does not mean target visibility is atomic.

Existing CLI commands, exit codes, stdout/stderr, Python entry points, and defaults
remain unchanged in the transport phase. New options require plan-visible
validation errors before writes. Explicit streaming requests must fail when
unsupported; existing automatic fallback remains governed by its existing policy.
No command is added solely to expose an internal implementation step.

The window contract must require an explicit interval anchor from execution
context or the caller, a positive fixed duration, timezone and timestamp precision.
The initial duration proposal treats a day as 24 hours in UTC; calendar-month and
local-calendar windows are excluded. Resolve the anchor once and persist it.
Old manifests do not acquire rolling behavior or different resume identities.

## Detailed runtime algorithm

1. Validate types, options, limits, source consistency, and target publication
   capabilities before source extraction or target mutation. Reject unsupported
   types instead of silently coercing them.
2. Fix `[start, end)` and construct parameterized dialect-specific predicates.
   Every logical chunk is `[boundary[i], boundary[i+1])`; the last upper bound
   is exactly `end`. Reject overlap, gaps, and unbounded tails.
3. Acquire a target-scoped fenced writer lease covering all entry points, then
   establish source snapshot identity or a verifiable immutable source version.
   Independent connections require a shared consistency mechanism. If the source
   version cannot survive restart, whole-run re-extraction is required; previous
   completed chunks cannot be reused against a different source version.
   Shadow publication additionally requires exclusion of all other target
   writers, including clients outside dpone, or a proven change-preservation
   mechanism. A dpone-only lease cannot protect concurrent external writes.
4. Derive run and chunk identities from canonical route identity, normalized
   query parameters, window, schema, source version, and execution-contract
   version. Never include credentials. Persist the plan before chunk work.
5. Allocate isolated staging per run and logical chunk attempt. Stream typed rows
   through the existing encoder using independent worker connections. Enforce a
   byte budget for encoder-owned payload buffers; also bound workers, batch rows,
   row bytes, and active requests. Driver allocations and total process RSS are
   outside this enforceable budget and must not be reported as bounded. Oversized rows
   fail explicitly. Stop pulling rows when downstream capacity is exhausted.
   Distinguish enforceable application-buffer budgets from driver/native
   allocations. Require an adapter-specific allocation bound or an isolated
   worker memory guard; reject a strict memory guarantee if neither is available.
6. Verify the server-side staging contents before marking a chunk complete.
   A durable record binds chunk range, attempt ownership, source/staging counts,
   schema/window/source-version fingerprints, and content reconciliation.
   Checkpoint persistence uses compare-and-swap and fencing. A local success flag
   alone cannot authorize replay suppression.
7. On a transient network/timeout failure, resolve whether the attempt was
   applied. Reuse a verified complete attempt or replace only its isolated staging
   after fencing the old writer and confirming cleanup. Allow at most two retries
   with bounded exponential backoff. Schema, contract, DQ, authentication, or
   unsupported-capability failures are terminal. Unknown write outcomes must be
   reconciled before retry; retry classification alone is insufficient.
8. After all chunks pass, build and verify the replacement generation. For a
   shadow-table implementation this includes preserved rows outside the window,
   including rows with NULL in the window column. Explicitly account for that
   copying cost, disk space, dependent objects, and reader behavior in eligibility.
9. Publish once through a proven atomic target primitive. The first ClickHouse
   candidate is a single table-pair EXCHANGE on an eligible local database engine;
   replicated/distributed topology and dependent-object support require separate
   proof. Refuse unsupported combinations before writes. Do not substitute a
   sequence of partition operations or RENAME operations.
10. Resolve publication using persistent generation identities and the observed
    target identity. Write durable commit evidence before advancing source state.
    After publication, an evidence failure requires completion/reconciliation;
    it cannot truthfully be described as preserving the old target.

```text
validate -> freeze interval -> lease -> freeze source version -> plan
  -> bounded isolated chunk attempts -> reconcile staging
  -> verify replacement -> publish once -> reconcile publication
  -> durable evidence -> source state -> success

unknown attempt outcome -> inspect/fence -> verified reuse or isolated replacement
unknown publication outcome -> inspect target generation -> complete or stop
```

### State and failure semantics

Proposed internal states are planned, staging, verified, publishing, published,
evidence-complete, and succeeded, with failed and outcome-unknown branches.
Their serialized vocabulary and version migration must be frozen in the recovery
phase. Existing artifact statuses must not be reinterpreted.

Before publication, failure preserves the old visible target. After publication,
recovery completes evidence for the observed generation. Never retry a table
EXCHANGE blindly: a second exchange can undo the first. Cancellation stops reads,
cancels/joins workers, closes cursors, and preserves enough owned staging and
identity for reconciliation. Cleanup never deletes another run's resources.

Empty source windows replace the requested interval with an empty set after
verification. NULL window values are outside the selected interval and retained
in the target. Duplicate rows retain their multiplicity. Drift, expired snapshots,
lease loss, or changed fingerprints reject resume. Unsupported nested types fail
explicitly; extending nested normalization is a separate feature.

## Evidence and measurable outcomes

Run evidence records source and staging counts, counts per UTC day, min/max,
outside-window count, NULL counts, schema/type fingerprints, per-chunk counts,
source version, limits, retries, phase timers, bytes, and publication identity.
Keep window-only staging checks separate from preserved outside-window rows.

Counts alone do not prove duplicate parity. Synthetic acceptance uses exact typed
multiset equality, including duplicate multiplicities. A scalable digest mode
must specify canonical type encoding, order independence, multiplicity, algorithm
version, and collision limitations; it must not be labeled an exact proof.

Performance compares the new route with a fixed existing dpone revision on the
same seeded dataset, schema, container versions, machine, and resource limits.
Measure extract/load/verification/publication separately and total elapsed time,
peak memory, and temporary bytes. Include wide rows, skew, duplicates, NULLs,
Decimals, and out-of-window target data. Run repeated trials and publish their
dispersion. The initial mandatory targets are fidelity, bounded resources, and
safe recovery; an acceleration threshold is set from this reproducible baseline
before a performance claim or default change. No speed result is asserted here.

All temporary files, when needed, belong under `runtime.storage.work_dir`.
Evidence contains metadata and sanitized identifiers, never credentials or row
payloads. Runtime evidence is generated by its producer, not hand-authored.

## Independent CI acceptance change

Keep all existing mandatory non-live, security, packaging, and release gates.
Extend acceptance planning rather than replacing `select_checks.py` or treating
its command list as proof that CI executes those commands.

1. Bind classification to exact base/head revisions and classifier version.
2. Classify changed paths conservatively; parse supported declarative inputs
   without executing code. A schedule-only result requires equality of all
   normalized non-schedule fields and unchanged interval derivation, runtime,
   schema, dependencies, and connection behavior. Unknown inputs select broader
   checks. Labels and branch names cannot override semantic evidence.
3. Schedule-only changes require parser, serialization, interval, and scheduling
   contracts, with no full data transfer solely because the schedule changed.
4. Transport/type/staging/recovery changes require focused contracts and a narrow
   synthetic container smoke for affected supported open-source source/sink pairs.
   Route claims for other adapters retain their own certification requirements.
5. Large synthetic performance/soak jobs remain separate opt-in jobs for PRs.
   This does not downgrade any release evidence required by existing policy.
6. Record selected checks, reasons, execution results, and artifact references.
   Missing infrastructure is SKIP/UNVERIFIED, never PASS. A required smoke that
   cannot run leaves the relevant acceptance incomplete.

## Architecture and alternatives

Reuse existing services; put new reusable contracts in `dpone.contracts`, narrow
ports in `dpone.ports`, policies in `dpone.runtime`, and vendor behavior in
adapters. Inject source consistency, staging, publication, checkpoint storage,
clock, and retry dependencies at composition roots. No import-time I/O or patches.

Two realistic variations are a typed source streaming into ClickHouse RowBinary,
and a typed source feeding a transactional sink with its own binary/batch adapter.
They share bounded execution and recovery semantics, not a mandated wire format.

| Alternative | Decision |
|---|---|
| New universal planner and codec registry | Reject duplicated capability ownership |
| More simultaneous runs against one target | Reject; parallelism stays within the fenced run |
| File-backed extraction | Preserve existing route; explicit storage/replay tradeoff |
| Multiple independent partition publications | Insufficient for whole-window atomicity |
| Whole shadow generation | Candidate; include outside-window copy and space cost |
| Full re-extraction after snapshot loss | Safe supported fallback with explicit recovery cost |

An ADR is required for durable chunk identity, fenced recovery, and publication
ordering; reconcile it with existing ADRs before implementation. New modules are
split by policy/port/adapter responsibility. Quality budgets remain those in
`docs/benchmarks/quality_budgets.yml`; no duplicated thresholds or debt exemptions.

## Market comparison

Official documentation checked 2026-09-09; these are living documentation sources,
not pinned benchmark versions. Product-performance superiority is unverified.

| System | Observed design and assessment | Adopt / reject |
|---|---|---|
| dlt, open-source documentation | Configurable worker counts, queued-item bounds, and file-based load packages give explicit concurrency controls; this source does not establish atomic rolling-window semantics | Adopt resource bounds; do not require a complete intermediate file for this stream route |
| Apache Beam, current programming guide | Splittable DoFn supports modular I/O and restriction-based work decomposition; its scope is a broader execution model | Adopt explicit bounded work ownership; do not introduce a Beam runner dependency |
| Airbyte | N/A for this focused comparison: no change to its connector protocol or sync engine is proposed | No comparative product claim |
| Pentaho | N/A for this focused comparison: transformation-graph execution is outside this change | No comparative product claim |
| gusty / Astronomer Cosmos | N/A: DAG authoring and dbt orchestration are not the transport or atomic publication layer | Keep scheduler integration thin |
| Informatica / Fivetran / Microsoft SSIS | N/A: the selected comparison is limited to open-source execution patterns | No comparative product claim |

Sources: [dlt performance](https://dlthub.com/docs/reference/performance),
[Beam programming guide](https://beam.apache.org/documentation/programming-guide/).
The adoption decisions are dpone design judgments, not claims about missing
capabilities in other products.

For adapter semantics, the official
[RowBinary reference](https://clickhouse.com/docs/reference/formats/RowBinary/RowBinary)
specifies binary NULL flags and typed values. The
[EXCHANGE reference](https://clickhouse.com/docs/reference/statements/exchange)
describes atomic exchange of one pair on supported engines and warns that
multiple pairs execute sequentially. This motivates the single-publication
constraint; topology eligibility still requires implementation evidence.

## Test, documentation, and rollout plan

Local Docker execution is mandatory before implementation acceptance. Use
isolated disposable containers, public images with recorded digests, and seeded
synthetic data. Running unit tests inside a container does not replace moving
real rows through a database. Required cases must have nonempty test collection
and zero skips. The matrix below defines the corner-case scope; extending the
contract requires extending this matrix, not claiming universal coverage.

| Layer | Required coverage |
|---|---|
| Unit | Integer boundaries, Decimal scale/overflow, date/time precision, NULL versus literal text, oversized rows, bounded buffers |
| Contract | Planner/runtime parity, half-open coverage, cancellation, worker isolation, unsupported capability rejection |
| Recovery | Crash before/after write acknowledgment, checkpoint persistence, publication acknowledgment, evidence, and state; lease loss and snapshot expiry |
| Synthetic integration | Exact multiset equality, empty windows, outside-window preservation, repeat/resume, one atomic visibility point |
| CI policy | Schedule-only and mixed diffs, renamed/deleted files, malformed inputs, unknown paths, exact-revision binding, mandatory-gate preservation |
| Performance | Seeded baseline and candidate, peak memory/storage, phase timings, skew and wide-row distributions |
| Compatibility | Old manifests, CLI/Python parity, artifact versions, explicit migration and rollback |

Boundary coverage includes zero/one/many rows; all-NULL columns versus empty
strings and literal NULL markers; every supported signed/unsigned width and its
overflow neighbors; Decimal precision and scale limits; epoch/date range and
timestamp precision; missing columns and schema drift; timestamps at both ends
and each chunk boundary; reversed/empty intervals; timezone transitions; duplicate
rows across chunks; skew and an oversized individual value; slow consumers;
queue saturation; cancellation; connection loss before/after acknowledgment;
checkpoint-write failure; expired source snapshot; stale or missing staging;
changed identity; lease expiry and competing writers; empty replacement;
outside-window and NULL-window preservation; publication acknowledgment loss;
evidence failure after publication; and repeat execution after each crash point.

Each implemented case maps to a test identifier and JUnit result. The bounded
window and recovery suites include synthetic Docker fault injection. Evidence
is valid only for the recorded source revision; unexecuted topologies and
performance comparisons remain UNVERIFIED.

Documentation changes accompany each phase: first-success tutorial, manifest and
capability reference, developer ports, architecture/ADR, recovery runbook, and
synthetic examples. Explain retained rows, retry refusals, outcome-unknown, and
required operator action. Link from [native transfer](native-transfer-industrial-runtime.md)
and [load strategies](load-strategies.md) when behavior is implemented. This design
does not present speculative YAML as currently runnable configuration.

Implement in independently reviewable phases: (A1) planner/runtime parity and
typed byte bounds; (A2) fenced chunk staging and recovery; (A3) window DSL and
atomic publication; (B) acceptance selection. A2/A3 are approved only with exact
serialized contracts, source consistency rules, and target capability matrices.
Enable new behavior explicitly, preserve existing defaults, and use synthetic
container verification before broadening support. Roll back new selection for
future runs if fidelity, memory bounds, or recovery fails; reconcile an active
run under its original contract version before cleanup or downgrade.

## Agent execution plan and approval

The parent agent is the integrator and sole shared-file owner. Read-only explorer,
architect, test/certification, and docs/UX analysis informed this design. Before
implementation, allocate disjoint owned paths per phase using the repository task
contract template and separate worktrees for parallel writers. All other paths
are read-only; shared schemas, registries, workflows, dependencies, navigation,
and changelog remain integrator-owned. Fresh-context review precedes integration.

- [x] Open-source scope and synthetic acceptance defined.
- [x] Existing implementation and compatibility boundaries identified.
- [x] Failure semantics, alternatives, and independent CI scope researched.
- [x] Public primary sources checked; performance claims withheld.
- [x] Per-phase exact implementation and serialized contracts frozen in v1 modules.
- [x] Initial target topology/source consistency scope fixed below.
- [x] Writer task contracts assigned and checked in isolated worktrees.
- [x] Maintainer approval recorded before production-code changes (2026-09-09).

## Implemented capability boundary

| Capability | Initial implementation |
|---|---|
| Typed transport | Existing explicit MSSQL ODBC row stream to ClickHouse RowBinary; encoded byte bounds are opt-in |
| Shared source snapshot | PostgreSQL ordinary table and timestamptz window; keeper lifetime limits prepublication resume |
| Window target | Local ClickHouse Atomic database and plain MergeTree; TTL, projections, dependencies, computed columns, row policies, active mutations and distributed/replicated topology rejected |
| Writer ownership | Injected all-writer authority required; local SQLite fencing alone is insufficient; no general deployment authority is bundled |
| Window authoring | Additive schema/parser, required explicit interval context, Python runner factory; default runner refuses missing capability |
| Governance | Intrinsic typed staging verification; incompatible authored legacy policies reject before execution |
| Recovery | Strict version-one invocation, chunk and publication metadata; immutable window column/ranges/schema/source identity; at most three total chunk attempts |
| Performance | No numeric speed or RSS guarantee; repeated performance/soak remains separate from correctness smoke |

Whole-generation publication retains old target data as backup and copies all
outside-window rows. Postpublication failure can leave new data visible; recovery
finishes evidence and state for that same generation. No blanket claim that every
failure leaves the old target applies after the publication point.

The initial composition deliberately has no default unsafe all-writer guard.
Broader adapters, deployment-specific authority, automatic backup cleanup, and
strict process-memory admission require further capability evidence. They cannot
be inferred from the local synthetic fixture.

See [ADR 0056](adr/0056-bounded-window-atomic-publication.md) and
[agent development](agent-development.md) for review and integration. Runtime test
artifacts are produced locally and are not committed as release certification.


## Implementation verification

The implemented boundary above is covered by 419 local Docker tests with no
failures, errors, or skips on source commit
`b608e86cfec7595405b1bfd140441275f314734b` (2026-09-09), including real synthetic
PostgreSQL/ClickHouse transfer and recovery. The SHA-bound receipt validates all
14 required test-file populations. Review and hosted verification are tracked in
[PR #3](https://github.com/PaulKov/dpone/pull/3). This is implementation evidence,
not release certification or an unmeasured performance claim. Local evidence is
under `test_artifacts/bounded-streaming-implementation/`: `docker-complete.xml`,
`docker-complete-receipt.json`, and `module-size-complete.log`.
