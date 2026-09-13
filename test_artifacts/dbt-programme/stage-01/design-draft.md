# Feature design: bounded dbt transfer configuration projection

- Status: DRAFT — implementation prohibited
- Owner: programme coordinator; maintainer approval pending
- Issue/target release: unassigned
- Source baseline: `46830976b214262c7772800523e832a5a6f6d78f`
- Last verified: 2026-09-13

## Problem and intended outcome

A platform engineer can tune ordinary MSSQL → ClickHouse native transfers but
cannot express those same settings in the closed dbt publish profile. Existing
compilation already preserves admitted options. Extend only the input contract
for a reviewed subset of existing runtime options; reuse existing execution.
This draft is separate from composition activation and the isolated fixes.

Personas: an author selects a trusted profile; the platform owner controls
bounded resource and transfer policy; the operator inspects effective settings
and retries through existing evidence/state rules.

Journey: discover the profile capability table → install the current pinned
toolchain → use a checked schema-valid synthetic policy/model → run read-only
check/explain → compile into a new immutable root → execute with the existing
verified runtime binding → inspect effective options and route evidence →
diagnose rejected option/identity → correct policy and regenerate → upgrade
readers before adopting a new policy version.

## Scope and public contract proposal

Initial route remains MSSQL → ClickHouse. Initial strategy/transport admission
must be explicit per existing capability evidence. No new CLI flags, dbt Python
API, author metadata fields, transport implementation, source projection query,
snapshot authority, arbitrary SQL setting map or composition activation.

The smallest candidate slice retains `native_transfer.mode: auto` and exposes
only bounded staged-file execution/resource tuning already consumed by the
runtime. Candidate groups are `source.options.native_transfer.execution`
resource policy and the required bounded ClickHouse ingest settings. Wire
encoding, streaming selection, async acknowledgement, native required-mode,
endpoint overrides, arbitrary insert settings and connector aliases stay out
until their combinations have independent route proof.

This is deliberately DRAFT: the coordinator must freeze the exact field list,
types, bounds, defaults and applicable strategy/transport coordinates before
the spec can become RESEARCHED or APPROVED. The stage-01 matrix is the agreed
observed input boundary, not approval for every runtime key.

Use a new versioned closed policy contract if older readers reject additions;
the coordinator assigns its identifier. V1/v2/v3 retain existing meaning and
byte expectations. Prefer one canonical contracts-level description for the
admitted subset and runtime consumption; do not import runtime policy modules
into manifest/services, weaken `additionalProperties`, or broaden a shared V1
schema helper. No generated schema is hand-edited.

TLS/driver/port/CA belong to deployment connection authority. Their inheritance
and explicit-override semantics need a separate narrowly approved decision
because current HTTP/native TCP/client omission behavior differs. Do not solve
it by placing credentials or endpoint overrides in publish profiles.

## Algorithm and failure semantics

1. Acquire bounded YAML and select its exact policy discriminator.
2. Validate a closed typed map. Reject unknown keys, invalid units, negative
   limits and unsupported combinations before artifact publication or I/O.
3. Resolve model identity from dbt's manifest. Preserve profile defaults and
   author overrides; attach explicit extraction scope independently from the
   physical partition expression/type/completeness claim.
   Preserve ordered source column types, wire format/bytes and validation receipt
   identity as one artifact contract. Target column projection must not silently
   rewrite source authority. A prevalidated boolean is not an authenticated
   receipt; temporal schema changes must match actual bytes. Business NULL,
   default and timezone rules remain authored. These decisions require the
   types/artifacts stage's retained public-interface evidence before approval.
4. Match the exact route variant and current certification dimensions. A
   tuning field must not select a different transport implicitly.
5. Copy validated transfer options into the generated workload. Keep fixed dbt
   transformation adapter policy and invocation in its separate execution pack.
6. Bind effective policy into existing selection/release fingerprints and
   generate all outputs through their current producers. Verify deterministic
   reruns; same destination/same bytes is a no-op, conflicting bytes reject.
7. Hydrate a single resolved deployment connection at the existing composition
   root. Check source/sink logical identity against physical authority before
   adapter/client construction; never let profile settings replace endpoints.
8. Execute with existing state, staging, quality and finalization services.
   Preserve target guard → target outcome → durable evidence → checkpoint order.
9. Preserve existing failure codes/real exit results and unknown-commit handling.
   No automatic second executor, cleanup permission or changed retry policy.

```text
validated policy + resolved dbt model + exact route variant
  -> deterministic transfer manifest -> immutable transfer pack
  -> verified deployment binding -> existing native runtime
  -> existing quality/finalizer/evidence/checkpoint sequence
```

No new state machine is introduced. Empty input, duplicates, timeout, partial
write, process crash and schema drift retain route-specific handling. The three
execution families must not share an invented common empty-data rule. Unsupported
physical partition completeness blocks partition publication; a transport
option cannot authorize deleting data outside the acquired source scope.

Concurrency uses existing limits and scheduling; no adaptive algorithm or new
defaults. Resume retains its frozen operation/attempt/identity and verified
staging requirements. Cancellation cannot turn commit-unknown into retryable.

## Components and dependency direction

| Component | Change | Responsibility |
|---|---|---|
| contracts policy/schema description | Additive, coordinator-owned | Closed field/type/version authority |
| manifest profile registry | Existing | Validate and normalize admitted shape |
| services compiler/planner | Reuse | Project policy and preserve model/scope identity |
| readiness pack producers | Reuse | Freeze exact transfer bytes and release authority |
| runtime / connector adapters | Reuse initially | Consume options behind existing injected capabilities |
| deployment connection resolver | Separate proposal if changed | Own endpoint and TLS projection |

Compatibility shims re-export/adapt only. An ADR is required if policy/version
authority or connection precedence changes; no speculative generic plugin
framework. Quality limits remain in `docs/benchmarks/quality_budgets.yml`; the
coordinator chooses cohesive modules after the field list is fixed.

Alternatives: reject a new transfer engine (existing engine suffices), unrestricted
passthrough (unbounded public contract), and generated-manifest hand edits
(break immutable authority). Keep old defaults rather than activating all
existing runtime features through schema acceptance alone.

## Market comparison

Checked 2026-09-13; observations are facts from current primary documentation,
design choices below are dpone inferences. No performance superiority claim.

| System/version | Observed pattern | Adopt / reject for this scope |
|---|---|---|
| dlt 1.30.0 docs | ClickHouse describes separate native/HTTP ports, credentials and destination options; local loads use clickhouse-connect | Adopt explicit transport-aware connection examples; do not infer one port works for every transport. [Source](https://dlthub.com/docs/dlt-ecosystem/destinations/clickhouse) |
| Astronomer Cosmos current docs | ProfileConfig accepts a profiles.yml file or an Airflow connection profile mapping with explicit profile/target | Adopt one connection authority and explicit profile identity; do not introduce a second graph/execution authority. [Source](https://astronomer.github.io/astronomer-cosmos/guides/connect_database/index.html) |
| Informatica / Airbyte / Fivetran / Pentaho / SSIS | N/A in this bounded draft | Managed/designer connector and replication orchestration are outside this existing-dbt-policy projection decision; no comparative capability claim |
| gusty / Apache Beam | N/A in this bounded draft | DAG file authoring / general pipeline programming do not define this existing policy-to-native-runtime contract |

Measurable target: for every newly admitted field, identical semantic values
survive profile → generated manifest → pack → runtime policy, with zero silent
drops and zero accepted unsupported combinations. Procedure: parameterized
positive/negative/migration matrix on a frozen commit; machine-readable result
contains input/effective option hashes, not credentials. This measures projection
correctness only, not throughput or live connector maturity.

## Test, documentation and rollout plan

Unit tests cover strict types, defaults and invalid combinations. Contract tests
cover every admitted field, immutable fingerprints, old-policy bytes and rejection
by unsupported readers. Integration tests use synthetic MSSQL/ClickHouse rows
only under separately approved disposable environments; live checks remain
SKIP until authorized. Include empty-scope, null/duplicate keys, schema mismatch,
timeout, replay and no-false-success tests at the owning layers. Performance
checks are N/A unless tuning behavior changes; then measure, never infer speed.

Documentation: add a bounded-options table, one schema-valid source model,
policy and deployment connection example, effective-config output, architecture
links and runbook. Keep a clean local recipe for offline admission/projection
separate from the credentialed runtime recipe. Use `uv sync --frozen`, then the
retained no-I/O probes and focused tests for first offline success. Real dbt parse
needs the pinned dbt extra; transfer execution additionally needs route authority,
approved services, bindings and native tools. This stage does not supply them.

Migration: upgrade compiler/provider/runtime readers together; opt into the new
policy; recompile to a new immutable output root. Retain old releases intact for
rollback. Never change an old pack schema label, lock, profile, checkpoint or
evidence by hand. Roll back by selecting an already compatible prior release,
subject to the current publication/recovery authority.

## Ownership and open approval items

Coordinator owns schema/version/registry/factory/shared fixtures/changelog and
assigns future writers disjoint paths. Stage 01 owns this draft only. Stages for
types/artifacts, publication/recovery, physical/resources and diagnostics receive
the interfaces in `baseline.md`. External composition PRs 42/43 and DDA PR48
remain reservations. No writer may infer an assignment from this table.

Before approval: freeze exact allowlist and validation messages, route proof
requirements, version number, connection precedence decision, target partition
completeness interface, new identity binding if any, and conflict-safe paths.
Maintainer approval has not been requested for an incomplete implementation.
