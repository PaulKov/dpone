# Industrial bounded-window delivery, first implementation stage

Status: APPROVED — maintainer explicitly approved T0–T5 in this task.
Owner/integrator: DDA maintainer task. Target: separate minor release, version TBD.
Baseline reviewed: 7e6091386983cf3cdef08a97b43a315fc5e54631.
Last verified: 2026-09-14.

This document contains generic design only. It contains no customer connection,
relation, schema, workload size, measurement, evidence digest or configuration.

## Outcome and journey

A data engineer authors a fixed UTC interval and receives an atomic replacement
of precisely that target interval. A failed or ambiguous load does not silently
advance progress. An operator can resume completed verified staging without the
source. A platform engineer supplies actual ownership and schema-exclusion
authorities and can explain each permission and resource limit.

Journey: inspect a synthetic manifest with the existing plan command; resolve
dependencies, source semantics, guards and budgets; construct the documented
Python runtime; execute one window; inspect publication/evidence/checkpoint
states; recover using the same persisted invocation; retain uncertain outcomes
for investigation; upgrade only after old invocations have settled.

Industrial quality means independently verified integrity, bounded resource use,
transactional visibility and recoverability. It does not mean an unmeasured claim
to be the fastest product or that an isolated driver probe certifies dpone.

## Scope and release boundaries

First minor: opt-in raw single-query source admission for local MergeTree,
ReplicatedMergeTree, ReplacingMergeTree and ReplicatedReplacingMergeTree in an
Atomic database, with exact source-policy identity and a runnable synthetic
composition example. Preserve the existing native BCP transport, scalar type
matrix, stage_complete meaning and one-window transaction finalizer.

No Distributed tables, FINAL, engine-level deduplication, custom SQL, arbitrary
engine allowlists, source DDL, automatic replica failover, source-side snapshots,
independent persistent capture subsystem, automatic watermark inference, or
multi-window atomicity. No dbt work or new automatic composition feature. No default backend swap.

Separate later minor contracts: protected staging verification reduction;
aligned native SWITCH; deterministic weighted days; optional typed bulk backend.
The binary staging experiment is not selected for production by this document.

## Public contract

### Manifest and API

Proposed closed opt-in mapping:

```yaml
source:
  options:
    native_transfer:
      source_read:
        mode: raw_single_query
```

All existing typed_binary/mssql_native/bounded_stream settings and explicit
cumulative capacities remain required. Absent source_read preserves strict
plain-MergeTree admission and legacy identity byte-for-byte. Null, empty mapping,
unknown mode and extra keys fail before row I/O; there is no silent fallback.

One new immutable policy value belongs in dpone.contracts. The existing manifest
normalization supplies it to ClickHouseNativeSource; do not add an independent
constructor switch which can disagree with recovered configuration. Proposed
errors: mssql_native.source_read_invalid, mssql_native.source_engine_unsupported,
mssql_native.source_policy_mismatch. Existing diagnostics remain for legacy mode.

Explicit configured window column and UTC [start,end) remain authoritative. The
framework does not select latest available data or wall-clock windows. Ingestion
time and event time are documented choices; neither is change-data capture.

### Plan, execution and recovery

Reuse `dpone plan ... --format json` and current runtime entry
`NativeMssqlRuntime.run(load_config, owner=...)`. No new --execute/--resume flags.
For explicit mode only, add `mssql_native.source_read_mode: raw_single_query`
to the existing plan JSON. Omit the key for legacy mode. Preserve:

```json
{
  "mssql_native": {
    "status": "composition_required",
    "live_preflight": "not_run",
    "source_read_mode": "raw_single_query",
    "source_query_count": 1,
    "transport": "bounded_native_files"
  },
  "native_transfer_route_decision": {
    "certification_status": "unverified",
    "release_gate": "composition_required"
  }
}
```

This excerpt omits unchanged fields; it is not a replacement payload schema.
Keep existing stdout/stderr and exit statuses. Plan success remains exit 0 for
valid offline planning, not proof of live admission. Invalid authored source_read
uses source_read_invalid at configuration validation; a disallowed live engine
uses source_engine_unsupported. A bound plan/config mismatch uses
source_policy_mismatch before resume; persisted journal mismatch uses the existing
journal_identity_changed. The existing configuration exception translation must
retain CLI diagnostics and exit behavior. Test JSON, text and Markdown output.
Do not populate the deliberately empty generic native-transfer plan objects. Existing ordinary native CLI remains composition_required until
a separately approved automatic composition exists. A complete synthetic example
must demonstrate real construction, not label no-op callbacks as protection.

The source policy is bound to durable invocation authority as follows:

- `native_source_read_mode(config)` in `manifest/mssql_native_policy.py` returns
  None for absence and raw_single_query for the sole admitted explicit mapping.
  Preserve the authored mapping through manifest compilation.
- `NativeChunkPlan` gains keyword-only `source_read_mode: str | None = None`.
  Its canonical `to_dict()` emits the original six fields in their original
  order and emits the seventh field only when non-None. Receipts are unchanged.
- Replace durable plan-specific `asdict(plan)` use in the chunk journal, native
  importer (stage names, consumed-part/owner bindings) and prepared-owner digest.
  Preserve insertion order because existing hashes include repr(dict).
- Retain the existing `mssql-native-chunks-v1/<digest(target_id,run_id)>` lookup
  namespace so changed policy cannot look like an absent journal. Absent mode
  emits byte-identical version-1 records. Explicit raw mode emits version-2
  records with the additional identity field. Readers accept only these closed
  version/mode pairs and exact requested identity; never upgrade stored records.
- Changed/removed mode for an existing invocation fails with
  `mssql_native.journal_identity_changed`. Unsupported versions fail closed.
- `NativeMssqlRuntime.run` compares parsed mode with `context.plan.source_read_mode`
  after target binding and before `service.resume`, hence before any source I/O.
- `invocation_route_fingerprint` already includes native_transfer options. Verify
  that source_read survives compilation and changes this fingerprint; do not
  change transaction receipt or completed-payload/prepared-snapshot schemas.

Existing source relation UUID and query ID retain their meaning. The factory must
use one physical source session without automatic replica failover. This stage
adds no source-snapshot token or cross-replica freshness claim. Engine admission
is checked on that session before the query; recovering verified completed stages
requires their authority, not a new inspection of the old source engine.

## Algorithm

1. Parse the closed policy and existing resource settings. Freeze authored UTC
   bounds from the execution interval. Resolve connection identities without
   recording credentials. Acquire the existing target lease/fence.
2. Construct target-only bindings and attempt recovery before opening a source.
   For publication intent or an uncertain commit, probe the exact transactional
   target receipt. Never use absence of a network reply as permission to replay.
3. For a new extraction, enter the operational source DDL exclusion guard before
   catalog reads. Admit Atomic, a supported local engine, nonzero relation UUID,
   ordinary unique columns and the existing supported scalar matrix. Use one
   selected physical session; do not imply synchronization with other replicas.
4. Run one native-driver SELECT with one query ID for the whole window. Do not
   pre-count business rows, use OFFSET, query individual days or append FINAL.
   For explicit raw mode set query-level `final=0`, even when the session/user
   inherits final=1. Reject the run if server settings constraints prevent this
   override; never silently accept engine deduplication. Preserve legacy query
   settings unchanged. Disable query cache and partial-results behavior. Enforce query and client
   resource budgets. Raw semantics preserve the multiplicity returned by that
   query; background merging makes a later query a different observation.
5. Restore typed values and feed existing bounded chunks. Check row allocation
   before encoding, seal each file, reserve aggregate capacity and import through
   independent owned BCP sessions. Keep sealed bytes until the current importer
   outcome is settled. Retry only classified transient import failures, within
   the existing configured attempt budget; never rerun an arbitrary SELECT as a
   chunk retry. Keep independent verification boundaries unchanged.
6. Declare stage_complete only after source EOF, contiguous chunk receipts,
   successful imports and verification. This is not a capture-first file seal.
   Existing protected ownership and fresh verification requirements still apply;
   a digest or cooperative application lock alone is not SQL immutability.
7. Run prepublication quality. Enter the existing SQL finalizer transaction with
   SERIALIZABLE/XACT_ABORT, canonical mutation locks, renewed ownership and target
   identity admission. Atomically delete only the authored target interval,
   insert prepared rows and write the exact publication receipt on the same SQL
   connection/transaction. Empty source still clears that interval. Preserve
   outside-window and NULL-window target rows. Do not detach old data early.
8. After confirmed commit, write durable evidence, then checkpoint/CAS, then
   success. Cleanup only objects whose owned identity and writer settlement are
   proven. Receipt reconciliation precedes cleanup after an unknown outcome.

```text
validate_policy_and_bounds()
lease = acquire_target_fence()
binding = construct_target_only_bindings()
outcome = recover_using_receipt_and_existing_journal(binding)
if outcome.requires_new_extraction:
    with real_source_schema_exclusion():
        admit_catalog_and_bind_source_policy()
        stream_one_query_through_existing_bounded_chunks()
        require_eof_and_verified_contiguous_stage_complete()
if outcome.needs_publication:
    verify_quality()
    transactionally_replace_window_and_record_receipt()
persist_evidence()
advance_checkpoint_with_fence()
cleanup_only_proven_owned_objects()
```

## State and failure semantics

| Boundary | Required behavior |
| --- | --- |
| Partial extraction | No source-free resume. Settle importers; controlled cleanup; a new invocation re-extracts the entire window. |
| Sealed chunk, failed import | Replay those exact bytes only after prior writer is settled. |
| stage_complete | Source-free restart with fresh stage/ownership validation. |
| Schema/guard loss before completion | Abort; no complete-stage authority or checkpoint. |
| Target mutation failure before commit | Transaction rollback preserves old target image. |
| Lost commit acknowledgement | Receipt-first reconciliation; unavailable/mismatching authority blocks replay and cleanup. |
| Commit succeeded, evidence failed | Finish evidence/checkpoint; do not publish again. |
| Checkpoint CAS/lease failure | Do not report success or override newer ownership. |
| Duplicate source rows | Preserve multiplicity, including across chunks. |
| Invalid UTF-8, unsupported type or overflow | Fail explicitly; no coercion, truncation or lossy conversion. |

Concurrency defaults remain unchanged. Encoding/import parallelism share the
existing aggregate retained-work budget; staged SQL allocation has its existing
explicit stop threshold. Retained staging from other invocations consumes real
capacity. Read planning must not raise user budgets or change database recovery,
files, indexes or server settings to obtain a benchmark result.

## Architecture and compatibility

Reuse ClickHouseNativeSource, NativeQueryArtifact, NativeChunkImporter,
compose_native_stage_context, MssqlNativeStagedLoadService,
MssqlGenericTransactionFinalizer, WindowStore and NativeMssqlRuntime.

Add one cohesive source policy and normalization/admission logic in canonical
contracts/manifest/runtime packages. Infrastructure authority implementations
remain injected at composition roots. No legacy namespace policy or universal
plugin registry. The same policy covers ordinary and replicated local engines;
it does not attempt arbitrary connector snapshots.

An ADR extending ADR 0057 is required. Version selection and final shared-file
integration happen against refreshed protected master; preserve concurrent work.
No source read-policy default change, old-journal reinterpretation, relaxed type
matrix or SQL receipt relocation. Rollback settles opt-in invocations first;
older releases must reject newer authority rather than misread it.

## Later algorithms, separately admitted

Protected staging: verify under actual SQL writer exclusion through consumption;
merge readbacks only after proving the transferred content authority and failure
semantics. Do not treat session-lock liveness as content stability.

SWITCH: stage all required rows first, admit matching schema/index/partition and
filegroup constraints, acquire locks and switch old-out/new-in plus receipt in
one transaction. Initial release stays with predicate DML until that contract
and live lock/recovery proof exist.

Weighted days: freeze every authored day, including empty days. Use finite
nonnegative byte estimates only for descending-weight scheduling with stable
date ties. Bound global resources; each day has its own identity/receipt. A
partially committed cycle must never be labelled atomic for the whole window.
Independent queries do not share a snapshot; this must be explicit.

Direct bulk: use typed streaming only after all scalar and failure cases pass.
Compare end-to-end verified publication against native BCP on identical inputs.
Do not adopt a transport because of a small insertion-only microbenchmark.

## Market patterns and measurable objective

Official documentation checked 2026-09-14; documentation versions are listed
where available. None of these products has been benchmarked here.

| System | Relevant documented pattern | Adopt / limit |
| --- | --- | --- |
| Microsoft SSIS, SQL Server docs | Fast-load controls batch commits, table locking, NULL preservation and constraints. | Adopt explicit batch/fidelity controls; not a claim that TABLOCK guarantees minimal logging. |
| dlt, current full-loading docs | insert-from-staging performs replacement in one transaction; truncate-and-insert exposes incomplete data. | Adopt prepared atomic replacement; reject early truncation for this contract. |
| Fivetran HVR 6 | Bulk refresh streams to bulk interfaces. | Adopt bounded streaming; product refresh semantics do not prove dpone transaction guarantees. |
| Informatica PowerExchange 10.5.9 | SQL Server bulk utility exposes connection/load controls. | Adopt explicit backend capabilities; do not copy historical no-logging assumptions. |
| Airbyte, current protocol | State acknowledgements and separate source/destination flow; socket mode is documented. | Adopt acknowledged progress; not evidence of atomic SQL window replacement. |
| Apache Beam, current guide | External side effects require idempotent processing under retries. | Adopt replay-safe ownership/receipts; Beam runner guarantees are not SQL transaction receipts. |
| Pentaho 10.2 | General transformation/bulk components. | N/A for exact native ClickHouse window publication guarantee; no equivalence asserted. |
| gusty / Astronomer Cosmos | DAG and orchestration integration. | N/A to row transport and SQL publication. |

Sources:
- https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination?view=sql-server-ver17
- https://learn.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlbulkcopy.enablestreaming?view=sqlclient-dotnet-core-6.1
- https://learn.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import?view=sql-server-ver17
- https://dlthub.com/docs/general-usage/full-loading
- https://fivetran.com/docs/hvr6/getting-started/concepts/refresh
- https://docs.informatica.com/data-integration/powerexchange-for-cdc-and-mainframe/10-5-9/bulk-data-movement-guide/microsoft-sql-server-bulk-data-movement/using-the-sql-server-bulk-load-utility-to-load-bulk-data/pwx-mssqlserver-connection-attributes-for-microsoft-sql-server-b.html
- https://github.com/airbytehq/airbyte/blob/master/docs/platform/understanding-airbyte/airbyte-protocol.md
- https://beam.apache.org/documentation/programming-guide/
- https://docs.pentaho.com/pdia-data-integration/10.2-data-integration/pdi-transformation-steps-reference-overview
- https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree

Objective: exact typed multiset fidelity and one atomic publication per invocation
under all required failure cases. Performance comparison uses the existing
synthetic native BCP route, balanced order, at least three measured trials per
variant and medians. A later selected performance optimization (not this source-admission stage) targets at least 15% improvement in
one declared visibility scenario with no >5% regression in the others. This is
an acceptance target, not a measured result; capacity and correctness gates take
precedence. Do not derive p95 or endurance claims from three trials.

## Validation and documentation DOD

T0 mandatory frozen vectors: journal bytes, stage names, consumed-part bindings,
prepared-owner digests and transaction route fingerprints. Both cross-mode
changes must fail before source I/O, mutation, checkpoint or destructive cleanup.
Reject Boolean/unknown versions, v1/raw, v2/absent, extra identity keys and malformed
mode mappings without rewriting stored records. Journal constructor truthiness
checks must use the omission-preserving canonical plan representation. Poison
source access in completed-stage recovery tests. New raw-mode source tests must
inherit final=1, verify query-level final=0 and exercise an override denied by the
server. Private workload outcomes are not evidence for these generic assertions.

Unit: frozen policy validation and legacy golden identities; duplicate/empty/NULL
and byte-budget boundaries. Contract: every engine/mode combination, all ordinary
types, guarded catalog/one-query lifecycle, deterministic invocation binding.

Live synthetic: narrow, wide100 and adversarial values (supplementary Unicode,
NUL, empty strings, trailing spaces, Decimal extrema, signed zero/subnormal floats,
microseconds, duplicate rows), independently encoded full-value multiset oracle.
Run real source mutations/DDL attempts, process restarts, staging tampering, lost
commit acknowledgement, evidence/checkpoint failure and empty-window clearing.
Explicit SKIP/UNVERIFIED where unavailable; mocked tests do not certify a route.

Private operational experiments cannot populate public certification reports.
Only synthetic fixtures, universal code and generic documentation may be public.

Docs DOD: synthetic manifest, executable Python construction and recovery example,
guard deployment requirements, source semantics, exact unsupported combinations,
capacity tuning and ownership cleanup runbook. Preserve plan/execution distinction.
Include expected console/results/artifacts; exact source-policy identity/error
reference; updated data-flow/state diagrams; navigation/backlinks and example,
schema/help drift checks. Demonstrate same-invocation completed-stage recovery
and a separate new invocation after incomplete extraction. Update canonical
schema/reference/examples and relevant ADRs with the code.

Run focused tests, the change-aware check selector, applicable Ruff/mypy/import/
layer/module-size/full unit gates, docs checks/language tests/strict MkDocs, then
independent review and explicitly approved synthetic live certification. Existing
quality-budget debt must not grow. Release remains a separate controller action.

## Task decomposition

| Task | Owner / paths | DOD / dependencies |
| --- | --- | --- |
| T0 policy and identity | Integrator: shared manifest/schema/journal identity and ADR | Reviewed exact versioning; legacy golden parity; all new mismatch paths fail closed. Approval prerequisite for T1–T4. |
| T1 source admission | Source worker: sources/clickhouse_native_source.py and new source-policy module/tests | Supported mode/engine matrix, one query, guard loss/type drift/cancellation tests; depends on T0. |
| T2 fidelity oracle | Test worker: new synthetic route scalar test module and oracle helper | Independent exact representations including float signed zero and microseconds; no changes to existing profile results; can develop after T0 in separate worktree. |
| T3 recovery proof | Test worker: new opt-in source-policy recovery tests | Source-free completed-stage resume, legacy parity, policy mismatch, receipt-first unknown outcome; depends on T0/T1. |
| T4 synthetic composition and docs | Docs/integration worker: new synthetic example and how-to | Actual authority adapters, runnable first load/recovery and honest plan state; depends on T1/T3. |
| T5 certification/integration | Integrator and independent reviewer | Clean exact commit, applicable checks, synthetic live evidence, no private data leakage; depends on all prior tasks. |
| T6 optional bulk backend | Future separate minor/design | Full fidelity/settlement/replay plus matched end-to-end benefit; not authorized by approving T0–T5. |
| T7 SWITCH and weighted days | Future separate contracts/releases | Aligned publication and explicit per-day/cycle semantics; no implicit activation. |

Parallel writers require separate worktrees and individual approved path contracts.
Only the integrator owns schemas, registries, shared fixtures, changelog, project
metadata, workflows and evidence indexes. Each patch/minor is independently
reviewable and revertible. No implementation-ready status until the identity
review is complete and the maintainer approves the concrete first-stage spec.

## Review status

Architecture: APPROVE after explicit final=0 correction. Test design: APPROVE.
UX: PASS after exact plan/error and documentation DOD corrections. Six task
contracts: validator PASS. Implementation/build/live certification: UNVERIFIED;
implementation proceeds under the approved specification. Maintainer subsequently approved T0–T5. Reviewer approval is not publication authority.

Maintainer approval: explicit confirmation after review of this specification; T0–T5 authorized. Later backend/SWITCH/day-scheduling contracts remain separate.
