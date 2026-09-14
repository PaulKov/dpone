# DDA SQL layout and publication research

Purpose: help platform owners and maintainers select the next SQL Server work
without confusing a researched design with an available runtime capability.
Start with [native transport](../mssql-native-transport.md); the next implementation
decision belongs to the [delivery roadmap](../delivery-acceleration/next-stages.md).

- Status: **RESEARCHED**. Neither design is APPROVED or implemented.
- Checked: 2026-09-14, dpone 0.80.0 source
  `6ae541d38ac223327d7edb23510859df91173bda`.
- Owner: DDA P04; integrator/shared-file owner:
  `01a08a6d-20f9-7900-9eef-ea42ecc0b124`.
- Release class: research documentation now; **two independent future minors**.
- Live execution owner: P01 only. SQL correctness, performance and production
  acceptance of these proposed features remain UNVERIFIED.

## Two independent decisions

| Design | User outcome | Initial scope | Decision before implementation |
|---|---|---|---|
| P04-L: loading profiles | Explicit loading policy matched to an existing target layout | Heap, supported rowstore, conservative CCI; current full-refresh/predicate publication | Select using measured SQL bottleneck and resource/read-workload evidence |
| P04-S: SWITCH activation | Atomic replacement of one authored window while preserving target identity | Aligned heap/rowstore, one finite RANGE RIGHT partition | Close resource ownership, transaction bridge, integrity and receipt gaps |

Neither design changes existing CLI, manifest, Python, state or evidence behavior
in this research PR. No production source, runtime finalizer or live SQL was
changed. P04-S does not depend on enabling P04-L. CCI SWITCH, full-refresh SWITCH,
CHECK admission and automatic physical conversion are excluded from initial
activation. [ADR 0062](../adr/0062-isolated-mssql-switch-activation.md) remains the
authority for the existing isolated component and its public activation boundary.

## What the released route actually executes

Native encoding produces bounded files. Each file is imported into an independent
raw heap; BCP can commit batches within that file. Preparation performs one
`INSERT SELECT` over all receipt stages using `UNION ALL`. Publication then
performs one final `INSERT SELECT` from prepared, after TRUNCATE or an authored
window DELETE, inside the existing transaction and receipt boundary.

The native handler currently forces `table_lock=True` for final insertion.
The local factory separately injects default BCP options: batch size 100000 and
table locking enabled. A generic manifest BCP option does not prove that this
factory applied it. The raw chunk row limit, BCP transaction batch, preparation
statement and final ingestion statement are four different boundaries.

The implementation trace is in
[native preparation](../../src/dpone/runtime/sinks/mssql_native_prepare.py),
[publication handler](../../src/dpone/runtime/sinks/mssql_native_staged_load.py),
and [local factory](../../tools/native_delivery_local/factory.py).

## P04-L: explicit loading policy

Proposed Python-only, keyword-only composition selection defaults to `None`,
preserving existing behavior. An explicit profile chooses `heap`, `rowstore` or
`columnstore` and final-insert locking `table` or `engine`. The latter omits the
hint; it does not promise that SQL Server will avoid lock escalation. The target
must already have the matching admitted layout. Raw and prepared stages stay
heaps. BCP options remain separate. No automatic index build, maintenance,
recovery-model change or target conversion is included.

Algorithm: validate the finite profile; inspect current catalog and capacity;
bind the effective policy and target identity to versioned invocation authority;
run existing native preparation and integrity checks; revalidate under the
existing finalizer's transaction protection; apply the selected final INSERT
hint; confirm the exact receipt; persist evidence; advance the fenced checkpoint.
Old records retain their bytes and readers. New policy authority requires an
explicit versioned integration contract before implementation approval.

Heap loading and range-oriented rowstore queries have different costs. Index
maintenance must be included in an end-to-end comparison.
[Microsoft heap guidance](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/heaps-tables-without-clustered-indexes?view=sql-server-ver17),
[index maintenance](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver17).

For CCI, inspect actual statement/partition/worker row distribution and rowgroups;
do not infer compressed loading from native chunk size. Microsoft distinguishes
concurrent direct bulk loading from one parallel staging INSERT SELECT, including
their different table-lock behavior.
[Columnstore loading guidance](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/columnstore-indexes-data-loading-guidance?view=sql-server-ver17).

Logging depends on actual recovery/index/locking conditions. CCI compressed-data
log reduction is not the same claim as rowstore minimal logging. Database backup
and recovery choices remain with the platform owner.
[Minimal logging prerequisites](https://learn.microsoft.com/en-us/sql/relational-databases/import-export/prerequisites-for-minimal-logging-in-bulk-import?view=sql-server-ver17).

On mismatch, fail before mutation with the requested/observed dimension and safe
next action. No automatic fallback. A known settled rollback permits retry of the
same policy after revalidation. An unknown commit retains resources. Downgrade
requires settling new-policy invocations with a supporting recovery reader.

## P04-S: required-only SWITCH

Proposed Python composition selection defaults to `None`. Explicit selection
requires a finite-window rowstore profile and has no automatic fallback. Existing
ordinary YAML/CLI SWITCH rejection remains; old `native_mode=auto` must not begin
using SWITCH through a new registry entry.

Admission retains the [isolated component's](../delivery-acceleration/partition-switch.md)
conservative SQL Server major 16 / editions 2 or 3 profile: full metadata
visibility, same database/function/scheme, one finite RANGE RIGHT interval,
non-nullable date or datetime2(0–6) key, matching ordered column metadata,
supported index shape and corresponding filegroup/compression. Missing catalog
facts reject. All CHECK and other excluded constraints/dependencies remain
unsupported, even when SQL Server could switch a broader layout.

Prepared and switch-out must have durable invocation ownership. The entire
prepared table contains only the authored interval; the entire switch-out table
is empty. Bind database generation, object ID/create time, invocation/generation,
mutation and catalog fingerprints. Names, copied markers and allocation IDs are
not stable target authority. Empty prepared still replaces the complete authored
window. Scope never comes from staging values.

Before implementation, close these gaps:

1. Reserve, provision and recover two aligned owned objects durably, with
   quarantine on ambiguous creation/ownership.
2. Supply a production transaction bridge using the existing finalizer's exact
   session, current fences and receipt authority; no second finalizer.
3. Verify prepared under executor locks against frozen business/framework
   evidence and the known transaction-clock metadata projection. Do not derive
   expected integrity from the rows being checked.
4. Bind publication choice and resource identities into versioned durable
   operation/receipt authority without rewriting existing strict report formats.

Transaction algorithm: resolve exact prior receipt; enter the existing
SERIALIZABLE/XACT_ABORT transaction; apply authoritative clock metadata; acquire
three whole-table locks in object-ID order; re-read catalog, owner and content;
replan and verify; count replaced rows; SWITCH old target out, then prepared in;
validate after-image; insert exact receipt; commit. Only then persist evidence
and advance checkpoint. Whole-table protection also serializes other partitions;
SWITCH needs schema-modification locks.
[Microsoft SWITCH documentation](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-ver17).

Either statement failure requires complete caller rollback. Lost acknowledgement
requires receipt-first recovery through a fresh session. Missing/unavailable or
mismatching receipt after possible commit blocks replay, fallback and cleanup;
empty tables prove nothing about transaction outcome. A confirmed commit is
corrected only by a new audited publication. Choosing predicate fallback is an
explicit new invocation after previous work settles. Retain old rows according
to policy; unknown outcomes override cleanup expiry.

## Evidence, user journey and implementation ownership

The engineer first checks capability and prerequisites, supplies a tested platform
composition, runs a small correctness case, inspects receipt/evidence/checkpoint,
then evaluates representative performance. The operator receives stable rejection
reasons, retained resource identities and exact recovery actions. A future release
must include a complete tested application example, separate profile/activation
reference and recovery runbook. Research examples are not runnable public APIs.

P01 requests distinguish existing heap baseline from external SQL diagnostics and
isolated SWITCH fixtures. The shipped local factory cannot execute SWITCH. Use
the harness's `candidate` adapter for source 0.80.0: its `baseline` adapter is
hard-pinned to an older commit. Threshold diagnostics stay within the 1000000-row
cap. Unsupported BCP hint/platform cells are explicit SKIP.

Required evidence includes exact typed multiset/multiplicity, empty/outside
invariance, stable catalog identity, lost-ACK and unknown-outcome recovery,
bounded reader/writer lock tests, effective BCP and final SQL boundaries, index
cost, log/allocation/tempdb/RSS observations and cleanup disposition. Three trials
are diagnostics; selected changes need at least ten balanced pairs with
uncertainty. Changed layout/method cells cannot claim the existing same-config
comparator's PASS. Production workload/SLA/hardware are still UNVERIFIED.

Research uses current official Microsoft, SSIS and dlt documentation. SSIS's
distinct fast-load batch/commit controls inform observability. dlt's
staging-optimized MSSQL replacement recreates tables, so it does not supply the
stable-target identity required here. No vendor speedup or superiority is claimed.
[SSIS OLE DB destination](https://learn.microsoft.com/en-us/sql/integration-services/data-flow/ole-db-destination?view=sql-server-ver17),
[dlt MSSQL destination](https://dlthub.com/docs/dlt-ecosystem/destinations/mssql).

The separate detailed specifications, ADR drafts, source register, P01 request,
resource/locking recipe, hermetic reference model, exact proposed path ownership
and completion evidence are retained in the task artifact directory:
`/Users/paulkov007/.codex/artifacts/dpone-dda-industrial-plan-20260914/P04/`.
`layout-design.md` and `switch-design.md` are independent approval units;
`completion.json` records their hashes. Integrator owns all shared admission,
finalizer, receipt, recovery, schema and navigation changes. Research completion
does not require approval for a future implementation stage.
