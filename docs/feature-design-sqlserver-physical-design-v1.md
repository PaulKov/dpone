# Feature design: bounded SQL Server physical-design admission

Purpose: define the smallest safe extension for data engineers who need
declarative SQL Server storage from native and dbt execution. Start with the
[roadmap](sqlserver-snapshot-roadmap.md); the
[research protocol](sqlserver-snapshot-research.md) defines the experiments.

- Status: RESEARCHED, pending maintainer decisions below; not APPROVED.
- Owner: maintainers; issue: none; target release: TBD.
- Last verified: 2026-09-09.
- Baseline: `6533c27fbf77c78a00b8013bcf72e2293e281e1f`.

## Executive summary and journey

Native dpone already supports rowstore compression and clustered columnstore
(CCI). The dbt producer deliberately accepts a smaller physical surface. Extend
that surface only after catalog truthfulness, publication safety and resource
accounting are established. This is not a general index-management framework.

| Persona | Need | First-success and recovery signal |
|---|---|---|
| Data engineer | Select one supported table layout without writing hooks | Plan names effective storage and exact producer; completed run proves observed design |
| Platform engineer | Bound DDL authority, resource use and permissions | Preflight reports supported operation or a specific blocker before mutation |
| Operator | Retry a failed build without damaging the old target | Receipt distinguishes precommit failure, unknown commit and cleanup pending |
| Maintainer | Extend native/dbt consistently | Shared semantic contract tests and separate execution-authority tests |

Journey: discover native-versus-dbt matrix → check server/version/edition/types
and permissions → select explicit layout → inspect plan and required capacity →
execute in isolated staging → verify data and observed design → publish → inspect
receipt → use exact recovery inventory when interrupted → migrate only through
an approved physical-change plan. The first tutorial must use `sample.events`,
synthetic data, no secrets, and no implicit live environment.

## Current capability matrix

References are repository-relative at the baseline commit; line numbers describe
that baseline only.

| Capability | Native authoring and execution | dbt producer admission | Proposed sequence |
|---|---|---|---|
| Rowstore NONE/ROW/PAGE | Implemented enum and validation, `src/dpone/contracts/mssql_physical_design.py:16`, `:73`; emitted by `src/dpone/runtime/sinks/mssql_table_ddl.py:48` | Requires rowstore (`as_columnstore is False`), but no matching canonical ROW/PAGE declaration | Preserve native behavior; add governed dbt ROW/PAGE bridge after ordinary CCI admission |
| Clustered columnstore | Separate Boolean; incompatible with ROW/PAGE and current clustered PK, contract `:117` | Rejected by `src/dpone/contracts/dbt_sqlserver_graph_policy.py:160` | First useful dbt extension: ordinary CCI for table creation/full replacement only |
| COLUMNSTORE compression | Implicit in native CCI; `compression=NONE` means no rowstore compression, not uncompressed CCI | Not currently admitted | Normalize to a typed internal storage family without breaking legacy serialization |
| COLUMNSTORE_ARCHIVE | Absent from enum | Not admitted | Deferred optional archival mode, after observation and benchmark evidence |
| Nonclustered columnstore | No general declaration | Nonempty indexes rejected, graph policy `:287` | Deferred; distinct index shape and rowstore coexistence contract |
| Clustered primary key | Native supported; mutually exclusive with native CCI | Model-level constraints rejected | Preserve native; do not claim dbt unique_key enforces physical uniqueness |
| Nonclustered/unique/covering/filtered indexes | General authoring absent; specialized preservation is a different capability | Nonempty indexes rejected | Deferred independently; unique constraints require duplicate-data gates |
| Column NOT NULL | Column/type contract | Admitted by `src/dpone/contracts/dbt_sqlserver_graph_rules.py:315` | Preserve; not evidence of general model-constraint support |
| FK/CHECK/model constraints | No new generic management proposed | Model constraints rejected, graph rules `:315` | Deferred; dependency and validation semantics need own spec |
| Filegroup/fillfactor | Bounded native fields exist, contract `:73` | No new admission proposed | Native unchanged; dbt N/A in minimum scope |
| Partitions and per-partition compression | Native declarations explicitly rejected, contract `:117` | Not admitted | Observe all actual partitions now; mutation deferred |
| Online/resumable/ordered CCI, automatic tuning | No new support asserted | No new admission proposed | Deferred; version/edition-specific experiments first |

Native creation is real execution: `mssql_table_ddl.py:48–124` creates the table
and then PK/CCI; `src/dpone/runtime/sinks/strategies/mssql/mssql_strategy_base.py:110–175`
executes the statements before subsequent load into that target/shadow. Existing
business-table full refresh has a separate transaction-preserving path; a new
CCI option must not silently convert that path into drop/recreate.

`src/dpone/runtime/sinks/mssql_backfill_publication_catalog.py:112–150` can
preserve some existing secondary indexes. Preservation does not establish
support for authoring arbitrary new indexes.

## Public contract and staged scope

All names below describe proposals, not accepted manifest examples.

**P1 observation correction.** Catalog observation must preserve storage kind,
actual compression, index identity and partition identity, or return explicit
unsupported/unknown status. It must not normalize archival or unknown catalog
values into a matching NONE state. Desired-state authoring stays unchanged.
An unrepresentable actual layout blocks automated reconciliation and identifies
the unsupported dimension. This is separate from adding archive DDL.

**P2a dbt ordinary CCI.** Proposed opt-in semantic selection is the existing
canonical `clustered_columnstore=true` with rowstore `compression=NONE`, mapped
to the pinned adapter's `as_columnstore=true`. Native fields retain their current
meaning. Admission is restricted to the reviewed table materialization, rename
refresh method, empty user indexes/hooks, no model constraints, and supported
resolved types. Legacy projects remain on the existing false/default policy;
there is no automatic switch to adapter defaults. Initial creation and table
full replacement each need an admitted publication path. Incremental CCI models
remain blocked in this first increment, even on their initial creation branch.

**P2b dbt ROW/PAGE.** Map the same canonical storage decision into governed
materialization behavior; build or rebuild only an owned intermediate relation
before publication. The exact dbt authoring envelope and packaged macro mapping
are approval decisions, not settled schema keys. Until a supported pinned macro
can be proved to implement this operation, this increment remains unavailable.
Do not label a runtime post-run ALTER as successful dbt materialization.

**P3 optional extensions.** Separate specs for CCI plus nonclustered uniqueness,
NCCI, archive and partition-level policy. A future storage union should model
rowstore compression NONE/ROW/PAGE separately from columnstore compression
COLUMNSTORE/COLUMNSTORE_ARCHIVE, and explicit partition selectors must be
nonoverlapping and complete or have an explicit default. Reject mixed or unknown
actual partitions in earlier versions. Do not change partition schemes,
filegroups or compression by heuristic.

CLI/Python: reuse existing planning/execution entrypoints and typed contracts;
no new command is needed for P1/P2a. Proposed plan/evidence fields include desired
and observed design digests, producer authority, supported operation, capability
facts and blockers. JSON stdout remains machine-readable, diagnostics remain on
stderr, existing exit codes stay unchanged. A blocked design returns the existing
nonzero failure boundary; no success receipt is emitted. Any new error code must
be added through the existing error registry, not invented as current behavior.
UTF-8 evidence files use existing atomic-write facilities; same identity/content
is replayable, conflicting content is rejected. No vendor SDK on base import or
help paths.

## Detailed algorithm and state transitions

1. Parse/normalize existing canonical physical fields. Bind manifest, selected
   model, target, schema contract and producer version. Reject unknown keys and
   conflicting legacy/new representations before I/O.
2. Resolve actual column types and engine version, edition, compatibility level,
   catalog visibility, required DDL privilege and operation support. Unknown
   mandatory facts block. Creation permission and ALTER permission are distinct;
   do not demand server-administrator access.
3. Compute design digest from normalized semantic fields. Bind it to existing
   run/attempt identity and verified macro authority. Do not introduce another
   wire identity scheme.
4. Acquire existing publication/physical authority and re-read target/catalog
   identity. Reuse database-level dbt serialization for absent relations:
   `src/dpone/adapters/dbt_workspace_mssql_physical_authority.py:13–20` documents
   why a weaker relation-name lock is insufficient.
5. Persist exact intermediate ownership and capacity admission using the
   [snapshot safety protocol](feature-design-snapshot-resource-safety-v1.md).
   Create only in the permitted namespace. Do not drop unmanaged objects to
   make the plan fit.
6. Build and load under the selected authority. Native retains its supported
   strategy; dbt retains its verified materialization branch. For after-load
   index construction, establish physical layout before externally visible
   publication or prove the adapter transaction supplies equivalent isolation.
7. Introspect the resulting object and compare actual design, types, row/data
   checks and budgets with the plan. Any drift/failure blocks publication.
8. Revalidate authority and target identity, publish with the admitted primitive,
   persist/read back commit evidence, then advance state. Physical/data evidence
   must never claim success before its operation is known complete.
9. Clean only exact owned intermediates after writer quiescence. Preserve old
   target/unknown-commit artifacts according to recovery policy. A failed
   cleanup does not erase a committed outcome.

```text
VALIDATED -> CAPABILITY_CHECKED -> LOCKED -> BUILDING -> DESIGN_VERIFIED
          -> PUBLISHING -> COMMITTED -> CLEANUP_PENDING -> COMPLETE
before commit failure -> FAILED_PRECOMMIT -> exact owned cleanup
lost acknowledgement -> COMMIT_UNKNOWN -> identity/receipt reconciliation
```

Empty input still creates or validates the required empty layout and follows the
normal publish contract. Nulls are checked against admitted NOT NULL constraints;
unsupported types fail before creating the destination. Timeout/cancel stops
writers and follows precommit cleanup, unless commit was sent and its outcome is
unknown. Same run/design retry reuses only verified owned state; different digest
or stale lease conflicts. A retry must not repeat an ambiguous rename/exchange.
Schema drift after preflight forces replan rather than coercion.

Steady incremental behavior is deferred for new physical options. Its proposed
contract is observe-before-DML, no-op for matching design, block on drift; never
rebuild every run. Initial/full branches, unique-key versus physical uniqueness,
concurrent mutations and transactional index reconciliation require a separate
acceptance matrix before admission.

## Architecture and authority

| Component | Reuse / narrow change | Responsibility |
|---|---|---|
| `dpone.contracts.mssql_physical_design` | Existing | Canonical semantic validation and legacy mapping |
| `dpone.runtime.sinks.mssql_physical_introspection` | Focused correction | Faithful observation and explicit unsupported/unknown result |
| `dpone.readiness.physical_reconciliation` and approval policy | Existing | Drift classification; safe-window migration, expiry and target binding |
| Native table DDL and transaction finalizer | Existing | Native realization and commit evidence |
| dbt graph/macro authority contracts | Bounded extension | Admit exact config/materialization/body/dispatch dependencies |
| dbt workspace physical guard | Existing | Database-scoped fencing and catalog observation |
| Runtime/dbt composition roots | Existing | Inject catalog, execution, clock and evidence dependencies |

Adapters/services must not import runtime renderers; see
[import rules](import-rules.md). Shared semantic validation is sufficient for
P2a: the pinned dbt macro renders its own DDL. Only if P2b demonstrates two real
consumers should a pure renderer move to a justified canonical shared layer;
no generic physical-design plugin system or legacy-facade policy is proposed.

Baseline macro authority is dbt-core 1.12.3, dbt-sqlserver 1.11.1, manifest v12
(`src/dpone/contracts/dbt_sqlserver_macro_authority_baseline.py:8–13`). Verification
covers macro bodies/dependencies/dispatch
(`src/dpone/contracts/dbt_sqlserver_macro_authority.py:68`, `:269`). Any packaged
macro extension needs producer-generated baseline evidence, exact version pins,
negative mutation tests and review of pre/post-transaction behavior. User hooks,
monkeypatching, validators disabled for compatibility, and hand-edited passing
hashes are prohibited. Existing pins are unchanged by this plan.

## Alternatives, migration and risks

| Alternative | Benefit | Risk / decision |
|---|---|---|
| Enable adapter defaults globally | Small change | Implicit layout change and unreviewed branches; reject |
| Allow arbitrary hooks | Flexible | Unbounded DDL authority; reject |
| Reuse canonical semantics with pinned macros | Smallest useful CCI admission | Requires branch-specific proof; recommended P2a |
| New universal DDL service | Broad extensibility | Premature abstraction and dependency violations; reject |
| Archive first | Potential storage saving | CPU/rebuild/query cost and missing representation; defer |
| Rebuild existing business target automatically | Convenient drift convergence | Locking, log, dependency and rollback risk; reject |

Keep old manifests and existing native layout unchanged. Old `NONE + CCI` maps
to ordinary columnstore internally without reinterpreting NONE for rowstore.
Observation correction can newly block an externally unsupported layout; explain
this safety correction and remediation in release notes, never silently mutate
it to match the old incomplete observation. No deprecation is required for P1 or
P2a; a future serialized union needs a versioned schema and a documented reader
compatibility window. Downgrade refuses unknown new fields; operators remove
unsupported selections only after explicit migration planning.

An ADR is required for dbt/native physical authority and any new public storage
representation. Reference [quality budgets](benchmarks/quality_budgets.yml) rather
than duplicating thresholds. Expected changes are small contract/adapter modules;
measure actual SLOC and graph impact at implementation review. No unrelated
module decomposition is in scope.

## Validation, diagnostics, docs and rollout

| Stage | Tests and binary acceptance | Rollout / rollback |
|---|---|---|
| P1 observation | Synthetic NONE/ROW/PAGE/CCI/archive/unknown/mixed partitions; unknown never compares equal by normalization; no DDL from blocked state | Safety fix plus runbook; rollback must retain block, not restore false observation |
| P2a CCI | Old graph cases unchanged; admitted table cases; forbidden hooks/indexes/macros fail; schema/type/version/edition/permission negatives; empty/null/Unicode; exact design before publish; retry/cancel/concurrency | Opt-in; disable new admission on regression while preserving native behavior and retained evidence |
| P2b ROW/PAGE | Same semantic cases through native and dbt; controlled intermediate rebuild; safe-window/expired approval tests; before/after failure injection | Separate release after macro authority review; no automatic physical downgrade |
| P3 advanced | Per-index/partition introspection, migration, benchmark correctness and maintenance evidence | Separate specs and independent opt-ins; no default archive |

Offline tests prove contracts and generated SQL, not engine support. Live tests
must bind exact approved server/edition, adapter versions, data types, permissions,
transaction visibility and current commit. Add the benchmark matrix only after
correctness tests pass. Diagnostics must identify layout, unsupported dimension,
operation, observed-versus-desired difference, safe next action and evidence
location without credentials or private identifiers in public fixtures.

Documentation per implementation PR: native-versus-dbt matrix, first-success
synthetic tutorial, config/API reference, architecture/ADR, capacity and recovery
runbook, migration notes, generated references and CHANGELOG where behavior
changes. Link from [physical design](physical-design.md), [dbt](dbt.md), and the
[MSSQL route](source-sink/mssql-to-clickhouse.md). Do not expand unrelated docs.

Market comparison and measurable differentiation: use the
[roadmap comparison](sqlserver-snapshot-roadmap.md#market-comparison) and
[benchmark protocol](sqlserver-snapshot-research.md#decisions-and-measurable-hypotheses).
No superiority claim or performance guarantee is made.

## Agent execution plan and approval

One integrator owns shared schemas, authority baselines, registries, changelog
and navigation. Future writers use separate worktrees and concrete task contracts.
P1 owns only the introspection module and focused tests; P2a owns graph admission
and its tests; P2b owns only the approved macro bridge and tests. All treat the
other components as read-only. Production release workflows, dependency changes
outside a separately approved authority update, and compact-delivery/wire-v2
implementation are forbidden. The integrator resolves shared semantic changes.

Open decisions: exact opt-in dbt authoring envelope; supported server/edition
matrix; placement of pre-publish physical verification in the pinned materialization;
ROW/PAGE macro authority; evidence compatibility and retained-target horizon.
These must be resolved and the spec marked APPROVED before implementation.

- [x] Baseline capability and intentional limitations separated.
- [x] Algorithm, failure states, architecture and alternatives researched.
- [x] Tests, CJM, evidence and rollout described.
- [ ] Open public mapping and authority decisions resolved by maintainer.
- [ ] Concrete future writer task contracts validated.
- [ ] Maintainer approval recorded.
