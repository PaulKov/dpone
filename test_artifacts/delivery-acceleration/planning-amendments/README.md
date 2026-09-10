# Delivery acceleration: bounded ownership amendments

Purpose: resolve concrete implementation constraints without changing the approved
delivery algorithms, public contracts, baseline or quality budgets.
Audience: DDA implementation owners and reviewers. Date: 2026-09-10.

## DDA-06 annotation-only dependency scope

The original full supplemental contract is `dda-06-native-annotation-scope.yml`
in this directory. Its successor, `dda-06-generated-metrics-scope.yml`, is the
producer-only DDA-06 contract. Its successor, `dda-06-quality-summary-scope.yml`,
records the completed summary correction and retains every annotation/staging limit and exclusion below,
with the exact manual-summary exception documented at the end of this page.
The original planning commit f368294 remains immutable. Other
task contracts remain in force, including independently recorded narrow
supplements.

A read-only architecture review checked real symbol use, exports, imports from
5,801 Python files and applicable runtime reflection. It identified six
function/constructor-only dependencies in five existing MSSQL helpers:

| Path below src/dpone/runtime | Annotation-only symbol | Contract edge |
|---|---|---|
| etl/mssql_fresh_target_preplan.py | MssqlTransactionAdmission | mssql_transaction_governance |
| etl/mssql_operation_lease.py | MssqlTransactionAdmission | mssql_transaction_governance |
| sinks/mssql_filegroup_authority.py | MssqlPhysicalDesignContract | mssql_physical_design |
| sinks/mssql_target_mutation_plan.py | MssqlTransactionAdmission | mssql_transaction_governance |
| state/mssql_database_authority_support.py | MssqlDatabaseAuthorityPin | mssql_database_authority |
| state/mssql_database_authority_support.py | ResolvedBindingConnection | runtime_connection |

These modules already use postponed annotations. Move only the named imports
under TYPE_CHECKING, following ADR 0058's existing annotation/reflection contract.
Retain raw annotations and canonical-namespace resolution. The permitted reserve
is MssqlCatalogColumn in sinks/mssql.py, used only by a return annotation; that
path permits no sink behavior change.

Do not move dataclass field types, constructors, exception types, supported
reexports or actual schema/model dependencies. NativeTransferExecutionPolicy's
NativeChunkLimits, the ProcessResult constructor and soft-delete exports are
explicit exclusions. The private _Work dataclass also retains its EncodedNativeFile
runtime type; removing a no-op cast does not exempt that field from ADR 0058.
Recheck symbol use on the integrated head before editing.

The recorded runtime-to-contracts baseline maximum is 209 and the existing
allowed flow regression is 5. The current audited production source has 213.
Removing these unnecessary runtime dependencies is an engineering change within
the affected SQL Server path. It does not permit editing baselines, ignoring
other graph issues or reporting a forecast as a passing gate. The reported
architecture-fitness/clustering failure still requires an actual integrated check.

Validation: full contract validation and effective cross-task ownership check
are required before handoff. The integrator runs existing lease, preplan,
database-authority, generic-governance and native recovery tests, mypy, all
canonical architecture gates and independent review after implementation.

The integrator additionally owns
`src/dpone/runtime/sinks/strategies/mssql/mssql_native_staging_checks.py` for a
cohesive collaborator containing the existing required-key, SQL-equivalence and
COUNT_BIG checks. The integrated normalizer measured 364 SLOC against its unchanged
350 warning threshold. Move those algorithms intact and reuse them in all three
normalization paths; keep metadata/evidence orchestration in the normalizer.
Public signatures, errors, ordering and row-count semantics remain unchanged.
This is a responsibility split, not permission to bypass checks or raise limits.

## DDA-01 artifact helper

DDA-06 granted DDA-01 the one new path
`src/dpone/runtime/native_delivery_benchmark_artifacts.py` to separate retained
bytes, confined paths and atomic publication from receipt/comparison policy.
The full supplement is held by DDA-06 at
`test_artifacts/delivery-acceleration/dda-06/supplemental-contracts/dda-01-observations.yml`.
The planning owner confirms that bounded authority. No competing supplement or
public schema change is introduced here. This cohesive split addresses a measured
module-size warning while retaining the existing warning and hard limits.

## Linear implementation history

GitHub rejected a later implementation-branch merge commit. Planning inputs can
be fast-forwarded; subsequent reviewed dependencies use cherry-pick -x when an
ordinary merge would violate the branch rule. Preserve the original local branch
and provenance. Published PR branches advance through ordinary fast-forward
pushes. Do not force-push, reset another checkout or change repository rules.

These adjustments are authorized within the maintainer's implementation dispatch.
They do not authorize live infrastructure, public SWITCH activation, merge of PRs,
release publication, broader ownership or a weakened validation result.

## Planning validation

PASS: all six effective contracts validate, and their code, documentation and
evidence ownership does not overlap. Documentation checks cover 817 Markdown
files and 3,260 local links; 44 documentation/policy tests and the strict MkDocs
build pass. Independent static review found no actionable issues in this
supplement. The baseline and budget files remain unchanged.

Implementation tests and the integrated architecture/module-size gates remain
UNVERIFIED for this supplement until DDA-06 executes and records them. The
dependency-removal count is a scope estimate, not a prediction of a passing gate.

## Excluded PostgreSQL reserve

A further function-annotation candidate, ResolvedBindingConnection in
`src/dpone/runtime/sources/postgres_source_authority.py`, is excluded from this
supplement. Read-only inspection found active edits in six PostgreSQL worktrees
and an explicit existing ownership contract for that file. Technical compatibility
of an import cleanup does not transfer ownership to DDA-06.

The file remains outside DDA-06's authorized paths. Preserve the PostgreSQL
owner's changes and do not import that separate prototype as part of this scope
adjustment. If actual integrated dependency or clustering metrics still exceed
their limits, report FAIL and keep integration on HOLD with the measured result.

## Generated quality dashboard

The full `dda-06-generated-metrics-scope.yml` contract grants only DDA-06 the
additional shared path `docs/quality-metrics.md`. Hosted CI requires the generated
dashboard to match the integrated tracked Python tree. The existing producer,
`uv run dpone docs update-dev-metrics`, writes only that Markdown file and
preserves content outside its generated markers; its implementation was checked
against integration commit 81a02033d4972a4a8a15795b87ef6efb1a15c267.

Run the producer in the integration checkout after staging or committing every
intended Python input, then run its `--check` mode and confirm byte-identical
regeneration. Review the generated diff, source identity and final documentation
checks. Retain all parallel changes and stop on a new ownership conflict.

This update synchronizes displayed measurements. It permits no manual metric
edits, generator changes, new output paths, baseline/budget changes, workflow
changes or threshold overrides. An up-to-date dashboard can still describe
failing architecture metrics; those FAIL results and integration HOLD remain.

Planning validation: all six effective contracts and their combined ownership
check PASS, including the exact one-path extension. The 44 documentation/policy
tests PASS. Independent static review approved this supplement without findings.
Generation and final integrated documentation/architecture checks remain DDA-06's
execution responsibility; no passing generation result is claimed here.

## DDA-05 recovery regression module

DDA-06 assigned DDA-05 the single additional path
`tests/test_native_delivery_live_recovery_boundaries.py`. The full effective
DDA-05 supplement remains with the integrator at
`test_artifacts/delivery-acceleration/dda-06/supplemental-contracts/dda-05-recovery-boundaries.yml`.
The planning owner confirms that assignment; this record introduces no competing
contract or additional source/shared-file ownership.

The cohesive module covers repeated source/publication operations, rollback
pipeline markers and unknown-commit partial publication found during independent
review. It avoids growing the existing 392-SLOC harness test module toward its
unchanged 400-SLOC hard limit. Correctness fixes remain within DDA-05's existing
helper paths, with frozen receipt IDs, schemas and explicit environment opt-in.

The effective focused pytest command must run both the existing benchmark tests
and the new recovery-boundary module. Preserve all other required checks. After
each implementation correction, independently review the current diff again
before handoff. The integrator must include the new tracked Python test file
before regenerating quality metrics and freezing the final validation tree.

No live execution, public SWITCH activation, shared fixture edits, threshold
changes, publication authority or revision of receipt semantics is authorized.
Component tests and the actual final integrated gates still require execution;
the scope confirmation does not claim a passing implementation or lift HOLD.

## DDA-02 component dashboard operation

The bounded operation in `dda-02-component-metrics-operation.md` allows the
existing DDA-06 shared-file owner to generate a reviewed dashboard dependency for
DDA-02's frozen component source in a separate integrator checkout. DDA-02 may
import that docs-only dependency; it receives no shared-file writing authority.
The combined integration dashboard is generated separately from the combined
tracked Python tree. Source changes invalidate a previously generated candidate.

The previously failing doctor scenarios passed on unchanged source during the
follow-up. No readiness source scope or speculative lazy-import fix is granted.

## Integrated manual quality summary

`dda-06-quality-summary-correction.md` specifies three exact replacements to the
obsolete manual quality summary in the integrated dashboard. They remove an
unsupported architecture-PASS claim only after the current canonical gate
receipts confirm FAIL. The full `dda-06-quality-summary-scope.yml` successor
permits this exception on the existing DDA-06-owned path and preserves all other
scope, generated bytes, metrics definitions and validation requirements.

The component dashboard operation remains producer-only. Independent review of
the exact summary correction and the final integrated gates are still required;
the corrected wording does not resolve the measured architecture regressions.

## Architecture remediation phase

The maintainer's continuation now authorizes completion of the remaining
architecture correction. `dda-06-architecture-remediation.md` records the
approved compatible impact/algorithm/validation plan; the full
`dda-06-architecture-remediation.yml` contract is effective for this new phase.

DDA-06 is the only active writer. DDA-01 through DDA-05 have completed their
component handoffs; their earlier contracts remain immutable historical inputs,
not concurrent writing grants for the paths transferred in the new contract.
This phase explicitly transfers only the listed limit/observer/digest/catalog
files, tests and developer guides. Other component files and the private
PostgreSQL source-authority path remain outside the correction.

The change centralizes duplicated limit validation and prepared hashing, puts
transaction metadata reads in the catalog, and classifies three existing
function-only imports under ADR 0058. It preserves public exports, signatures,
runtime dataclass dependencies, algorithms, checks and all quality budgets.
Graph predictions remain unverified until the implemented source is tested.
The final current-summary update must follow actual gates; prior FAIL evidence
is retained without relabelling.

## Independent review correction phase

The maintainer requested fixes for both P2 findings at f164f2b.
The current successor is `dda-06-review-fixes.yml`, with the compatible
impact and validation plan in `dda-06-review-fixes.md`. DDA-06 remains the
sole writer. This phase transfers only the listed baseline regression, live
entrypoint/support and certification guide paths from completed DDA-05.
It also preserves the exact pinned 0.77.0 and PostgreSQL upstream commits
through reviewed linear imports; no foreign editing authority is granted.
Historical specifications, contracts and evidence retain their original meaning.
