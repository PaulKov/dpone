# Delivery acceleration: bounded ownership amendments

Purpose: resolve concrete implementation constraints without changing the approved
delivery algorithms, public contracts, baseline or quality budgets.
Audience: DDA implementation owners and reviewers. Date: 2026-09-10.

## DDA-06 annotation-only dependency scope

The full supplemental contract is `dda-06-native-annotation-scope.yml` in this
directory. It replaces the effective DDA-06 contract for path/acceptance checks;
the original planning commit f368294 remains immutable. Other task contracts
remain in force, including independently recorded narrow supplements.

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
