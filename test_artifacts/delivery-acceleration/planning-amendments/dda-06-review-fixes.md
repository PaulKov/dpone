# Delivery review corrections

Status: APPROVED under the maintainer's instruction to fix the independently
reviewed PR #25 findings. Date: 2026-09-11. DDA-06 is the sole implementation
writer in its b364 checkout. Original approved specifications and earlier
receipts remain immutable. This is a compatible bug correction, with no new
feature, migration, release or live-environment authority.

## Frozen inputs and upstream preservation

Reviewed PR source: f164f2bdb0987442f1dd5afab931e5f9a5d1436d.
Benchmark baseline: d5ad9aaecc900c24df421b160ed36b4cfc726e45.
Adopted upstream: fa20f554bab3024f240b92aadf176527ee967925, comprising
release 0.77.0 at e15ad32b850708207c2f8f1b6faf597ef0f5b0b1 and the
subsequent PostgreSQL preservation change. Pin these objects in all checks;
a mutable origin/master is not the preservation oracle.

The active Source history integrity ruleset requires linear history for all
branches. Import the two upstream commits in order with cherry-pick -x and
ordinary fast-forward pushes. Do not merge commits, force-push, rewrite other
worktrees, or change rules. Resolve only CHANGELOG.md and docs/quality-metrics.md
conflicts, preserving both histories/prose; regenerate metrics using the
canonical producer after all intended Python inputs are tracked. Verify the
auto-merged docs/architecture.md and mkdocs.yml retain both contributions.
Every upstream-only path must match the pinned upstream blob exactly, including
versions, package pins, lockfile, PG sources/tests, schemas, workflows, agent
instructions and upstream budget/baseline files. Importing these exact upstream
changes grants no independent editing authority over those paths.

The ownership audit may distinguish these two immutable reviewed imports from
DDA-06 edits. Verify imported path sets and per-path patch identity or exact
upstream blobs, with only the explicitly listed conflict resolutions separately
reviewed. Also compare the final upstream-only tree to the pinned upstream.
Do not blanket-whitelist upstream paths or skip merge/import commits silently.
Keep the original audit receipts; a new producer/report must reject an altered
upstream-only blob, unexpected import path or unapproved resolution. Future
master movement is reported and assessed; it is not automatically imported.

## P2: baseline harness configuration

The current harness imports a candidate-only normalization module. The
prescribed baseline Python plus absolute current harness fails before factory
loading. Keep the runtime consumer's existing contract helper unchanged.
In the harness adapter, derive the exact field set from dataclasses.fields of
NativeChunkLimits imported from the actually loaded subject, construct that
canonical model, and serialize with asdict. The model exists in the frozen
baseline. Preserve exact_limits_required, canonical value errors, detached
normalized output and SHA-256. Do not copy field names or load candidate dpone
into the baseline interpreter.

Before fixing, reproduce the failure in a subprocess using the exact baseline
source and an isolated interpreter whose dpone resolves only to that source.
Run the absolute current CLI, prove actual import/commit identity and factory
reachability, and use a hermetic factory with honest UNVERIFIED evidence.
Three CLI approval flags may be set solely in that controlled hermetic child
to cover factory loading; this is not live authorization. No host approval,
credentials or services may be used. Keep generated files outside the baseline
checkout. Include no-approval SKIP/no-factory and invalid-field/value rejection
before factory loading, plus unchanged normalization/hash/error tests.

## P2: final live-test result

Both actual live entrypoints must call one shared test-support assertion.
Require execution == live; reject FAIL, SKIP, unknown/missing status and clean
UNVERIFIED. Permit UNVERIFIED only for an explicit boolean True dirty subject
or producer, retaining the deliberately supported dirty development journey.
PASS still requires both fidelity and recovery receipts PASS and exactly four
PASS samples. The dirty allowance requires the same proofs. Keep the isolated
SWITCH route-mode assertion and the existing approval/redaction boundaries.

Regress both actual decorated entrypoints, not only a helper-shaped oracle.
Reproduce late identity drift using the real hermetic producer: final cleanup
changes identity after all component proofs pass, making the envelope FAIL.
Both entrypoints must then reject it. Cover clean PASS, explicit dirty allowance,
clean UNVERIFIED, FAIL/SKIP, malformed status, missing/failed receipts and partial
samples. Hermetic execution cannot obtain the live allowance. No live calls.

## Documentation and validation

Correct certification.md's baseline prerequisites and observations.md's helper
ownership statement. Document final-status handling and dirty-code limitations.
Update the existing changelog entry without changing release versions/history.
Use review-fixes-* evidence; do not overwrite previous final-* or remediation-*
reports or relabel historical FAIL results.

Run regressions red, implement, run focused suites green, and request independent
subagent review after each correction (the two narrow fixes may be reviewed
in one fresh pass). Resolve findings and independently review again. Then run
change selection, formatter/linter, mypy, canonical import/layer/module/fitness,
metrics freshness/idempotence and docs/language/strict-MkDocs checks. Freeze
source and execute the complete non-live gate, with CI collection accounting
on supported Python versions; coordinate local workers to avoid shared-host
contention. Do not rerun an unchanged full gate repeatedly without a new failure
or source change. No threshold relaxation or live/PyPI check is authorized.

Publish the updated existing PR with exact source identity, independent review,
upstream preservation and retained test evidence. Code readiness is separate
from still UNVERIFIED live interoperability/performance and public SWITCH
activation. This task does not merge PRs or publish releases.

## Final integration supplement

The two P2 corrections were independently approved at
f5705f83249d79f5db944b4b7fa75ae95f428609: 60 independent regression cases
and the integrator's 251-case focused run passed. Import conflict blobs were
separately reviewed. Two integration constraints require the following bounded
supplement; it supersedes conflicting earlier phase details only.

**Annotation ownership.** The combined layer flow is 215 against the unchanged
214 ceiling. Upstream verified-pack error normalization introduces one genuine
runtime dependency; it must remain. The original MSSQL annotation reserve is
already applied. Independent analysis found one remaining function-only import
in src/dpone/runtime/sources/postgres_source_authority.py:
ResolvedBindingConnection is used only by from_connection's parameter annotation,
with postponed annotations. Its public blob is unchanged from d5 to fa20.

The PostgreSQL migration owner confirmed no active writer or pending working-tree
edit on that path and no conflict with this exact DDA-only import change. This
supersedes the older exclusion based on then-active parallel writers. DDA-06 may
move only this import under TYPE_CHECKING, adding that typing import as needed.
Keep every runtime/dataclass model, verifier decision, SQL statement and error
unchanged. Do not import the private migration prototype or alter its checkouts.
Preserve the raw annotation and explicitly supplied canonical namespace for
get_type_hints under ADR0058; test those supported introspection semantics in the
existing DDA architecture-compatibility module and run existing PostgreSQL source
authority tests unchanged. Independently review the implemented diff, rerun actual
layer/fitness/type gates, and preserve the prior FAIL evidence. No threshold or
baseline changes, incidental reexports or dataclass import hiding are authorized.
This path is not among the 138 adopted upstream-only changed paths; all 138 still
require exact pinned upstream mode/blob equality.

**Linear reviewed-tree transfer.** Cherry-picked upstream history preserves
content but not common ancestry. Actual merge-tree still reports a changelog
conflict; regenerated dashboard values can share the same problem. Do not alter
readable changelog structure or omit metrics merely to affect merge heuristics.

After the sole integrator finishes code, regression/provenance tests, independent
review and required static/docs checks, freeze and push a clean source H on the
existing integration branch. Run its historical ownership audit at H, retaining
its actual PASS or FAIL and reviewed import evidence. Keep every old branch,
commit, PR and receipt available; do not rewrite or force-push.

Create codex/dda-06-reviewed-delivery from pinned upstream fa20. Apply the exact
binary/mode-aware diff fa20..H in that new branch, and require the staged tree to
equal H's tree before committing. Record H in the new commit message. Require
new commit parent == fa20 and whole-tree equality with H after committing, then
publish a replacement PR immediately. This equality links the original ownership
and independent review evidence to the new tree without pretending the historical
lineage audit was run against different ancestry. Retain those source identities
and comparisons through an evidence producer. Do not create a broader governance
subsystem or weaken existing checks.

Run the complete required CI population on the new PR head, including supported
Python 3.11 and 3.12. Avoid duplicating the unchanged full suite locally when CI
provides exact complete collection accounting; label the local run N/A with that
reason. A concrete failure still requires appropriate local reproduction. Compute
receipt applicability again from the actual new PR diff; do not carry forward an
assumed PASS or N/A. Retain final provider artifacts outside the source tree when
that avoids needless source-head churn. Mark PR25 as superseded and link the new
PR, retaining its history. Do not merge either PR or publish a release.
