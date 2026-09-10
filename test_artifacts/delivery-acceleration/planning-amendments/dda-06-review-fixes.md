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
