# Delivery architecture remediation

Status: APPROVED for compatible internal remediation under the maintainer's
instruction to finish the integrated work. No merge, release or live execution
authority is granted. The original delivery specification and public behavior
remain unchanged.

## Problem and evaluated source

PR #25 is reviewable but blocked at
`1281b83e2900cacf5dcf9201e3909464d5f7b659`. Its Python sources match the tested
`be7655ac2e36cd1e9601217c6b0a1fbd10e2eb3f`: the retained full suite has 21,240
passes, 570 skips and two clustering failures. The layer gate separately reports
runtime-to-contracts flow 215 against the existing 214 ceiling.

The checked-in quality budget is the authority: clustering must be at most
0.182; 0.180 remains the preferred target. The repository cross-layer test also
requires ratio at most 0.300. The maximum layer-flow ceiling is baseline 209 plus
the existing default allowance 5. No budget, baseline, parser default, gate,
workflow, dependency or test threshold may change.

The correction removes duplicated validation and gives existing components
cohesive responsibilities. It does not hide real dependencies or remove checks
from the delivery path.

## Algorithm and responsibility changes

1. **Benchmark limits.** Add one normalization function beside `RUN_SCHEMA` in
   `contracts/native_delivery_observations.py`. It requires exactly the canonical
   `NativeChunkLimits` fields, constructs that existing model with its unchanged
   validation, and returns the same normalized mapping. The runtime comparison
   and live harness configuration producer both use it. Keep producer
   `exact_limits_required` for field-set errors, canonical value-validation
   errors, consumer `BenchmarkInputError("invalid_limits")`, normalized values
   and configuration digests unchanged. Preserve exception class identities and
   every existing exported benchmark helper.
2. **Prepared integrity.** Move the existing single-projection typed hashing,
   metadata allowance and row-count verification into a dedicated function in
   the existing `mssql_native_prepared_digests.py` module. `_stage_digest` still
   chooses the same wire contract, builds the same SELECT and opens a fresh SQL
   iterator at the existing prepublication boundary. The primitive encodes each
   row once. Do not implement it by calling the dual-projection function and
   discarding one digest. Preserve finite allowance, NULL checks, duplicate
   multiplicity, native framing, digest spelling and errors. Four raw and two
   prepared readbacks remain; the second prepared readback is independent.
3. **SWITCH metadata.** Add `NativeSwitchCatalog.transaction_state()` as a
   query/row-shape reader only. Keep `TRANSACTION_SQL` at its current import path.
   The executor calls this reader for the same two observations and retains all
   authority predicates, session comparison, deterministic table-lock order,
   catalog snapshot/replanning, prepared integrity verification and old-out/new-in
   SWITCH order. Catalog construction must remain free of I/O. No commit, retry,
   receipt creation or public route activation is added.
4. **Function annotations.** Move only three genuine parameter-only imports into
   `TYPE_CHECKING`: `NativeDeliveryObserver` in runtime observations, and
   `MssqlNativeLineageProjection`/`ResolvedMssqlNativeSchema` in prepared INSERT.
   These modules already postpone annotations. ADR 0058 permits this existing
   classification: raw annotations remain identical and function introspection
   supplies the canonical type namespace. Dataclass field types, bases,
   constructors, type checks, errors and supported reexports remain runtime
   dependencies. No extra wire-contract annotation cleanup is included.

State, source lifetime, replay, transaction boundaries, evidence/checkpoint
ordering, diagnostic failure isolation and all schemas remain unchanged.
SWITCH package exports and canonical classes retain identity and signatures.
No new ADR or user migration is needed while ADR 0062's boundaries are retained.

## Test-first proof and compatibility

Before replacing the old digest loop, retain independent typed golden results
from the old implementation and cover empty rows, duplicates, NULL, Decimal,
Unicode, binary, generated metadata and malformed/count-mismatched inputs.
The existing `legacy()` helper calling `_stage_digest` must not become a circular
oracle for the new primitive. Demonstrate one encoding per row and an independent
prepublication readback, including post-preparation tampering.

Exercise limit field omissions/additions, bad values and existing error mappings
through both real producer and consumer paths. Confirm unchanged serialized
configuration and digests. Verify SWITCH queries and authority/lock/mutation
ordering, and unchanged rollback/unknown-outcome behavior using existing fakes.

Compatibility checks cover raw function annotations and canonical-namespace
resolution only for the three approved annotation imports. Keep ordinary
reflection for runtime dataclass fields and retained supported exports; check
canonical object identity and existing pickle/spawn behavior. Do not write tests
that merely assert the new import layout or hard-code a simulated graph result.

## Ownership and execution

`dda-06-architecture-remediation.yml` is the full effective DDA-06 contract. It
transfers only its listed completed DDA-01/02/04/05 paths to the sole integrator
for this correction. Former owners must not concurrently edit those paths.
Other completed component boundaries remain unchanged. The conflicting private
PostgreSQL source-authority path stays excluded.

Use the existing integration branch after checking upstream and worktree state.
Make cohesive, reviewable corrections with focused tests, and obtain independent
subagent review after each correction; resolve findings and review again before
final acceptance. Commit provenance and existing public PR history are retained.

The explorer's canonical graph simulation predicts clustering approximately
0.181939 and maximum runtime-to-contracts flow 214. This predicts feasibility,
not a passing result. Only actual checks on implemented source determine status.
If a constraint remains unmet, report the precise cause and revise the design
within explicit ownership; do not conceal a dependency or weaken validation.

## Final validation and documentation

Run new and existing focused regressions first, then canonical lint, format,
typing, import, layer, architecture and module-size gates plus the repository
architecture tests. All must pass before starting a new full suite. Keep the
existing thresholds, defaults and ratchet procedure. Class warnings and the
preferred clustering target remain visible even when hard gates pass.

Update developer explanations of shared limit validation, independent integrity
readbacks and catalog observation responsibility. Regenerate quality metrics
from all intended tracked Python inputs through the existing producer, preserving
reproducibility and source identity. Once actual gates pass, update only the
current manual quality-summary headline, explanation and coupling sentence to
reflect the new result; use YELLOW when advisory debt remains, and never infer
release readiness or live performance from graph success. This completion-phase
exception supersedes the earlier FAIL-only manual-summary wording; historical
FAIL receipts and reports remain immutable and linked.

Complete documentation/reference/language/strict-MkDocs and relevant packaging
checks, freeze the reviewed final source/docs tree, then run one full non-live
suite with two workers and a verified platform temporary directory. Preserve
exact before/after source identities, environment, log, JUnit and receipt hashes.
Use new `remediation-*` artifact names; never overwrite the old `final-*` run.

Have a fresh reviewer audit final implementation and evidence. Update the
integration report, specification status/evidence links and PR #25 with actual
local and hosted-CI results. Mark the PR ready for review only after relevant
required checks are confirmed; retain SKIP/UNVERIFIED live and performance
claims. The goal of this correction is a reviewable integrated result without
the two architecture blockers, not a release publication.
