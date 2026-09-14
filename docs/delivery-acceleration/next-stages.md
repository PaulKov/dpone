# Delivery acceleration release stages

This plan is for data engineers and maintainers extending the ClickHouse-to-MSSQL
native route after 0.79.1. The maintainer authorized staged implementation and
separate patch or minor releases on 2026-09-13. Each stage has its own reviewed
diff, validation evidence and release decision. Version numbers are selected
against the current protected branch before release; concurrent work must remain
intact. Start with the [delivery overview](index.md).

## Release sequence and definition of done

| Stage | Release class | Deliverable | Definition of done |
| --- | --- | --- | --- |
| 1: source sizing reuse | Patch; initial candidate 0.79.2 | Reuse the source adapter's native row size in framing. | Identical rows, frame partitions, wire bytes, exceptions, closure and resource bounds; one sizing pass per adapted row under matching contracts; focused and broad checks, local Docker route evidence and independent review. |
| 2: independent stage limits | Minor; 0.80.0 | Separately configure encoding and import concurrency with bounded admission. | Written approved contract; old settings retain their behavior; deterministic queue/byte limits; cancellation, saturation, retry and recovery tests; narrow/wide local measurements and documented configuration. Automatic tuning is deferred. |
| 3: verification CPU cost | Patch if contracts remain identical | Optimize typed verification transport/encoding after profiling the remaining passes. | Exact existing digest and duplicate semantics; unchanged verification boundaries; corruption and mutation rejection; measured CPU/end-to-end comparison. Removing or merging authority boundaries requires a separate design and minor stage. |
| 4: Arrow bulk backend | Minor | Optional typed Arrow transport into owned raw staging. | Dependency and type admission, durable replay, independent bulk transaction handling, NULL/decimal/time/binary fidelity, partial-batch failure and source-free recovery; comparison against native BCP; installation and migration documentation. |
| 5: eligible SWITCH publication | Minor | Activate the isolated SWITCH component for explicitly admitted layouts. | Schema/index/filegroup admission before mutation; owned stages, transactional publication receipt and unknown-outcome recovery; lock/concurrency tests, live full-refresh/window replacement cases and rollback runbook. |
| 6: target layout profiles | Minor | Explicit tested loading profiles for heap, rowstore and columnstore. | Capability checks and compatible defaults; correct actual ingestion batch boundaries and locking behavior; resource measurements; no automatic recovery-model change; operational guidance. |
| 7: changed-window delivery | Minor | Transfer only windows changed according to an authoritative producer revision. | Approved change authority and watermark contract covering deletes, late arrivals, corrections, replay, missing history and fallback; reconciliation and recovery evidence; user-facing configuration and migration. |

Stage 1 is an internal optimization under the existing approved DDA contract.
Later rows authorize direction, not undocumented new public semantics: their
detailed specifications must be written and reviewed before implementation.
Each can be reverted or released independently; shared benchmarks may be reused
as tooling, but results belong to the exact tested commit. dbt and the separate
composition feature remain outside this plan.

## Research decisions after 0.80.0

The following specifications record the 2026-09-14 research against
`6ae541d38ac223327d7edb23510859df91173bda`. Research completion does not activate
a backend, change recovery policy, or establish production acceptance. Each
specification retains its detailed prerequisites and definition of done.

| Workstream | Current decision | Next evidence needed |
| --- | --- | --- |
| [Resilience and endurance](../feature-specs/dda-industrial-resilience.md) | Protocol researched; industrial recovery remains unverified. | Real interruption, restart, reconciliation and endurance trials on admitted resources. |
| [Verification CPU](../feature-specs/dda-industrial-verification.md) | Keep the current implementation; isolated experiments do not demonstrate the required benefit. | Complete route profiling and a measured candidate preserving every integrity boundary. |
| [SQL layouts and SWITCH](../feature-specs/dda-industrial-sql-publication.md) | Separate future designs; SWITCH remains unregistered. | Explicit layout admission, transaction and ownership integration, then live certification. |
| [Arrow bulk](../feature-specs/dda-industrial-arrow.md) | Do not adopt the backend yet. | Independent bulk-connection outcome handling, runtime integration and full-route comparison. |
| [Changed windows](../feature-specs/dda-industrial-changed-windows.md) | Authority model researched; no automatic incremental selection. | A real producer revision/history service with fencing and complete change coverage. |
| [Operational readiness](../feature-specs/dda-industrial-readiness.md) | Apply documentation corrections; deployment acceptance remains unverified. | Workload-specific performance and failure evidence, plus source/target governance. |

The initial local baseline completed narrow 10,000-row trials, but wide trials
failed during preparation and later staging admission. Their failed results
cannot serve as throughput measurements. Retained staging consumes capacity
across invocations, and preparation needs additional allocation beyond the raw
payload. A follow-up using a different resource policy is a separate experiment;
it cannot turn the original failed policy into a pass. Observer overhead and a
complete matched route comparison are still required before selecting a speed
optimization.

Maintainer evidence locator: retained artifact bundle
`dpone-dda-industrial-plan-20260914/P01`, experiment subject
`6ae541d38ac223327d7edb23510859df91173bda`. Its original
`artifact-manifest.json` has SHA-256
`2f989b541ec93f995b432170bad04c1057a3f8495c05ada0bd6be7e5139a4309`;
`baseline-summary.json`, `diagnosis.md` and `wide-failed-journals.json` describe
the measurements and failures. Follow-ups are retained separately under
`P01/followups/`. These local artifacts are not shipped in the package; request
the bundle from the maintainer to audit the claim. Without the matching bundle,
independent reproduction remains unverified.

## Stage 1 implementation contract

- Classification: internal implementation, no public-contract change.
- Base: `2639abc0bbe5531a27b8478cfe579e6915f717d6` (0.79.1).
- Owner/integrator: the DDA task; independent architect and test reviewers are
  read-only. Only the integrator edits this worktree and shared semantic files.
- Preserve the standalone `native_source_rows` signature and plain tuple output.
  It still rejects malformed source columns, invalid UTF-8, unexpected NULL and
  excessive native row bytes at the source-adaptation boundary.
- The production adapter produces a private immutable row reservation after the
  existing normalization and sizing checks. Do not admit mutable binary values
  that the previous adapter rejected.
- Framing unwraps the row and reuses its size only for the identical wire-contract
  object and row-byte limit. Other consumers perform normal sizing. The carrier
  never enters worker pickle payloads, files, journals or public evidence.
- Preserve per-row, aggregate-frame and task IPC admission, native/cumulative
  byte limits, worker encoding and actual-size verification. A reservation is
  neither validation of fixed-width scalar domains nor content evidence.
- Preserve source closure and validation before the next cancellation callback.
  Empty input, lookahead, duplicate multiplicity and EOF authority are unchanged.
- Changes belong to source-value adaptation, framing, preparer wiring, focused
  tests and delivery documentation. No SQL, schema, state, retry or finalizer
  changes are required. No ADR or migration is required for this internal reuse.

## Stage 2 implementation contract

The [approved specification](../feature-specs/dda-independent-stage-limits.md)
and [ADR 0063](../adr/0063-independent-native-stage-limits.md) define independent
encoding/import limits with one shared max-based retained-work capacity. The
[concurrency how-to](concurrency.md) covers defaults, manifest versus Python null
semantics, resolved planning, exact recovery policy and report-reader migration.
Legacy-effective settings keep the eight-field policy and v1 run envelope;
extended settings use ten canonical fields and v2 runs. All verification and
publication authorities remain in place.

Release acceptance requires exact-source evidence. Narrow/wide measurements must retain each
configuration separately; exact comparison equality is unchanged and no
cross-policy speedup is certified. Existing stage 1 observations cannot certify
stage 2. Automatic tuning, dbt and composition changes remain out of scope.

## Evidence and release acceptance

### Bounded text encoding after wide100 profiling

The first follow-up to the completed 100-column, 100,000-row local diagnostic
optimizes scalar work inside the existing verification passes. It does not adopt
the rejected tuple-projection experiment or remove any SQL readback. Small exact
strings with variable `varchar`/`nvarchar` layouts use strict UTF-8/UTF-16LE
encoding directly only when four bytes per code point fit in the remaining row
budget. Actual field and prefix limits are still checked. Other values retain
the allocation-free scan, including its diagnostic order. The standalone sizing
API remains allocation-free. No setting, migration or new dependency is needed;
existing composition and recovery instructions remain applicable.

The implementation is a patch candidate until independent review and required
checks complete. Local CPU microbenchmarks are distinct from route latency: a
faster encoder does not establish the same percentage reduction in confirmed
visibility. The earlier 450-second diagnostic included provisioning and final
readback; its approximately 299-second visibility interval also included
independent verification. It is not a transport-only benchmark.

The maintainer's 2026-09-14 follow-up prioritizes these independent milestones:

| Milestone | Release class | Completion requirement |
| --- | --- | --- |
| Bounded text encoding | Patch | Identical bytes and error precedence, allocation bounds, retained mutation/recovery tests, measured CPU benefit and scoped route evidence. |
| Protected staging verification | Minor if authority changes | New design/ADR with SQL-enforced writer exclusion through consumption, object/incarnation binding, reconnect/crash invalidation, tamper tests and fresh recovery verification. An application lock alone is insufficient. |
| Native partition switching | Minor | Explicitly admitted aligned layouts, atomic old-out/new-in plus receipt, unknown-commit reconciliation, empty-window replacement, reader/lock tests and an executable composition example. The researched initial scope does not admit arbitrary CHECK-based staging or whole-window switching. |
| Weighted day scheduling | Minor | Frozen authored-day universe including empty days, deterministic volume weights/ties, bounded aggregate resources, source-consistency contract, per-day receipts and explicit cycle visibility/recovery semantics. |
| Direct typed bulk comparison | Experiment; minor only if selected | Actual driver type/NULL fidelity, bounded memory, independent writer settlement, durable replay design and matched comparison with binary BCP. Retain BCP when benefit is not established. |

Each later milestone needs an implementation-ready specification and task
contract. Reusing already researched components does not activate a public route.
These milestones exclude dbt, composition-feature changes and automatic
changed-window selection; refreshing every authored day does not require the
separate producer-revision service.

Run the change-aware check selector and required repository checks. Focused tests
cover UTF-8/native size boundaries, contract/limit mismatch, mutable buffers,
Mapping/tuple compatibility, one-shot iteration, cancellation and source closure.
Exercise the existing real-row Docker ClickHouse/SQL Server/BCP route with unique
owned resources. Retain source identity, commands and outcomes outside the source
tree; never retain credential values. A fresh-context reviewer inspects the final
diff and evidence before merge.

For later performance stages, measure confirmed visibility time, phase spans,
CPU and memory where available using the same inputs, layouts and resource
budgets. Overlapping phase durations must not be added as elapsed time. No
percentage improvement is claimed from a call counter or a synthetic test.
Unavailable metrics remain UNVERIFIED. Local emulated SQL Server timings do not
establish production throughput.

Every release follows the [current runbook](../release.md), preserving exact
source checks and controller publication evidence. Additional retrospective PyPI
verification is outside the maintainer's requested scope. A stage stays open
until its applicable checks, independent review and publication have completed.

Return to [frame sizing](frames.md) for the preserved limits or the
[operations guide](operations.md) for observation and recovery instructions.
