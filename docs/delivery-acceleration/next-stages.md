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
