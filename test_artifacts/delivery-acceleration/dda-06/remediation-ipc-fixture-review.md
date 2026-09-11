# IPC test-fixture correction

The first remediation full run on `c7d86e1449afde30f401127b6eda941ef43a116a`
**FAILED**: 16 failures, 21,254 passed, 570 skipped. Its
[receipt](remediation-final-full-nonlive.json),
[JUnit](remediation-final-full-nonlive-junit.xml) and
[failure analysis](remediation-full-failure-analysis.json) remain unchanged.
All failures were in the positive chunk-execution fixture. The production
scheduler, frame producer and failing test module were unchanged between
`1281b83` and that source.

## Diagnosis and impact

The scheduler includes the serialized contract and destination path in its IPC
reservation. The longer platform temporary parent made the first tuple require
1,025 bytes against the fixture's 1,024-byte cap; source admission correctly
failed before workers started. The cancellation barrier timeout followed that
first-frame failure. [Exact measurements](remediation-ipc-path-diagnosis.json)
retain both long and shorter path lengths. The short parent was a diagnostic
comparison only; the final full replay uses the original long naming pattern.

The coordinator approved a test-only correction in the already owned module.
The positive fixture now has a 4,096-byte IPC budget for contract/path metadata;
row count, per-row size, cumulative bytes, allocation caps and parallelism stay
unchanged. An existing real-spawn overlap/recovery test now covers default and
explicitly long work paths. A new real-scheduler test uses a valid 64-byte cap
and proves metadata admission rejects before reading the source, closes it,
imports nothing and writes no completion receipt. Existing tamper, row/frame,
worker, cumulative-byte and backpressure negatives remain intact.

No production code, public contract, resource default, budget or baseline changed.
No migration, ADR or user-journey change is needed. Canonical metrics must be
regenerated after the updated Python input is committed; final checks and the
complete non-live suite then run against a new frozen candidate.

## Evidence and independent review

- **Expected FAIL:** long-path regression before the fixture correction,
  [red JUnit](remediation-ipc-fixture-red-junit.xml) and
  [log](remediation-ipc-fixture-red.log).
- **PASS:** 69 execution/frame cases after positive headroom correction,
  [intermediate JUnit](remediation-ipc-fixture-green-junit.xml).
- **PASS:** 70 execution/frame cases including real insufficient-capacity
  rejection, [final focused JUnit](remediation-ipc-fixture-final-junit.xml).
  Both green runs have zero failures, errors or skips. Parent tool exits were
  zero. These development runs precede the final committed-source receipts.
- **PASS:** focused lint, format and diff whitespace.
- Coordinator architect **APPROVED** exact test blob
  `82a8d1a36d7fc4e0f3975420eb8df3281a74c46b`, no findings. It independently
  checked unchanged limits/negatives and real positive/negative scheduler
  behavior. This was read-only source review; its test execution was **SKIP**.

Independent fresh certifier `review_ipc_fixture` **APPROVED**, no findings.
It independently reproduced the 1,025-byte long-path requirement and ran 78
execution/frame/file tests with two workers: zero failures/errors/skips, 3.05
seconds. Real workers/files use fake target sessions. The reviewer retained
all positive, negative, boundary, compatibility, retry/replay, idempotency and
failure coverage; exact submitted-task/frame boundary tests remain local-double
unit evidence. Focused lint, format and whitespace also passed.

The initial owned `/tmp` probe had GID 0 rather than effective GID 20 and lacked
SGID. Before pytest, the reviewer corrected only its temporary directories and
verified GID 20, parent mode 0700, probe mode 02777. The resolved basetemp was
153 characters; the long-path test adds another 138-character component.
[Copied environment/command](remediation-ipc-independent/review-environment.json),
[JUnit](remediation-ipc-independent/focused-junit.xml),
[log](remediation-ipc-independent/focused.log) and
[causality measurements](remediation-ipc-independent/independent-causality.json)
are retained byte-for-byte with [origin hashes](remediation-ipc-independent/origins.json).

The test-file SHA-256 stayed
`b06a651e69af9467720b47d227faefa11566609f20535ad1501ead5c03f32c68`
through review; no repository edits were made. No live or full acceptance is
inferred from these focused results.
