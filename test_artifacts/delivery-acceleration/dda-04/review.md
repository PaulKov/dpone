# Independent review record

Scope: DDA-04 unregistered component, owned paths only. Planning dependency:
`f3682940f8864563cde0e6b6ecee60f746b49020`.

## First fresh-context review

Reviewer: `review_switch_v1` (dpone_architect), 2026-09-10. Read-only review;
no live SQL or shared-runtime edits. The reviewer reported these concrete
findings, which were corrected before implementation commit:

- P1: database/server DDL triggers escaped table child metadata and could mutate
  during SWITCH. Added explicit trigger exclusion and server metadata visibility.
- P1: same-count in-window prepared tampering escaped catalog/count equality.
  Added mandatory injected verify_prepared(plan), invoked under held table locks.
- P2: standalone bound rules/defaults escaped table child metadata. Added their
  required column IDs and explicit fulltext-index exclusion.
- P2: unsupported boundary types could undergo unintended datetime conversion.
  SQL now converts only date/datetime2 and leaves other types for rejection.

Reviewer reran the 98 component tests successfully after fixes and reported no
remaining blocking finding for the unregistered scope. The public rejection
regressions are additional focused checks, not part of that 98-test count.

Limitations retained: SQL syntax/atomicity/locking, real same-session transaction
bridge, global administrative configuration freeze and live performance remain
UNVERIFIED. The caller must freeze privileged database/server DDL-trigger changes
through transaction completion. DDA-06 owns the numbered ADR and activation work
requires separate authorization.

## Second fresh-context review

Reviewer: `review_switch_v2` (dpone_test_certifier), 2026-09-10, reviewed exact
implementation commit `9795f01cb9a7ad01f404ce69e1356fa2aa32a399`. Read-only; no live SQL.
No additional component correctness blocker found. Reviewer confirmed 103
focused tests and exact-candidate lint/type/module checks.

P2 evidence finding: the original producer captured HEAD before a command and
its diff afterward; a commit during execution could report inconsistent source
identity. Corrected producer captures full source/producer identities both before
and after, preserves both and returns UNVERIFIED if either changed. A four-case
regression proves unchanged PASS/FAIL versus changed UNVERIFIED. The prior
non-live suite was interrupted because its source identity was no longer frozen;
required checks are rerun on the frozen producer revision.

The reviewer correctly retains the layer gate failure as a merge blocker:
runtime-to-contracts 217 exceeds 214. The DDA-04 constructors require those actual
imports; DDA-06/root are coordinating an independently authorized dependency
refactor. No baseline or budget changes were made here.

## Independent verification of the evidence correction

The second reviewer re-reviewed commit
`d8b09de644ab3bf72f9eee7b51cc8b8e38ca8a3c`, reran all four producer regression
cases (PASS), and independently recomputed the source/producer fingerprints
matching both observations in producer.json. P2 is resolved; no further blocking
finding in the correction. No production code changed after 9795f01.
