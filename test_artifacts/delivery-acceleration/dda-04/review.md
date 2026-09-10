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

A second independent fresh-context test/certification review is running against
the corrected candidate. Its conclusion will be recorded before final handoff.
