# Independent review

2026-09-10; reviewer: `fresh_review` (fresh context, read-only).

Initial decision: REQUEST CHANGES. Observed the selected-strategy/materialization
boundary and sink-owned transaction are architecturally correct. No new ADR is
needed to restore the documented behavior.

Findings and corrections:

- P1: NULL partition equality retained old rows on replay. Real PostgreSQL RED
  retained under `red-null-partition`; predicate matching uses IS NOT DISTINCT
  FROM, native NULL input falls back or fails in required mode. Added ordinary
  and declarative LIST NULL replay tests.
- P2: standalone file loaders no longer released their temporary file. Restore
  historical best-effort cleanup and test genuine files on success/failure.
- P2: sample SQL failure was swallowed, hiding the original error behind an
  aborted-transaction cleanup error. Propagate SQL errors; logger rendering alone
  remains best-effort. Added unit RED and live SQL fault injection.
- Import gate: compatibility implementations moved out of strategy modules;
  historical imports remain re-export shims. Their COPY helper delegates to
  PostgresStagingManager, retaining one transport implementation.

The 28-case `live-direct-second` run passed on its recorded intermediate tree.
The subsequent `live-direct-reviewed` campaign overlapped edits and is retained
as intermediate evidence only. Neither substitutes for the required final frozen
commit run. Its producer now fingerprints source and tests before/after execution
and fails certification if either changes.

## Repeat disposition on frozen source

APPROVE code and architecture at `1ff83879fc7640d358cad15402672eafddcabf41`.
The independent reviewer checked all 27 review-manifest files with zero
mismatches and found no remaining code blockers. Reviewer-executed tests: SKIP.
Broad and exact-image gates remain separate from this code-review approval.

## Final bounded evidence review

APPROVE the synthetic proof at the same frozen production source. The reviewer
required exact Pod UID and boolean checks, a bijection between direct receipts
and passed tests, deployment and outcome-gate identity joins, structured CHECK
errors after the target-truncated event, exported-file checksums, and the
controller's admitted image and embedded source provenance. All were corrected.

The independent reviewer then checked controller
`1826409a-ecdc-488c-9f58-6e1c38542224`, its admitted image and observed digest,
the installed-file verification, embedded provenance, and manifest digest.
They agree on source `1ff83879fc7640d358cad15402672eafddcabf41`.
All five strict DAG cases pass the strengthened verifier. Reviewer-executed
heavy tests: SKIP. This approval does not certify other routes, merge readiness,
or publication authority.
