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

Repeat reviewer disposition and final exact-source checks will be added after
source/test freeze. No merge or release readiness is asserted here.
