# DDA-05 receipt atomicity and diagnostic evidence correction

> Historical corrective record authored before final validation. The subsequent
> independent approvals and completed frozen full gate are recorded in
> [the final completion report](completion.md). Original logs remain unchanged.

A second fresh-context read-only certification reviewer assessed exact commit
`b5ad9d28e9273543b5c49932f1d458df7942caa8`. All 114 focused tests passed, but an
independent reproduction found two remaining issues and returned REQUEST CHANGES:

1. **P1:** an unknown operation could leave the old target and zero publications
   while a new exact operation receipt appeared. The negative recovery fixture
   incorrectly accepted this violation of atomic target/receipt publication.
2. **P3:** a metadata-only failure could report FAIL with True/True as the retained
   expected/observed values, losing the actual binding mismatch.

Both are corrected. An unknown old state must preserve its prior receipt; an
absent receipt can no longer become a new one without target publication. The
hermetic fixture now models absence of an operation receipt before publication.
Recovery checks retain structured state validity and binding verdicts/values in
the existing `expected` and `observed` fields. The composite known/unknown check
retains both branches, including mismatching hashes. No raw connector exception,
credential, SQL text or business value is added to diagnostics.

The approved task scope and its supplemental test path remain unchanged. Frozen
v1 check IDs, outer schemas, methods, status/evidence fields, opt-in requirements
and production runtime contracts are unchanged. DDA-01's owner independently
confirmed that expected/observed are opaque JSON; canonical typed equality and
matching retained raw checks still apply. That confirmation is a contract review,
not execution evidence for this producer revision.

Six new negative tests failed on `b5ad9d2`; see [second-red.log](second-red.log).
The combined focused run passed 119 tests (70 original and 49 boundary cases);
see [final-focused.log](final-focused.log). The final receipt-preservation
diagnostic refinement has separate targeted coverage in
[final-targeted.log](final-targeted.log). Tests validate actual retained proof/raw
artifacts through the v1 reader, including metadata mismatch values.

This record is authored before the third fresh independent review and before the
required final broad run. No successor approval or full-suite PASS is asserted.
Heavy suites are serialized by the source coordinator: DDA-02, then integrated
DDA-06, then this component. A new full run must use an effective-GID temporary
parent and retain its exact checked commit. Historical broad failures remain in
the parent directory under their original identities.

The developer guide explains atomic receipt publication and structured failure
evidence. There is no migration. Live SQL/BCP, containers, credentials and
performance certification remain **SKIP/UNVERIFIED**. Release work is **N/A**.
