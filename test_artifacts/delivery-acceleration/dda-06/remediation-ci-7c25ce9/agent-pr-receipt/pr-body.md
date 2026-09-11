Native SQL Server delivery repeated row sizing and preparation work. This integration carries the admitted encoded-byte reservation through spawned workers, inserts business values and framework metadata together, and computes the two initial prepared digests from one read. Independent verification retains four raw reads and two prepared reads, including the separate prepublication scan and finalizer target-clock UPDATE.

Optional bounded phase observations preserve existing Mapping/tuple inputs, resource limits, wire/journal/recovery formats, source lifetime, ownership/fencing and publication ordering. Producer/consumer checks reject invalid retained proofs, repeated timed operations and invalid recovery boundaries. Public native SWITCH admission still rejects before I/O; its implementation remains isolated and unregistered.

Validation on frozen source/docs `be7655ac2e36cd1e9601217c6b0a1fbd10e2eb3f`:

- **PASS:** 786 focused tests; lint, format, mypy, import rules, exact-commit module budgets, ownership/provenance, docs/references/language, strict MkDocs, generated metrics freshness, Airflow contracts and both Airflow distribution builds/Twine.
- **FAIL, full non-live suite:** 21,240 passed, 570 skipped, two failed, zero errors; two workers, exit 1, 1,257.00 seconds. Both failed tests assert the same clustering value `0.18322547520065463 > 0.182`.
- **Architecture HOLD:** the separate layer gate also reports runtime→contracts `215 > 214`. GitHub Quality preflight independently confirms the flow failure. Budgets, baselines, gate code and workflows remain unchanged.
- **SKIP / UNVERIFIED:** live SQL/BCP fidelity and measured acceleration; no approved disposable environment or live speed claim.

The seven delivery guides, operations runbook, ADR 0062, navigation and changelog describe first success, diagnostics, recovery and compatibility. Independent documentation review approved the rendered journey and 4,074 local links/anchors. The corrected quality dashboard explicitly displays RED/HOLD. Fresh implementation, interoperability and evidence reviews are retained with their exact scopes.

Final evidence and source/retention identity: [completion report](https://github.com/PaulKov/dpone/blob/1281b83e2900cacf5dcf9201e3909464d5f7b659/test_artifacts/delivery-acceleration/dda-06/completion.md) (retention `1281b83e2900cacf5dcf9201e3909464d5f7b659`). DDA-02 and DDA-05 final component completions are linked immutably there; their separate passing suites do not replace integrated gates. The evidence-retention commit changes no tested source, Python input, dependency or product documentation.

This PR remains a reviewable draft with architecture HOLD. Merge and release readiness are withheld.
