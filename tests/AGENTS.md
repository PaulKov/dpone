# Test rules

These rules extend the repository-level `AGENTS.md` for `tests/**`.

- Follow red-green-refactor and add the smallest failing regression test before
  implementation for defects.
- Test public behavior, invariants, and failure semantics, not private call
  choreography unless that choreography is itself a contract.
- Cover positive, negative, boundary, retry/replay, idempotency, and
  backward-compatibility cases appropriate to the change.
- Data-movement changes require a real-row integration case in the applicable
  `integration_*` profile. A mocked contract test does not certify a live route.
- Use existing pytest markers and fixtures. Do not silently broaden default test
  collection or add live credentials to tests.
- Assertions on evidence must verify producer inputs, ordering, status, and
  digests where relevant. Never make skipped work look passed.
- CLI tests must check exit code, stdout/stderr, machine-readable output, file
  output, invalid combinations, and absence of unintended side effects.
- Nested-data tests must verify deterministic IDs, parent/root lineage, empty
  and null semantics, arrays, deep nesting, collision handling, replay, and no
  orphaned children when relevant.
