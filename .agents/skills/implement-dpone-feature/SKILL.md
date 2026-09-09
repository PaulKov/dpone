---
name: implement-dpone-feature
description: Implement an approved dpone feature specification using path-scoped agents, red-green-refactor, compatibility discipline, and one integrator. Requires an APPROVED specification and task contract.
---

# Implement an approved feature

1. Verify the specification status is `APPROVED` and load the task contract.
2. Trace the current execution path and confirm that assumptions still match the
   base commit.
3. Create separate worktrees for parallel writers. Give each writer disjoint
   `owned_paths`; reserve shared schemas, registries, dependency files, workflows,
   MkDocs nav, changelog, and common fixtures for the integrator.
4. Add a focused failing test at the lowest useful layer.
5. Implement the smallest coherent design that satisfies the approved algorithm.
6. Preserve public behavior or implement the approved migration/deprecation path.
7. Update examples, docs, diagrams, generated reference, and runbooks with code.
8. Run `validate-dpone-change`, then ask a fresh-context reviewer to inspect
   correctness, silent-data risk, compatibility, tests, docs, and evidence.
9. Mark the specification `IMPLEMENTED` only after evidence is linked. Report
   every check as PASS/FAIL/SKIP/N/A/UNVERIFIED.
