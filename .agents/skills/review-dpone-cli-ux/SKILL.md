---
name: review-dpone-cli-ux
description: Audit dpone CLI correctness and user experience, including valid and invalid parameter combinations, stdout/stderr, JSON/file output, help, exit codes, and side effects. Use for CLI changes and release gate R1.
---

# Review CLI correctness and UX

1. Build the command and option inventory from the executable CLI and generated
   reference.
2. Model option interactions and select equivalence classes, boundary values,
   negative/mutually-exclusive cases, and pairwise combinations. Use exhaustive
   Cartesian coverage only for small high-risk groups.
3. Verify help/version/import without optional connector SDKs.
4. Check exit codes, stdout/stderr separation, JSON stability, Unicode/non-TTY,
   file encoding, overwrite policy, atomic writes, and cleanup after failure.
5. Confirm invalid input fails before durable side effects and errors identify
   the invalid value, expected form, and recovery action without leaking secrets.
6. Compare CLI behavior with Python API and docs where the feature promises
   parity.
7. Produce R1 evidence with uncovered combinations and a risk-based rationale.
