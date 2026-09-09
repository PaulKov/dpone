# DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID

**Audience:** CI maintainers and platform engineers.

The promotion request did not choose exactly one state guard.

Use `--expect-baseline-absent` only for first-baseline bootstrap. For an
existing baseline, provide both `--expected-baseline-sha256` and
`--expected-baseline-fingerprint` from the approved impact report.

This is a CLI/configuration error and returns exit code `2`. It does not open
the candidate, create a baseline, or mutate an existing baseline.

[Domain-first discovery and CI](../domain-first-discovery-ci.md) ·
[Domain-first error overview](index.md)
