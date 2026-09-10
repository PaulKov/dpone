# Independent post-implementation review

Requested by the maintainer on 2026-09-10. Reviewer:
`/root/post_feature_independent_review`, a read-only `dpone_architect` subagent
started with no inherited conversation. The reviewer did not implement the
feature. Previous approvals were not premises of this review.

## Initial review

Reviewed commit: `648237b6ebe3d3e59252c37aa5c72e88b38a2095`.
Baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
Verdict: **REQUEST CHANGES**; two confirmed P2 findings, no confirmed P0/P1.

1. `src/dpone/runtime/sinks/compatibility/postgres_query_loader.py:33` and
   `postgres_file_loader.py:77` discarded the caller's target manager and sample
   callback when constructing the canonical sink. A before/after probe called
   both injected collaborators on the baseline; the candidate called neither
   and emitted a default target sample. Custom target policy and sample
   suppression/redaction could therefore be bypassed.
2. `src/dpone/runtime/sinks/strategies/postgres/native_partition_replace.py:102`
   returned a physical partition count as `replaced_rows`. Preserving that field
   exposed it through the public projection as `loaded_rows`: two inserted rows
   in one partition were reported as one loaded row. Existing single-row live
   fixtures did not distinguish the units.

The reviewer traced execution, transaction ownership, failure/cleanup paths,
compatibility, documentation and retained evidence. **PASS:** 42 focused tests;
43 direct and 261 Kubernetes file checksums; 40 direct and five Kubernetes case
receipts. **FAIL:** both bounded regression probes. **SKIP:** new full/live suites
within this read-only review. Receipt inspection is not a new live run.

The independent review also approved the working documentation diff requiring
a fresh-context subagent after every feature. No ADR was required for restoring
the existing compatibility and truthful-metrics contracts.

## Fix disposition

Both findings have implementations and regression tests; final independent
review and validation of the fixed source are pending.

- Legacy adapters inject their target manager and sample callback through
  `PostgresSinkCompositionFactory` into each canonical strategy. Default sink
  composition remains available. Callback failure occurs before commit and
  rolls back. Query, file and named helper paths have success/failure coverage.
- PostgreSQL native and predicate partition replacement report inserted
  replacement rows in `replaced_rows` and removed old rows in
  `hard_deleted_rows`. Native outgoing rows are counted under the existing
  exclusive lock before DETACH. The common result projection and other engines
  remain outside this correction. Tests distinguish three old rows, two new
  rows, one partition, replay, empty old scope and a failed native count.
- `AGENTS.md` and the development guide require independent review after every
  feature, a recorded commit/verdict, resolution of blockers and review of fixes.

RED evidence: `legacy-dependencies-red.log`, `partition-metrics-red.log`.
The updated design and compatibility guide define the bounded fixes.

The original live source `1ff8387` and complete CI source `30e9221` remain
historical evidence. These production fixes change source bytes; those earlier
receipts do not certify the fixed candidate. The original Kubernetes proof is
retained without relabelling it. Final candidate CI remains the merge gate;
publication readiness is outside this review.
