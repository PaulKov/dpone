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

Both findings are **CLOSED** after regression tests and independent follow-up
review of `48c123daddac0ec7c4692e0beaa55582a37f286c`.

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

## Final independent verdict

Reviewer: `/root/post_feature_independent_review` (read-only; no implementation
edits). Reviewed correction: `48c123daddac0ec7c4692e0beaa55582a37f286c`, against
`648237b6ebe3d3e59252c37aa5c72e88b38a2095`. Verdict: **APPROVE**. Both P2 findings
are closed; no new confirmed findings. This documentation/receipt follow-up
preserves the production, package, dependency and test bytes of `48c123d`.

The reviewer independently reran all 65 focused tests and `git diff --check`,
then reconsumed the 40 new direct cases and all 43 retained file checksums using
the existing verifier. The source/test fingerprint and receipt identify
`48c123d`. The reviewer confirmed the workflow rule, compatibility documentation
and design clarification. It did not run a new live/full/broad suite itself.

## Validation of the correction

| Check | Status | Evidence and limits |
| --- | --- | --- |
| Legacy dependency and partition metric regression probes before fixes | FAIL | `legacy-dependencies-red.log`, `partition-metrics-red.log`; retained RED evidence |
| Focused regressions after fixes | PASS | 65 cases, including 23 new cases; `independent-review-focused.log`; independently repeated by reviewer |
| Neighboring composition, processor, production-strategy and schema-evolution contracts | PASS | 98 cases across six test modules; `review-fix-neighbor-contracts.log` |
| Frozen real PostgreSQL campaign | PASS | [40 cases at 48c123d](verification-direct-48c123d.json); includes three old rows to two new rows, replay and real injected collaborators |
| Ruff and format | PASS | Whole repository; `review-fix-ruff.log`, `review-fix-format.log`; final source checked again |
| Mypy | PASS | 1163 configured source files; `review-fix-mypy.log` |
| Imports, layers, architecture, module size | PASS | `review-fix-imports.log`, `review-fix-layers.log`, `review-fix-architecture.log`, `review-fix-module-size.log`; existing advisory debt retained |
| Generated metrics and compatibility registry | PASS | `review-fix-dev-metrics-check.log`, `review-fix-compatibility.log`; generated changes came from the producer |
| Agent-policy, module-size and docs-language tests | PASS | 1562 cases; `policy-independent-review.log`, collection confirmed in `review-policy-collection.log`; policy bytes unchanged by code follow-up |
| Agent setup, task contract, branch/workflow policy and governance gate | PASS | [Local governance receipt at 48c123d](independent-review-agent_governance_gate.json); does not substitute for the CI-attested artifact |
| Documentation, generated references and strict build | PASS | `review-fix-docs.log`, `review-fix-generated.log`, `review-fix-docs-language.log`, `review-fix-mkdocs.log`; final report follow-up: `review-final-docs.log`, `review-final-docs-language.log`, `review-final-mkdocs.log` |
| Independent review | PASS | APPROVE at 48c123d; both original P2 findings closed |
| New full CI matrix | UNVERIFIED | Runs on the new MR head; earlier successful CI is historical evidence only |
| New Kubernetes image campaign | SKIP | This bounded correction has new direct PostgreSQL evidence; the previous Kubernetes campaign remains attached to 1ff8387 |
| Packaging/publication | N/A | No package/dependency changes or release request |
| Production performance | UNVERIFIED | Large production partitions were not benchmarked |

Public manifest/result shapes remain unchanged. Keyword-only dependency
injection is additive, and PostgreSQL partition row-counter semantics are
documented. Existing transaction ownership and dependency direction remain
intact. Native counting adds work under the exclusive lock; its performance on
large production partitions remains unverified.

The implementation and review are complete. Merge requires the final MR head's
mandatory checks and owner acceptance under the repository's existing policy.
The MR remains a draft; no merge or publication was performed.
