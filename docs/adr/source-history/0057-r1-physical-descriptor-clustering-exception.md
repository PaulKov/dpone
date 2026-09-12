<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Source ADR 0057: R1 physical descriptor may integrate under a bounded clustering exception

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

## Status

Accepted.

Accepted on 2026-09-05 for the internal, activation-blocked PostgreSQL→MSSQL
R1 physical descriptor only. It does not certify or activate a route.

The later provider-install authority amendment targets a 20-table descriptor.
That amended implementation is outside this ADR until its exact commit is
remeasured and reviewed; the evidence and ceiling below continue to describe
the accepted 19-table commit only.

## Context

The approved SQL-free descriptor introduces a complete immutable algebra for
19 tables, 29 shared procedures and six binding templates. Its final reviewed
implementation is split into 16 cohesive modules, every new module remains at
or below 350 SLOC, imports point directly to canonical owners, the graph is
acyclic, cross-layer ratio improves, maximum module fan-out remains 24 and no
class-responsibility finding is introduced.

The repository already exceeds the global average-clustering budget of 0.182.
The exact descriptor base measured 0.18802741173900075; the compliant candidate
measures 0.18963409316899685. Earlier lower measurements depended on 411–457
line modules and pass-through `# noqa: F401` re-export tunnels. Further reviewed
splits or merges either increased clustering, duplicated validation, erased
type ownership or recreated oversized/god modules. Hiding the dependency is
worse than recording the real cohesive contract graph.

The module-size baseline gate also reports 52 inherited issues: 51 ledger
entries whose recorded baseline commits are unavailable or not ancestors of
this feature lineage, plus one deterministic shrink-ratchet/tightening issue in
`src/dpone/services/readiness.py`. The descriptor-owned package scan is green.
Those inherited ancestry/ratchet issues are separate repository tooling debt
and are not waived by this decision.

## Decision

The internal descriptor implementation may integrate with a visible,
time-bounded engineering exception. The global budget remains 0.182 and the
repository-wide architecture command must continue to report its real FAIL.
This exception changes neither the budget nor certification semantics.

```yaml
rule: repository architecture average clustering must not exceed 0.182 and feature debt should not grow
reason: complete strongly typed descriptor algebra has an irreducible cohesive DAG; reviewed alternatives create oversized modules, duplicate authority, or metric-only dependency tunnels
owner: dpone architecture maintainers
scope: src/dpone/contracts/mssql_r1_v3_physical_descriptor_*.py and src/dpone/contracts/mssql_r1_v3_physical_schema_descriptor.py at or below avg_clustering 0.18963409316899685
introduced_at: 2026-09-05
review_by: 2026-10-31
risk: dense contract dependencies can make later changes propagate across multiple descriptor leaves
mitigation: activation remains blocked; direct canonical imports only; no re-export tunnels; every new module <=350 SLOC; descriptor Ce <=11 and repository max Ce <=24; no cross-layer or class-responsibility regression; focused mutation and round-trip suites required
remediation_plan: complete a separately approved repository architecture-remediation task, remeasure the exact merged candidate, and remove this exception before any R1 provider production promotion
```

Integration is permitted; production promotion is not. Any descriptor change
that raises the recorded ceiling, introduces a new module-size warning,
re-export tunnel, cross-layer regression, fan-out above 24 or class finding is
outside this exception and fails closed.

The exact acceptance evidence is:

```text
repository architecture command: expected visible FAIL above 0.182
candidate avg_clustering:         <= 0.18963409316899685
candidate cross_layer_ratio:      <= 0.3050300945829751
candidate max_module_ce:          <= 24
descriptor-subgraph max Ce:       <= 11
candidate class findings:         none
descriptor module-size package:   PASS, no warning debt
exact baseline ledger command:    visible FAIL: 51 ancestry + 1 inherited ratchet
```

## Consequences

- Provider child specifications may pin the integrated descriptor commit and
  continue hermetic contract work.
- Route activation, vendor-live evidence and GA claims remain blocked by their
  own approved specifications and certification gates.
- The raw architecture report remains red and must never be relabelled PASS.
- A separate remediation may improve unrelated repository topology only when
  independently specified and reviewed; the descriptor writer cannot add
  metric-only facades or unrelated cleanup.
- The exception expires at `review_by`; renewal requires a new explicit review
  with current exact-commit evidence.

## Related material

- `docs/feature-design-postgres-mssql-r1-v3-physical-descriptor-contract-v1.md`
- `docs/agent-task-contracts/postgres-mssql-r1-v3-physical-descriptor-contract-v1.yml`
- `docs/benchmarks/quality_budgets.yml`
- `docs/engineering-standards.md`
- ADR 0056


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
