# Source ADR 0058: R1 provider security may integrate under a bounded clustering exception

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

## Status

Accepted on 2026-09-05 after separate exact-commit architecture review.

This decision is scoped to exact implementation commit
`source record 001`. It covers only the internal,
SQL-free, activation-blocked PostgreSQL→MSSQL R1 V3 provider-security
authority. It does not approve SQL rendering, installation, route activation,
certification or production use.

## Context

The approved provider-security amendment requires one self-contained canonical
authority for principals, permission closure, certificate lifecycle, secret
template policy, shared signers, binding lifecycle references and replay
evidence. The exact implementation is split into five cohesive production
modules. Each remains at or below 350 SLOC, imports its canonical defining
modules directly and introduces no cycle, reverse security-to-binding edge,
re-export tunnel, facade-only module or public export.

The repository already exceeds the global average-clustering limit of 0.182.
The approved task base measures `0.18963409316899685`; exact security commit
`source record 001` measures
`0.19002986611536074`, an increase of `0.0003957729463638926`. At the same
time, cross-layer ratio improves from `0.3022141792633596` to
`0.30135823429541597`, repository maximum efferent coupling remains 24 and no
class-responsibility finding appears.

The exact architecture reviewer found the increase small and bounded. Removing
real authority edges would require duplicate schema truth, a re-export tunnel
or a facade created only to influence the metric. Those alternatives violate
the approved contract and make the security boundary harder to audit.

The commit-aware module-size baseline command reports the exact inherited set
of 52 issues: 51 baseline ledger entries whose commits are unavailable or not
ancestors of this feature lineage, plus the existing deterministic shrink
ratchet for `src/dpone/services/readiness.py`. None names a security-owned path;
the package-local module-size gate is green. This ADR does not waive that
repository tooling debt.

## Decision

The exact security implementation may integrate under a visible, time-bounded
engineering exception. The global limit remains 0.182, and the repository-wide
architecture command must continue to expose its real failure. This decision
does not redefine a failing check as passing.

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded requirement **rule**: repository architecture average clustering must not exceed 0.182 and feature debt should not grow
- Recorded requirement **reason**: the exact typed security authority needs visible dependencies on its canonical schema, descriptor and lifecycle owners; reviewed edge hiding would duplicate authority or create metric-only facades
- Recorded requirement **owner**: dpone architecture maintainers
- Recorded requirement **remediation_owner**: PostgreSQL-to-MSSQL R1 integrator
- Recorded requirement **scope / entry 1**: src/dpone/contracts/mssql_r1_v3_provider_security_enums.py
- Recorded requirement **scope / entry 2**: src/dpone/contracts/mssql_r1_v3_provider_security_principals.py
- Recorded requirement **scope / entry 3**: src/dpone/contracts/mssql_r1_v3_provider_security_permissions.py
- Recorded requirement **scope / entry 4**: src/dpone/contracts/mssql_r1_v3_provider_security_certificate.py
- Recorded requirement **scope / entry 5**: src/dpone/contracts/mssql_r1_v3_provider_security_profile.py
- Historical authority/requirement record **accepted_commit**: source record 001
- Recorded requirement **introduced_at**: 2026-09-05
- Recorded requirement **review_by**: 2026-10-31
- Recorded requirement **ceiling / avg_clustering**: 0.19002986611536074
- Recorded requirement **ceiling / cross_layer_ratio**: 0.30135823429541597
- Recorded requirement **ceiling / max_module_ce**: 24
- Recorded requirement **ceiling / class_findings**: 0
- Recorded requirement **activation_block**: PostgreSQL-to-MSSQL R1 V3 provider and every dependent route remain activation-blocked; no vendor-live or GA claim is authorized
- Recorded requirement **remediation_plan**: before production promotion, complete an independently approved repository architecture-remediation task, remeasure the exact integrated dependency graph, remove this exception, and restore the global gate to green without facades or duplicated authority


Any change to the five scoped modules is outside this decision until the new
exact commit is remeasured and reviewed. Integration fails closed if it raises
the recorded average-clustering or cross-layer ceiling, raises maximum Ce above
24, introduces a class finding, produces a module above 350 SLOC or adds a
cycle, facade, re-export tunnel or reverse dependency.

Exact acceptance evidence for the scoped commit is:

```text
focused provider-security tests:       24 PASS
descriptor/schema/core regressions:    682 PASS
scoped Ruff/format/mypy:                PASS
import and layer gates:                 PASS
security module-size ceiling:          PASS, every production module <=350 SLOC
repository architecture command:       visible FAIL above global 0.182
candidate avg_clustering:               0.19002986611536074
candidate cross_layer_ratio:            0.30135823429541597
candidate max_module_ce:                24
candidate class findings:               none
commit-aware module-size raw result:    visible FAIL, exact inherited 51 ancestry + 1 ratchet set
live PostgreSQL/MSSQL evidence:          N/A for this SQL-free contract task
full non-live repository suite:         FAIL; environment/baseline attribution remains unverified
```

## Consequences

- Approved provider children may pin the exact security commit and continue
  hermetic contract work.
- Provider installation, concrete SQL, adapters, runtime, route activation,
  vendor-live evidence and GA promotion remain blocked by their own approved
  specifications and gates.
- The raw architecture result remains red and is never reported as `PASS`.
- The exception expires at `review_by`. Renewal requires a new exact-commit
  measurement and separate review.
- A production promotion cannot rely on this exception; it must either remove
  it through the remediation plan or obtain a new explicit architectural
  decision that satisfies the then-current release policy.

## Related material

- `docs/feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md`
- `docs/agent-task-contracts/postgres-mssql-r1-v3-provider-security-contract-v2.yml`
- `docs/benchmarks/quality_budgets.yml`
- `docs/engineering-standards.md`
- ADR 0056
- ADR 0057


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
