# Source ADR 0066: R1 type and target authority may integrate under a bounded clustering exception

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

## Status

Accepted on 2026-09-05 after separate exact-commit architecture, binding,
migration and documentation reviews.

This accepted decision is scoped to implementation commit
`source record 002`. It covers only the internal,
SQL-free, activation-blocked PostgreSQL→MSSQL R1 V3 canonical type policy,
registered-target catalog and current-registration/rotation admission
authority. It does not approve SQL persistence, catalog observation, binding,
rendering, route activation, certification or production use.

## Context

The approved child specification requires explicit dependency edges from the
new contracts to the existing physical descriptor R2, provider security V2,
registration and canonical codec authorities. The implementation is split
into ten cohesive modules. Every module remains below 350 SLOC, imports its
defining authority directly and introduces no import cycle, reverse adapter
dependency, re-export tunnel, facade-only module or public export.

The repository already exceeds the global average-clustering limit of 0.182.
The approved base graph at `b9b47f634` measures
`0.19002986611536074`; exact implementation commit `98b8fea1d` measures
`0.19130855252670886`, an increase of `0.00127868641134812`. Cross-layer ratio
improves from `0.30135823429541597` to `0.30015847860538825`; maximum efferent
coupling remains 24 and class-responsibility findings remain empty.

Removing the real authority edges would require duplicated policy, a
re-export tunnel or metric-only facades. Those options would weaken the audit
boundary and violate the approved specification. This ADR does not hide or
reclassify the failing repository gate.

The commit-aware module-size command still reports the inherited exact set of
52 issues: 51 unavailable/non-ancestor baseline entries and the existing
`src/dpone/services/readiness.py` shrink ratchet. None names an owned module;
the package-local module-size gate is green. This decision does not waive that
tooling debt.

## Decision

The exact implementation may integrate only under this visible, time-bounded
engineering exception. The global limit stays 0.182 and the raw architecture
command must remain visibly red.

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded requirement **rule**: repository average clustering must not exceed 0.182 and feature debt must not grow
- Recorded requirement **reason**: the exact type/target admission boundary requires direct dependencies on its canonical descriptor, security, registration and codec authorities; hiding them would duplicate truth or create metric-only facades
- Recorded requirement **owner**: dpone architecture maintainers
- Recorded requirement **remediation_owner**: PostgreSQL-to-MSSQL R1 integrator
- Recorded requirement **scope / entry 1**: src/dpone/contracts/postgres_mssql_type_target_enums.py
- Recorded requirement **scope / entry 2**: src/dpone/contracts/postgres_mssql_type_target_shapes.py
- Recorded requirement **scope / entry 3**: src/dpone/contracts/postgres_mssql_type_derivation.py
- Recorded requirement **scope / entry 4**: src/dpone/contracts/postgres_mssql_value_admission.py
- Recorded requirement **scope / entry 5**: src/dpone/contracts/postgres_mssql_type_authority.py
- Recorded requirement **scope / entry 6**: src/dpone/contracts/mssql_r1_v3_registered_target_catalog_items.py
- Recorded requirement **scope / entry 7**: src/dpone/contracts/mssql_r1_v3_registered_target_catalog.py
- Recorded requirement **scope / entry 8**: src/dpone/contracts/mssql_r1_v3_verified_target_validation.py
- Recorded requirement **scope / entry 9**: src/dpone/contracts/mssql_r1_v3_verified_target_authority.py
- Recorded requirement **scope / entry 10**: src/dpone/contracts/mssql_r1_v3_registration_rotation.py
- Historical authority/requirement record **accepted_commit**: source record 002
- Recorded requirement **evidence_head**: source record 003
- Recorded requirement **introduced_at**: 2026-09-05
- Recorded requirement **review_by**: 2026-10-31
- Recorded requirement **risk**: dense direct authority dependencies can make changes propagate across the type, catalog, admission and rotation contracts and keep the repository-wide clustering gate above its hard limit
- Recorded requirement **mitigation**: activation remains blocked; all dependencies stay direct and acyclic; no facade or re-export tunnel is permitted; each scoped module remains below 350 SLOC; focused semantic-mutation and replay tests plus exact-commit remeasurement are required for every change
- Recorded requirement **ceiling / avg_clustering**: 0.19130855252670886
- Recorded requirement **ceiling / cross_layer_ratio**: 0.30015847860538825
- Recorded requirement **ceiling / max_module_ce**: 24
- Recorded requirement **ceiling / class_findings**: 0
- Recorded requirement **activation_block**: PostgreSQL-to-MSSQL R1 V3 and every dependent route remain activation-blocked; no vendor-live or GA claim is authorized
- Recorded requirement **remediation_plan**: before production promotion, complete an independently approved repository architecture-remediation task, remeasure the exact integrated graph, remove this exception and restore the global gate to green without facades, re-export tunnels or duplicated authority


Any change to a scoped production module falls outside this decision until the
new exact commit is remeasured and reviewed. Integration fails closed if it
raises either recorded ratio, raises maximum Ce above 24, creates any class
finding, introduces a cycle/facade/re-export/reverse dependency or makes a
production module exceed 350 SLOC.

Exact implementation evidence, reproduced at the separately pinned evidence
head above, is:

```text
focused type/target authority tests:  597 PASS
legacy type/R1 contract regressions:   64 PASS
all PostgreSQL→MSSQL R1 V3 tests:      1451 PASS
closed mutation inventory:             15 dataclasses/154 fields, 8 enums/76 members, 13 optional arms, 50 external cross-authority refs PASS
scoped Ruff/format/mypy:               PASS
import and layer gates:                PASS
owned production module-size ceiling: PASS, every module <350 SLOC
repository architecture command:      visible FAIL above global 0.182
candidate avg_clustering:              0.19130855252670886
candidate cross_layer_ratio:           0.30015847860538825
candidate max_module_ce:               24
candidate class findings:              none
commit-aware module-size raw result:   visible FAIL, inherited 51 ancestry + 1 ratchet set
live PostgreSQL/MSSQL evidence:         UNVERIFIED and out of scope
full non-live repository suite:        FAIL, 52 failed/20709 passed/590 skipped/2 collection errors from disclosed optional-dependency, git-lineage and repository-gate issues
```

Each of the 50 external cross-authority cases first constructs and canonical-
round-trips an independently valid foreign nested authority or field-local
owner, splices only the named field, then expects rejection from the owning
construction or admission boundary. The closed embedded feature observation
and the transition receipt payload/digest integrity pair remain in the complete
field inventory; they are not external authority references.

## Consequences

- Later provider children may pin this commit only after the feature and ADR
  reviews accept it.
- SQL persistence, current catalog fidelity, binding, rendering, runtime,
  route activation, vendor-live evidence and GA remain independently blocked.
- The raw architecture result stays `FAIL`; this exception is not a passing
  certification artifact.
- The exception expires at `review_by`; renewal requires a new exact-commit
  measurement and separate review.
- Production promotion must remove this exception through the remediation
  plan or make a new explicit decision under the then-current release policy.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Duplicate descriptor, registration or security primitives locally | Creates competing authority and weakens anti-splice validation |
| Add re-export/facade modules solely to alter graph shape | Hides real dependencies without reducing responsibility |
| Treat the raw architecture failure as passing | Would manufacture false evidence |
| Activate before vendor-live persistence and catalog proof | Outside this pure-contract scope and unsafe |

## Related material

- [R1 type and target authority specification](../../feature-design-postgres-mssql-r1-v3-type-target-authority-v1.md)
- [Historical task requirements — R1 type and target task contract (not executable)](../../agent-task-history/postgres-mssql-r1-v3-type-target-authority-v1.md)
- [R1 provider implementation map](../../developer-postgres-mssql-r1-v3-provider-implementation.md)
- [ADR 0056](0056-mssql-same-database-target-authority-v2.md)
- [ADR 0057](0057-r1-physical-descriptor-clustering-exception.md)
- [ADR 0058](0058-r1-provider-security-clustering-exception.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
