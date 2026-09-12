
> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.

<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Source ADR 0070: R1 provider Binding V2 uses a bounded clustering exception

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

## Status

Accepted on 2026-09-07 for the internal implementation commit
`PENDING_PUBLIC_COMMIT_BINDING` and candidate evidence head
`source record 026`.

Acceptance is limited to the pure, activation-blocked Binding V2 compiler and
the exact eight modules below. It does not approve Migration V2, Renderer V2,
SQL installation, route activation, vendor-live certification, GA or release.

## Context

Binding V2 combines six already governed authorities into one deterministic
portable pack. The implementation keeps canonical models, validation,
derivation, stage projections, signer intent and permission closure in cohesive
modules. Their dependencies are direct and acyclic; no adapter, runtime, port,
I/O, SQL, facade, re-export tunnel or public package export was added.

The repository already exceeded the global average-clustering hard limit of
`0.182`. The exact pre-implementation graph at
`source record 027` measured
`0.19122248517720344`; exact implementation
`PENDING_PUBLIC_COMMIT_BINDING` measured
`0.1915226770223`, an increase of `0.00030019184509656`. Cross-layer ratio
improved from `0.29969515399978974` to `0.29836854214599456`, maximum module
efferent coupling remained `24`, and class-responsibility findings remained
zero.

The raw architecture gate therefore remains `FAIL`. The raw layer gate also
remains `FAIL` at the inherited `dpone.runtime -> dpone.contracts = 207`, above
the baseline-plus-tolerance threshold of `199`; the contracts-only diff did not
increase that flow. The commit-aware module-size command remains raw `FAIL`
with 52 repository-wide ancestry/ratchet findings. None names a new Binding V2
module, and every scoped module remains below the 400-SLOC hard limit.

Merging modules to optimize the graph number would create a broad compiler god
module. Hiding dependencies behind facades, re-exports, dynamic imports or
duplicated primitives would make the graph look better while weakening the
authority boundary. The selected decomposition follows stable contract
responsibilities and keeps the measured regression explicit.

## Decision

The exact implementation may integrate under this visible, time-bounded
engineering exception. The global quality budgets remain unchanged and every
raw failing gate remains reported as `FAIL`.

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded requirement **rule**: repository average clustering must not exceed 0.182 and feature debt must not grow without an exact reviewed exception
- Recorded requirement **reason**: Binding V2 requires direct, auditable composition of existing descriptor, security, source-schema, target-catalog and codec authorities; metric-only indirection would hide real dependencies or duplicate policy
- Recorded requirement **owner**: dpone architecture maintainers
- Recorded requirement **remediation_owner**: PostgreSQL-to-MSSQL R1 integrator
- Recorded requirement **scope / entry 1**: src/dpone/contracts/mssql_r1_v3_binding_enums.py
- Recorded requirement **scope / entry 2**: src/dpone/contracts/mssql_r1_v3_binding_mapping.py
- Recorded requirement **scope / entry 3**: src/dpone/contracts/mssql_r1_v3_binding_modules.py
- Recorded requirement **scope / entry 4**: src/dpone/contracts/mssql_r1_v3_binding_pack.py
- Recorded requirement **scope / entry 5**: src/dpone/contracts/mssql_r1_v3_binding_permissions.py
- Recorded requirement **scope / entry 6**: src/dpone/contracts/mssql_r1_v3_binding_signer.py
- Recorded requirement **scope / entry 7**: src/dpone/contracts/mssql_r1_v3_binding_stage.py
- Recorded requirement **scope / entry 8**: src/dpone/contracts/mssql_r1_v3_binding_validation.py
- Historical authority/requirement record **accepted_commit**: PENDING_PUBLIC_COMMIT_BINDING
- Recorded requirement **evidence_head**: source record 026
- Recorded requirement **introduced_at**: 2026-09-07
- Recorded requirement **review_by**: 2026-10-31
- Recorded requirement **risk**: dense direct contract dependencies can propagate changes across Binding identities and keep the repository-wide clustering gate above its hard limit
- Recorded requirement **mitigation**: activation remains blocked; dependencies stay direct and acyclic; no facade, re-export, dynamic import or duplicated authority is permitted; focused mutation and semantic tests plus exact-commit graph measurement are required for every scoped change
- Recorded requirement **ceiling / avg_clustering**: 0.1915226770223
- Recorded requirement **ceiling / cross_layer_ratio**: 0.29836854214599456
- Recorded requirement **ceiling / max_module_ce**: 24
- Recorded requirement **ceiling / class_findings**: 0
- Recorded requirement **activation_block**: PostgreSQL-to-MSSQL R1 V3 and every dependent route remain activation-blocked; vendor-live certification and GA are not authorized
- Recorded requirement **remediation_plan**: before production activation, complete an independently approved architecture-remediation task that reduces real dependency density through stable domain ownership, remeasure the integrated graph below the global limit, and remove or supersede this exception without facades, re-export tunnels, dynamic imports or duplicated authority


Expiry at `review_by` blocks further changes to the scoped implementation until
the exception is retired or explicitly renewed from a new exact measurement.
Any changed scoped module requires fresh review. Acceptance fails closed if a
future candidate exceeds a recorded ceiling, adds a cycle or reverse-layer
edge, creates a class-responsibility finding, exposes a public API or widens the
activation claim.

Exact reviewed evidence is:

```text
focused Binding authority:       269/269 PASS
semantic cases:                  246/246 PASS
evidence protocol:               79 PASS
upstream authority regressions:  727 PASS
Ruff / format / mypy:            PASS for all eight modules
import rules:                    PASS
repository architecture:        FAIL, 0.1915226770223 > 0.182
repository layer metrics:        FAIL, runtime -> contracts 207 > 199
repository module-size gate:     FAIL, 52 repository-wide findings
full non-live repository suite:  FAIL, 61 failed / 21377 passed / 588 skipped / 1 error
vendor-live evidence:            UNVERIFIED
```

## Consequences

- Binding V2 is an implemented internal contract with hermetic `local_pass`
  evidence, not a production route capability.
- Migration V2 and Renderer V2 may consume the pack only under their own
  approved specifications and evidence.
- The raw architecture, layer, module-size and broad-suite failures remain
  visible and block release promotion.
- The exception can be removed only by a reviewed architectural remediation,
  not by changing the quality budget or hiding imports.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Merge the eight responsibilities into one module | Creates a god module and couples validation, models and derivation |
| Add facade or re-export modules | Hides dependencies without reducing responsibility |
| Duplicate upstream authority primitives | Creates competing truth and weakens anti-splice checks |
| Mark the raw architecture gate passing | Manufactures false evidence |
| Activate after hermetic evidence | SQL installation and vendor-live behavior remain unimplemented and unverified |

## Related material

- [Binding V2 specification](../../feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md)
- [Binding V2 maintainer guide](../../postgres-mssql-r1/binding-v2-maintainer.md)
- [Provider implementation map](../../developer-postgres-mssql-r1-v3-provider-implementation.md)
- [ADR 0066](0066-r1-type-target-authority-clustering-exception.md)
- [ADR 0069](0069-postgres-source-schema-layer-flow-exception.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
