# Source ADR 0067: Security V2 runtime EXECUTE repair may integrate under a bounded clustering exception

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

## Status

Accepted on 2026-09-05 after fresh security, test/certification,
documentation/governance and architecture reviews of exact evidence head
`source record 004`.

The accepted decision is scoped to implementation commit
`source record 005`. It covers only the internal,
SQL-free, activation-blocked correction that admits exact runtime-principal
`EXECUTE` paths for one R1 V3 binding module. It does not approve Binding V2,
SQL rendering or installation, route activation, vendor certification or
production use.

## Context

The accepted Security V2 permission closure at `source record 001`
admits only binding-signer target DML paths. The approved binding model also
requires the runtime principal to receive direct `EXECUTE` on each signed
binding module. Consequently, a complete future binding pack would be rejected
by the security authority that is intended to validate it.

The correction keeps the canonical domains, dataclass layouts, codecs,
digests, versions and public exports unchanged. It admits only:

```text
runtime environment principal
+ direct GRANT EXECUTE without grant option
+ OBJECT scope with descendants
+ schema dpone_authority
+ dpone_b_<32 lowercase hex>_<exact module kind>_v3
```

Binding V2 remains responsible for deriving the complete six-module set, one
binding UUID, exact target and row-hash coordinates and one-to-one access
projection. Security validates the closed shape of each supplied path and does
not infer binding completeness.

Keeping that predicate inside
`mssql_r1_v3_provider_security_permissions.py` would grow the module from 350
to 379 SLOC and create new module-size warning debt. The candidate instead
places this distinct binding-path grammar in a 54-SLOC cohesive helper and
keeps the permission authority at 347 SLOC. The helper is not a facade or
re-export: it owns the one reason to change when the closed Security V2
binding-path grammar changes.

The repository already exceeds the global average-clustering limit of 0.182.
The accepted base graph at `source record 006`
measures `0.19130855252670886`; the exact candidate measures
`0.19137170217275384`, an increase of `0.00006314964604498`. Cross-layer ratio
improves from `0.30015847860538825` to `0.3`, maximum efferent coupling remains
24 and class-responsibility findings remain empty.

The commit-aware module-size command reports the same inherited 52 issues as
the accepted base and none names an owned security path. This ADR does not
waive that repository tooling debt and does not turn the raw architecture gate
green.

## Decision

The exact correction may integrate only under this visible, time-bounded
engineering exception. The global 0.182 limit remains unchanged and the raw
architecture command remains a reported failure.

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded requirement **rule**: repository average clustering must not exceed 0.182 and feature debt must not grow
- Recorded requirement **reason**: the closed runtime-module permission grammar is a distinct auditable responsibility; keeping it inline creates module-size debt while hiding its dependency would duplicate authority or create a metric-only facade
- Recorded requirement **owner**: dpone architecture maintainers
- Recorded requirement **remediation_owner**: PostgreSQL-to-MSSQL R1 integrator
- Recorded requirement **scope / entry 1**: src/dpone/contracts/mssql_r1_v3_provider_security_binding_paths.py
- Recorded requirement **scope / entry 2**: src/dpone/contracts/mssql_r1_v3_provider_security_permissions.py
- Historical authority/requirement record **accepted_commit**: source record 005
- Recorded requirement **evidence_head**: source record 004
- Recorded requirement **introduced_at**: 2026-09-05
- Recorded requirement **review_by**: 2026-10-31
- Recorded requirement **risk**: one new cohesive module and five direct import edges marginally increase repository clustering; the physical module-name grammar can drift unless later children consume one approved lower-level authority
- Recorded requirement **mitigation**: activation remains blocked; both modules stay at or below 350 SLOC; closed positive and negative grammar tests cover all module kinds; no public export, cycle, reverse dependency, facade or re-export is permitted
- Recorded requirement **ceiling / avg_clustering**: 0.19137170217275384
- Recorded requirement **ceiling / cross_layer_ratio**: 0.3
- Recorded requirement **ceiling / max_module_ce**: 24
- Recorded requirement **ceiling / class_findings**: 0
- Recorded requirement **activation_block**: PostgreSQL-to-MSSQL R1 V3 and every dependent route remain activation-blocked; no vendor-live or GA claim is authorized
- Recorded requirement **remediation_plan**: Security must never import Binding; Binding continues to own six-module semantic completeness while Security owns per-path structural closure; any extracted shared physical-name grammar must live in an independently approved lower-level authority imported by both, preserving Descriptor/Security to Binding to Migration direction; the integrator must then remeasure the exact graph and remove or supersede this exception before production promotion


Any further change to either scoped production module falls outside this
decision until its exact commit is remeasured and reviewed. Integration fails
closed if it raises either recorded ratio, raises maximum Ce above 24,
introduces a class finding, cycle, facade, re-export or reverse dependency, or
makes an owned production module exceed 350 SLOC.

Candidate evidence for the scoped implementation commit is:

**Historical algorithm summary — not an executable current procedure.** The original executable excerpt and its exact inputs are retained privately. The source locators below identify those original inputs; they are not Git refs and do not grant current execution or certify a current candidate.

- Recorded historical statement: task contract authorized before tests/code:       source record 007
- Recorded historical statement: red test commit/result:                            source record 008, 15 failed/26 passed
- Recorded historical statement: focused corrected + existing security tests: 65 PASS
- Recorded historical statement: all six runtime module kinds:                 PASS
- Recorded historical statement: closed runtime/signer mutation matrix:        PASS
- Recorded historical statement: scoped Ruff/format/mypy:                      PASS
- Recorded historical statement: task contract validation:                     PASS
- Recorded historical statement: worktree PYTHONPATH:                           src:packages/apache-airflow-providers-dpone/src:packages/dpone-airflow-pack/src:packages/dpone-native-accel/src
- Recorded historical statement: import and layer gates:                       PASS
- Recorded historical statement: docs check/generated references/language:     PASS; 819 files/3185 links, 3/3 references, 32 tests
- Recorded historical statement: MkDocs strict:                                PASS
- Recorded historical statement: owned production module-size ceiling:         PASS, 54 and 347 SLOC
- Recorded historical statement: repository architecture command:              visible FAIL above global 0.182
- Recorded historical statement: candidate avg_clustering:                      0.19137170217275384
- Recorded historical statement: candidate cross_layer_ratio:                   0.3
- Recorded historical statement: candidate max_module_ce:                       24
- Recorded historical statement: candidate class findings:                      none
- Recorded historical statement: commit-aware module-size raw result:           visible FAIL, exact inherited 52-issue set
- Recorded historical statement: live PostgreSQL/MSSQL evidence:                 UNVERIFIED; out of scope and no approved live environment, so activation remains blocked
- Recorded historical statement: full non-live base suite:                       FAIL, 59 failed/20722 passed/588 skipped/2 collection errors
- Recorded historical statement: full non-live candidate suite:                  FAIL, 59 failed/20763 passed/588 skipped/2 collection errors
- Recorded historical statement: base/candidate failing node-ID set:             byte-identical, SHA256 historical outcome-set digest, assertion 1: exact expected value privately archived
- Recorded historical statement: base/candidate collection-error node-ID set:    byte-identical, SHA256 historical outcome-set digest, assertion 2: exact expected value privately archived
- Recorded historical statement: candidate-only failures/errors:                 none; 41 added test cases all passed


Local documentation and Python evidence used CPython 3.12.11, pytest 9.0.3,
MkDocs 1.6.1, the bundled workspace environment and worktree-local source roots
for the main package and all three workspace packages. A bare shared
environment without those source roots is not evidence for this worktree.

The comparison was produced from two `pytest --junitxml` runs in the same
workspace dependency environment at exact base
`source record 006` and exact candidate
`source record 005`. Each digest is SHA-256 over the
sorted fully qualified failing or collection-error node IDs separated by LF
and terminated by LF. The equality is attribution evidence only: both raw
suite outcomes remain `FAIL` and are not release certification.

## Consequences

- Binding V2 may pin the corrected Security V2 commit only after this decision
  is accepted by fresh review.
- The future binding factory can provide its exact runtime module paths without
  being rejected by Security V2.
- Binding completeness, mapping semantics and SQL remain outside Security.
- The raw architecture and module-size repository results remain visible and
  are never reported as `PASS`.
- Production promotion must remove this exception through its remediation plan
  or obtain a new explicit decision under the then-current release policy.

## Rejected alternatives

| Alternative | Reason |
|---|---|
| Keep the predicate in the permission authority | Grows the changed module to 379 SLOC and violates the accepted 350-SLOC ceiling |
| Duplicate the six-module inventory in Security | Moves binding completeness into the wrong authority and creates competing truth |
| Add a facade or re-export to alter graph shape | Hides a real dependency without reducing responsibility |
| Introduce Security V3 persisted bytes | No canonical field, byte or version contract changes |
| Treat the repository architecture failure as passing | Would manufacture false evidence |

## Related material

- [Provider security authority amendment V2](../../feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md)
- [Historical task requirements — Security runtime EXECUTE task contract (not executable)](../../agent-task-history/postgres-mssql-r1-v3-provider-security-runtime-execute-v2.md)
- [Provider implementation map](../../developer-postgres-mssql-r1-v3-provider-implementation.md)
- [ADR 0058](0058-r1-provider-security-clustering-exception.md)
- [ADR 0065](0065-r1-provider-single-binding-install-authority.md)
- [ADR 0066](0066-r1-type-target-authority-clustering-exception.md)


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
