# Source ADR 0071: R1 provider attestation is a dependency-neutral foundation

> Historical provenance: “source record NNN” names a privately archived original, not a Git ref, executable task grant or current validation result. Outcomes attached to these references retain their original historical scope. Current execution requires a separate genuine public authority chain.


> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

- Status: Accepted
- Date: 2026-09-07
- Accepted: 2026-09-08, based on exact reviewed commit
  `source record 028` and ADR content SHA-256
  `c63def46bb29f523068ecf6bc30b2e85b4b5bedea23d2f199d6ff37c06b379d5`.

Acceptance authorizes only the internal, activation-blocked Provider
Attestation foundation RED/GREEN implementation sequence. It does not approve
SQL Server I/O, public API/CLI/manifest changes, vendor-live certification,
route activation, GA or release.

## Context

The approved Security V2 amendment defines stable catalog composition but uses
query and observation leaves described only in historical Migration/Binding V1
research. Implementing the missing types in Security would create reverse
dependencies from Security to Binding and Migration. Implementing them in
Migration would make Security-owned stable types depend on their consumer.

## Decision

Introduce an internal `provider_attestation` contracts boundary:

```text
physical descriptor + Security V2 + Binding V2 + schema attestation/observation
→ provider_attestation
→ Migration V2
```

The boundary owns target/build/statement references needed by observation,
finite typed catalog results, binding observation leaves and stable-attestation
composition. It may import exact descriptor, Security, Binding and schema
contracts. Security and Binding never import it. Migration consumes it and does
not duplicate its symbols.

The foundation is pure and activation-blocked. SQL, catalog I/O, rendering,
transactions, receipts, public exports and route activation remain outside it.
Approval requires the complete ABI, bounds and compatibility rules in the
linked specification.

## Consequences

- The Security → Migration/Binding cycle is removed.
- Historical V1 prose is no longer accidental implementation authority.
- Migration V2 RED must wait for this foundation's GREEN acceptance.
- Eight small cohesive modules are added instead of expanding near-budget
  Security modules.
- Retained V1 symbol names are an internal compatibility cost; their canonical
  domains remain fixed while V2 aggregates reject V1 aggregate bytes.
- A future provider aggregate can compose Renderer registry equality without
  moving catalog semantics into the renderer.

## Alternatives rejected

- **Implement inside Security:** creates forbidden reverse dependencies and
  expands already near-budget modules.
- **Implement inside Migration:** makes a producer-owned stable authority depend
  on its consumer and leaves Binding observations without a neutral owner.
- **Treat historical V1 research as approved ABI:** defeats the repository's
  specification gate and leaves several fields/bounds undefined.
- **Use opaque digests between layers:** permits cross-authority splicing and
  cannot prove complete catalog observation.

## Related material

- [Provider attestation foundation V2](../../feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md)
- [Provider attestation evidence V1](../../feature-design-postgres-mssql-r1-v3-provider-attestation-evidence-v1.md)
- [Security V2 amendment](../../feature-design-postgres-mssql-r1-v3-provider-security-authority-amendment-v2.md)
- [Binding V2](../../feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md)
- [Migration V2](../../feature-design-postgres-mssql-r1-v3-provider-migration-contract-v2.md)

Back: [ADR index](../../adr-index.md).
Next: [provider attestation foundation V2](../../feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md).


Historical source decision: numbering and approval statements in this document belong to the source project. Current public ADRs with the same numeric identifier remain authoritative in their own scope; these source statements do not waive migration validation.
